/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.cook

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.lang.Thread.UncaughtExceptionHandler
import java.net.{InetAddress, ServerSocket, URI}
import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import org.apache.mesos._
import org.apache.mesos.Protos._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.{ExecutorExited, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.cluster.mesos.{MesosCoarseGrainedSchedulerBackend, MesosSchedulerUtils}
import org.json.JSONObject
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.twosigma.cook.jobclient.{Job, JobClient, JobListener => CJobListener}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcAddress

import scala.collection.mutable

object CoarseCookSchedulerBackend {

  // A collection of regexes for extracting information from an URI
  private val HTTP_URI_REGEX = """http://(.*)""".r
  private val RSYNC_URI_REGEX = """rsync://(.*)""".r
  private val SPARK_RSYNC_URI_REGEX = """spark-rsync://(.*)""".r
  private val HDFS_URI_REGEX = """hdfs://(.*)""".r

  private[spark] def fetchURI(uri: String): String = uri.toLowerCase match {
    case HTTP_URI_REGEX(httpURI) =>
      s"curl -O http://$httpURI"
    case RSYNC_URI_REGEX(file) =>
      s"rsync $file ./"
    case SPARK_RSYNC_URI_REGEX(file) =>
      "RSYNC_CONNECT_PROG=\"knc spark-rsync@%H $SPARK_DRIVER_PULL_PORT\"" +
      s" rsync $$SPARK_DRIVER_PULL_HOST::spark/$file ./"
    case HDFS_URI_REGEX(file) =>
      s"$$HADOOP_COMMAND fs -copyToLocal hdfs://$file ."
    case _ =>
      sys.error(s"$uri not supported yet")
  }

  def apply(scheduler: TaskSchedulerImpl, sc: SparkContext, cookHost: String,
    cookPort: Int): CoarseGrainedSchedulerBackend = {
    new CoarseCookSchedulerBackend(scheduler, sc, cookHost, cookPort)
  }
}



/**
 * A SchedulerBackend that runs tasks using Cook, using "coarse-grained" tasks, where it holds
 * onto Cook instances for the duration of the Spark job instead of relinquishing cores whenever
 * a task is done. It launches Spark tasks within the coarse-grained Cook instances using the
 * CoarseGrainedSchedulerBackend mechanism. This class is useful for lower and more predictable
 * latency.
 *
 * Since Spark 2.0.0, executor id must be an integer even though its type is string. This backend
 * uses task id which is also an integer and created via
 * {{{
 *   MesosCoarseGrainedSchedulerBackend.newMesosId
 * }}}
 * as executor id.
 *
 * To ensure the mapping from executor id (task id) to its Cook job instance is 1-1 and onto,
 * we only allow one instance per Cook job and we are using the mapping from
 * <task id> -> <cook job id> to track this relationship.
 */
class CoarseCookSchedulerBackend(
  scheduler: TaskSchedulerImpl,
  sc: SparkContext,
  cookHost: String,
  cookPort: Int
) extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) with Logging with MesosSchedulerUtils {

  val maxCores = conf.getInt("spark.cores.max", 0)
  val maxCoresPerJob = conf.getInt("spark.executor.cores", 1)
  val priority = conf.getInt("spark.cook.priority", 75)
  val jobNamePrefix = conf.get("spark.cook.job.name.prefix", "sparkjob")
  val maxFailures = conf.getInt("spark.executor.failures", 5)
  val dynamicAllocationEnabled = conf.getBoolean("spark.dynamicAllocation.enabled", false)

  if (conf.contains("spark.cores.max") && dynamicAllocationEnabled) {
    logWarning("spark.cores.max is ignored when dynamic allocation is enabled. Use spark.dynamicAllocation.maxExecutors instead")
  }

  def currentInstancesToRequest: Int = (executorsToRequest - totalInstancesRequested)
  var executorsToRequest: Int = if (dynamicAllocationEnabled) {
    conf.getInt("spark.dynamicAllocation.minExecutors", 0)
  } else {
    maxCores / maxCoresPerJob
  }
  var totalInstancesRequested = 0
  var totalFailures = 0
  val jobIds = mutable.Set[UUID]()
  val abortedJobIds = mutable.Set[UUID]()
  private val executorIdToJobId = mutable.HashMap[String, UUID]()

  private[this] val jobClient = new JobClient.Builder()
    .setHost(cookHost)
    .setPort(cookPort)
    .setEndpoint("rawscheduler")
    .setStatusUpdateInterval(10)
    .setBatchRequestSize(24)
    .setKerberosAuth()
    .build()

  private[this] val jobListener = new CJobListener {
    // These are called serially so don't need to worry about race conditions
    def onStatusUpdate(job: Job) {
      val isCompleted = job.getStatus == Job.Status.COMPLETED
      val isAborted = abortedJobIds.contains(job.getUUID)

      if (isCompleted) {
        totalInstancesRequested -= 1
        abortedJobIds -= job.getUUID
        jobIds -= job.getUUID

        if (isAborted) {
          logInfo(s"Job ${job.getUUID} has been successfully aborted.")
        }

        if (!job.isSuccess && !isAborted) {
          totalFailures += 1
          logWarning(s"Job ${job.getUUID} has died. Failure ($totalFailures/$maxFailures)")
          jobIds -= job.getUUID
          if (totalFailures >= maxFailures) {
            // TODO should we abort the outstanding tasks now?
            logError(s"We have exceeded our maximum failures ($maxFailures)" +
              "and will not relaunch any more tasks")
          }
        }
      }
    }
  }
  def executorUUIDWriter: UUID => Unit =
    conf.getOption("spark.cook.executoruuid.log").fold { _: UUID => () } { _file =>
      def file(ct: Int) = s"${_file}.$ct"
      def path(ct: Int) = Paths.get(file(ct))

      // Here we roll existing logs.
      @annotation.tailrec
      def findFirstFree(ct: Int = 0): Int =
        if (Files.exists(path(ct))) findFirstFree(ct + 1)
        else ct

      @annotation.tailrec
      def rollin(ct: Int) {
        if (ct > 0) {
          Files.move(path(ct - 1), path(ct))
          rollin(ct - 1)
        }
      }

      rollin(findFirstFree())

      { uuid: UUID =>
        val bw = new BufferedWriter(new FileWriter(file(0), true))
        bw.write(uuid.toString)
        bw.newLine()
        bw.close()
      }
    }

  val sparkMesosScheduler =
    new MesosCoarseGrainedSchedulerBackend(scheduler, sc, "", sc.env.securityManager)

  override def applicationId(): String = conf.get("spark.cook.applicationId", super.applicationId())
  override def applicationAttemptId(): Option[String] = Some(applicationId())

  def createJob(numCores: Double): Job = {
    import CoarseCookSchedulerBackend.fetchURI

    val jobId = UUID.randomUUID()
    val taskId = sparkMesosScheduler.newMesosTaskId()
    executorIdToJobId += taskId -> jobId
    logInfo(s"Creating job with id: $jobId. The corresponding executor id and task id is $taskId")
    val fakeOffer = Offer.newBuilder()
      .setId(OfferID.newBuilder().setValue("Cook-id"))
      .setFrameworkId(FrameworkID.newBuilder().setValue("Cook"))
      .setHostname("$(hostname)")
      .setSlaveId(SlaveID.newBuilder().setValue(jobId.toString))
      .build()
    val commandInfo = sparkMesosScheduler.createCommand(fakeOffer, numCores.toInt, taskId)
    val commandString = commandInfo.getValue
    val environmentInfo = commandInfo.getEnvironment

    // Note that it is critical to export these variables otherwise when
    // we invoke the spark scripts, our values will not be picked up
    val environment =
      environmentInfo.getVariablesList.asScala
        .map { v => s"export ${v.getName}=" + "\"" + v.getValue + "\"" } ++
        Seq("export SPARK_LOCAL_DIRS=$MESOS_SANDBOX/spark-temp", "mkdir $SPARK_LOCAL_DIRS") ++
        Seq(s"export SPARK_EXECUTOR_APP_ID=$applicationId") ++
        rsyncServer.fold(Seq[String]()) { server =>
          val port = Await.result(server.port, Duration.Inf)
          Seq(
            s"export SPARK_DRIVER_PULL_HOST=${server.hostname}",
            s"export SPARK_DRIVER_PULL_PORT=$port")
        } ++
        conf.getOption("spark.python.command").fold(Seq[String]()) { pythonCommand =>
          Seq(
            s"echo $pythonCommand \\$$@ > python_command",
            "chmod 755 python_command")
        }

    val uriValues = commandInfo.getUrisList.asScala.map(_.getValue)

    val keystoreUri = conf.getOption("spark.executor.keyStoreFilename")
    val keystorePull = keystoreUri.map { uri =>
      s"${fetchURI(uri)} && mv $$(basename $uri) spark-executor-keystore"
    }

    val urisCommand =
      uriValues.map { uri =>
          s"[ ! -e $$(basename $uri) ] && ${fetchURI(uri)} && tar -xvzf $$(basename $uri)" +
            " || (echo \"ERROR FETCHING\" && exit 1)"
        }

    val shippedTarballs: Seq[String] = conf.getOption("spark.cook.shippedTarballs")
      .fold(Seq[String]()){ tgz => tgz.split(",").map(_.trim).toList }

    val shippedTarballsCommand = shippedTarballs.map { uri =>
      s"[ ! -e $$(basename $uri) ] && ${fetchURI(uri)} && tar -xvzf $$(basename $uri)"
    }

    logDebug(s"command: $commandString")
    val debug = Seq(
      "if [ -z \"${MESOS_SANDBOX}\" ]; then MESOS_SANDBOX=\"$MESOS_DIRECTORY\"; fi",
      "pwd",
      "whoami",
      "klist",
      "mount",
      "cd $MESOS_SANDBOX"
    )
    val remoteHdfsConf = conf.get("spark.executor.cook.hdfs.conf.remote", "")
    val remoteConfFetch = if (remoteHdfsConf.nonEmpty) {
      val name = Paths.get(remoteHdfsConf).getFileName
      Seq(
        fetchURI(remoteHdfsConf),
        "mkdir HADOOP_CONF_DIR",
        s"tar --strip-components=1 -xvzf $name -C HADOOP_CONF_DIR",
        // This must be absolute because we cd into the spark directory
        s"export HADOOP_CONF_DIR=`pwd`/HADOOP_CONF_DIR",
        "export HADOOP_CLASSPATH=$HADOOP_CONF_DIR")
    } else Seq()

    val cleanup = Seq(
      "cd $MESOS_SANDBOX",
      "if [ -z $KEEP_SPARK_LOCAL_DIRS ]",
      "then rm -rf $SPARK_LOCAL_DIRS",
      "echo deleted $SPARK_LOCAL_DIRS",
      "fi" // Deletes all of the tar files we fetched to save space on mesos
      ) ++ uriValues.map { uri =>
        s"[ -z $$KEEP_SPARK_LOCAL_TARS ] || rm -f $$(basename $uri)"
      }

    val cmds =
      debug ++
      environment ++
      urisCommand ++
      shippedTarballsCommand ++
      remoteConfFetch ++
      keystorePull.map(Seq(_)).getOrElse(Seq[String]()) ++
      Seq("set", commandString) ++
      cleanup

    val builder = new Job.Builder()
      .setUUID(jobId)
      .setName(jobNamePrefix)
      .setCommand(cmds.mkString("; "))
      .setMemory(executorMemory(sc).toDouble)
      .setCpus(numCores)
      .setPriority(priority)
      .disableMeaCulpaRetries()
      .setRetries(1)

    val container = conf.get("spark.executor.cook.container", null)
    if(container != null) {
      builder.setContainer(new JSONObject(container))
    }

    conf.getOption("spark.executor.cook.principalsThatCanView").foreach(builder.addLabel("principalsThatCanView", _))

    builder.build()
  }

  private[this] val minExecutorsNecessary = currentInstancesToRequest * minRegisteredRatio

  override def sufficientResourcesRegistered(): Boolean =
    totalRegisteredExecutors.get >= minExecutorsNecessary

  private[this] var lastIsReadyLog = 0L

  override def isReady(): Boolean = {
    val ret = super.isReady()
    val cur = System.currentTimeMillis
    if (!ret && cur - lastIsReadyLog > 5000) {
      logInfo("Backend is not yet ready. Registered executors " +
        s"[${totalRegisteredExecutors.get}] vs minimum necessary " +
        s"to start [$minExecutorsNecessary]")
      lastIsReadyLog = cur
    }
    ret
  }

  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new DriverEndpoint(rpcEnv, properties) {
      override def onDisconnected(remoteAddress: RpcAddress): Unit = {
        addressToExecutorId
          .get(remoteAddress)
          .foreach(handleDisconnectedExecutor(_))
      }

      def handleDisconnectedExecutor(executorId: String): Unit = {
        logInfo(s"Received disconnect message from executor with id $executorId." +
          s" Its related cook job id is ${executorIdToJobId(executorId)}")
        // TODO: we end up querying for everything, not sure of the perf implications here
        val jobId = executorIdToJobId(executorId)
        val jobInstances = jobClient.query(Seq(jobId).asJava).asScala.values
          .flatMap(_.getInstances.asScala).toSeq
        val slaveLostReason = SlaveLost("Remote RPC client disassociated likely due to " +
          "containers exceeding thresholds or network issues. Check driver logs for WARN " +
          "message.")
        if (jobInstances.isEmpty) {
          // This can happen in the case of an aborted executor when the Listener removes it first.
          // We can just mark it as lost since it wouldn't be preempted anyways.
          removeExecutor(executorId, slaveLostReason)
        }
        jobInstances.foreach { instance =>
          if (instance.getPreempted) {
            logInfo(s"Executor $executorId was removed due to preemption. Marking as killed.")
            removeExecutor(executorId, ExecutorExited(instance.getReasonCode.toInt,
              exitCausedByApp = false, "Executor was preempted by the scheduler."))
          } else {
            removeExecutor(executorId, slaveLostReason)
          }
        }
      }
    }
  }

  /*
   * Kill the given list of executors through the cluster manager.
   * @return whether the kill request is acknowledged.
   */
  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = Future.successful {
    val jobIdsToKill = executorIds.flatMap(executorIdToJobId.get)
    jobClient.abort(jobIdsToKill.asJava)
    jobIdsToKill.foreach(abortedJobIds.add)
    true
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = Future.successful {
    logInfo(s"Setting total amount of executors to request to $requestedTotal")
    executorsToRequest = requestedTotal
    requestRemainingInstances()
    true
  }

  /**
   * Generate a list of jobs expected to run in Cook according to the difference of the total
   * number of requested cores so far and the desired maximum number of cores.
   *
   * @return a list of jobs expected to run in Cook
   */
  private[this] def createRemainingJobs(): List[Job] = {
    @annotation.tailrec
    def loop(instancesRemaining: Double, jobs: List[Job]): List[Job] =
      if (instancesRemaining <= 0) jobs
      else loop(instancesRemaining - 1, createJob(maxCoresPerJob) :: jobs)
    loop(currentInstancesToRequest, Nil).reverse
  }

  /**
   * Request cores from Cook via cook jobs.
   */
  private[this] def requestRemainingInstances(): Unit = {
    val jobs = createRemainingJobs()
    if (jobs.nonEmpty) {
      Try[Unit](jobClient.submit(jobs.asJava, jobListener)) match {
        case Failure(e) => logWarning("Can't request more instances", e)
        case Success(_) => {
          logInfo(s"Successfully requested ${jobs.size} instances")
          totalInstancesRequested += jobs.size
          jobs.map(_.getUUID).foreach(jobIds.add)
        }
      }
    }
  }

  /**
   * Periodically check if the requested cores meets the requirement. If not, request more.
   */
  val resourceManagerService = Executors.newScheduledThreadPool(1,
    new ThreadFactoryBuilder().setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      override def uncaughtException(t: Thread, e: Throwable) =
        logWarning(s"Can not handle exception ${e}")
    }).build())

  private[this] val rsyncServer: Option[RsyncServer] =
    conf.getOption("spark.executor.rsyncDir").map(rsyncDir =>
      new RsyncServer(rsyncDir, conf.getOption("spark.driver.rsyncExportDirs")))

  override def start(): Unit = {
    super.start()

    requestRemainingInstances()
    resourceManagerService.scheduleAtFixedRate(new Runnable() {
      override def run(): Unit = requestRemainingInstances()
    }, 10, 10, TimeUnit.SECONDS)
  }

  override def stop(): Unit = {
    super.stop()

    rsyncServer.foreach(_.stop())
    jobClient.abort(jobIds.asJava)
    jobClient.close()
    resourceManagerService.shutdown()
  }
}
