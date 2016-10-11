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

package org.apache.spark.scheduler

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.lang.Thread.UncaughtExceptionHandler
import java.net.{InetAddress, ServerSocket, URI}
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.PosixFilePermissions
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.sys.process.{BasicIO, Process, ProcessLogger}
import scala.util.{Failure, Success, Try}
import org.apache.mesos._
import org.apache.mesos.Protos._
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.cluster.mesos.CoarseMesosSchedulerBackend
import org.json.JSONObject
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.twosigma.cook.jobclient.{Job, JobClient, JobListener => CJobListener}
import org.apache.spark.rpc.RpcAddress

import scala.collection.mutable

object CoarseCookSchedulerBackend {
  def fetchUri(uri: String): String =
    Option(URI.create(uri).getScheme).map(_.toLowerCase) match {
      case Some("http") => s"curl $uri"
      case Some("spark-rsync") =>
        val regex = "^spark-rsync://".r
        val cleanURI = regex.replaceFirstIn(uri, "")
        "RSYNC_CONNECT_PROG=" + "\"" + "knc spark-rsync@%H $SPARK_DRIVER_PULL_PORT" + "\"" +
          s" rsync $$SPARK_DRIVER_PULL_HOST::spark/${cleanURI} ./"
      case Some("hdfs") => s"$$HADOOP_COMMAND fs -copyToLocal $uri ."
      case Some("rover") =>
        val storage = "/opt/ts/services/storage-client.ts_storage_client/bin/storage"
        s"$storage -X-Dtwosigma.logdir=$${MESOS_SANDBOX} cp $uri ."
      case None | Some("file") => s"cp $uri ."
      case Some(x) => sys.error(s"$x not supported yet")
    }

  def apply(scheduler: TaskSchedulerImpl, sc: SparkContext, cookHost: String,
    cookPort: Int): CoarseGrainedSchedulerBackend = {
    new CoarseCookSchedulerBackend(scheduler, sc, cookHost, cookPort)
  }
}

private[spark] class RsyncServer(rsyncDir: String, rsyncExportDirs: Option[String])
  extends Logging
{
  private[this] val currentUsername = sys.props.get("user.name").getOrElse("nobody")
  private[this] val currentHostname = java.net.InetAddress.getLocalHost.getHostName
  val hostname = s"$currentUsername.$currentHostname"

  private[this] val rsyncConf = s"""
[spark]
     path = $rsyncDir
     use chroot = no
     reverse lookup = no
     read only = yes
""" + rsyncExportDirs.fold("") { exportDir =>
      s"""
[${exportDir.split("/").last}]
     path = $exportDir
     use chroot = no
     reverse lookup = no
     read only = yes
"""
    }

  private[this] val rsyncConfFile: File = {
    val tmpFile = File.createTempFile("spark-rsync", ".tmp")
    tmpFile.deleteOnExit()
    new PrintWriter(tmpFile) {
      write(rsyncConf)
      close()
    }
    logDebug(s"Created rsync conf at ${tmpFile.getAbsolutePath}")
    tmpFile
  }

  private[this] val kncShellFile = {
    // Create a shell script that will be executed by KNC
    // that verifies that the user is the only one allowed to connect
    // to the listening port.
    //
    // It is simpler to do this here than inline with all of the shell escaping
    // since the connection will be setup be: socat -> knc -> bash -> rsync
    //
    // socat is used to listen on a TCP port (the reason why socat is used is
    // that is outputs the port number it is listening on to stderr, knc doesn't
    // do that.
    //
    // knc is used to establish authentication
    //
    // bash is used to check that the supplied Kerberos credential matches
    // the expected user's credential
    //
    // rsync is used to synchronize the data
    val tmpFile = File.createTempFile("knc-spark-rsync", ".tmp")
    tmpFile.deleteOnExit()
    Files.setPosixFilePermissions(tmpFile.toPath, PosixFilePermissions.fromString("rwx------"))
    new PrintWriter(tmpFile) {
      val credVariable = "$KNC_CREDS"
      write(s"""#!/bin/sh
case $credVariable in
       $currentUsername@CB.TWOSIGMA.COM)
/usr/bin/rsync --config ${rsyncConfFile.getAbsolutePath} --daemon
;;
       $currentUsername@N.TWOSIGMA.COM)
/usr/bin/rsync --config ${rsyncConfFile.getAbsolutePath} --daemon
;;
esac""")
      close()
    }
    logDebug(s"Created knc shell file at ${tmpFile.getAbsolutePath}")
    tmpFile
  }

  private[this] val rsyncPort: Promise[Int] = Promise()
  private[this] val rsyncProcess: Process = {
    val keytabCommand = s"/usr/sbin/krb5_keytab spark-rsync/$currentUsername.$currentHostname"
    val keytabResult = Process(keytabCommand).!

    if (keytabResult != 0) {
      throw new Exception("Unable to create Kerberos keytab for " +
        s"spark-rsync/$currentUsername.$currentHostname")
    }

    // socat will output a line to stderr that tells us where it is listening
    // we'll parse the port number out of that line with a regular expression
    val listenerPattern = ".*listening on AF=[0-9]+ .+:([0-9]+)".r

    // This is the ProcessLogger that will listen to the lines written
    // by socat to stderr and stdout, stdout lines are discarded.
    val logger = ProcessLogger(
      line => logDebug(s"Ignoring socat output line: $line"),
      line => {
        logDebug(s"Attempting to parse socat line: $line")
        line match {
          case listenerPattern(portNumber) =>
            if (!rsyncPort.isCompleted) {
              rsyncPort.success(portNumber.toInt)
            } else {
              logDebug("Ignoring socat output line since port is already set")
            }
          case _ => logDebug(s"Ignoring socat output line: $line")
        }
      }
    )
    val io = BasicIO(withIn = false, logger).daemonized

    val proc = Process(
      Seq(
        "/usr/bin/socat",
        "-d", "-d",
        "TCP-LISTEN:0,fork",
        "EXEC:\"/usr/bin/knc -il " + kncShellFile.getAbsolutePath + "\""
      ),
      None,
      "KRB5_KTNAME" -> s"/var/spool/keytabs/$currentUsername").run(io)
    logDebug(s"Started rsync process")
    proc
  }

  val port: Future[Int] = rsyncPort.future

  private[this] var stopped = false

  def stop() {
    this.synchronized {
      if (!stopped) {
        logDebug("Stopping rsync process")
        Try(rsyncProcess.destroy()).recover {
          case exc => logWarning(s"Error stopping rsync: $exc")
        }
        stopped = true
      }
    }
  }

  // Cleanup the rsync listener upon termination if necessary.
  sys.addShutdownHook {
    logDebug("In shutdown hook, calling stop")
    stop()
  }
}

/**
 * A SchedulerBackend that runs tasks using Cook, using "coarse-grained" tasks, where it holds
 * onto Cook instances for the duration of the Spark job instead of relinquishing cores whenever
 * a task is done. It launches Spark tasks within the coarse-grained Cook instances using the
 * CoarseGrainedSchedulerBackend mechanism. This class is useful for lower and more predictable
 * latency.
 */
class CoarseCookSchedulerBackend(
  scheduler: TaskSchedulerImpl,
  sc: SparkContext,
  cookHost: String,
  cookPort: Int)
    extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) with Logging {

  val maxCores = conf.getInt("spark.cores.max", 0)
  val maxCoresPerJob = conf.getInt("spark.cook.cores.per.job.max", 5)
  val priority = conf.getInt("spark.cook.priority", 75)
  val jobNamePrefix = conf.get("spark.cook.job.name.prefix", "sparkjob")
  val maxFailures = conf.getInt("spark.executor.failures", 5)

  def currentCoresToRequest: Int = executorsToRequest.map(_ * maxCoresPerJob)
    .getOrElse(maxCores) - totalCoresRequested
  var executorsToRequest: Option[Int] = None
  var totalCoresRequested = 0
  var totalFailures = 0
  val jobIds = mutable.Set[UUID]()
  val abortedJobIds = mutable.Set[UUID]()

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
        totalCoresRequested -= job.getCpus.toInt
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
    new CoarseMesosSchedulerBackend(scheduler, sc, "", sc.env.securityManager)

  override def applicationId(): String = conf.get("spark.cook.applicationId", super.applicationId())
  override def applicationAttemptId(): Option[String] = Some(applicationId())

  def createJob(numCores: Double): Job = {
    import CoarseCookSchedulerBackend.fetchUri

    val jobId = UUID.randomUUID()
    executorUUIDWriter(jobId)
    logInfo(s"Creating job with id: $jobId")
    val fakeOffer = Offer.newBuilder()
      .setId(OfferID.newBuilder().setValue("Cook-id"))
      .setFrameworkId(FrameworkID.newBuilder().setValue("Cook"))
      .setHostname("$(hostname)")
      .setSlaveId(SlaveID.newBuilder().setValue("${MESOS_EXECUTOR_ID}"))
      .build()
    val taskId = sparkMesosScheduler.newMesosTaskId()
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
      s"${fetchUri(uri)} && mv $$(basename $uri) spark-executor-keystore"
    }

    val urisCommand =
      uriValues.map { uri =>
          s"[ ! -e $$(basename $uri) ] && ${fetchUri(uri)} && tar -xvzf $$(basename $uri)" +
            " || (echo \"ERROR FETCHING\" && exit 1)"
        }

    val shippedTarballs: Seq[String] = conf.getOption("spark.cook.shippedTarballs")
      .fold(Seq[String]()){ tgz => tgz.split(",").map(_.trim).toList }

    val shippedTarballsCommand = shippedTarballs.map { uri =>
      s"[ ! -e $$(basename $uri) ] && ${fetchUri(uri)} && tar -xvzf $$(basename $uri)"
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
        fetchUri(remoteHdfsConf),
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
      .setMemory(sparkMesosScheduler.calculateTotalMemory(sc).toDouble)
      .setCpus(numCores)
      .setPriority(priority)

    val container = conf.get("spark.executor.cook.container", null)
    if(container != null) {
      builder.setContainer(new JSONObject(container))
    }

    conf.getOption("spark.executor.cook.principalsThatCanView").foreach(builder.addLabel("principalsThatCanView", _))

    builder.build()
  }

  private[this] val minExecutorsNecessary =
    math.ceil(maxCores.toDouble / maxCoresPerJob) * minRegisteredRatio

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

  // In our fake offer mesos adds some autoincrementing ID per job but
  // this sticks around in the executorId so we strop it out to get the actual executor ID
  private def instanceIdFromExecutorId(executorId: String): UUID = {
    UUID.fromString(executorId.split('/')(0))
  }

  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new DriverEndpoint(rpcEnv, properties) {
      override def onDisconnected(remoteAddress: RpcAddress): Unit = {
        addressToExecutorId
          .get(remoteAddress)
          .foreach(handleDisconnectedExecutor(_))
      }

      def handleDisconnectedExecutor(executorId: String): Unit = {
        logInfo(s"Recieved disconnect message from executor with ID: ${executorId}")
        // TODO: we end up querying for everything, not sure of the perf implications here
        val allInstances = jobClient.query(jobIds.asJava).asScala.values
          .flatMap(_.getInstances.asScala).toSeq
        val instanceId = instanceIdFromExecutorId(executorId)
        val correspondingInstance = allInstances.find(_.getTaskID == instanceId)
        if (correspondingInstance.isEmpty) {
          logWarning(s"Unable to find executorId: ${executorId} from ${allInstances}")
        }
        correspondingInstance.foreach(instance => {
          val wasPreempted = instance.getPreempted
          val exitCode = instance.getReasonCode
          if (wasPreempted) {
            logInfo(s"Executor ${executorId} was removed due to preemption. Marking as killed.")
            removeExecutor(executorId, ExecutorExited(exitCode.toInt,
              false, "Executor was preempted by the scheduler."))
          } else {
            removeExecutor(executorId, SlaveLost("Remote RPC client disassociated likely due to " +
              "containers exceeding thresholds or network issues. Check driver logs for WARN " +
              "message."))
          }
        })
      }
    }
  }

  /*
   * Kill the given list of executors through the cluster manager.
   * @return whether the kill request is acknowledged.
   */
  override def doKillExecutors(executorIds: Seq[String]): Boolean = {
    val instancesToKill = executorIds.map(instanceIdFromExecutorId).toSet
    val jobsToInstances = jobClient.query(jobIds.asJava).asScala.values
      .flatMap(job => job.getInstances.asScala.map((job.getUUID, _))).toSeq
    val correspondingJobs = jobsToInstances.filter(i => instancesToKill.contains(i._2.getTaskID))
      .map(_._1).toSet
    jobClient.abort(correspondingJobs.asJava)
    correspondingJobs.foreach(abortedJobIds.add)
    true
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Boolean = {
    logInfo(s"Setting total amount of executors to request to $requestedTotal")
    executorsToRequest = Some(requestedTotal)
    requestRemainingCores()
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
    def loop(coresRemaining: Double, jobs: List[Job]): List[Job] =
      if (coresRemaining <= 0) jobs
      else if (coresRemaining <= maxCoresPerJob) createJob(coresRemaining) :: jobs
      else loop(coresRemaining - maxCoresPerJob, createJob(maxCoresPerJob) :: jobs)
    loop(currentCoresToRequest, Nil).reverse
  }

  /**
   * Request cores from Cook via cook jobs.
   */
  private[this] def requestRemainingCores(): Unit = {
    val jobs = createRemainingJobs()
    if (jobs.nonEmpty) {
      Try[Unit](jobClient.submit(jobs.asJava)) match {
        case Failure(e) => logWarning("Can't request more cores", e)
        case Success(_) => {
          val c = jobs.map(_.getCpus.toInt).sum
          logInfo(s"Successfully requested ${c} cores")
          totalCoresRequested += c
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

    requestRemainingCores()
    resourceManagerService.scheduleAtFixedRate(new Runnable() {
      override def run(): Unit = requestRemainingCores()
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
