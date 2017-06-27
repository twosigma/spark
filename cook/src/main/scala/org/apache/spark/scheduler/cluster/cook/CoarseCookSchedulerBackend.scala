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

import java.lang.Thread.UncaughtExceptionHandler
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.twosigma.cook.jobclient.{
  Job,
  JobClient,
  JobListener => CJobListener
}
import org.apache.mesos.Protos._
import org.json.JSONObject

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.{
  ExecutorExited,
  SlaveLost,
  TaskSchedulerImpl
}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.cluster.mesos.{
  MesosCoarseGrainedSchedulerBackend,
  MesosSchedulerUtils
}
import org.apache.spark.util.ThreadUtils

object CoarseCookSchedulerBackend {

  // A collection of regular expressions for extracting information from an URI
  private[this] val HTTP_URI_REGEX = """http://(.*)""".r
  private[this] val RSYNC_URI_REGEX = """rsync://(.*)""".r
  private[this] val SPARK_RSYNC_URI_REGEX = """spark-rsync://(.*)""".r
  private[this] val HDFS_URI_REGEX = """hdfs://(.*)""".r

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

  def apply(
      scheduler: TaskSchedulerImpl,
      sc: SparkContext,
      cookHost: String,
      cookPort: Int
  ): CoarseGrainedSchedulerBackend = {
    new CoarseCookSchedulerBackend(scheduler, sc, cookHost, cookPort)
  }
}

/*
 * A SchedulerBackend that runs tasks using Cook, using "coarse-grained" tasks, where it holds
 * onto Cook instances for the duration of the Spark job instead of relinquishing cores whenever
 * a task is done. It launches Spark tasks within the coarse-grained Cook instances using the
 * CoarseGrainedSchedulerBackend mechanism. This class is useful for lower and more predictable
 * latency.
 *
 * Since Spark 2.0.0, executor id must be an integer even though its type is String. This backend
 * uses task id which is also an integer created by
 * {{{
 *   MesosCoarseGrainedSchedulerBackend.newMesosTaskId
 * }}}
 * as executor id.
 *
 * To ensure the mapping from executor id to its Cook job instance is 1-1 and onto,
 * we only allow one instance per Cook job and we are using the mapping from
 * <executor id> -> <cook job id> to track this relationship.
 */
class CoarseCookSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    cookHost: String,
    cookPort: Int
) extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
    with Logging
    with MesosSchedulerUtils {

  private[this] val schedulerContext = CookSchedulerContext.get(sc)

  /**
    * The total number of executors we aim to have. Undefined when not using dynamic allocation.
    * Initially set to 0 when using dynamic allocation, the executor allocation manager will send
    * the real initial limit later.
    */
  private var executorLimitOption: Option[Int] = {
    if (schedulerContext.isDynamicAllocationEnabled) {
      Some(0)
    } else {
      None
    }
  }

  /**
    * The current executor limit, which may be [[Int.MaxValue]]
    * before properly initialized. For dynamic allocation, it is properly set
    * when creating `SparkContext` via `ExecutorAllocationManager.start()`;
    * otherwise, it is set via the `start()` of this backend.
    */
  private def executorLimit: Int = this.synchronized {
    executorLimitOption.getOrElse(Int.MaxValue)
  }

  private def setExecutorLimit(limit: Int): Unit = this.synchronized {
    executorLimitOption = Some(limit)
  }

  /**
    * The set of UUIDs for non-completed jobs.
    *  - A job whose UUID will be added to this set if the job has been submitted to
    *    Cook successfully;
    *  - A job whose UUID will be removed from this set if this backend receives its
    *    "complete" status via Cook job listener.
    */
  private[this] val nonCompletedJobUUIDs = mutable.HashSet[UUID]()

  /**
    * Note that the mapping between Cook job and executor is 1-1 and onto, thus
    * the number of acquired executors equals to the number non-completed jobs.
    */
  private def totalExecutorsAcquired: Int = nonCompletedJobUUIDs.size

  /**
    * The set of UUIDs for the jobs that are aborted intentionally, e.g.
    * via dynamic allocation. This data is only for logging purpose.
    */
  private[this] val abortedJobUUIDs = mutable.HashSet[UUID]()

  private[this] val executorIdToJobUUID = mutable.HashMap[String, UUID]()

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
      val isAborted = abortedJobUUIDs.contains(job.getUUID)

      if (isCompleted) {
        nonCompletedJobUUIDs.remove(job.getUUID)
        abortedJobUUIDs.remove(job.getUUID)

        if (isAborted) {
          logInfo(
            s"Job ${job.getUUID} has been successfully aborted per request.")
        }

        if (!job.isSuccess && !isAborted) {
          logWarning(s"Job ${job.getUUID} has died unintentionally.")
        }
      }
    }
  }

  private[this] val mesosSchedulerBackend =
    new MesosCoarseGrainedSchedulerBackend(scheduler, sc, "", sc.env.securityManager)

  override def sufficientResourcesRegistered(): Boolean =
    totalExecutorsAcquired >= executorLimit * schedulerContext.minRegisteredResourceRatio

  override def applicationId(): String =
    schedulerContext.cookApplicationIdOption.getOrElse(super.applicationId())

  override def applicationAttemptId(): Option[String] = Some(applicationId())

  // Idempotent
  private def createJob(cores: Int, jobUUID: UUID, executorId: String): Job = {
    import CoarseCookSchedulerBackend.fetchURI

    val fakeOffer = Offer
      .newBuilder()
      .setId(OfferID.newBuilder().setValue("Cook-id"))
      .setFrameworkId(FrameworkID.newBuilder().setValue("Cook"))
      .setHostname("$(hostname)")
      .setSlaveId(SlaveID.newBuilder().setValue(jobUUID.toString))
      .build()

    val commandInfo =
      mesosSchedulerBackend.createCommand(fakeOffer, cores, executorId)
    val commandString = commandInfo.getValue
    val environmentInfo = commandInfo.getEnvironment

    // Note that it is critical to export these variables otherwise when
    // we invoke the spark scripts, our values will not be picked up
    val envSettingCommand =
      environmentInfo.getVariablesList.asScala
        .map { v =>
          s"export ${v.getName}=" + "\"" + v.getValue + "\""
        } ++
        Seq("export SPARK_LOCAL_DIRS=$MESOS_SANDBOX/spark-temp",
            "mkdir $SPARK_LOCAL_DIRS") ++
        Seq(s"export SPARK_EXECUTOR_APP_ID=${applicationId()}") ++
        rsyncServer.fold(Seq[String]()) { server =>
          val port = ThreadUtils.awaitResult(server.port, Duration.Inf)
          Seq(s"export SPARK_DRIVER_PULL_HOST=${server.hostname}",
              s"export SPARK_DRIVER_PULL_PORT=$port")
        } ++
        schedulerContext.sparkPythonCommandOption.fold(Seq[String]()) {
          pythonCommand =>
            Seq(s"echo $pythonCommand \\$$@ > python_command",
                "chmod 755 python_command")
        }

    val uriValues = commandInfo.getUrisList.asScala.map(_.getValue)

    val keystorePullCommand = schedulerContext.executorKeyStoreURIOption.map {
      uri =>
        s"${fetchURI(uri)} && mv $$(basename $uri) spark-executor-keystore"
    }

    val uriFetchCommand =
      uriValues.map { uri =>
        s"[ ! -e $$(basename $uri) ] && ${fetchURI(uri)} && tar -xvzf $$(basename $uri)" +
          " || (echo \"ERROR FETCHING\" && exit 1)"
      }

    val shippedTarballsCommand = schedulerContext.cookShippedTarballs.map {
      uri =>
        s"[ ! -e $$(basename $uri) ] && ${fetchURI(uri)} && tar -xvzf $$(basename $uri)"
    }

    logDebug(s"command: $commandString")
    val debugCommand = Seq(
      "if [ -z \"${MESOS_SANDBOX}\" ]; then MESOS_SANDBOX=\"$MESOS_DIRECTORY\"; fi",
      "pwd",
      "whoami",
      "klist",
      "mount",
      "cd $MESOS_SANDBOX"
    )

    val remoteConfFetchCommand =
      schedulerContext.executorRemoteHDFSConfOption.fold(Seq.empty[String]) {
        remoteHDFSConf =>
          val name = Paths.get(remoteHDFSConf).getFileName
          Seq(
            fetchURI(remoteHDFSConf),
            "mkdir HADOOP_CONF_DIR",
            s"tar --strip-components=1 -xvzf $name -C HADOOP_CONF_DIR",
            // This must be absolute because we cd into the spark directory
            s"export HADOOP_CONF_DIR=`pwd`/HADOOP_CONF_DIR",
            "export HADOOP_CLASSPATH=$HADOOP_CONF_DIR"
          )
      }

    val cleanup = Seq(
      "cd $MESOS_SANDBOX",
      "if [ -z $KEEP_SPARK_LOCAL_DIRS ]",
      "then rm -rf $SPARK_LOCAL_DIRS",
      "echo deleted $SPARK_LOCAL_DIRS",
      "fi" // Deletes all of the tar files we fetched to save space on mesos
    ) ++ uriValues.map { uri =>
      s"[ -z $$KEEP_SPARK_LOCAL_TARS ] || rm -f $$(basename $uri)"
    }

    val commandSeq =
      debugCommand ++
        envSettingCommand ++
        uriFetchCommand ++
        shippedTarballsCommand ++
        remoteConfFetchCommand ++
        keystorePullCommand.map(Seq(_)).getOrElse(Seq[String]()) ++
        Seq("set", commandString) ++
        cleanup

    val builder = new Job.Builder()
      .setUUID(jobUUID)
      .setName(schedulerContext.cookJobNamePrefix)
      .setCommand(commandSeq.mkString("; "))
      .setMemory(executorMemory(sc).toDouble)
      .setCpus(cores.toDouble)
      .setPriority(schedulerContext.cookJobPriority)
      // The following two setting ensure each Cook job has only 1 instance.
      .disableMeaCulpaRetries()
      .setRetries(1)

    schedulerContext.executorCookContainerOption.foreach { container =>
      builder.setContainer(new JSONObject(container))
    }

    conf
      .getOption(schedulerContext.SPARK_EXECUTOR_COOK_PRINCIPALS_THAT_CAN_VIEW)
      .foreach(builder.addLabel(schedulerContext.PRINCIPALS_THAT_CAN_VIEW, _))

    builder.build()
  }

  private[this] var lastIsReadyLog = 0L

  override def isReady(): Boolean = {
    val ret = super.isReady()

    val cur = System.currentTimeMillis
    if (!ret && cur - lastIsReadyLog > 5000) {
      logInfo(
        s"Backend is not yet ready. Total acquired executors [$totalExecutorsAcquired] " +
          s"vs executor limit [$executorLimit]")
      lastIsReadyLog = cur
    }
    ret
  }

  override def createDriverEndpoint(
      properties: Seq[(String, String)]): DriverEndpoint =
    new DriverEndpoint(rpcEnv, properties) {

      override def onDisconnected(remoteAddress: RpcAddress): Unit =
        addressToExecutorId
          .get(remoteAddress)
          .foreach(handleDisconnectedExecutor)

      def handleDisconnectedExecutor(executorId: String): Unit = {
        val jobUUID = executorIdToJobUUID(executorId)
        nonCompletedJobUUIDs.remove(jobUUID)

        logInfo(
          s"Received disconnect message from executor with id $executorId." +
            s" whose Cook job UUID is $jobUUID")

        val jobInstances = jobClient
          .queryJobs(Seq(jobUUID).asJava)
          .asScala
          .values
          .flatMap(_.getInstances.asScala)
          .toSeq

        val slaveLostReason = SlaveLost(
          "Remote RPC client disassociated likely due to " +
            "containers exceeding thresholds or network issues. Check driver logs for WARN " +
            "message.")

        if (jobInstances.isEmpty) {
          // This can happen in the case of an aborted executor when the Listener removes it first.
          // We can just mark it as lost since it wouldn't be preempted anyways.
          removeExecutor(executorId, slaveLostReason)
        }

        jobInstances.foreach { instance =>
          if (instance.getPreempted) {
            logInfo(
              s"Executor $executorId was removed due to preemption. Marking as killed.")
            removeExecutor(
              executorId,
              ExecutorExited(instance.getReasonCode.toInt,
                             exitCausedByApp = false,
                             "Executor was preempted by the scheduler."))
          } else {
            removeExecutor(executorId, slaveLostReason)
          }
        }
      }
    }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] =
    Future.successful {
      // No need to adjust `executorLimitOption` since the AllocationManager already communicated
      // the desired limit through a call to `doRequestTotalExecutors`.
      // See [[o.a.s.scheduler.cluster.CoarseGrainedSchedulerBackend.killExecutors]]
      val unknownExecutorIds =
        executorIds.filterNot(executorIdToJobUUID.contains)
      if (unknownExecutorIds.nonEmpty) {
        logWarning(
          s"Ignore killing unknown executor ids ${unknownExecutorIds.mkString(",")}")
      }

      val jobUUIDsToAbort = executorIds.flatMap(executorIdToJobUUID.get)

      if (jobUUIDsToAbort.nonEmpty) {
        val abortJobMessage = s"abort jobs ${jobUUIDsToAbort.mkString(",")}" +
          s" when calling doKillExecutors(${executorIds.mkString(",")})"

        Try[Unit](jobClient.abort(jobUUIDsToAbort.asJava)) match {
          case Failure(e) =>
            logWarning(s"Failed to $abortJobMessage", e)
            false
          case Success(_) =>
            jobUUIDsToAbort.foreach { jobUUID =>
              abortedJobUUIDs.add(jobUUID)
              nonCompletedJobUUIDs.remove(jobUUID)
            }
            logInfo(s"Succeed to $abortJobMessage")
            true
        }
      } else {
        true
      }
    }

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] =
    Future.successful {
      logInfo(
        s"Setting total amount of executors to request to $requestedTotal")
      setExecutorLimit(requestedTotal)
      true
    }

  private def shouldRequestExecutors(): Boolean =
    totalExecutorsAcquired < executorLimit

  private def executorStatusMessage(): String =
    s"Currently, the total acquired executors is [$totalExecutorsAcquired] " +
      s"and the executor limit is [$executorLimit]."

  private def requestExecutorsIfNecessary(): Unit =
    if (shouldRequestExecutors()) {
      val requestedExecutors = executorLimit - totalExecutorsAcquired

      if (requestedExecutors > 0) {
        val executorIdAndJob = (1 to requestedExecutors).map { _ =>
          val jobUUID = UUID.randomUUID()
          val executorId = mesosSchedulerBackend.newMesosTaskId()
          val job =
            createJob(schedulerContext.coresPerCookJob, jobUUID, executorId)
          (executorId, job)
        }

        Try[Unit](
          jobClient
            .submit(executorIdAndJob.map(_._2).asJava, jobListener)) match {
          case Failure(e) =>
            logWarning(
              s"Failed to request executors from Cook. ${executorStatusMessage()}",
              e)
          case Success(_) =>
            executorIdAndJob.foreach {
              case (executorId, job) =>
                logInfo(s"Creating job with id: ${job.getUUID} " +
                  s"The corresponding executor id and task id is $executorId")
                executorIdToJobUUID += executorId -> job.getUUID
                nonCompletedJobUUIDs += job.getUUID
            }
            logInfo(
              s"Successfully requested ${executorIdAndJob.size} executors from Cook. " +
                executorStatusMessage())
        }
      }
    }

  private[this] val resourceManagerService = Executors.newScheduledThreadPool(
    1,
    new ThreadFactoryBuilder()
      .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
        override def uncaughtException(t: Thread, e: Throwable): Unit =
          logWarning(s"Can not handle exception $e")
      })
      .build()
  )

  private[this] val rsyncServer: Option[RsyncServer] =
    conf
      .getOption("spark.executor.rsyncDir")
      .map(
        rsyncDir =>
          new RsyncServer(rsyncDir,
                          conf.getOption("spark.driver.rsyncExportDirs")))

  override def start(): Unit = {
    super.start()

    // We rely on client to set the `executorLimitOption` via calling `doRequestTotalExecutors`
    // indirectly/directly. In the case of dynamic allocation, it is done when creating
    // `SparkContext` and calling `ExecutorAllocationManager.start()`, otherwise, we need to
    // set it explicitly.
    if (!schedulerContext.isDynamicAllocationEnabled) {
      doRequestTotalExecutors(schedulerContext.maxExecutors)
    }

    requestExecutorsIfNecessary()

    resourceManagerService.scheduleAtFixedRate(new Runnable() {
      override def run(): Unit = {
        requestExecutorsIfNecessary()
      }
    }, 10, 10, TimeUnit.SECONDS)
  }

  override def stop(): Unit = {
    super.stop()

    rsyncServer.foreach(_.stop())
    jobClient.abort(nonCompletedJobUUIDs.asJava)
    jobClient.close()
    resourceManagerService.shutdown()
  }
}
