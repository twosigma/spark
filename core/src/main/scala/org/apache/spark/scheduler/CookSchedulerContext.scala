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

import org.apache.spark.{SparkConf, SparkContext}

case class CookSchedulerContext(
    @transient sc: SparkContext
) {

  @transient val conf: SparkConf = sc.getConf

  // ==========================================================================
  // Configurations that will be used by Cook scheduler backend

  val SPARK_CORES_MAX = "spark.cores.max"

  val SPARK_EXECUTOR_CORES = "spark.executor.cores"

  val SPARK_DYNAMIC_ALLOCATION_ENABLED =
    "spark.dynamicAllocation.enabled"

  val SPARK_COOK_PRIORITY = "spark.cook.priority"

  val SPARK_COOK_JOB_NAME_PREFIX = "spark.cook.job.name.prefix"

  val SPARK_SCHEDULER_MIN_REGISTERED_RESOURCE_RATIO =
    "spark.scheduler.minRegisteredResourcesRatio"

  val SPARK_COOK_APPLICATION_ID = "spark.cook.applicationId"

  val SPARK_EXECUTOR_COOK_CONTAINER = "spark.executor.cook.container"

  val PRINCIPALS_THAT_CAN_VIEW = "principalsThatCanView"

  val SPARK_EXECUTOR_COOK_PRINCIPALS_THAT_CAN_VIEW =
    s"spark.executor.cook.$PRINCIPALS_THAT_CAN_VIEW"

  val SPARK_EXECUTOR_KEY_STORE_FILENAME = "spark.executor.keyStoreFilename"

  val SPARK_COOK_SHIPPED_TARBALLS = "spark.cook.shippedTarballs"

  val SPARK_EXECUTOR_COOK_HDFS_CONF_REMOTE =
    "spark.executor.cook.hdfs.conf.remote"

  val SPARK_PYTHON_COMMAND = "spark.python.command"

  // ==========================================================================

  val minRegisteredResourceRatio: Double = math.min(
    1,
    conf.getDouble(SPARK_SCHEDULER_MIN_REGISTERED_RESOURCE_RATIO, 0))

  val isDynamicAllocationEnabled: Boolean =
    conf.getBoolean(SPARK_DYNAMIC_ALLOCATION_ENABLED, defaultValue = false)

  val coresPerCookJob: Int = conf.getInt(SPARK_EXECUTOR_CORES, 1)

  val maxCores: Int = conf.getInt(SPARK_CORES_MAX, 0)

  val maxExecutors: Int = Math.ceil(maxCores / coresPerCookJob).toInt

  val cookJobPriority: Int = conf.getInt(SPARK_COOK_PRIORITY, 75)

  val cookJobNamePrefix: String =
    conf.get(SPARK_COOK_JOB_NAME_PREFIX, "sparkjob")

  val executorCookContainerOption: Option[String] =
    conf.getOption(SPARK_EXECUTOR_COOK_CONTAINER)

  val executorKeyStoreURIOption: Option[String] =
    conf.getOption(SPARK_EXECUTOR_KEY_STORE_FILENAME)

  val executorRemoteHDFSConfOption: Option[String] =
    conf.getOption(SPARK_EXECUTOR_COOK_HDFS_CONF_REMOTE)

  val cookApplicationIdOption: Option[String] =
    conf.getOption(SPARK_COOK_APPLICATION_ID)

  val sparkPythonCommandOption: Option[String] =
    conf.getOption(SPARK_PYTHON_COMMAND)

  val cookShippedTarballs: Seq[String] =
    conf
      .getOption(SPARK_COOK_SHIPPED_TARBALLS)
      .fold(Seq[String]()) { tgz =>
        tgz.split(",").map(_.trim).toList
      }

  /**
    * @return executor ids of alive executors.
    */
  def getExecutorIds: Seq[String] = {
    require(sc.schedulerBackend.isInstanceOf[CoarseCookSchedulerBackend],
            "The current scheduler backend is not Cook.")
    sc.schedulerBackend
      .asInstanceOf[CoarseCookSchedulerBackend]
      .getExecutorIds()
  }

}

object CookSchedulerContext {

  @volatile
  private[this] var context: CookSchedulerContext = _

  private[spark] def get(sc: SparkContext): CookSchedulerContext = {
    if (context == null) {
      this.synchronized {
        if (context == null) {
          context = new CookSchedulerContext(sc)
        }
      }
    }
    context
  }

  def get(): CookSchedulerContext = {
    require(context != null,
            "CookSchedulerContext haven't been initialized yet.")
    context
  }
}
