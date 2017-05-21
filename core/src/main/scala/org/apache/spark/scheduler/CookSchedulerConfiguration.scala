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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
  * To use this configuration in PySpark, one could do
  * {{{
  *   >>> cook_conf_obj = sc._jvm.org.apache.spark.scheduler.CookSchedulerConfiguration
  *   >>> cook_conf = cook_conf_obj.conf()
  *   >>> cook_conf.setMaximumCores(8)
  * }}}
  */
class CookSchedulerConfiguration(
    @transient val conf: SparkConf
) extends Logging {

  private[this] val SPARK_CORES_MAX = "spark.cores.max"
  private[this] val SPARK_EXECUTOR_CORES = "spark.executor.cores"
  private[this] val SPARK_EXECUTOR_FAILURES = "spark.executor.failures"
  private[this] val SPARK_DYNAMICALLOCATION_ENABLED =
    "spark.dynamicAllocation.enabled"
  private[this] val SPARK_DYNAMICALLOCATION_MIN_EXECUTORS =
    "spark.dynamicAllocation.minExecutors"
  private[this] val SPARK_DYNAMICALLOCATION_MAX_EXECUTORS =
    "spark.dynamicAllocation.maxExecutors"
  private[this] val SPARK_COOK_PRIORITY = "spark.cook.priority"
  private[this] val SPARK_COOK_JOB_NAME_PREFIX = "spark.cook.job.name.prefix"

  private[this] val dynamicAllocationEnabled =
    conf.getBoolean(SPARK_DYNAMICALLOCATION_ENABLED, defaultValue = false)
  private[this] val coresPerCookJob = conf.getInt(SPARK_EXECUTOR_CORES, 1)

  // ==========================================================================
  // Config options
  private[this] var maximumCores = if (dynamicAllocationEnabled) {
    conf.getInt(SPARK_DYNAMICALLOCATION_MIN_EXECUTORS, 0) * coresPerCookJob
  } else {
    conf.getInt(SPARK_CORES_MAX, 0)
  }
  private[this] var maximumExecutorFailures =
    conf.getInt(SPARK_EXECUTOR_FAILURES, 5)
  private[this] var priorityOfCookJob = conf.getInt(SPARK_COOK_PRIORITY, 75)
  private[this] var prefixOfCookJobName =
    conf.get(SPARK_COOK_JOB_NAME_PREFIX, "sparkjob")

  // ==========================================================================1

  if (conf.getOption(SPARK_CORES_MAX).isDefined && dynamicAllocationEnabled) {
    logWarning(
      s"$SPARK_CORES_MAX is ignored when dynamic allocation is enabled. " +
        s"Use $SPARK_DYNAMICALLOCATION_MAX_EXECUTORS instead.")
  }

  def getCoresPerCookJob: Int = coresPerCookJob

  def getMaximumCores: Int = maximumCores

  def setMaximumCores(cores: Int): CookSchedulerConfiguration = {
    require(cores >= 0, "The maximum number of cores should be non-negative.")
    maximumCores = cores
    logInfo(
      s"The maximum cores of Cook scheduler has been set to $maximumCores")
    this
  }

  def getPriorityPerCookJob: Int = priorityOfCookJob

  def setPriorityPerCookJob(priority: Int): CookSchedulerConfiguration = {
    require(
      0 < priority && priority <= 100,
      "The priority of Cook job must be within range of (0, 100]."
    )
    priorityOfCookJob = priority
    logInfo(
      s"The priority of jobs in Cook scheduler has been set to $priorityOfCookJob")
    this
  }

  def getPrefixOfCookJobName: String = prefixOfCookJobName

  def setPrefixOfCookJobName(prefix: String): CookSchedulerConfiguration = {
    prefixOfCookJobName = prefix
    logInfo(
      s"The name prefix of jobs in Cook scheduler has been set to $prefixOfCookJobName")
    this
  }

  def getMaximumExecutorFailures: Int = maximumExecutorFailures

  def setMaximumExecutorFailures(maxExecutorFailures: Int): CookSchedulerConfiguration = {
    require(
      maxExecutorFailures > 0,
      "The maximum executor failures must be positive."
    )
    maximumExecutorFailures = maxExecutorFailures
    this
  }

  def getExecutorsToRequest(executorsRequested: Int = 0): Int =
    Math.max(maximumCores / coresPerCookJob - executorsRequested, 0)

  def getExecutorsToKil(executorsRequested: Int = 0): Int =
    Math.max(executorsRequested - maximumCores / coresPerCookJob, 0)

}

object CookSchedulerConfiguration {

  @volatile
  private[this] var configuration: CookSchedulerConfiguration = _

  private[spark] def conf(conf: SparkConf): CookSchedulerConfiguration = {
    if (configuration == null) {
      this.synchronized {
        if (configuration == null) {
          configuration = new CookSchedulerConfiguration(conf)
        }
      }
    }
    configuration
  }

  def conf(): CookSchedulerConfiguration = {
    require(configuration != null, "It haven't been initialized yet.")
    configuration
  }
}
