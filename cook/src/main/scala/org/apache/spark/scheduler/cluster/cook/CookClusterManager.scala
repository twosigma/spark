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

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}

/**
 * Cluster Manager for creation of Cook scheduler and backend
 */
private[spark] class CookClusterManager extends ExternalClusterManager {
  val COOK_REGEX = """cook://(.*):([0-9]+)""".r

  override def canCreate(masterURL: String): Boolean = {
    masterURL.startsWith("cook")
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskSchedulerImpl(sc)
  }

  override def createSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {

    // TODO: Investigate; Cook may not need this restriction.
    require(!sc.conf.get(IO_ENCRYPTION_ENABLED),
            "I/O encryption is currently not supported in Mesos (or Cook).")

    val cookUrl = COOK_REGEX.findFirstMatchIn(masterURL)
    val cookHostname = cookUrl.get.group(1)
    val cookPort = cookUrl.get.group(2).toInt
    new CoarseCookSchedulerBackend(
      scheduler.asInstanceOf[TaskSchedulerImpl],
      sc,
      cookHostname,
      cookPort)
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit =
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
}

