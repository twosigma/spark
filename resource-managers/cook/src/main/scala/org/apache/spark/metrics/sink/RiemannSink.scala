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

package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.riemann.{Riemann, RiemannReporter}
import org.apache.spark.internal.Logging
import org.apache.spark.SecurityManager
import org.apache.spark.metrics.MetricsSystem

private[spark] class RiemannSink(val property: Properties, val registry: MetricRegistry,
    securityMgr: SecurityManager) extends Sink with Logging {
  val RIEMANN_DEFAULT_PORT = 5555
  val RIEMANN_DEFAULT_PERIOD = 10
  val RIEMANN_DEFAULT_UNIT = "SECONDS"

  val RIEMANN_KEY_HOST = "host"
  val RIEMANN_KEY_PORT = "port"
  val RIEMANN_KEY_PERIOD = "period"
  val RIEMANN_KEY_UNIT = "unit"

  val host = Option(property.getProperty(RIEMANN_KEY_HOST))

  val port = Option(property.getProperty(RIEMANN_KEY_PORT))
    .map(_.toInt)
    .getOrElse(RIEMANN_DEFAULT_PORT)

  val pollPeriod = Option(property.getProperty(RIEMANN_KEY_PERIOD))
    .map(_.toInt)
    .getOrElse(RIEMANN_DEFAULT_PERIOD)

  val pollUnit = TimeUnit.valueOf(Option(property.getProperty(RIEMANN_KEY_UNIT))
    .map(_.toUpperCase)
    .getOrElse(RIEMANN_DEFAULT_UNIT))

  val tags = property.stringPropertyNames()
    .filterNot(Seq(
      RIEMANN_KEY_HOST, RIEMANN_KEY_PORT, RIEMANN_KEY_PERIOD, RIEMANN_KEY_UNIT, "class")
      .contains(_))
    .map(key => (key, property.getProperty(key)))

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val riemann = host.map(new Riemann(_, port))

  val reporter = riemann match {
    case Some(r) => Try(RiemannReporter.forRegistry(registry)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .tags(tags.map { case (key, value) => s"$key=$value" })
        .build(r))
    case None => Failure(new IllegalArgumentException("host not provided"))
  }

  override def start() {
    reporter match {
      case Success(r) =>
        logInfo(s"starting: ${host.get}:$port")
        r.start(pollPeriod, pollUnit)
      case Failure(e) =>
        logWarning(s"not starting: $e")
    }
  }

  override def stop() {
    reporter match {
      case Success(r) =>
        logInfo(s"stopping: ${host.get}:$port")
        r.stop()
      case Failure(e) =>
        logWarning(s"not stopping: $e")
    }
    riemann.foreach(_.close())
  }

  override def report() {
    reporter match {
      case Success(r) =>
        logInfo(s"reporting: ${host.get}:$port")
        r.report()
      case Failure(e) =>
        logWarning(s"not reporting: $e")
    }
  }
}

