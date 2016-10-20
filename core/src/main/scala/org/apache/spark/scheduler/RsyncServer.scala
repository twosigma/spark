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

import java.io.{File, PrintWriter}
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions

import org.apache.spark.Logging

import scala.concurrent.{Future, Promise}
import scala.sys.process.{BasicIO, Process, ProcessLogger}
import scala.util.Try

/**
  * Created by hkothari on 10/14/16.
  */
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
