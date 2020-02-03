/**
  * © 2019 Refinitiv. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */


package com.github.israel.sbt.zookeeper


import java.io.{FileOutputStream, InputStream}

import sbt._
import sbt.Keys._
import cmwell.build.util._

/**
  * Created by Israel Klein on 28/12/2015.
  */
object SbtZookeeperPlugin extends sbt.AutoPlugin{

  object autoImport {
    lazy val helloTask = taskKey[Unit]("prints hello world")

    /** Settings **/
    lazy val zookeeperVersion = settingKey[String]("version of zookeeper")
    lazy val zookeeperServerConfig = settingKey[File]("zookeeper server configuration file")
    lazy val zookeeperServerRunDir = settingKey[File]("Run zookeeper server process from this directory. ")
    lazy val stopAfterTests = settingKey[Boolean]("Stop zookeeper server after tests finish")
    lazy val startBeforeTests = settingKey[Boolean]("Auto start zookeeper server before tests start")
    lazy val cleanAfterTests = settingKey[Boolean]("Clean data after tests finish")
    lazy val cleanBeforeTests = settingKey[Boolean]("Clean data before test starts")

    /** Tasks **/
    lazy val startZookeeper = taskKey[Unit]("start the zookeeper server")
    lazy val stopZookeeper = taskKey[Unit]("stop the zookeeper server")
    lazy val cleanZookeeper = taskKey[Unit]("clean zookeeper run dir")
    lazy val cleanZookeeperFunc = taskKey[()=>Unit]("return a function that clean zookeeper's run dir")
  }

  override def requires = cmwell.build.CMWellBuild

  lazy val Zookeeper = config("zk") extend(Compile) describedAs("Dependencies for using Zookeeper.")

  override def projectConfigurations = Seq(Zookeeper)

  import autoImport._

  var zookeeperProcess:java.lang.Process = null

  private def doCleanZookeeperBeforeTest = Def.taskDyn{
    streams.value.log.error("doCleanZookeeperBeforeTests")
    if(cleanBeforeTests.value)
      cleanZookeeper
    else
      Def.task{}
  }

  private def doCleanZookeeperAfterTest = Def.taskDyn{
    streams.value.log.error("doCleanZookeeperAfterTests")
    if(cleanAfterTests.value)
      cleanZookeeper
    else
      Def.task{}
  }

  private def doStartZookeeperBeforeTest = Def.taskDyn{
    streams.value.log.error("doStartZookeeperBeforeTests")
    if(startBeforeTests.value)
      startZookeeper
    else
      Def.task{}
  }

  private def doStopZookeeperAfterTest = Def.taskDyn{
    streams.value.log.error("doStopZookeeperBeforeTests")
    if(stopAfterTests.value)
      stopZookeeper
    else
      Def.task{}
  }

  private def isZookeeperRunning:Boolean = {
    val p = sys.runtime.exec("jps -l")
    val lines = scala.io.Source.fromInputStream(p.getInputStream).getLines()
    lines.exists(_.contains("org.apache.zookeeper.server.quorum.QuorumPeerMain"))
  }

  private def killZookeeper(force:Boolean = false)(implicit logger:Logger) = {
    val p = sys.runtime.exec("jps -l")
    val lines = scala.io.Source.fromInputStream(p.getInputStream).getLines()
    val pidOpt = lines.collectFirst({case s if (s.contains("org.apache.zookeeper.server.quorum.QuorumPeerMain")) => s.split(" ")(0)})
    pidOpt match {
      case Some(pid) =>
        val command = if(force)s"kill -9 $pid" else s"kill $pid"
        sys.runtime.exec(command)
      case None => logger.debug("requested to kill zookeeper process but none was found")
    }
  }

  override def projectSettings = Seq(


    /** Settings **/
    zookeeperVersion := "3.4.7",
    libraryDependencies += "org.apache.zookeeper" % "zookeeper" % zookeeperVersion.value % Zookeeper,
    zookeeperServerConfig := (resourceDirectory in Runtime).value / "zookeeper.server.cfg",
    zookeeperServerRunDir := {
      val f = target.value / "zookeeper-server"
      f.mkdir()
      f
    },
    stopAfterTests := true,
    startBeforeTests := true,
    cleanAfterTests := false,
    cleanBeforeTests := true,

    /** Tasks **/

    externalDependencyClasspath in Zookeeper := (externalDependencyClasspath or (externalDependencyClasspath in Runtime)).value,
    startZookeeper := {
      val logger = streams.value.log
        logger.info("preparing to start ZooKeeper")
      val depClasspath = (externalDependencyClasspath in Zookeeper).value
      if(isZookeeperRunning)
        logger.info("zookeeper is already running. doing nothing")
      else {
        val baseDir = zookeeperServerRunDir.value
        if (!baseDir.isDirectory)
          baseDir.mkdir()
        val classpath = Attributed.data(depClasspath)
        val serverConfigFile = zookeeperServerConfig.value
        if (!serverConfigFile.exists()) {
          val is: InputStream = resourceFromJarAsIStream("zookeeper.server.cfg")
          val fos = new FileOutputStream(serverConfigFile)
          IO.transferAndClose(is, fos)
          fos.close()
        }

        val configFile = serverConfigFile.absolutePath
        val cp = classpath.map(_.getAbsolutePath).mkString(":")
        val javaExec = System.getProperty("java.home") + "/bin/java"
        val mainClass = "org.apache.zookeeper.server.quorum.QuorumPeerMain"
        val pb = new java.lang.ProcessBuilder(javaExec, "-classpath", cp, mainClass, configFile).inheritIO()
        pb.directory(baseDir)
        zookeeperProcess = pb.start()
        // This is a temp solution for waiting for zookeeper to be ready
        Thread.sleep(10000)
        if(isZookeeperRunning)
          logger.info("successfuly started zookeeper process")
        else {
          logger.error("failed to start zookeeper process")
        }
      }
    },

    stopZookeeper := {
      implicit val logger = streams.value.log
      logger.info("preparing to stop zookeeper process")
      if(zookeeperProcess != null)
        zookeeperProcess.destroy()
      else
        killZookeeper()
      var triesLeft = 20
      while(isZookeeperRunning && triesLeft > 0) {
        logger.info("waiting 500ms for zookeeper process to finish...")
        Thread.sleep(500)
        triesLeft -= 1
      }
      if(triesLeft == 0) {
        logger.error("failed to stop zookeeper process nicely, using the heavy guns...")
        killZookeeper(true)
        logger.info("zookeeper process was forcefully killed (-9)")
      } else {
        logger.info("zookeeper process successfully stopped")
      }
      zookeeperProcess = null
    },

    cleanZookeeper := {
      cleanZookeeperFunc.value()
    },

    cleanZookeeperFunc := {
      () => {
        val dir = zookeeperServerRunDir.value
        IO.delete(dir)
      }
    }
  )

}
