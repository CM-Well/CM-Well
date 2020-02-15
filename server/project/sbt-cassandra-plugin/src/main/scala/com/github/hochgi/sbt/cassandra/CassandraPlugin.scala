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


package com.github.hochgi.sbt.cassandra

import java.io._
import java.util.Properties

import org.yaml.snakeyaml.Yaml
import sbt.Keys._
import sbt._
import nl.gn0s1s.bump.SemVer

import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try

object CassandraPlugin extends AutoPlugin {
	
	//defaults:
	private[this] val defaultConfigDir = "NO_DIR_SUPPLIED"
	private[this] val defaultCliInit = "NO_CLI_COMMANDS_SUPPLIED"
	private[this] val defaultCqlInit = "NO_CQL_COMMANDS_SUPPLIED"

	object autoImport {
		val cassandraVersion = SettingKey[String]("cassandra-version")
		val cassandraConfigDir = SettingKey[String]("cassandra-config-dir")
		val cassandraCliInit = SettingKey[String]("cassandra-cli-init")
		val cassandraCqlInit = SettingKey[String]("cassandra-cql-init")
		val cassandraHost = SettingKey[String]("cassandra-host")
		val cassandraPort = SettingKey[String]("cassandra-port")
		val cassandraCqlPort = SettingKey[String]("cassandra-cql-port")
		val stopCassandraAfterTests = SettingKey[Boolean]("stop-cassandra-after-tests")
		val cleanCassandraAfterStop = SettingKey[Boolean]("stop-cassandra-after-tests")
		val configMappings = SettingKey[Seq[(String, java.lang.Object)]]("cassandra-conf", "used to override values in conf/cassandra.yaml. values are appropriate java objects")
		val cassandraJavaArgs = SettingKey[Seq[String]]("cassandra-java-args")
		val cassandraApplicationArgs = SettingKey[Seq[String]]("cassandra-application-args")

		val cassandraHome = TaskKey[File]("cassandra-home")
		val cassandraStartDeadline = TaskKey[Int]("cassandra-start-deadline")
		val deployCassandra = TaskKey[File]("deploy-cassandra")
		val startCassandra = TaskKey[String]("start-cassandra")
		val cassandraPid = TaskKey[String]("cassandra-pid")
		val stopCassandra = TaskKey[Unit]("stop-cassandra")
	}

	import autoImport._

	override def projectSettings = Seq(
    cassandraHost := "localhost",
    cassandraPort := "9160",
		configMappings := Seq(),
		configMappings ++= {
			val port = cassandraPort.value
			val targetDir = target.value
			val data = targetDir / "data"

			def d(s: String): String = (data / s).getAbsolutePath

			Seq(
				"rpc_port" -> port,
				"data_file_directories" -> {
					val l = new java.util.LinkedList[String]()
					l.add(d("data"))
					l
				},
				"commitlog_directory" -> d("commitlog"),
				"saved_caches_directory" -> d("saved_caches")
			)
		},
	  cassandraJavaArgs := Nil,
	  cassandraApplicationArgs := Nil,
		cassandraConfigDir := defaultConfigDir,
		cassandraCliInit := defaultCliInit,
		cassandraCqlInit := defaultCqlInit,
		stopCassandraAfterTests := true,
		cleanCassandraAfterStop := true,
        cassandraStartDeadline := 20,
		cassandraHome := {
			val ver = cassandraVersion.value
			val targetDir = target.value
			targetDir / s"apache-cassandra-${ver}"
		},
		cassandraVersion := "2.1.2",
		cassandraCqlPort := {
			val oldPort = cassandraHost.value
			val ver = cassandraVersion.value
				if(SemVer(ver) > SemVer("2.1.0")) "9042"
				else oldPort
		},
		classpathTypes ~=  (_ + "tar.gz"),
		libraryDependencies += {
			"org.apache.cassandra" % "apache-cassandra" % cassandraVersion.value artifacts(Artifact("apache-cassandra", "tar.gz", "tar.gz","bin")) intransitive()
		},
		deployCassandra := {
			val ver = cassandraVersion.value
			val targetDir = target.value
			val classpath = (dependencyClasspath in Runtime).value
			val logger = streams.value.log
			val cassandraTarGz = Attributed.data(classpath).find(_.getName == s"apache-cassandra-$ver-bin.tar.gz").get
			if (cassandraTarGz == null) sys.error("could not load: cassandra tar.gz file.")
			logger.info(s"cassandraTarGz: ${cassandraTarGz.getAbsolutePath}")
			Process(Seq("tar","-xzf",cassandraTarGz.getAbsolutePath),targetDir).!
			val cassHome = targetDir / s"apache-cassandra-${ver}"
			//old cassandra versions used log4j, newer versions use logback and are configurable through env vars
			val oldLogging = cassHome / "conf" / "log4j-server.properties"
			if(oldLogging.exists) {
				val in: FileInputStream = new FileInputStream(oldLogging)
				val props: Properties = new Properties
				props.load(in)
				in.close
				val out: FileOutputStream = new FileOutputStream(oldLogging)
				props.setProperty("log4j.appender.R.File", (targetDir / "data" / "logs").getAbsolutePath)
				props.store(out, null)
				out.close
			}
			cassHome
		},
		startCassandra := {
			//if compilation of test classes fails, cassandra should not be invoked. (moreover, Test.Cleanup won't execute to stop it...)
			(compile in Test).value
			val javaArgs = cassandraJavaArgs.value
			val appArgs = cassandraApplicationArgs.value
			val targetDir = target.value
			val cassHome = deployCassandra.value
			val confDirAsString = cassandraConfigDir.value
			val cli = cassandraCliInit.value
			val cql = cassandraCqlInit.value
			val host = cassandraHost.value
			val port = cassandraPort.value
			val cqlPort = cassandraCqlPort.value
			val startDeadline = cassandraStartDeadline.value
			val confMappings = configMappings.value
			val logger = streams.value.log

			val pidFile = targetDir / "cass.pid"
			val jarClasspath = sbt.IO.listFiles(cassHome / "lib").collect { case f: File if f.getName.endsWith(".jar") => f.getAbsolutePath }.mkString(":")
			val conf: String = {
				if (confDirAsString == defaultConfigDir) {
					val configDir = cassHome / "conf"
					configDir.getAbsolutePath
				} else confDirAsString
			}
			val classpath = conf + ":" + jarClasspath
			val bin = cassHome / "bin" / "cassandra"
			val args = Seq(bin.getAbsolutePath, "-p", pidFile.getAbsolutePath) ++ appArgs
			overrideConfigs(conf, confMappings, logger)
			if (!isCassandraRunning(port)) {
				Process(args, cassHome, "CASSANDRA_CONF" -> conf, "CASSANDRA_HOME" -> cassHome.getAbsolutePath, "JVM_OPTS" -> javaArgs.mkString(" ")).run
				logger.info("going to wait for cassandra:")
				waitForCassandra(port, startDeadline, (s: String) => logger.info(s))
				logger.info("going to initialize cassandra:")
				initCassandra(cli, cql, classpath, cassHome, host, port, cqlPort)
			} else {
				logger.warn("cassandra already running")
			}
			val pid = Try(sbt.IO.read(pidFile).filterNot(_.isWhitespace)).getOrElse("NO PID")
			cassandraPid := pid
			pid
		},
		cassandraPid := {
			val cassPid = target.value / "cass.pid"
			if(cassPid.exists) sbt.IO.read(cassPid).filterNot(_.isWhitespace)
			else "NO PID" // did you run start-cassandra task?
		},
		stopCassandra := {
			val pid = cassandraPid.value
			val clean = cleanCassandraAfterStop.value
			val targetDir = target.value
      stopCassandraMethod(clean, targetDir/ "data", pid)
    },
		//make sure to Stop cassandra when tests are done.
		testOptions in Test += {
			val pid = cassandraPid.value
			val stop = stopCassandraAfterTests.value
			val clean = cleanCassandraAfterStop.value
			val targetDir = target.value
			Tests.Cleanup(() => {
				if(stop) stopCassandraMethod(clean, targetDir / "data", pid)
			})
		}
	)

	def stopCassandraMethod(clean: Boolean, dataDir: File, pid: String) = if(pid != "NO PID") {
		s"kill $pid" !
		//give cassandra a chance to exit gracefully
		var counter = 40
		val never = Promise().future
		while((s"jps" !!).split("\n").exists(_ == s"$pid CassandraDaemon") && counter > 0) {
			try{
				Await.ready(never, 250 millis)
			} catch {
				case _ : Throwable => counter = counter - 1
			}
		}
		if(counter == 0) {
			//waited to long...
			s"kill -9 $pid" !
		}
		if(clean) sbt.IO.delete(dataDir)
	}

  def waitCassandraShutdown(pid: String) = {
    var counter = 40
    val never = Promise[Boolean].future
    while((s"jps" !!).split("\n").exists(_ == s"$pid CassandraDaemon") && counter > 0) {
      try{
        Await.ready(never, 250 millis)
      } catch {
        case _ : Throwable => counter = counter - 1
      }
    }
  }

  def isCassandraRunning(port: String): Boolean = {
    import org.apache.thrift.transport.{TFramedTransport, TSocket}
    val rpcAddress = "localhost"
    val rpcPort = port.toInt
    val tr = new TFramedTransport(new TSocket(rpcAddress, rpcPort))
    Try { tr.open }.isSuccess
  }

  def waitForCassandra(port: String, deadline: Int, infoPrintFunc: String => Unit): Unit = {
    import org.apache.thrift.transport.{TFramedTransport, TSocket, TTransport, TTransportException}

    import scala.concurrent.duration._

    val rpcAddress = "localhost"
    val rpcPort = port.toInt
    var retry = true
    val deadlineTime = deadline.seconds.fromNow
    while (retry && deadlineTime.hasTimeLeft) {
      val tr: TTransport = new TFramedTransport(new TSocket(rpcAddress, rpcPort))
      try {
        tr.open
        retry = false
      } catch {
        case e: TTransportException => {
          infoPrintFunc(s"waiting for cassandra to boot on port $rpcPort")
          Thread.sleep(500)
        }
      }
      if (tr.isOpen) {
        tr.close
      }
    }
  }

  def initCassandra(cli: String, cql: String, classpath: String, cassHome: File, host: String, port: String, cqlPort: String): Unit = {
    if(cli != defaultCliInit && cql != defaultCqlInit) {
      sys.error("use cli initiation commands, or cql initiation commands, but not both!")
    } else if(cli != defaultCliInit) {
      val bin = cassHome / "bin" / "cassandra-cli"
      val args = Seq(bin.getAbsolutePath, "-f", cli,"-h",host,"-p",port)
      Process(args,cassHome).!
    } else if(cql != defaultCqlInit) {
      val bin = cassHome / "bin" / "cqlsh"
      val cqlPath = new File(cql).getAbsolutePath
      val args = Seq(bin.getAbsolutePath, "-f", cqlPath,host,cqlPort)
      Process(args,cassHome).!
    }
  }

  def overrideConfigs(confDir: String, confMappings:  Seq[(String,java.lang.Object)], logger: Logger): Unit = {
    val cassandraYamlPath = s"$confDir/cassandra.yaml"
    val yaml = new Yaml
    val cassandraYamlMap = yaml.load(new FileInputStream(new File(cassandraYamlPath)))
                           .asInstanceOf[java.util.LinkedHashMap[String, java.lang.Object]]
		confMappings.foreach{
			case (prop,value) => {
				logger.info(s"setting configuration [$prop] with [$value]")
				cassandraYamlMap.put(prop, value)
			}
		}

		val it = cassandraYamlMap.entrySet().iterator()
		val m = new java.util.LinkedHashMap[String, java.lang.Object]()
		while(it.hasNext) {
			val cur = it.next()
			if(cur.getValue != null) {
				m.put(cur.getKey, cur.getValue)
			}
		}
		val ymlContent = yaml.dump(m)
		logger.debug(ymlContent)
		sbt.IO.write(file(cassandraYamlPath), ymlContent, java.nio.charset.StandardCharsets.UTF_8, false)
  }
}
