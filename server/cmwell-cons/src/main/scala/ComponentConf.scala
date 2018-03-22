/**
  * Copyright 2015 Thomson Reuters
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
import cmwell.ctrl.config.Jvms

import scala.collection.GenSeq

/**
  * Created by michael on 8/27/14.
  */
object JVMOptimizer {

  def gcLoggingJVM(filename: String, jPrefix: Boolean = false): Seq[String] = {
    val s = Seq(
      "-verbose:gc",
      "-XX:+PrintGCDetails",
      "-XX:+PrintGCDateStamps",
      "-XX:+PrintHeapAtGC",
      "-XX:+PrintTenuringDistribution",
      "-XX:+PrintGCApplicationConcurrentTime",
      "-XX:+PrintGCApplicationStoppedTime",
      "-XX:+PrintPromotionFailure",
      "-XX:PrintFLSStatistics=1",
      "-Xloggc:" + filename,
      "-XX:+UseGCLogFileRotation",
      "-XX:NumberOfGCLogFiles=9",
      "-XX:GCLogFileSize=10M"
    )
    s
  }
  lazy val globalJVM: Seq[String] = Seq("-server", "-XX:+UseTLAB")

  lazy val java7ExtraArgs: Seq[String] = globalJVM ++ Seq(
    "-XX:+UseCondCardMark",
    "-Duser.timezone=GMT0"
  )
  /* def machineSpecs(): (Int, Long, Long) = {
     import Props.os
     (os.getAvailableProcessors, os.getTotalPhysicalMemorySize, os.getFreePhysicalMemorySize)
   }*/
}

case class ConfFile(fileName: String,
                    content: String,
                    executable: Boolean = false,
                    path: Option[String] = None)

abstract class ComponentConf(var host: String,
                             var scriptDir: String,
                             var scriptName: String,
                             var confDir: String,
                             var confName: String,
                             var moduleIndex: Int) {
  val PATH = "$PATH"
  val BMSG = "bmsg; if [ $? -eq 1 ] ; then exit 13; fi"
  val CHKSTRT =
    s"if [ `ps aux | grep -v grep | grep -v starter | grep '${getPsIdentifier}' | wc -l` -gt 0 ] ; then exit 0; fi"
  //val BMSG = ""

  def createExportEnvStr(environmentVar: String) =
    sys.env.get(environmentVar).map(value => s"export $environmentVar='$value'")

  def genDebugStr(port: Int) =
    s"""if [ "$$CMWELL_DEBUG" = "true" ] ; then DEBUG_STR=`echo -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$port`; else DEBUG_STR=''; fi"""

  def getIndexTxt = {
    if (moduleIndex == 1) "" else s"$moduleIndex"
  }

  class IllegalFileException(msg: String) extends Exception(msg) {}

  /*def buildClasspathFromDirJars(dir: java.io.File): String = buildClasspathFromDirJars(dir, _ => true)

  def buildClasspathFromDirJars(dir: java.io.File, jarFilter: (String) => Boolean): String = {
    if(!dir.exists) throw new IllegalFileException("Dir " + dir.getAbsolutePath + " was not found.")
    else if(!dir.isDirectory) throw new IllegalFileException("File " + dir.getAbsolutePath + " is not a directory.")

    dir.getAbsolutePath + "/" + dir.list.filter(_.endsWith(".jar")).filter(jarFilter(_)).mkString(":" + dir.getAbsolutePath + "/")
  }*/

  def sleepScript = "sleep ${1:-0}"

  def mkScript: ConfFile

//  def templateToFile(src: String, valueMap: Map[String, String]): String = {
//    val st = scala.io.Source.fromFile(src,"UTF-8").mkString
//    replaceTemplates(st, valueMap)
//  }

//  def replaceTemplates(text: String, templates: Map[String, String]): String =
//    """\{\{([^{}]*)\}\}""".r replaceSomeIn ( text,  { case scala.util.matching.Regex.Groups(name) => templates get name } )

  def mkConfig: List[ConfFile]

  def getPsIdentifier: String
}

case class CassandraConf(home: String,
                         seeds: String,
                         clusterName: String,
                         resourceManager: JvmMemoryAllocations,
                         snitchType: String,
                         ccl_dir: String,
                         dir: String = "cas",
                         rowCacheSize: Int,
                         replicationFactor: Int = 3,
                         template: String = "cassandra.yaml",
                         listenAddress: String = "127.0.0.1",
                         rpcAddress: String = "127.0.0.1",
                         sName: String,
                         index: Int,
                         rs: RackSelector,
                         g1: Boolean,
                         hostIp: String)
    extends ComponentConf(
      hostIp,
      s"$home/app/cas/cur",
      sName,
      s"$home/conf/$dir",
      "cassandra.yaml",
      index
    ) {
  override def getPsIdentifier = s"/log/cas${getIndexTxt}/"

  override def mkScript: ConfFile = {
    //import scala.math._

    def getJarAbsPath(dir: java.io.File, prefix: String): String = {
      if (!dir.exists)
        throw new IllegalFileException(
          "Dir " + dir.getAbsolutePath + " was not found."
        )
      else if (!dir.isDirectory)
        throw new IllegalFileException(
          "File " + dir.getAbsolutePath + " is not a directory."
        )

      val files = dir.list.filter(f => f.startsWith(prefix))

      if (files.length > 1)
        throw new Exception("multiple jars were found: " + files.mkString(", "))
      else if (files.length == 0)
        throw new Exception("no jars were found (prefix = " + prefix + ").")
      else dir.getAbsolutePath + "/" + files.head
    }
    val pidFile = s"$home/app/cas/cur/cassandra.pid"
    val mXmx = resourceManager.getMxmx
    //val mXms = "-Xms" + heapSize + "M"
    val mXms = resourceManager.getMxms

    //Young gen: min(max_sensible_per_modern_cpu_core * num_cores, 1/4 * heap size)
    val mXmn = resourceManager.getMxmn

    // reduce the per-thread stack size to minimize the impact of Thrift
    // thread-per-client.  (Best practice is for client connections to
    // be pooled anyway.) Only do so on Linux where it is known to be
    // supported.
    val mXss = resourceManager.getMxss

    /*lazy val jamm = {
      val f = new java.io.File(home+"/app/cas/cur/lib")
      val arr = f.listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = name.startsWith("jamm")
      })
      require(arr.size == 1)
      Seq(s"-javaagent:${arr.head.getAbsolutePath}")
    }*/

    val jamm = Seq(s"-javaagent:$home/app/cas/cur/lib/$$JAMMJAR")

    /*val jvmArgs = JVMOptimizer.java7ExtraArgs ++ Seq("-ea") ++ jamm ++ Seq(
      "-XX:+UseThreadPriorities",
      "-XX:ThreadPriorityPolicy=42",
      mXmx,
      mXms,
      mXmn,
      "-XX:+HeapDumpOnOutOfMemoryError",
      mXss,
      "-XX:+UseG1GC",
      "-XX:SurvivorRatio=8",
      "-XX:MaxTenuringThreshold=1",
      "-Djava.net.preferIPv4Stack=true",
      "-Dcom.sun.management.jmxremote.port=" + jmxremotePort,
      "-Dcom.sun.management.jmxremote.ssl=false",
      "-Dcom.sun.management.jmxremote.authenticate=false",
      "-Dlog4j.configuration=log4j-server.properties",
      "-Dlog4j.defaultInitOverride=true",
      s"-Dlog4j.appender.R.File=$home/log/$dir/system.log",
      "-Dmx4jaddress=0.0.0.0",
      "-Dmx4jport=" + dmx4jport,
      "-Dcassandra-foreground=yes",
      s"-Dcassandra-pidfile=$pidFile")*/
    //"-Dcassandra.config=file://${path}/conf/cas/cassandra.yaml")

    val cmsGc = Seq(
      "-XX:+HeapDumpOnOutOfMemoryError",
      "-XX:+UseParNewGC",
      "-XX:+UseConcMarkSweepGC",
      "-XX:+CMSParallelRemarkEnabled",
      "-XX:SurvivorRatio=8",
      "-XX:MaxTenuringThreshold=1",
      "-XX:CMSInitiatingOccupancyFraction=75",
      "-XX:+UseCMSInitiatingOccupancyOnly"
    )

    val g1Gc = Seq("-XX:+UseG1GC", "-XX:SurvivorRatio=8")

    val jvmArgs = JVMOptimizer.java7ExtraArgs ++ Seq("-ea") ++ jamm ++ Seq(
      "-XX:+UseThreadPriorities",
      "-XX:ThreadPriorityPolicy=42",
      mXmx,
      mXms,
      mXmn,
      mXss
    ) ++
      (if (g1) g1Gc else cmsGc) ++
      Seq(
        "-Djava.net.preferIPv4Stack=true",
        "-Dcom.sun.management.jmxremote.port=" + PortManagers.cas.jmxPortManager
          .getPort(index),
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dlog4j.configuration=log4j-server.properties",
        "-Dlog4j.defaultInitOverride=true",
        "-Dmx4jaddress=0.0.0.0",
        "-Dmx4jport=" + PortManagers.cas.dmx4jPortManager.getPort(index),
        "-Dcassandra-foreground=yes",
        /*"-Dconsistent.rangemovement=false",*/
        s"-Dcassandra-pidfile=$pidFile",
        s"-Dcassandra.logdir=$home/log/$dir/"
      )
    def classpath =
      s"$home/conf/" + dir + ":" +
        home + "/build/classes/main:" +
        home + "/build/classes/thrift:" +
        s"$home/app/cas/cur/lib/*"

    val agentLibArgs = Seq.empty //Seq(s"-javaagent:$home/app/ctrl/cur", s"-Dctrl.listenAddress=$listenAddress", s"-Dctrl.seedNodes=${host}", s"-Dctrl.clusterName=$clusterName", s"-Dctrl.roles=Metrics,CassandraNode")

    val args = Seq("starter", "java") ++ agentLibArgs ++ jvmArgs ++ JVMOptimizer
      .gcLoggingJVM(s"$home/log/" + dir + "/gc.log") ++ Seq(
      "-cp",
      classpath,
      "org.apache.cassandra.service.CassandraDaemon"
    )

    val scriptString =
      s"""export PATH=$home/app/java/bin:$home/bin/utils:$PATH
       |export CASSANDRA_HOME=$home/app/cas/cur
       |export CASSANDRA_CONF=$home/conf/${dir}
       |export JAMMJAR=`ls -1 $home/app/cas/cur/lib/ | grep jamm`
       |$CHKSTRT
       |$BMSG
       |${args.mkString(" ")} > $home/log/$dir/stdout.log 2> $home/log/$dir/stderr.log &""".stripMargin

    ConfFile(sName, scriptString, true)
  }

  override def mkConfig: List[ConfFile] = {
    val confContent = ResourceBuilder.getResource(
      s"scripts/templates/${template}",
      Map(
        "clustername" -> clusterName,
        "seeds" -> seeds,
        "listen_address" -> listenAddress,
        "rpc_address" -> rpcAddress,
        "ccl_dir" -> ccl_dir,
        "dir" -> dir,
        "root_dir" -> home,
        "endpoint_snitch" -> snitchType,
        "row_cache_size" -> rowCacheSize.toString
      )
    )

//    val log4jContent = templateToFile(s"scripts/templates/log4j-server.properties",
//      Map("file_path" -> s"$home/log/$dir/system.log"))

    val logBackContent =
      ResourceBuilder.getResource(
        s"scripts/templates/logback-cassandra.xml",
        Map.empty
      )

    val rackConfContent = ResourceBuilder.getResource(
      "scripts/templates/cassandra-rackdc.properties",
      Map("rack_id" -> rs.getRackId(this))
    )

    val cqlInit = ResourceBuilder.getResource(
      "scripts/templates/cassandra-cql-init-cluster",
      Map("replication_factor" -> replicationFactor.toString)
    )

    val cqlInit2 = ResourceBuilder.getResource(
      "scripts/templates/cassandra-cql-init-cluster-new",
      Map("replication_factor" -> replicationFactor.toString)
    )

    val cqlInit3 = ResourceBuilder.getResource(
      "scripts/templates/zstore-cql-init-cluster",
      Map("replication_factor" -> replicationFactor.toString)
    )

    val cassandraStatus = ResourceBuilder.getResource(
      "scripts/templates/cassandra-status-viewer-template",
      Map("home" -> home, "host" -> hostIp)
    )

    List(
      ConfFile("cassandra.yaml", confContent, false),
      ConfFile("logback.xml", logBackContent),
      ConfFile("cassandra-rackdc.properties", rackConfContent, false),
      ConfFile("cassandra-status-viewer", cassandraStatus, true),
      ConfFile("cassandra-cql-init-cluster", cqlInit),
      ConfFile("cassandra-cql-init-cluster-new", cqlInit2),
      ConfFile("zstore-cql-init-cluster", cqlInit3)
    )
  }
}

case class ElasticsearchConf(clusterName: String,
                             nodeName: String,
                             dataNode: Boolean,
                             masterNode: Boolean,
                             expectedNodes: Int,
                             numberOfReplicas: Int,
                             seeds: String,
                             seedPort: Int = 9300,
                             home: String,
                             resourceManager: JvmMemoryAllocations,
                             dir: String = "es",
                             template: String = "es.yml",
                             listenAddress: String = "127.0.0.1",
                             masterNodes: Int,
                             sName: String,
                             index: Int,
                             rs: RackSelector,
                             autoCreateIndex: Boolean,
                             g1: Boolean,
                             hostIp: String)
    extends ComponentConf(
      hostIp,
      s"$home/app/es/cur",
      sName,
      s"$home/conf/$dir",
      "es.yml",
      index
    ) {
  val classpath = s"""'$home/app/es/cur/lib/*:$home/app/es/cur/lib/sigar/*'"""

  override def getPsIdentifier = {
    if (dir == "es-master")
      s"/log/es-master/"
    else
      s"/log/es${getIndexTxt}/"
  }
  override def mkScript: ConfFile = {
    /*def jvmArgs = {
      import scala.math._
      val mXmx = resourceManager.getMxmx
      val mXms = resourceManager.getMxms
      val mXmn = resourceManager.getMxmn
      val mXss = resourceManager.getMxss
      Seq("-XX:+UseCondCardMark",
        "-Duser.timezone=GMT0",
        mXmx,
        mXms,
        mXmn,
        mXss,
        "-Djava.awt.headless=true",
        "-XX:+UseG1GC",
        "-XX:+HeapDumpOnOutOfMemoryError",
        s"-Dcom.sun.management.jmxremote.port=${jmxremotePort}",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Delasticsearch",
        s"-Des.path.home=$home/app/es/cur",
        s"-Des.config=$home/conf/${dir}/es.yml")
    }*/
    val agentLibArgs = Seq.empty //Seq(s"-javaagent:$home/app/ctrl/cur", s"-Dctrl.listenAddress=$listenAddress", s"-Dctrl.seedNodes=${host}", s"-Dctrl.clusterName=$clusterName", s"-Dctrl.roles=Metrics,ElasticsearchNode")
    val cmsGc = Seq(
      "-XX:+UseCondCardMark",
      "-XX:+UseParNewGC",
      "-XX:+UseConcMarkSweepGC",
      "-XX:CMSInitiatingOccupancyFraction=75",
      "-XX:+UseCMSInitiatingOccupancyOnly",
      "-XX:+HeapDumpOnOutOfMemoryError"
    )

    val g1Gc = Seq("-XX:+UseG1GC", "-XX:SurvivorRatio=8")

    def jvmArgs = {
      val mXmx = resourceManager.getMxmx
      val mXms = resourceManager.getMxms
      val mXmn = resourceManager.getMxmn
      val mXss = resourceManager.getMxss
      Seq(
        "-Duser.timezone=GMT0",
        mXmx,
        mXms,
        mXmn,
        mXss,
        "-Djava.awt.headless=true"
      ) ++
        (if (g1) g1Gc else cmsGc) ++
        Seq(
          s"-Dcom.sun.management.jmxremote.port=${PortManagers.es.jmxPortManager
            .getPort(index)}",
          "-Dcom.sun.management.jmxremote.ssl=false",
          "-Dcom.sun.management.jmxremote.authenticate=false",
          "-Delasticsearch",
          s"-Des.path.home=$home/app/es/cur",
          s"-Des.config=$home/conf/${dir}/es.yml"
        )
    }

    val args = Seq("starter", "java") ++ agentLibArgs ++ jvmArgs ++ JVMOptimizer
      .gcLoggingJVM(s"$home/log/${dir}/gc.log") ++ Seq(
      "-cp",
      classpath,
      "org.elasticsearch.bootstrap.Elasticsearch"
    )

    val scriptString =
      s"""export PATH=$home/app/java/bin:$home/bin/utils:$PATH
         |export ES_HOME=$home/app/es/cur
         |$CHKSTRT
         |$BMSG
         |${args.mkString(" ")} > $home/log/$dir/stdout.log 2> $home/log/$dir/stderr.log &""".stripMargin

    ConfFile(sName, scriptString, true)
  }

  override def mkConfig: List[ConfFile] = {
    val httpHost = if (masterNode && !dataNode) s"http.host: $host" else ""
    val httpPort =
      if (masterNode && !dataNode) 9200
      else PortManagers.es.httpPortManager.getPort(index)
    val transportPort =
      if (masterNode && !dataNode) 9300
      else PortManagers.es.transportPortManager.getPort(index)

    val m = Map[String, String](
      "clustername" -> clusterName,
      "nodename" -> listenAddress,
      "node-master" -> masterNode.toString,
      "node-data" -> dataNode.toString,
      "recoverafternodes" -> {
        if (expectedNodes > 3) expectedNodes - 2 else expectedNodes - 1
      }.toString,
      "expectednodes" -> expectedNodes.toString,
      "hosts" -> seeds
        .split(',')
        .mkString("", s":$seedPort,", s":$seedPort"),
      "dir" -> dir,
      "listen_address" -> listenAddress,
      "root_dir" -> home,
      "rack_id" -> rs.getRackId(this),
      "http_host" -> httpHost,
      "min_masters" -> (Math.round(masterNodes / 2) + 1).toString,
      "http_port" -> httpPort.toString,
      "transport_port" -> transportPort.toString,
      "num_of_shards" -> expectedNodes.toString,
      "num_of_replicas" -> { if (expectedNodes > 2) 2 else 0 }.toString
    )

    val confContent =
      ResourceBuilder.getResource(s"scripts/templates/${template}", m)

    val m2 = Map[String, String](
      "number_of_shards" -> expectedNodes.toString,
      "number_of_replicas" -> numberOfReplicas.toString
    )
    val mappingContent =
      ResourceBuilder.getResource(s"scripts/templates/mapping.json", m2)
    val mappingContentNew =
      ResourceBuilder.getResource(
        s"scripts/templates/indices_template_new.json",
        m2
      )

    val loggerConf =
      ResourceBuilder.getResource(
        "scripts/templates/es-logger.yml",
        Map.empty[String, String]
      )

    List(
      ConfFile("es.yml", confContent, false),
      ConfFile("mapping.json", mappingContent, false),
      ConfFile("indices_template_new.json", mappingContentNew, false),
      ConfFile(
        "logging.yml",
        loggerConf,
        false,
        Some(s"$home/app/es/cur/config")
      )
    )
  }
}

case class KafkaConf(home: String,
                     logDirs: Seq[String],
                     zookeeperServers: Seq[String],
                     brokerId: Int,
                     hostIp: String)
    extends ComponentConf(
      hostIp,
      s"$home/app/kafka",
      "start.sh",
      s"$home/conf/kafka",
      "server.properties",
      1
    ) {
  override def mkScript: ConfFile = {
    val dir = "kafka"
    val exports = s"export PATH=$home/app/java/bin:$home/bin/utils:$PATH"
    val cp = ":cur/libs/*"
    val scriptString =
      s"""
         |$exports
          |$CHKSTRT
          |$BMSG
          |starter java -Xmx1G -Xms1G -server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true -Xloggc:$home/log/$dir/kafkaServer-gc.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -Dcom.sun.management.jmxremote.port=${PortManagers.kafka.jmxPortManager
           .getPort(1)} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dkafka.logs.dir=$home/log/$dir -Dlog4j.configuration=file:$home/conf/$dir/log4j.properties -cp $cp kafka.Kafka $home/conf/$dir/server.properties  > $home/log/$dir/stdout.log 2>  $home/log/$dir/stderr.log &
     """.stripMargin
    ConfFile("start.sh", scriptString, true)
  }

  override def getPsIdentifier: String = "kafka.Kafka"

  override def mkConfig: List[ConfFile] = {
    val m = Map[String, String](
      "broker-id" -> brokerId.toString,
      "log-dirs" -> logDirs.mkString(","),
      "zookeeper-connect" -> zookeeperServers
        .map(zkServer => s"$zkServer:2181")
        .mkString(",")
    )

    val confContent = ResourceBuilder.getResource(
      s"scripts/templates/kafka.server.properties",
      m
    )

    val loggerConf =
      ResourceBuilder.getResource(
        "scripts/templates/log4j-kafka.properties",
        Map[String, String]()
      )

    List(
      ConfFile("server.properties", confContent, false),
      ConfFile("log4j.properties", loggerConf, false)
    )
  }
}

case class ZookeeperConf(home: String,
                         clusterName: String,
                         servers: Seq[String],
                         hostIp: String)
    extends ComponentConf(
      hostIp,
      s"$home/app/zookeeper",
      "start.sh",
      s"$home/conf/zookeeper",
      "zoo.conf",
      1
    ) {
  val dir = "zookeeper"
  private def genServersStr: String = {
    var serverId = 0
    servers
      .map { server =>
        serverId += 1
        s"server.$serverId=$server:2888:3888"
      }
      .mkString("\n")
  }

  override def mkScript: ConfFile = {
    val exports = s"export PATH=$home/app/java/bin:$home/bin/utils:$PATH"
    val cp =
      s"cur/lib/slf4j-log4j12-1.6.1.jar:cur/lib/slf4j-api-1.6.1.jar:cur/lib/netty-3.7.0.Final.jar:cur/lib/log4j-1.2.16.jar:cur/lib/jline-0.9.94.jar:cur/zookeeper-${cmwell.util.build.BuildInfo.zookeeperVersion}.jar:$home/conf/$dir"
    val scriptString =
      s"""
         |$exports
          |$CHKSTRT
          |$BMSG
          |starter java -Xmx300m -Xms300m -XX:+UseG1GC -cp $cp -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false org.apache.zookeeper.server.quorum.QuorumPeerMain $home/conf/$dir/zoo.cfg > $home/log/$dir/stdout.log 2>  $home/log/$dir/stderr.log &
      """.stripMargin
    ConfFile("start.sh", scriptString, true)
  }

  override def getPsIdentifier: String = "QuorumPeerMain"

  override def mkConfig: List[ConfFile] = {
    val m = Map[String, String](
      "dataDir" -> s"$home/data/$dir",
      "servers" -> genServersStr
    )

    val confContent =
      ResourceBuilder.getResource(s"scripts/templates/zoo.cfg", m)
    val myId = (servers.indexOf(hostIp) + 1).toString

    val loggerMap = Map[String, String]("zookeeperLogDir" -> s"$home/log/$dir")

    val loggerConf =
      ResourceBuilder.getResource(
        "scripts/templates/log4j-zookeeper.properties",
        loggerMap
      )

    List(
      ConfFile("zoo.cfg", confContent, false),
      ConfFile("log4j.properties", loggerConf, false),
      ConfFile("myid", myId, false, Some(s"$home/data/$dir"))
    )
  }
}

case class BgConf(home: String,
                  zookeeperServers: Seq[String],
                  clusterName: String,
                  dataCenter: String,
                  hostName: String,
                  resourceManager: JvmMemoryAllocations,
                  sName: String,
                  isMaster: Boolean,
                  minMembers: Int = 1,
                  partition: Int = 0,
                  numOfPartitions: Int = 1,
                  logLevel: String,
                  debug: Boolean,
                  hostIp: String,
                  seeds: String,
                  seedPort: Int = 9301,
                  dir: String = "bg")
    extends ComponentConf(
      hostIp,
      s"$home/app/bg",
      sName,
      s"$home/conf/bg",
      "bg.yml",
      1
    ) {
  override def mkScript: ConfFile = {
    def jvmArgs = {
      val aspectj =
        if (/*hasOption("useAspectj")*/ false)
          s"$home/app/tools/aspectjweaver.jar"
        else ""
      val mXmx = resourceManager.getMxmx
      val mXms = resourceManager.getMxms
      val mXmn = resourceManager.getMxmn
      val mXss = resourceManager.getMxss
      val jmx = Seq(
        s"-Dcom.sun.management.jmxremote.port=${PortManagers.bg.jmxPortManager.getPort(1)}",
        "-XX:-OmitStackTraceInFastThrow",
        "-XX:+UseG1GC",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.authenticate=false"
      )
      Seq(
        "-XX:+UseCondCardMark",
        "-Duser.timezone=GMT0",
        "-XX:+HeapDumpOnOutOfMemoryError",
        aspectj,
        "-Dfile.encoding=UTF-8",
        s"-Dlog.level=$logLevel",
        mXmx,
        mXms,
        mXmn,
        mXss
      ) ++ jmx ++ JVMOptimizer.gcLoggingJVM(s"$home/log/bg/gc.log")
    }

    val args = Seq("starter", "java", "$DEBUG_STR", s"-Dcmwell.home=$home") ++ jvmArgs ++ Seq(
      "-cp",
      s""" "conf:$home/app/bg/lib/*" """,
      "cmwell.bg.Runner"
    )

    //new java.io.File(s"$home/log/bg").mkdirs

    val scriptString =
      s"""export PATH=$home/app/java/bin:$home/bin/utils:$PATH
          |$CHKSTRT
          |$BMSG
          |${genDebugStr(5014)}
          |${args.mkString(" ")} > $home/log/bg/stdout.log 2> $home/log/bg/stderr.log &""".stripMargin

    ConfFile("start.sh", scriptString, true)
  }

  override def getPsIdentifier: String = "/log/bg/"

  override def mkConfig: List[ConfFile] = {
    val applicationConfMap = Map[String, String](
      "cmwell.grid.dmap.persistence.data-dir" -> s"$home/log/bg/dmap/",
      "cmwell.grid.bind.host" -> s"$hostIp",
      "cmwell.grid.bind.port" -> s"${Jvms.BG.systemPort}",
      "cmwell.grid.seeds" -> s"$hostIp:7777",
      "cmwell.grid.min-members" -> s"$minMembers",
      "cmwell.grid.monitor.port" -> s"${PortManagers.bg.monitorPortManager.getPort(1)}",
      "cmwell.clusterName" -> s"$clusterName",
      "dataCenter.id" -> s"$dataCenter",
      "cmwell.dataCenter.id" -> s"$dataCenter",
      "cmwell.kafka.numOfPartitions" -> s"$numOfPartitions",
      "cmwell.kafka.zkServers" -> s"${zookeeperServers.map(zkServer => s"$zkServer:2181").mkString(",")}",
      "cmwell.bg.persist.commands.partition" -> s"$partition",
      "cmwell.bg.index.commands.partition" -> s"$partition",
      "irwServiceDao.clusterName" -> s"$clusterName",
      "irwServiceDao.hostName" -> s"$hostName",
      "ftsService.clusterName" -> s"$clusterName",
      "ftsService.transportAddress" -> s"$hostName"
    )
    val m = Map[String, String](
      "clustername" -> clusterName,
      "hosts" -> seeds.split(',').mkString("", s":$seedPort,", s":$seedPort"),
      "dir" -> dir,
      "root_dir" -> home
    )
    val logbackConf =
      ResourceBuilder.getResource("conf/bg/logback.xml", Map[String, String]())
    val confContent =
      ResourceBuilder.getResource(s"scripts/templates/es.node.client.yml", m)
    val applicationConfConf =
      ResourceBuilder.getResource(
        "conf/bg/application.conf",
        applicationConfMap
      )

    List(
      ConfFile("logback.xml", logbackConf, false),
      ConfFile("bg.es.yml", confContent, false),
      ConfFile("application.conf", applicationConfConf, false)
    )
  }
}

case class CwConf(home: String,
                  clusterName: String,
                  dataCenter: String,
                  hostName: String,
                  resourceManager: JvmMemoryAllocations,
                  sName: String,
                  minMembers: Int = 1,
                  logLevel: String,
                  debug: Boolean,
                  hostIp: String,
                  seeds: String,
                  seedPort: Int)
    extends ComponentConf(
      hostIp,
      s"$home/app/ws",
      sName,
      s"$home/conf/cw",
      "ws.yml",
      1
    ) {
  override def mkScript: ConfFile = {
    {
      val mXmx = resourceManager.getMxmx
      val mXms = resourceManager.getMxms
      val mXmn = resourceManager.getMxmn
      val mXss = resourceManager.getMxss
      val args = Seq("starter", "java", "$DEBUG_STR", mXmx, mXms, mXmn, mXss) ++
        Seq(
          "-XX:+UseG1GC",
          s"-Dcmwell.home=$home",
          s"-Dlog.level=$logLevel",
          "-XX:ReservedCodeCacheSize=128m",
          "-Dfile.encoding=UTF-8",
          s"-Dcom.sun.management.jmxremote.port=${PortManagers.ws.jmxPortManager
            .getPort(2)}",
          "-Dcom.sun.management.jmxremote.ssl=false",
          "-Dcom.sun.management.jmxremote.authenticate=false",
          "-Duser.timezone=GMT0"
        ) ++ JVMOptimizer.gcLoggingJVM(s"$home/log/ws/gc.log") ++
        Seq(
          "-cp",
          s""" "cw-conf:$home/app/ws/lib/*" """,
          "cmwell.crashableworker.WorkerMain"
        )

      val scriptString =
        s"""export PATH=$home/app/java/bin:$home/bin/utils:$PATH
       |$CHKSTRT
       |$BMSG
       |${genDebugStr(5012)}
       |${args.mkString(" ")} > $home/log/cw/stdout.log 2> $home/log/cw/stderr.log &""".stripMargin

      ConfFile(sName, scriptString, true, Some(s"$home/conf/cw"))
    }
  }

  override def getPsIdentifier: String = s"crashableworker"

  override def mkConfig: List[ConfFile] = {
    val applicationConfMap = Map[String, String](
      "cmwell.grid.dmap.persistence.data-dir" -> s"$home/log/ws/dmap-cw",
      "cmwell.grid.bind.host" -> s"$hostIp",
      "cmwell.grid.bind.port" -> s"${Jvms.CW.systemPort}",
      "cmwell.grid.seeds" -> s"$hostIp:7777",
      "cmwell.grid.min-members" -> s"$minMembers",
      "cmwell.grid.monitor.port" -> s"${PortManagers.cw.monitorPortManager.getPort(1)}",
      "cmwell.clusterName" -> s"$clusterName",
      "dataCenter.id" -> s"$dataCenter",
      "ftsService.clusterName" -> s"$clusterName",
      "cmwell.home" -> s"$home",
      "irwServiceDao.hostName" -> s"$hostName",
      "ftsService.transportAddress" -> s"$hostName",
      "ftsService.defaultPartitionNew" -> s"cm_well",
      "quads.cache.size" -> s"1000",
      "quads.globalOperations.results.maxLength" -> s"10000",
      "crashableworker.results.maxLength" -> s"1400000",
      "arq.extensions.embedLimit" -> s"10000",
      "crashableworker.results.baseFileName" -> s"tmpSpResults"
    )

    val m = Map[String, String](
      "clustername" -> clusterName,
      "hosts" -> seeds.split(',').mkString("", s":$seedPort,", s":$seedPort"),
      "dir" -> "cw",
      "root_dir" -> home
    )

    val confContent =
      ResourceBuilder.getResource(s"scripts/templates/es.node.client.yml", m)
    val logbackConf = ResourceBuilder.getResource(
      "conf/ws/cw-logback.xml",
      Map[String, String]()
    )
    val applicationConfConf =
      ResourceBuilder.getResource(
        "conf/ws/cw-application.conf",
        applicationConfMap
      )

    List(
      ConfFile("ws.es.yml", confContent, false),
      ConfFile("logback.xml", logbackConf, false),
      ConfFile("application.conf", applicationConfConf, false)
    )
  }
}

case class WebConf(home: String,
                   zookeeperServers: Seq[String],
                   clusterName: String,
                   dataCenter: String,
                   hostName: String,
                   resourceManager: JvmMemoryAllocations,
                   sName: String,
                   minMembers: Int = 1,
                   useAuthorization: Boolean,
                   numOfPartitions: Int = 1,
                   logLevel: String,
                   debug: Boolean,
                   hostIp: String,
                   seeds: String,
                   seedPort: Int)
    extends ComponentConf(
      hostIp,
      s"$home/app/ws",
      sName,
      s"$home/conf/ws",
      "ws.yml",
      1
    ) {
  def genMemStr(mem: String): String = {
    if (!mem.isEmpty) s"-J$mem" else mem
  }

  override def getPsIdentifier = s"Webserver"
  override def mkScript: ConfFile = {
    val auth = if (useAuthorization) "-Duse.authorization=true" else ""
    val mXmx = resourceManager.getMxmx
    val mXms = resourceManager.getMxms
    val mXmn = resourceManager.getMxmn
    val mXss = resourceManager.getMxss

    // todo: fix debug string.
    val args = Seq("starter", "java", "$DEBUG_STR", mXmx, mXms, mXmn, mXss) ++
      Seq(
        s"-DpsId=$getPsIdentifier",
        "-XX:+UseG1GC",
        "-XX:ReservedCodeCacheSize=128m",
        auth,
        s"-Dcom.sun.management.jmxremote.port=${PortManagers.ws.jmxPortManager.getPort(1)}",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        s"-Dcmwell.home=$home",
        s"-Dlog.level=$logLevel",
        "-Dfile.encoding=UTF-8",
        "-Duser.timezone=GMT0"
      ) ++
      JVMOptimizer.gcLoggingJVM(s"$home/log/ws/gc.log", true) ++ Seq(
      "-cp",
      s""" "conf:$home/app/ws/lib/*" """,
      "play.core.server.ProdServerStart"
    )

    val scriptString =
      s"""export PATH=$home/app/java/bin:$home/bin/utils:$PATH
       |export HOST_NAME=${cmwell.util.os.Props.machineName}
       |${createExportEnvStr("PLAY_CRYPTO_SECRET").getOrElse("")}
       |${createExportEnvStr("PLAY_CRYPTO_SECRET2").getOrElse("")}
       |$CHKSTRT
       |$BMSG
       |${genDebugStr(5010)}
       |${args.mkString(" ")} > $home/log/ws/stdout.log 2> $home/log/ws/stderr.log &""".stripMargin

    ConfFile("start.sh", scriptString, true)
  }

  override def mkConfig: List[ConfFile] = {

    val applicationConfMap = Map[String, String](
      "http.port" -> s"${PortManagers.ws.playHttpPortManager.getPort(1)}",
      "kafka.numOfPartitions" -> s"$numOfPartitions",
      "cmwell.grid.dmap.persistence.data-dir" -> s"$home/log/ws/dmap-ws",
      "cmwell.grid.bind.host" -> s"$hostIp",
      "cmwell.grid.bind.port" -> s"${Jvms.WS.systemPort}",
      "cmwell.grid.seeds" -> s"$hostIp:7777",
      "cmwell.grid.min-members" -> s"$minMembers",
      "cmwell.grid.monitor.port" -> s"${PortManagers.ws.monitorPortManager.getPort(1)}",
      "cmwell.clusterName" -> s"$clusterName",
      "dataCenter.id" -> s"$dataCenter",
      "kafka.zkServers" -> s"${zookeeperServers.map(zkServer => s"$zkServer:2181").mkString(",")}",
      "kafka.url" -> s"localhost:9092,${zookeeperServers.map(kafkaNode => s"$kafkaNode:9092").mkString(",")}",
      "ftsService.clusterName" -> s"$clusterName",
      "cmwell.home" -> s"$home",
      "irwServiceDao.hostName" -> s"$hostName",
      "ftsService.transportAddress" -> s"$hostName"
    )

    val m = Map[String, String](
      "clustername" -> clusterName,
      "hosts" -> seeds.split(',').mkString("", s":$seedPort,", s":$seedPort"),
      "dir" -> "ws",
      "root_dir" -> home
    )

    val confContent =
      ResourceBuilder.getResource(s"scripts/templates/es.node.client.yml", m)
    val logbackConf =
      ResourceBuilder.getResource("conf/ws/logback.xml", Map[String, String]())
    val applicationConfConf =
      ResourceBuilder.getResource(
        "conf/ws/application.conf",
        applicationConfMap
      )

    List(
      ConfFile("ws.es.yml", confContent, false),
      ConfFile("logback.xml", logbackConf, false),
      ConfFile("application.conf", applicationConfConf, false)
    )
  }
}

case class CtrlConf(home: String,
                    sName: String,
                    seeds: String,
                    clusterName: String,
                    resourceManager: JvmMemoryAllocations,
                    singletonStarter: Boolean,
                    pingIp: String,
                    minMembers: Int = 1,
                    user: String,
                    logLevel: String,
                    debug: Boolean,
                    hostIp: String)
    extends ComponentConf(
      hostIp,
      s"$home/app/ctrl",
      sName,
      s"$home/conf/ctrl",
      "ctrl.yml",
      1
    ) {
  val port = 7777
  override def getPsIdentifier = s"CtrlServer"
  override def mkScript: ConfFile = {
    val mXmx = resourceManager.getMxmx
    val mXms = resourceManager.getMxms
    val mXmn = resourceManager.getMxmn
    val mXss = resourceManager.getMxss

    val args = Seq(
      "-XX:+UseG1GC",
      "-Dfile.encoding=UTF-8",
      s"-Dcom.sun.management.jmxremote.port=${PortManagers.ctrl.jmxPortManager.getPort(1)}",
      s"-Dcmwell.home=$home",
      s"-Dlog.level=$logLevel",
      "-Duser.timezone=GMT0",
      "-Dcom.sun.management.jmxremote.authenticate=false",
      "-Dcom.sun.management.jmxremote.ssl=false"
    ) ++ JVMOptimizer.gcLoggingJVM(s"$home/log/ctrl/gc.log")

    val scriptString =
      s"""
        |export PATH=$home/app/java/bin:$home/bin/utils:$PATH
        |$CHKSTRT
        |$BMSG
        |${genDebugStr(5011)}
        |starter java $$DEBUG_STR $mXmx $mXms $mXmn $mXss ${args.mkString(" ")} -cp "conf:$home/app/ctrl/lib/*" cmwell.ctrl.server.CtrlServer > $home/log/ctrl/stdout.log 2> $home/log/ctrl/stderr.log &
      """.stripMargin
    ConfFile(sName, scriptString, true)
  }

  override def mkConfig: List[ConfFile] = {
    val m = Map[String, String]("user" -> user)
    val confContent = ResourceBuilder.getResource(s"scripts/templates/ctrl", m)
    val applicationConfMap = Map[String, String](
      "cmwell.grid.dmap.persistence.data-dir" -> s"$home/log/ctrl/dmap",
      "cmwell.grid.bind.host" -> s"$hostIp",
      "cmwell.grid.bind.port" -> s"$port",
      "cmwell.grid.seeds" -> s"${seeds.split(',').mkString("", s":$port,", s":$port")}",
      "cmwell.grid.min-members" -> s"$minMembers",
      "cmwell.grid.monitor.port" -> s"${PortManagers.ctrl.monitorPortManager.getPort(1)}",
      "cmwell.clusterName" -> s"$clusterName",
      "ctrl.home" -> s"$home",
      "ctrl.pingIp" -> s"$pingIp",
      "ctrl.externalHostName" -> s"$hostIp",
      "ctrl.singletonStarter" -> s"$singletonStarter"
    )

    val logbackConf = ResourceBuilder.getResource(
      "conf/ctrl/logback.xml",
      Map[String, String]()
    )
    val applicationConfConf =
      ResourceBuilder.getResource(
        "conf/ctrl/application.conf",
        applicationConfMap
      )

    List(
      ConfFile("ctrl", confContent, true),
      ConfFile("logback.xml", logbackConf, false),
      ConfFile("application.conf", applicationConfConf, false)
    )
  }
}

case class DcConf(home: String,
                  sName: String,
                  clusterName: String,
                  resourceManager: JvmMemoryAllocations,
                  pingIp: String,
                  minMembers: Int = 1,
                  logLevel: String,
                  debug: Boolean,
                  target: String,
                  hostIp: String)
    extends ComponentConf(
      hostIp,
      s"$home/app/dc",
      sName,
      s"$home/conf/dc",
      "dc.yml",
      1
    ) {
  val port = 7777
  override def mkScript: ConfFile = {
    val mXmx = resourceManager.getMxmx
    val mXms = resourceManager.getMxms
    val mXmn = resourceManager.getMxmn
    val mXss = resourceManager.getMxss
    val debugStr =
      if (debug)
        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5013"
      else ""
    val args = Seq(
      "-XX:+UseG1GC",
      "-Dfile.encoding=UTF-8",
      s"-Dcom.sun.management.jmxremote.port=${PortManagers.dc.jmxPortManager.getPort(1)}",
      "-Duser.timezone=GMT0",
      /*"-Dcom.sun.management.jmxremote.port=6789",*/
      "-Dcom.sun.management.jmxremote.authenticate=false",
      "-Dcom.sun.management.jmxremote.ssl=false",
      s"-Dcmwell.home=$home",
      s"-Dlog.level=$logLevel"
    ) ++ JVMOptimizer.gcLoggingJVM(s"$home/log/dc/gc.log")

    val scriptString =
      s"""
         |export PATH=$home/app/java/bin:$home/bin/utils:$PATH
         |${createExportEnvStr("DCA_USER_TOKEN").getOrElse("")}
         |${createExportEnvStr("STP_USER_TOKEN").getOrElse("")}
         |$CHKSTRT
         |$BMSG
         |${genDebugStr(5013)}
         |starter java $$DEBUG_STR $mXmx $mXms $mXmn $mXss ${args.mkString(" ")} -cp "conf:$home/app/dc/lib/*" cmwell.dc.stream.Main > $home/log/dc/stdout.log 2> $home/log/dc/stderr.log &
      """.stripMargin
    ConfFile(sName, scriptString, true)
  }

  override def mkConfig: List[ConfFile] = {

    val applicationConfMap = Map[String, String](
      "cmwell.grid.dmap.persistence.data-dir" -> s"$home/log/dc/dmap/",
      "cmwell.grid.bind.host" -> s"$hostIp",
      "cmwell.grid.bind.port" -> s"${Jvms.DC.systemPort}",
      "cmwell.grid.seeds" -> s"$hostIp:7777",
      "cmwell.grid.min-members" -> s"$minMembers",
      "cmwell.grid.monitor.port" -> s"${PortManagers.dc.monitorPortManager.getPort(1)}",
      "cmwell.clusterName" -> s"$clusterName",
      "irwServiceDao.clusterName" -> s"$clusterName",
      "irwServiceDao.hostName" -> s"$pingIp",
      "ctrl.home" -> s"$home",
      "ctrl.pingIp" -> s"$pingIp",
      "ctrl.externalHostName" -> s"$hostIp",
      "cmwell.dc.target" -> s"$target"
    )

    val logbackConf =
      ResourceBuilder.getResource("conf/dc/logback.xml", Map[String, String]())
    val applicationConfConf =
      ResourceBuilder.getResource(
        "conf/dc/application.conf",
        applicationConfMap
      )
    List(
      ConfFile("logback.xml", logbackConf, false),
      ConfFile("application.conf", applicationConfConf, false)
    )
  }

  override def getPsIdentifier: String = "log/dc"
}

object LogstashConf {
  def genLogstashConfFile(clusterName: String,
                          esHost: String,
                          globalFields: Map[String, String],
                          logslocation: String,
                          subDivision: Int): String = {
    val logsDir = logslocation //s"${instDirs.globalLocation}/cm-well/log"

    def getFileInput(pathToLog: String,
                     fields: Map[String, String] = Map.empty) =
      s"""
         |file {
         | path => "$pathToLog"
         | add_field => {${(globalFields ++ fields)
           .map(m => s""" "${m._1}" => "${m._2}" """)
           .mkString("\n")}}
         |}
      """.stripMargin

    def getRemoteElasticsearchOutput =
      s"""
         | elasticsearch {
         |   hosts => ["$esHost"]
         |   flush_size => 5000
         |   idle_flush_time => 30
         | }
      """.stripMargin

    def getComponentFieldsMap(component: String,
                              logName: String,
                              componentIndex: Int) =
      Map(
        "component" -> component,
        "logName" -> logName,
        "componentIndex" -> componentIndex.toString
      )

    def createComponentEntries(component: String,
                               dir: String,
                               amount: Int,
                               logs: List[String]): GenSeq[String] = {
      (for (i <- 1 to amount)
        yield {
          val logDirName = ResourceBuilder.getIndexedName(dir, i)
          val logDirFullPath = s"$logsDir/$logDirName"
          logs.map(
            log =>
              getFileInput(
                s"$logDirFullPath/$log",
                getComponentFieldsMap(component, log.split('.')(0), i)
            )
          )
        }).flatten
    }

    val casLogs = createComponentEntries(
      "Cassandra",
      "cas",
      subDivision,
      List("gc.log.*", "stdout.log", "stderr.log", "system.log")
    )
    val esLogs = createComponentEntries(
      "Elasticsearch",
      "es",
      subDivision,
      List(
        s"${clusterName}_index_indexing_slowlog.log",
        s"${clusterName}_index_search_slowlog.log",
        s"${clusterName}.log",
        "gc.log.*",
        "stderr.log",
        "stdout.log"
      )
    )
    val esMasterLogs = createComponentEntries(
      "ElasticsearchMaster",
      "es-master",
      1,
      List(
        s"${clusterName}_index_indexing_slowlog.log",
        s"${clusterName}_index_search_slowlog.log",
        s"${clusterName}.log",
        "gc.log.*",
        "stderr.log",
        "stdout.log"
      )
    )
    val wsLogs = createComponentEntries(
      "Ws",
      "ws",
      1,
      List(
        "access.log",
        "application.log",
        "gc.log.*",
        "stderr.log",
        "stdout.log"
      )
    )
    val bgLogs = createComponentEntries(
      "Bg",
      "bg",
      1,
      List(
        "application.log",
        "gc.log.*",
        "heartbeat.log",
        "stderr.log",
        "stdout.log"
      )
    )
    val ctrlLogs = createComponentEntries(
      "Ctrl",
      "ctrl",
      1,
      List(
        "application.log",
        "stderr.log",
        "stdout.log",
        "cluster_state.log",
        "alerts.log",
        "gc.log.*"
      )
    )
    val dcLogs = createComponentEntries(
      "DC",
      "dc",
      1,
      List("application.log", "stderr.log", "stdout.log", "gc.log.*")
    )

    s"""
       |input {
       |  ${casLogs.mkString("\n")}
       |  ${esLogs.mkString("\n")}
       |  ${esMasterLogs.mkString("\n")}
       |  ${bgLogs.mkString("\n")}
       |  ${wsLogs.mkString("\n")}
       |  ${ctrlLogs.mkString("\n")}
       |  ${dcLogs.mkString("\n")}
       |}
       |
       |filter {
       | if [component] == "Ctrl" {
       |   # 18:32:58.198 TKD [cm-well-p-akka.actor.default-dispatcher-3] INFO  cmwell.ctrl.hc.HealthActor - GotHeakupLatencyStats 127.0.0.1 27010 ElasticsearchNode 0 0 0 0 1 23 979
       |   grok {
       |     match => [ "message", "%{IPORHOST:ip} %{NUMBER:pid} %{WORD:component} %{NUMBER:p25} %{NUMBER:p50} %{NUMBER:p75} %{NUMBER:p90} %{NUMBER:p99} %{NUMBER:p995} %{NUMBER:max}" ]
       |   }
       | }
       |
       | if [component] == "Ctrl" {
       |   grok {
       |     match => [ "message", "DiskUsage: %{NOTSPACE:device_name} %{NUMBER:device_usage:int}" ]
       |   }
       | }
       |
       | if [logName] == "access" {
       |   grok {
       |    match => ["message", "%{NOTSPACE:date} %{NOTSPACE:time} +%{NOTSPACE:zeros} method=%{WORD:method} uri=%{NOTSPACE:uri} remote-address=%{IPORHOST:ip} status=%{NUMBER:status:int} process-time=%{NUMBER:processTime:int}ms x-forwarded-for=%{NOTSPACE:forward}"]
       |   }
       | }
       | if [logName] == "gc" {
       |   grok {
       |     match => ["message", "%{WORD:date}: %{NUMBER:num}: Total time for which application threads were stopped: %{NUMBER:duration:float} %{WORD:seconds}"]
       |   }
       | }
       |}
       |
       |output {
       |  $getRemoteElasticsearchOutput
       |}
    """.stripMargin
  }
}

case class LogstashConf(clusterName: String,
                        elasticsearchUrl: String,
                        home: String,
                        dir: String = "logstash",
                        sName: String,
                        subdivision: Int,
                        hostIp: String)
    extends ComponentConf(
      hostIp,
      s"$home/app/logstash",
      sName,
      s"$home/conf/logstash",
      "logstash.yml",
      1
    ) {
  override def mkScript: ConfFile = {
    val scriptContent =
      s"""
         |export PATH=$home/app/java/bin:$home/bin/utils:$PATH
         |$CHKSTRT
         |$BMSG
         |starter $home/app/logstash/cur/bin/logstash agent -f $home/conf/logstash/ > $home/log/logstash/stdout.log 2> $home/log/logstash/stderr.log &""".stripMargin
    ConfFile("start.sh", scriptContent, true)
  }

  override def getPsIdentifier: String = "logstash/logstash"

  override def mkConfig: List[ConfFile] = {
    val confContent = LogstashConf.genLogstashConfFile(
      clusterName,
      elasticsearchUrl,
      Map.empty[String, String],
      s"$home/log",
      subdivision
    )
    List(ConfFile("logstash.conf", confContent, false))
  }
}

case class KibanaConf(hostIp: String,
                      home: String,
                      listenPort: String,
                      listenAddress: String,
                      elasticsearchUrl: String)
    extends ComponentConf(
      hostIp,
      s"$home/app/kibana",
      "start.sh",
      s"$home/conf/kibana",
      "kibana.yml",
      1
    ) {
  override def mkScript: ConfFile = {
    val scriptContent =
      s"""
         |export PATH=$home/bin/utils:$PATH
         |$CHKSTRT
         |$BMSG
         |starter $home/app/kibana/cur/bin/kibana -c $home/conf/kibana/kibana.yml > $home/log/kibana/stdout.log 2> $home/log/kibana/stderr.log &""".stripMargin
    ConfFile("start.sh", scriptContent, true)
  }

  override def getPsIdentifier: String = "bin/kibana"

  override def mkConfig: List[ConfFile] = {
    val m = Map[String, String](
      "port" -> listenPort,
      "host" -> listenAddress,
      "elasticsearch_url" -> s"http://$elasticsearchUrl"
    )
    val confContent =
      ResourceBuilder.getResource(s"scripts/templates/kibana.yml", m)

    List(ConfFile("kibana.yml", confContent, false))
  }
}
