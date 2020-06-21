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
import cmwell.ctrl.config.Jvms

import scala.collection.parallel.ParSeq

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

  lazy val java7ExtraArgs: Seq[String] = globalJVM ++ Seq("-XX:+UseCondCardMark", "-Duser.timezone=GMT0")
  /* def machineSpecs(): (Int, Long, Long) = {
     import Props.os
     (os.getAvailableProcessors, os.getTotalPhysicalMemorySize, os.getFreePhysicalMemorySize)
   }*/
}

case class ConfFile(fileName: String, content: String, executable: Boolean = false, path: Option[String] = None)

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
                         hostIp: String,
                         casDataDirs:Seq[String],
                         casUseCommitLog:Boolean,
                         numOfCores:Integer,
                         diskOptimizationStrategy:String)
    extends ComponentConf(hostIp, s"$home/app/cas/cur", sName, s"$home/conf/$dir", "cassandra.yaml", index) {
  override def getPsIdentifier = s"/log/cas${getIndexTxt}/"

  override def mkScript: ConfFile = {

    val scriptString =
      s"""export PATH=$home/app/java/bin:$home/bin/utils:$PATH
       |export CASSANDRA_HOME=$home/app/cas/cur
       |export CASSANDRA_CONF=$home/conf/${dir}
       |export CASSANDRA_INCLUDE=$home/conf/${dir}/cassandra.in.sh
       |export CASSANDRA_HEAPDUMP_DIR=$home/log/${dir}
       |export CASSANDRA_LOG_DIR=$home/log/${dir}
       |$CHKSTRT
       |$BMSG
       |starter bin/cassandra -f > $home/log/$dir/stdout.log 2> $home/log/$dir/stderr.log &""".stripMargin

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
        "row_cache_size" -> rowCacheSize.toString,
        "cas_data_dirs" -> casDataDirs.map(dir=> s"$home/data/$dir/data").mkString("\n    - "),
        "concurrent_reads" -> (16 * casDataDirs.size).toString,
        "concurrent_writes" ->  (8 * Math.max(1, numOfCores / 2)).toString,
        "concurrent_counter_writes" -> (16 * casDataDirs.size).toString,
        "disk_optimization_strategy" -> diskOptimizationStrategy,
        "concurrent_compactors" -> (if (diskOptimizationStrategy == "SSD") Math.max(1, numOfCores / 2) else 1).toString
      )
    )


//    val log4jContent = templateToFile(s"scripts/templates/log4j-server.properties",
//      Map("file_path" -> s"$home/log/$dir/system.log"))

    val logBackContent = ResourceBuilder.getResource(s"scripts/templates/logback-cassandra.xml", Map.empty)

    val jvmOptionsMap = Map[String, String](
      "ps_id" -> getPsIdentifier,
      "cas_ms" -> s"${resourceManager.mxms}",
      "cas_mx" -> s"${resourceManager.mxmx}",
      "cas_mn" -> s"${resourceManager.mxmn}"
    )
    val jvmOptContent = ResourceBuilder.getResource(s"scripts/templates/cas-jvm.options", jvmOptionsMap)

    val cassandraEnvContent = ResourceBuilder.getResource(s"scripts/templates/cassandra-env.sh", Map.empty)

    val casIncludeContent = ResourceBuilder.getResource(s"scripts/templates/cassandra.in.sh", Map.empty)

    val rackConfContent =
      ResourceBuilder.getResource("scripts/templates/cassandra-rackdc.properties", Map("rack_id" -> rs.getRackId(this)))

    val cqlInit2 = ResourceBuilder.getResource("scripts/templates/cassandra-cql-init-cluster-new",
                                               Map("replication_factor" -> replicationFactor.toString, "durable_writes" -> casUseCommitLog.toString))

    val cqlInit3 = ResourceBuilder.getResource("scripts/templates/zstore-cql-init-cluster",
                                               Map("replication_factor" -> replicationFactor.toString))

    val cassandraStatus = ResourceBuilder.getResource("scripts/templates/cassandra-status-viewer-template",
                                                      Map("home" -> home, "host" -> hostIp))

    List(
      ConfFile("cassandra.yaml", confContent, false),
      ConfFile("logback.xml", logBackContent),
      ConfFile("cassandra-env.sh", cassandraEnvContent),
      ConfFile("cassandra.in.sh", casIncludeContent),
      ConfFile("jvm.options", jvmOptContent),
      ConfFile("cassandra-rackdc.properties", rackConfContent, false),
      ConfFile("cassandra-status-viewer", cassandraStatus, true),
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
                             template: String = "elasticsearch.yml",
                             listenAddress: String = "127.0.0.1",
                             masterNodes: Int,
                             sName: String,
                             index: Int,
                             rs: RackSelector,
                             g1: Boolean,
                             hostIp: String,
                             dirsPerEs: Int = 1)
    extends ComponentConf(hostIp, s"$home/app/es/cur", sName, s"$home/conf/$dir", "elasticsearch.yml", index) {

  val classpath = s"""'$home/app/es/cur/lib/*:'"""

  override def getPsIdentifier = {
    if (dir == "es-master")
      s"PsIdElasticMasterNode"
    else
      s"PsIdElasticDataNode$getIndexTxt"
  }
  override def mkScript: ConfFile = {

    val scriptString =
      s"""export PATH=$home/app/java/bin:$home/bin/utils:$PATH
         |export ES_PATH_CONF=$home/conf/$dir/config
         |$CHKSTRT
         |$BMSG
         |starter bin/elasticsearch > $home/log/$dir/stdout.log 2> $home/log/$dir/stderr.log &""".stripMargin

    ConfFile(sName, scriptString, true)
  }

  override def mkConfig: List[ConfFile] = {
    val httpHost = if(masterNode) s"http.host: $host" else ""
    val httpPort = if(masterNode) 9200 else PortManagers.es.httpPortManager.getPort(index)
    val transportPort = if(masterNode) 9300 else PortManagers.es.transportPortManager.getPort(index)

    val m = Map[String, String](
      "clustername" -> clusterName,
      "nodename" -> nodeName,
      "node-master" -> masterNode.toString,
      "node-data" -> dataNode.toString,
      "recoverafternodes" -> { if (expectedNodes > 3) expectedNodes - 2 else expectedNodes - 1 }.toString,
      "expectednodes" -> expectedNodes.toString,
      "seed_hosts" -> seeds.split(',').mkString("", s":$seedPort,", s":$seedPort"),
      "initial_master_nodes" -> seeds.split(',').mkString("", "-master,", "-master"),
      "dir" -> dir,
      "listen_address" -> listenAddress,
      "root_dir" -> home,
      "rack_id" -> rs.getRackId(this),
      "http_host" -> httpHost,
      "min_masters" -> (Math.round(masterNodes / 2) + 1).toString,
      "http_port" -> httpPort.toString,
      "transport_port" -> transportPort.toString,
      "num_of_shards" -> expectedNodes.toString,
      "num_of_replicas" -> { if (expectedNodes > 2) 2 else 0 }.toString,
      "path_data" -> { if (dirsPerEs == 1) s"$home/data/$dir"
                       else {
                         (2 to dirsPerEs).map(_.toString).fold(s"$home/data/es")((acc, x) => s"$home/data/es$x,$acc")}}
    )

    val confContent = ResourceBuilder.getResource(s"scripts/templates/$template", m)

    val m2 = Map[String, String]("number_of_shards" -> Math.min(expectedNodes, 10).toString,
                                 "number_of_replicas" -> numberOfReplicas.toString)
    val mappingContent = ResourceBuilder.getResource(s"scripts/templates/indices_template_new.json",m2)

    val loggerConf = ResourceBuilder.getResource("scripts/templates/es-log4j2.properties", Map.empty[String, String])

    val m3 = Map[String, String](
      "ps_id" -> getPsIdentifier,
      "es_ms" -> s"${resourceManager.mxms}",
      "es_mx" -> s"${resourceManager.mxmx}",
      "log_dir" -> s"$home/log/$dir"
    )
    val jvmOpts = ResourceBuilder.getResource("scripts/templates/es-jvm.options", m3)

    List(ConfFile("elasticsearch.yml", confContent, false, Some(s"$home/conf/$dir/config")),
         ConfFile("indices_template_new.json", mappingContent, false),
         ConfFile("log4j2.properties", loggerConf, false, Some(s"$home/conf/$dir/config")),
         ConfFile("jvm.options", jvmOpts, false, Some(s"$home/conf/$dir/config")))
  }
}

case class KafkaConf(home: String, logDirs: Seq[String], zookeeperServers: Seq[String], brokerId: Int, hostIp: String)
    extends ComponentConf(hostIp, s"$home/app/kafka", "start.sh", s"$home/conf/kafka", "server.properties", 1) {
  override def mkScript: ConfFile = {
    val dir = "kafka"
    val exports = s"export PATH=$home/app/java/bin:$home/bin/utils:$PATH"
    val cp = ":cur/libs/*"
    // scalastyle:off
    val scriptString =
      s"""
         |$exports
          |$CHKSTRT
          |$BMSG
          |starter java -Xmx1G -Xms1G -server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true -Xloggc:$home/log/$dir/kafkaServer-gc.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -Dcom.sun.management.jmxremote.port=${PortManagers.kafka.jmxPortManager
           .getPort(1)} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dkafka.logs.dir=$home/log/$dir -Dlog4j.configuration=file:$home/conf/$dir/log4j.properties -cp $cp kafka.Kafka $home/conf/$dir/server.properties  > $home/log/$dir/stdout.log 2>  $home/log/$dir/stderr.log &
     """.stripMargin
    // scalastyle:on
    ConfFile("start.sh", scriptString, true)
  }

  override def getPsIdentifier: String = "kafka.Kafka"

  override def mkConfig: List[ConfFile] = {
    val m = Map[String, String](
      "broker-id" -> brokerId.toString,
      "log-dirs" -> logDirs.mkString(","),
      "zookeeper-connect" -> zookeeperServers.map(zkServer => s"$zkServer:2181").mkString(",")
    )

    val confContent = ResourceBuilder.getResource(s"scripts/templates/kafka.server.properties", m)

    val loggerConf = ResourceBuilder.getResource("scripts/templates/log4j-kafka.properties", Map[String, String]())

    List(ConfFile("server.properties", confContent, false), ConfFile("log4j.properties", loggerConf, false))
  }
}

case class ZookeeperConf(home: String, clusterName: String, servers: Seq[String], hostIp: String)
    extends ComponentConf(hostIp, s"$home/app/zookeeper", "start.sh", s"$home/conf/zookeeper", "zoo.conf", 1) {
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
    val exports = s"""export PATH=$home/app/java/bin:$home/bin/utils:$PATH
                     |export ZOO_LOG_DIR=$home/log/$dir
                     |export ZOO_LOG4J_PROP="INFO, ROLLINGFILE"
                     |export ZOOCFGDIR=$confDir
                     |export JMXDISABLE=true
                     |export JVMFLAGS="-Xmx500m -Xms500m -Dlog4j.configuration=file:$confDir/log4j.properties"""".stripMargin
    // scalastyle:off
    val scriptString =
      s"""
          |$exports
          |$CHKSTRT
          |$BMSG
          |starter $home/app/zookeeper/cur/bin/zkServer.sh start-foreground > $home/log/$dir/stdout.log 2>  $home/log/$dir/stderr.log &
      """.stripMargin
    // scalastyle:on
    ConfFile("start.sh", scriptString, true)
  }

  override def getPsIdentifier: String = "QuorumPeerMain"

  override def mkConfig: List[ConfFile] = {
    val m = Map[String, String](
      "dataDir" -> s"$home/data/$dir",
      "servers" -> genServersStr
    )

    val confContent = ResourceBuilder.getResource(s"scripts/templates/zoo.cfg", m)
    val myId = (servers.indexOf(hostIp) + 1).toString

    val loggerMap = Map[String, String]("zookeeperLogDir" -> s"$home/log/$dir")
    val loggerConf = ResourceBuilder.getResource("scripts/templates/log4j-zookeeper.properties", loggerMap)

    List(ConfFile("zoo.cfg", confContent, false),
         ConfFile("log4j.properties", loggerConf, false),
         ConfFile("myid", myId, false, Some(s"$home/data/$dir")))
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
                  dir: String = "bg",
                  defaultRdfProtocol: String,
                  transportAddress: String)
    extends ComponentConf(hostIp, s"$home/app/bg", sName, s"$home/conf/bg", "bg.yml", 1) {
  override def mkScript: ConfFile = {
    def jvmArgs = {
      val aspectj = if (/*hasOption("useAspectj")*/ false) s"$home/app/tools/aspectjweaver.jar" else ""
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
        "-Des.set.netty.runtime.available.processors=false",
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
      "ftsService.transportAddress" -> s"$transportAddress",
      "cmwell.rdfDefaultProtocol" -> defaultRdfProtocol,
      "kafka.bootstrap.servers" -> s"localhost:9092,${zookeeperServers.map(kafkaNode => s"$kafkaNode:9092").mkString(",")}"
    )
    val m = Map[String, String](
      "clustername" -> clusterName,
      "hosts" -> seeds.split(',').mkString("", s":$seedPort,", s":$seedPort"),
      "dir" -> dir,
      "root_dir" -> home
    )
    val logbackConf = ResourceBuilder.getResource("conf/bg/logback.xml", Map[String, String]())
    val confContent = ResourceBuilder.getResource(s"scripts/templates/es.node.client.yml", m)
    val applicationConfConf = ResourceBuilder.getResource("conf/bg/application.conf", applicationConfMap)

    List(ConfFile("logback.xml", logbackConf, false),
         ConfFile("bg.es.yml", confContent, false),
         ConfFile("application.conf", applicationConfConf, false))
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
                  seedPort: Int,
                  subjectsInSpAreHttps: Boolean,
                  transportAddress: String)
    extends ComponentConf(hostIp, s"$home/app/ws", sName, s"$home/conf/cw", "ws.yml", 1) {
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
          s"-Dcom.sun.management.jmxremote.port=${PortManagers.ws.jmxPortManager.getPort(2)}",
          "-Dcom.sun.management.jmxremote.ssl=false",
          "-Dcom.sun.management.jmxremote.authenticate=false",
          "-Duser.timezone=GMT0",
          s"-Dcrashableworker.subjectsAreHttps=$subjectsInSpAreHttps"
        ) ++ JVMOptimizer.gcLoggingJVM(s"$home/log/ws/gc.log") ++
        Seq("-cp", s""" "cw-conf:$home/app/ws/lib/*" """, "cmwell.crashableworker.WorkerMain")

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
      "ftsService.transportAddress" -> s"$transportAddress",
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

    val confContent = ResourceBuilder.getResource(s"scripts/templates/es.node.client.yml", m)
    val logbackConf = ResourceBuilder.getResource("conf/ws/cw-logback.xml", Map[String, String]())
    val applicationConfConf = ResourceBuilder.getResource("conf/ws/cw-application.conf", applicationConfMap)

    List(ConfFile("ws.es.yml", confContent, false),
         ConfFile("logback.xml", logbackConf, false),
         ConfFile("application.conf", applicationConfConf, false))
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
                   seedPort: Int,
                   defaultRdfProtocol: String,
                   transportAddress: String)
    extends ComponentConf(hostIp, s"$home/app/ws", sName, s"$home/conf/ws", "ws.yml", 1) {
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
        "-Des.set.netty.runtime.available.processors=false",
        s"-Dcmwell.home=$home",
        s"-Dlog.level=$logLevel",
        "-Dfile.encoding=UTF-8",
        "-Duser.timezone=GMT0"
      ) ++
      JVMOptimizer.gcLoggingJVM(s"$home/log/ws/gc.log", true) ++ Seq("-cp",
                                                                     s""" "conf:$home/app/ws/lib/*" """,
                                                                     "play.core.server.ProdServerStart")

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
      "ftsService.transportAddress" -> s"$transportAddress",
      "cmwell.rdfDefaultProtocol" -> defaultRdfProtocol
    )

    val m = Map[String, String](
      "clustername" -> clusterName,
      "hosts" -> seeds.split(',').mkString("", s":$seedPort,", s":$seedPort"),
      "dir" -> "ws",
      "root_dir" -> home
    )

    val confContent = ResourceBuilder.getResource(s"scripts/templates/es.node.client.yml", m)
    val logbackConf = ResourceBuilder.getResource("conf/ws/logback.xml", Map[String, String]())
    val applicationConfConf = ResourceBuilder.getResource("conf/ws/application.conf", applicationConfMap)

    List(ConfFile("ws.es.yml", confContent, false),
         ConfFile("logback.xml", logbackConf, false),
         ConfFile("application.conf", applicationConfConf, false))
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
    extends ComponentConf(hostIp, s"$home/app/ctrl", sName, s"$home/conf/ctrl", "ctrl.yml", 1) {
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

    // scalastyle:off
    val scriptString =
      s"""
        |export PATH=$home/app/java/bin:$home/bin/utils:$PATH
        |$CHKSTRT
        |$BMSG
        |${genDebugStr(5011)}
        |starter java $$DEBUG_STR $mXmx $mXms $mXmn $mXss ${args.mkString(" ")} -cp "conf:$home/app/ctrl/lib/*" cmwell.ctrl.server.CtrlServer > $home/log/ctrl/stdout.log 2> $home/log/ctrl/stderr.log &
      """.stripMargin
    // scalastyle:on
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

    val logbackConf = ResourceBuilder.getResource("conf/ctrl/logback.xml", Map[String, String]())
    val applicationConfConf = ResourceBuilder.getResource("conf/ctrl/application.conf", applicationConfMap)

    List(ConfFile("ctrl", confContent, true),
         ConfFile("logback.xml", logbackConf, false),
         ConfFile("application.conf", applicationConfConf, false))
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
    extends ComponentConf(hostIp, s"$home/app/dc", sName, s"$home/conf/dc", "dc.yml", 1) {
  val port = 7777
  override def mkScript: ConfFile = {
    val mXmx = resourceManager.getMxmx
    val mXms = resourceManager.getMxms
    val mXmn = resourceManager.getMxmn
    val mXss = resourceManager.getMxss
    val debugStr = if (debug) "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5013" else ""
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

    val scriptString = {
      val sb = new StringBuilder
      sb ++= "export PATH="
      sb ++= home
      sb ++= "/app/java/bin:"
      sb ++= home
      sb ++= "/bin/utils:"
      sb ++= PATH
      sb += '\n'
      createExportEnvStr("DCA_USER_TOKEN").foreach { dut =>
        sb ++= dut
        sb += '\n'
      }
      createExportEnvStr("STP_USER_TOKEN").foreach { sut =>
        sb ++= sut
        sb += '\n'
      }
      sb ++= CHKSTRT
      sb += '\n'
      sb ++= BMSG
      sb += '\n'
      sb ++= genDebugStr(5013)
      sb += '\n'
      sb ++= "starter java $DEBUG_STR "
      sb ++= mXmx
      sb += ' '
      sb ++= mXms
      sb += ' '
      sb ++= mXmn
      sb += ' '
      sb ++= mXss
      sb += ' '
      args.foreach { arg =>
        sb ++= arg
        sb += ' '
      }
      sb ++= "-cp \"conf:"
      sb ++= home
      sb ++= "/app/dc/lib/*\" cmwell.dc.stream.Main > "
      sb ++= home
      sb ++= "/log/dc/stdout.log 2> "
      sb ++= home
      sb ++= "/log/dc/stderr.log &\n"
      sb.result()
    }
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

    val logbackConf = ResourceBuilder.getResource("conf/dc/logback.xml", Map[String, String]())
    val applicationConfConf = ResourceBuilder.getResource("conf/dc/application.conf", applicationConfMap)
    List(ConfFile("logback.xml", logbackConf, false), ConfFile("application.conf", applicationConfConf, false))
  }

  override def getPsIdentifier: String = "log/dc"
}
