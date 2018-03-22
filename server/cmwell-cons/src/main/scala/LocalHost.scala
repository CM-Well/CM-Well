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
import scala.collection.GenSeq
import scala.util.Try
import scala.sys.process._

case class LocalHost(dataCenter: String = "lh",
                     dataDirs: DataDirs,
                     instDirs: InstDirs,
                     useAuthorization: Boolean,
                     deployJava: Boolean = false,
                     casRowCacheSize: Int = 0,
                     allocationPlan: ModuleAllocations = DevAllocations(),
                     withElk: Boolean = false,
                     symLinkLib: Boolean = true,
                     newBg: Boolean = true,
                     oldBg: Boolean = true,
                     nbg: Boolean = false,
                     isDebug: Boolean = false)
    extends Host(
      System.getProperty("user.name"),
      "",
      IpMappings(List(IpMapping("127.0.0.1", None))),
      1,
      "lo",
      "cm-well-p",
      dataCenter,
      dataDirs,
      instDirs,
      1,
      allocationPlan,
      useAuthorization,
      deployJava,
      false,
      false,
      minMembers = Some(1),
      haProxy = None,
      withElk = withElk,
      isDebug = isDebug
    ) {

//  LogLevel.debug
  override def install: Unit = install(ips.par)
  override def install(hosts: GenSeq[String]) {
    createDataDirs(hosts)
    //createCmwellSymLink(hosts)
    super.install(hosts)
  }

  override def esHealthAddress = ":9201/_cluster/health?pretty=true"

  override def getElasticsearchMasters: Int = 0

  //def hosts = ips.map(ip => s"${user}@${ip}")
  override def getCassandraHostIDs(host: String): String = ???

  //def bashcWrapper(com : String) = s"""bash -c \"\"\"${com}\"\"\""""

  override def prepareMachines(hosts: GenSeq[String] = ips.par,
                               sudoer: String,
                               sudoerPass: String,
                               userPass: String) {
    createDataDirs(hosts)
    command(
      s"cd ${instDirs.globalLocation}/ ; ln -s ${instDirs.intallationDir} cm-well",
      hosts,
      false
    )
  }

  override def getSeedNodes: List[String] = ips
  override val esMasterPort = 9201

  override def getMode: String = "local"

  override def command(com: String,
                       hosts: GenSeq[String],
                       sudo: Boolean): GenSeq[Try[String]] = {
    hosts.map { host =>
      command(com, host, sudo)
    }
  }

  override def command(com: String,
                       host: String,
                       sudo: Boolean): Try[String] = {
    command(com, sudo)
  }

  //no need for pe to deploy ssh keys or to remove login message
  override def refreshUserState(user: String,
                                sudoer: Option[Credentials],
                                hosts: GenSeq[String] = ips): Unit = {}

  override def path: String = {
    val PATH = "$PATH"
    s"PATH=$utilsPath:$PATH"
  }

  override def command(com: String, sudo: Boolean = false): Try[String] = {
    if (verbose) println(s"localhost: ${com}")
    Try(Seq("bash", "-c", s"$com").!!)
  }

  override def rsync(from: String,
                     to: String,
                     hosts: GenSeq[String],
                     sudo: Boolean = false): GenSeq[Try[String]] = {
    //println("executing rsync from " + from + " to " + host + ":" + to)
    val seq = s"rsync -Pavz --delete ${from} ${to}"
    //val seq = s"cp -al ${from} ${to}"
    if (verbose) println("command: " + seq.mkString(" "))
    List(Try(Seq("bash", "-c", seq).!!))
  }

  override def startElasticsearch(hosts: GenSeq[String]): Unit = {
    //command(s"cp ${instDirs.globalLocation}/cm-well/app/scripts/pe/elasticsearch.yml ${instDirs.globalLocation}/cm-well/conf/es/es.yml", hosts(0), false)
    command(
      s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript("./start.sh")}",
      hosts(0),
      false
    )
  }

  override def startCassandra(hosts: GenSeq[String]): Unit = {
    //command(s"cp ${instDirs.globalLocation}/cm-well/app/scripts/pe/cassandra.yaml ${instDirs.globalLocation}/cm-well/conf/cas/cassandra.yaml", hosts(0), false)
    //command(s"cp ${instDirs.globalLocation}/cm-well/app/scripts/pe/log4j-server.properties ${instDirs.globalLocation}/cm-well/conf/cas/log4j-server.properties", hosts(0), false)
    command(
      s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}",
      hosts(0),
      false
    )
  }

  override def initCassandra(hosts: GenSeq[String] = ips.par): Unit = {
    startCassandra(hosts)
  }

  override def initElasticsearch(hosts: GenSeq[String] = ips.par): Unit = {
    startElasticsearch(hosts)
  }

  override def createDataDirs(i: GenSeq[String] = ips.par) {

    command(s"mkdir -p ${instDirs.intallationDir}", i, false)

    dataDirs.casDataDirs.foreach { cas =>
      command(s"mkdir -p ${cas}", i, false)
    }

    dataDirs.casCommitLogDirs.foreach { ccl =>
      command(s"mkdir -p ${ccl}", i, false)
    }

    dataDirs.esDataDirs.foreach { es =>
      command(s"mkdir -p ${es}", i, false)
    }

    dataDirs.kafkaDataDirs.foreach { kafka =>
      command(s"mkdir -p $kafka", i, false)
    }

    command(s"mkdir -p ${dataDirs.zookeeperDataDir}", i, false)

    command(s"mkdir -p ${dataDirs.logsDataDir}", i, false)
  }

  override def injectMetaData(i: String) {
    Try(WebServiceLock().waitForModule("localhost", 1))
    //println("Uploading meta data")
    dataInitializer.uploadMetaData
    dataInitializer.uploadNameSpaces()
  }

  override def mkScripts(hosts: GenSeq[String]): GenSeq[ComponentConf] = {
    val aloc = DevAllocations().getJvmAllocations
    val casAllocations = aloc.cas //DefaultAlocations(4000,4000,1000,0)
    val esAllocations = aloc.es //DefaultAlocations(6000,6000,400,0)

    val esMasterAllocations = DevAllocations().getElasticsearchMasterAllocations

    val bgAllocations = aloc.bg //DefaultAlocations(1000,1000,512,0)
    val wsAllocations = aloc.ws
    val ctrlAllocations = aloc.ctrl

    val homeDir = s"${instDirs.globalLocation}/cm-well"
    val ip = "127.0.0.1"

    val cas = CassandraConf(
      home = homeDir,
      seeds = getSeedNodes.mkString(","),
      clusterName = cn,
      resourceManager = casAllocations,
      snitchType = "GossipingPropertyFileSnitch",
      ccl_dir = "ccl",
      dir = "cas",
      rowCacheSize = casRowCacheSize,
      replicationFactor = 1,
      template = "cassandra.yaml",
      listenAddress = ip,
      rpcAddress = ip,
      sName = "start.sh",
      index = 1,
      rs = IpRackSelector(),
      g1 = false,
      hostIp = ip
    )

    val es = ElasticsearchConf(
      clusterName = cn,
      nodeName = ip,
      masterNode = true,
      dataNode = true,
      expectedNodes = ips.size,
      numberOfReplicas = 0,
      seeds = getSeedNodes.mkString(","),
      seedPort = 9301,
      home = homeDir,
      resourceManager = esAllocations,
      dir = "es",
      template = "es.yml",
      listenAddress = ip,
      masterNodes = 1,
      sName = "start.sh",
      index = 1,
      rs = IpRackSelector(),
      g1 = false,
      hostIp = ip,
      autoCreateIndex = withElk
    )

    val esMaster = ElasticsearchConf(
      clusterName = cn,
      nodeName = s"$ip-master",
      masterNode = true,
      dataNode = false,
      expectedNodes = ips.size,
      numberOfReplicas = 0,
      seeds = getSeedNodes.mkString(","),
      home = homeDir,
      resourceManager = esMasterAllocations,
      dir = "es-master",
      template = "es.yml",
      listenAddress = ip,
      masterNodes = 1,
      sName = "start-master.sh",
      index = 2,
      rs = IpRackSelector(),
      g1 = false,
      hostIp = ip,
      autoCreateIndex = withElk
    )

    val bg = BgConf(
      home = homeDir,
      zookeeperServers = ips.take(3),
      clusterName = cn,
      dataCenter = dc,
      hostName = ip,
      resourceManager = bgAllocations,
      sName = "start.sh",
      isMaster = true,
      logLevel = BgProps(this).LogLevel.getLogLevel,
      debug = deb,
      hostIp = ip,
      minMembers = getMinMembers,
      numOfPartitions = hosts.size,
      seeds = getSeedNodes.mkString(",")
    )

    val web = WebConf(
      home = homeDir,
      zookeeperServers = ips.take(3),
      clusterName = cn,
      dataCenter = dc,
      hostName = ip,
      resourceManager = wsAllocations,
      sName = "start.sh",
      useAuthorization = useAuthorization,
      logLevel = WebserviceProps(this).LogLevel.getLogLevel,
      debug = deb,
      hostIp = ip,
      minMembers = getMinMembers,
      seedPort = 9301,
      seeds = getSeedNodes.mkString(",")
    )

    val cw = CwConf(
      home = homeDir,
      clusterName = cn,
      dataCenter = dc,
      hostName = ip,
      resourceManager = wsAllocations,
      sName = "cw-start.sh",
      logLevel = WebserviceProps(this).LogLevel.getLogLevel,
      debug = deb,
      hostIp = ip,
      minMembers = getMinMembers,
      seeds = getSeedNodes.mkString(","),
      seedPort = 9301
    )

    val ctrl = CtrlConf(
      home = homeDir,
      sName = "start.sh",
      seeds = getSeedNodes.mkString(","),
      clusterName = cn,
      resourceManager = ctrlAllocations,
      singletonStarter = true,
      pingIp = ip,
      user = "",
      logLevel = CtrlProps(this).LogLevel.getLogLevel,
      debug = deb,
      hostIp = ip,
      minMembers = getMinMembers
    )

    val dcConf = DcConf(
      home = homeDir,
      sName = "start.sh",
      clusterName = cn,
      resourceManager = bgAllocations.copy(mxms = 0),
      target = "localhost:9000",
      debug = deb,
      logLevel = DcProps(this).LogLevel.getLogLevel,
      pingIp = ip,
      hostIp = ip,
      minMembers = getMinMembers
    )

    val kibana = KibanaConf(
      hostIp = ip,
      home = homeDir,
      listenPort = "9090",
      listenAddress = ip,
      elasticsearchUrl = "localhost:9201"
    )

    val logstash = LogstashConf(
      clusterName = cn,
      elasticsearchUrl = "localhost:9201",
      home = homeDir,
      dir = "logstash",
      sName = "start.sh",
      subdivision = 1,
      hostIp = ip
    )

    val zookeeper =
      ZookeeperConf(
        home = homeDir,
        clusterName = cn,
        servers = ips.take(3),
        hostIp = ip
      )

    val kafka = KafkaConf(
      home = homeDir,
      logDirs = dataDirs.kafkaDataDirs.toList,
      zookeeperServers = ips.take(3),
      brokerId = 1,
      hostIp = ip
    )

    List(cas, es, esMaster, bg, web, cw, ctrl, dcConf, zookeeper, kafka) ++ (if (withElk)
                                                                               List(
                                                                                 logstash,
                                                                                 kibana
                                                                               )
                                                                             else
                                                                               List.empty[
                                                                                 ComponentConf
                                                                               ])

  }

  override def syncLib(hosts: GenSeq[String] = ips) = {
    if (symLinkLib)
      command(
        s"ln -s `pwd`/lib ${instDirs.globalLocation}/cm-well/lib",
        hosts,
        false
      )
    else
      super.syncLib(hosts)
  }

  override def getNewHostInstance(ipms: IpMappings): Host = ???
}
