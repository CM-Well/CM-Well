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

case class Grid(user: String,
                password: String,
                clusterIps: Seq[String],
                inet: String,
                clusterName: String,
                dataCenter: String,
                dataDirs: DataDirs,
                instDirs: InstDirs,
                esMasters: Int,
                allocationPlan: ModuleAllocations,
                useAuthorization: Boolean,
                deployJava: Boolean,
                production: Boolean,
                g1: Boolean = false,
                su: Boolean = true,
                casRowCacheSize: Int = 256,
                ctrlService: Boolean = false,
                haProxy: Option[HaProxy] = None,
                dcTarget: Option[String] = None,
                minMembers: Option[Int] = None,
                newBg: Boolean = true,
                oldBg: Boolean = true,
                nbg: Boolean = false,
                subjectsInSpAreHttps: Boolean = false,
                defaultRdfProtocol: String = "http",
                diskOptimizationStrategy:String = "ssd",
                // we refrain from using Cas Commitlog on cluster, to save disk space and performance,
                // given we always write in Quorum so there will be no data loss
                casUseCommitLog:Boolean = false)
    extends Host(
      user,
      password,
      clusterIps,
      clusterIps.size,
      clusterIps.size,
      inet,
      clusterName,
      dataCenter,
      dataDirs,
      instDirs,
      1,
      allocationPlan,
      useAuthorization,
      deployJava,
      production,
      su,
      ctrlService,
      minMembers,
      haProxy,
      subjectsInSpAreHttps = subjectsInSpAreHttps,
      defaultRdfProtocol = defaultRdfProtocol,
      diskOptimizationStrategy = diskOptimizationStrategy,
      casUseCommitLog = casUseCommitLog
    ) {

  require(clusterIps.distinct equals  clusterIps, "must be unique")

  //if(!validateNumberOfMasterNodes(esMasters, ips.size)) throw new Exception("Bad number of Elasticsearch master nodes")

  override def getElasticsearchMasters: Int = esMasters
  //def hosts = ips.map(ip => s"${user}@${ip}")
  override def getCassandraHostIDs(host: String): String = {
    //    val ip = command(s"ifconfig ${inet}", host, false).get.split("\n")(1).trim.split(" ")(1).split(":")(1)
    //    command(s"$nodeToolPath status | $ip",host,false)
    ???
  }

  override def mkScripts(hosts: GenSeq[String]): GenSeq[ComponentConf] = {
    val aloc = allocationPlan.getJvmAllocations
    val casAllocations = aloc.cas //DefaultAlocations(4000,4000,1000,0)
    val esAllocations = aloc.es //DefaultAlocations(6000,6000,400,0)

    val esMasterAllocations = allocationPlan.getElasticsearchMasterAllocations

    val bgAllocations = aloc.bg //DefaultAlocations(1000,1000,512,0)
    val wsAllocations = aloc.ws
    val ctrlAllocations = aloc.ctrl
    val homeDir = s"${instDirs.globalLocation}/cm-well"
    hosts.flatMap { host =>
      val cas = CassandraConf(
        home = homeDir,
        seeds = getSeedNodes.mkString(","),
        clusterName = clusterName,
        resourceManager = casAllocations,
        snitchType = "GossipingPropertyFileSnitch",
        ccl_dir = "ccl",
        dir = "cas",
        rowCacheSize = casRowCacheSize,
        replicationFactor = 3,
        template = "cassandra.yaml",
        listenAddress = host,
        rpcAddress = host,
        sName = "start.sh",
        index = 1,
        rs = IpRackSelector(),
        g1 = g1,
        hostIp = host,
        casDataDirs = Seq("cas"),
        casUseCommitLog = casUseCommitLog,
        numOfCores = calculateCpuAmount,
        diskOptimizationStrategy = diskOptimizationStrategy
      )

        val es = ElasticsearchConf(
          clusterName = clusterName,
          nodeName = host,
          masterNode = false,
          dataNode = true,
          expectedNodes = ips.size,
          numberOfReplicas = 2,
          seeds = getSeedNodes.mkString(","),
          home = homeDir,
          resourceManager = esAllocations,
          dir = "es",
          template = "elasticsearch.yml",
          listenAddress = host,
          masterNodes = esMasters,
          sName = "start.sh",
          index = 1,
          rs = IpRackSelector(),
          g1 = g1,
          hostIp = host
        )

        val esMaster = ElasticsearchConf(
          clusterName = clusterName,
          nodeName = s"$host-master",
          masterNode = true,
          dataNode = false,
          expectedNodes = ips.size,
          numberOfReplicas = 2,
          seeds = getSeedNodes.mkString(","),
          home = homeDir,
          resourceManager = esAllocations,
          dir = "es-master",
          template = "elasticsearch.yml",
          listenAddress = host,
          masterNodes = esMasters,
          sName = "start-master.sh",
          index = 2,
          rs = IpRackSelector(),
          g1 = true,
          hostIp = host
        )

      val bg = BgConf(
        home = homeDir,
        zookeeperServers = ips.take(3),
        clusterName = clusterName,
        dataCenter = dataCenter,
        hostName = host,
        resourceManager = bgAllocations,
        sName = "start.sh",
        isMaster = host == ips(0),
        partition = ips.indexOf(host),
        logLevel = BgProps(this).LogLevel.getLogLevel,
        debug = deb,
        hostIp = host,
        minMembers = getMinMembers,
        numOfPartitions = hosts.size,
        seeds = getSeedNodes.mkString(","),
        defaultRdfProtocol = defaultRdfProtocol,
        transportAddress = this.getThreesome(ips, host)
      )

      val web = WebConf(
        home = homeDir,
        zookeeperServers = ips.take(3),
        clusterName = clusterName,
        dataCenter = dataCenter,
        hostName = host,
        resourceManager = wsAllocations,
        sName = "start.sh",
        useAuthorization = useAuthorization,
        numOfPartitions = ips.size,
        logLevel = WebserviceProps(this).LogLevel.getLogLevel,
        debug = deb,
        hostIp = host,
        minMembers = getMinMembers,
        seeds = getSeedNodes.mkString(","),
        seedPort = 9300,
        defaultRdfProtocol = defaultRdfProtocol,
        transportAddress = this.getThreesome(ips, host)
      )

      val cw = CwConf(
        home = homeDir,
        clusterName = clusterName,
        dataCenter = dataCenter,
        hostName = host,
        resourceManager = wsAllocations,
        sName = "cw-start.sh",
        logLevel = WebserviceProps(this).LogLevel.getLogLevel,
        debug = deb,
        hostIp = host,
        minMembers = getMinMembers,
        seeds = getSeedNodes.mkString(","),
        seedPort = 9300,
        subjectsInSpAreHttps = subjectsInSpAreHttps,
        transportAddress = this.getThreesome(ips, host)
      )

      val ctrl = CtrlConf(
        home = homeDir,
        sName = "start.sh",
        seeds = getSeedNodes.mkString(","),
        clusterName = clusterName,
        resourceManager = ctrlAllocations,
        singletonStarter = true,
        pingIp = host,
        user = user,
        logLevel = CtrlProps(this).LogLevel.getLogLevel,
        debug = deb,
        hostIp = host,
        minMembers = getMinMembers
      )

      val dcConf = DcConf(
        home = homeDir,
        sName = "start.sh",
        clusterName = cn,
        resourceManager = bgAllocations.copy(mxms = 0),
        target = dcTarget.getOrElse(ips.map(ip => s"$ip:9000").mkString(",")),
        debug = deb,
        logLevel = DcProps(this).LogLevel.getLogLevel,
        pingIp = host,
        hostIp = host,
        minMembers = getMinMembers
      )

      val zookeeper = ZookeeperConf(
        home = homeDir,
        clusterName = cn,
        servers = ips.take(3),
        hostIp = host
      )

      val kafka = KafkaConf(
        home = homeDir,
        logDirs = dataDirs.kafkaDataDirs.toList,
        zookeeperServers = ips.take(3),
        brokerId = brokerId(host),
        hostIp = host
      )

      List(
        cas,
        es,
        esMaster,
        web,
        cw,
        ctrl,
        dcConf,
        zookeeper,
        kafka,
        bg
      )
    }
  }

  override def getMode: String = "grid"

  override def getSeedNodes: List[String] = ips.take(3).toList

  override def startElasticsearch(hosts: GenSeq[String]): Unit = {
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript("./start-master.sh")}",
            ips.par.take(esMasters).intersect(hosts),
            false)
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript("./start.sh")}", hosts, false)
  }

  override def startCassandra(hosts: GenSeq[String]): Unit = {
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}", hosts, false)
  }

  override def initCassandra(hosts: GenSeq[String]): Unit = {
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}", hosts(0), false)
    Try(CassandraLock().waitForModule(ips(0), 1))
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}", hosts(1), false)
    Try(CassandraLock().waitForModule(ips(0), 2))
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}", hosts.drop(2), false)
  }

  override def initElasticsearch(hosts: GenSeq[String]): Unit = {
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript("./start-master.sh")}",
            hosts.take(esMasters),
            false)
    Try(ElasticsearchLock().waitForModule(ips(0), esMasters))
    //    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ./start-master.sh", hosts(1), false)
    //    ElasticsearchLock().waitForModule(ips(0), 2)
    //    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ./start-master.sh", hosts.drop(2).take(esMasters - 2), false)
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript("./start.sh")}", hosts, false)
  }

  override def getNewHostInstance(ipms: Seq[String]): Host = {
    this.copy(clusterIps = ipms)
  }
}
