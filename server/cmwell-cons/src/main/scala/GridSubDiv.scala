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
import scala.collection.{GenMap, GenSeq}
import scala.util.Try

case class GridSubDiv(user: String,
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
                      g1: Boolean = false,
                      casRowCacheSize: Int = 256,
                      production: Boolean,
                      ctrlService: Boolean = false,
                      haProxy: Option[HaProxy] = None,
                      dcTarget: Option[String] = None,
                      minMembers: Option[Int] = None,
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
      clusterIps.size * dataDirs.esDataDirs.size,
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
      true,
      ctrlService,
      minMembers = minMembers,
      haProxy,
      subjectsInSpAreHttps = subjectsInSpAreHttps,
      defaultRdfProtocol = defaultRdfProtocol,
      diskOptimizationStrategy = diskOptimizationStrategy,
      casUseCommitLog = casUseCommitLog) {

  require(clusterIps.distinct equals  clusterIps, "must be unique")
  //var persistentAliases = false
  override def getElasticsearchMasters: Int = esMasters
  //def hosts = ips.map(ip => s"${user}@${ip}")
  override def getCassandraHostIDs(host: String): String = ???

  override def getMode: String = "gridSubDiv"

  override def startElasticsearch(hosts: GenSeq[String]): Unit = {
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript("./start-master.sh")}",
            ips.par.take(esMasters).intersect(hosts),
            false)
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript("./start.sh")}", hosts, false)
    (2 to dataDirs.esDataDirs.size).foreach{
      index =>
        command(s"bash -c 'sleep $index ; cd ${instDirs.globalLocation}/cm-well/app/es/cur; " +
          s"${startScript(s"./start$index.sh")}' > /dev/null 2> /dev/null &", hosts, false)
    }
  }

  override def startCassandra(hosts: GenSeq[String]): Unit = {
    var s = 1
    for (host <- hosts) {
        val sb = new StringBuilder
        sb ++= "bash -c 'sleep "
        sb ++= s.toString
        sb ++= " ; cd "
        sb ++= instDirs.globalLocation
        sb ++= "/cm-well/app/cas/cur/; "
        sb ++= startScript("./start.sh")
        sb ++= "' > /dev/null 2> /dev/null &"
        command(sb.result(), Seq(host), false)
        s += 1
    }
  }

  override def initCassandra(hosts: GenSeq[String]): Unit = {
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}", hosts(0), false)
    Try(CassandraLock().waitForModule(ips(0), 1))
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}", hosts(1), false)
    Try(CassandraLock().waitForModule(ips(0), 2))
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}", hosts(2), false)
    Try(CassandraLock().waitForModule(ips(0), 3))

    var s = 1
    for (host <- hosts.drop(3)) {
      command(
        s"bash -c 'sleep ${s} ; cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript(s"./start.sh")}' > /dev/null 2> /dev/null &",
        Seq(host),
        false
      )
      s += 1
    }

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
    (2 to dataDirs.esDataDirs.size).foreach{
      index =>
        command(s"bash -c 'sleep $index ; cd ${instDirs.globalLocation}/cm-well/app/es/cur ; ${startScript(s"./start$index.sh")}'&", hosts, false)
    }
  }


  override def unprepareMachines(hosts: GenSeq[String]): Unit = {
    throw new Exception(
      "Nothing was done! Please use unprepareMachines with sudoer only this way the network configuration will be changed"
    )
  }


  override def getSeedNodes: List[String] = ips.take(3).toList

  override def getNewHostInstance(ipms: Seq[String]): Host = {
    this.copy(clusterIps = ipms)
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
    val casDataDirs = (1 to dataDirs.casDataDirs.size).map(ResourceBuilder.getIndexedName("cas", _))
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
        casDataDirs = casDataDirs,
        numOfCores = calculateCpuAmount,
        diskOptimizationStrategy = diskOptimizationStrategy,
        casUseCommitLog = casUseCommitLog
      )

        val esSubDivs = for(i <- 1 to dataDirs.esDataDirs.size)
        yield {
          ElasticsearchConf(
            clusterName = clusterName,
            nodeName = s"$host-$i",
            masterNode = false,
            dataNode = true,
            expectedNodes = getEsSize,
            numberOfReplicas = 2,
            seeds = getSeedNodes.mkString(","),
            home = homeDir,
            resourceManager = esAllocations,
            dir = ResourceBuilder.getIndexedName("es", i),
            template = "elasticsearch.yml",
            listenAddress = host,
            masterNodes = esMasters,
            sName = s"${ResourceBuilder.getIndexedName("start", i)}.sh",
            index = i,
            rs = IpRackSelector(),
            g1 = g1,
            hostIp = host
          )
        }

        val esMaster = ElasticsearchConf(
          clusterName = clusterName,
          nodeName = s"$host-master",
          masterNode = true,
          dataNode = false,
          expectedNodes = getEsSize ,
          numberOfReplicas = 2,
          seeds = getSeedNodes.mkString(","),
          home = homeDir,
          resourceManager = esAllocations,
          dir = "es-master",
          template = "elasticsearch.yml",
          listenAddress = host,
          masterNodes = esMasters,
          sName = "start-master.sh",
          index = dataDirs.esDataDirs.size + 1,
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
        numOfPartitions = ips.size,
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
        seeds = ips.take(3).mkString(","),
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
        esMaster,
        web,
        cw,
        ctrl,
        dcConf,
        zookeeper,
        kafka,
        bg,
        cas
      ) ++ esSubDivs
    }
  }

}
