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
                      ipMappings: IpMappings,
                      inet: String,
                      clusterName: String,
                      dataCenter: String,
                      dataDirs: DataDirs,
                      instDirs: InstDirs,
                      esMasters: Int,
                      topology: NetTopology,
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
                      withElk: Boolean = false)
    extends Host(
      user,
      password,
      ipMappings,
      ipMappings.getIps.size * dataDirs.casDataDirs.size,
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
      withElk = withElk
    ) {
  //var persistentAliases = false
  override def getElasticsearchMasters: Int = esMasters
  //def hosts = ips.map(ip => s"${user}@${ip}")
  override def getCassandraHostIDs(host: String): String = ???

  /*  def addInerNetwork(hosts : GenSeq[String] = ips.par, persistent : Boolean = false) = {
      createNetwork(topology.getTopologyMap(hosts), topology.getCidr, persistent)
    }*/

  def removeNetwork(sudoer: Credentials): Unit = removeNetwork(ips.par, sudoer)
  def removeNetwork(hosts: GenSeq[String], sudoer: Credentials) {
    topology match {
      case n: VLanTopology =>
        val tag = n.tag
        val path = "/etc/sysconfig/network-scripts"
        val fileName = s"ifcfg-$inet.$tag"
        command(s"sudo rm $path/$fileName", hosts, true, Some(sudoer))
        command(s"sudo ifconfig $inet.$tag down", hosts, true, Some(sudoer))
        for (i <- 0 to n.amountPerMachine - 1) {
          val fileName = s"ifcfg-$inet.$tag:$i"
          command(s"sudo rm $path/$fileName", hosts, true, Some(sudoer))
          command(
            s"sudo ifconfig $inet.$tag:$i down",
            hosts,
            true,
            Some(sudoer)
          )
        }
        command("sudo service network restart", hosts, true, Some(sudoer))
    }
  }

  def removeInetAliases(r: Range,
                        i: GenSeq[String] = ips.par,
                        sudoer: Credentials) {
    r.foreach { index =>
      command(s"sudo ifconfig $inet:$index down", i, true, Some(sudoer))
    }
  }

  def interNetAddress(x: Int, y: Int) =
    topology.getTopologyMap(ipMappings).get(ips(x)).get(y)

  //if(!validateNumberOfMasterNodes(esMasters, ips.size)) throw new Exception("Bad number of Elasticsearch master nodes")
  override def nodeToolPath =
    s"${super.nodeToolPath} -h ${interNetAddress(0, 0)}"

  override def pingAddress = interNetAddress(0, 0)
  override def getMode: String = "gridSubDiv"

  override def getSeedNodes: List[String] = {
    val m = topology.getTopologyMap(ipMappings)
    List(m.get(ips(0)).get(0), m.get(ips(1)).get(0), m.get(ips(2)).get(0))
  }

  override def startElasticsearch(hosts: GenSeq[String]): Unit = {
    command(
      s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript("./start-master.sh")}",
      ips.par.take(esMasters).intersect(hosts),
      false
    )
    command(
      s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript("./start.sh")}",
      hosts,
      false
    )
    (2 to dataDirs.esDataDirs.size).foreach { index =>
      command(
        s"bash -c 'sleep $index ; cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript(s"./start$index.sh")}' > /dev/null 2> /dev/null &",
        hosts,
        false
      )
    }
  }

  override def startCassandra(hosts: GenSeq[String]): Unit = {
    var s = 1
    for (host <- hosts) {
      for (index <- 1 to dataDirs.casDataDirs.size) {
        command(
          s"bash -c 'sleep ${s} ; cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript(
            s"./start${Host.getIndexTxt(index)}.sh"
          )}' > /dev/null 2> /dev/null &",
          Seq(host),
          false
        )
        s += 1
      }
    }
  }

  override def initCassandra(hosts: GenSeq[String]): Unit = {
    command(
      s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}",
      hosts(0),
      false
    )
    Try(CassandraLock().waitForModule(ips(0), 1))
    command(
      s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}",
      hosts(1),
      false
    )
    Try(CassandraLock().waitForModule(ips(0), 2))
    command(
      s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}",
      hosts(2),
      false
    )
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

    for (host <- hosts) {
      for (index <- 2 to dataDirs.casDataDirs.size) {
        command(
          s"bash -c 'sleep ${s} ; cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript(s"./start$index.sh")}' > /dev/null 2> /dev/null &",
          Seq(host),
          false
        )
        s += 1
      }
    }

  }

  override def initElasticsearch(hosts: GenSeq[String]): Unit = {
    command(
      s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript("./start-master.sh")}",
      hosts.take(esMasters),
      false
    )
    Try(ElasticsearchLock().waitForModule(ips(0), esMasters))
    //    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ./start-master.sh", hosts(1), false)
    //    ElasticsearchLock().waitForModule(ips(0), 2)
    //    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ./start-master.sh", hosts.drop(2).take(esMasters - 2), false)
    command(
      s"cd ${instDirs.globalLocation}/cm-well/app/es/cur; ${startScript("./start.sh")}",
      hosts,
      false
    )
    (2 to dataDirs.esDataDirs.size).foreach { index =>
      command(
        s"bash -c 'sleep $index ; cd ${instDirs.globalLocation}/cm-well/app/es/cur ; ${startScript(s"./start$index.sh")}'&",
        hosts,
        false
      )
    }
  }

  override def deployApplication(hosts: GenSeq[String] = ips.par) {
    super.deployApplication(hosts)
    //createCassandraRackProperties(hosts)
  }

  override def prepareMachines(hosts: GenSeq[String] = ips.par,
                               sudoerName: String,
                               sudoerPass: String,
                               userPass: String) {
    super.prepareMachines(hosts, sudoerName, sudoerPass, userPass)
    createNetwork(topology, topology.persistent, hosts, sudoerCredentials.get)
    super.finishPrepareMachines(hosts, sudoerCredentials.get)
  }

  override protected def finishPrepareMachines(hosts: GenSeq[String],
                                               sudoer: Credentials): Unit = {}

  override def unprepareMachines(hosts: GenSeq[String]): Unit = {
    throw new Exception(
      "Nothing was done! Please use unprepareMachines with sudoer only this way the network configuration will be changed"
    )
  }

  def unprepareMachines(hosts: GenSeq[String], sudoer: Credentials) {
    super.unprepareMachines(hosts)
    removeNetwork(hosts, sudoer)
  }

  override def getNewHostInstance(ipms: IpMappings): Host = {
    this.copy(ipMappings = ipms)
  }

  override def getCassandraAddresses(host: String): Seq[String] = {
    ipMappings.getMap(host)
  }

  override def mkScripts(hosts: GenSeq[String]): GenSeq[ComponentConf] = {
    val aloc = allocationPlan.getJvmAllocations
    val casAllocations = aloc.cas //DefaultAlocations(4000,4000,1000,0)
    val esAllocations = aloc.es //DefaultAlocations(6000,6000,400,0)

    val esMasterAllocations = allocationPlan.getElasticsearchMasterAllocations

    val bgAllocations = aloc.bg //DefaultAlocations(1000,1000,512,0)
    val wsAllocations = aloc.ws
    val ctrlAllocations = aloc.ctrl

    val topologyMap = topology.getTopologyMap(ipMappings)
    val homeDir = s"${instDirs.globalLocation}/cm-well"
    hosts.flatMap { host =>
      val aliases = topologyMap(host)

      val casSubDivs = for (i <- 1 to dataDirs.casDataDirs.size)
        yield {
          CassandraConf(
            home = homeDir,
            seeds = getSeedNodes.mkString(","),
            clusterName = clusterName,
            resourceManager = casAllocations,
            snitchType = "GossipingPropertyFileSnitch",
            ccl_dir = ResourceBuilder.getIndexedName("ccl", i),
            dir = ResourceBuilder.getIndexedName("cas", i),
            rowCacheSize = casRowCacheSize,
            replicationFactor = 3,
            template = "cassandra.yaml",
            listenAddress = aliases(i - 1),
            rpcAddress = aliases(i - 1),
            sName = s"${ResourceBuilder.getIndexedName("start", i)}.sh",
            index = i,
            rs = IpRackSelector(),
            g1 = g1,
            hostIp = host
          )

        }

      val esSubDivs = for (i <- 1 to dataDirs.esDataDirs.size)
        yield {
          ElasticsearchConf(
            clusterName = clusterName,
            nodeName = aliases(i - 1),
            masterNode = false,
            dataNode = true,
            expectedNodes = getSize,
            numberOfReplicas = 2,
            seeds = getSeedNodes.mkString(","),
            home = homeDir,
            resourceManager = esAllocations,
            dir = ResourceBuilder.getIndexedName("es", i),
            template = "es.yml",
            listenAddress = aliases(i - 1),
            masterNodes = esMasters,
            sName = s"${ResourceBuilder.getIndexedName("start", i)}.sh",
            index = i,
            rs = IpRackSelector(),
            g1 = g1,
            hostIp = host,
            autoCreateIndex = withElk
          )
        }

      val esMaster = ElasticsearchConf(
        clusterName = clusterName,
        nodeName = s"${aliases(0)}-master",
        masterNode = true,
        dataNode = false,
        expectedNodes = getSize,
        numberOfReplicas = 2,
        seeds = getSeedNodes.mkString(","),
        home = homeDir,
        resourceManager = esAllocations,
        dir = "es-master",
        template = "es.yml",
        listenAddress = aliases(0),
        masterNodes = esMasters,
        sName = "start-master.sh",
        index = dataDirs.esDataDirs.size + 1,
        rs = IpRackSelector(),
        g1 = true,
        hostIp = host,
        autoCreateIndex = withElk
      )

      val bg = BgConf(
        home = homeDir,
        zookeeperServers = ips.take(3),
        clusterName = clusterName,
        dataCenter = dataCenter,
        hostName = aliases(0),
        resourceManager = bgAllocations,
        sName = "start.sh",
        isMaster = host == ips(0),
        partition = ips.indexOf(host),
        logLevel = BgProps(this).LogLevel.getLogLevel,
        debug = deb,
        hostIp = host,
        minMembers = getMinMembers,
        numOfPartitions = ips.size,
        seeds = getSeedNodes.mkString(",")
      )

      val web = WebConf(
        home = homeDir,
        zookeeperServers = ips.take(3),
        clusterName = clusterName,
        dataCenter = dataCenter,
        hostName = aliases(0),
        resourceManager = wsAllocations,
        sName = "start.sh",
        useAuthorization = useAuthorization,
        numOfPartitions = ips.size,
        logLevel = WebserviceProps(this).LogLevel.getLogLevel,
        debug = deb,
        hostIp = host,
        minMembers = getMinMembers,
        seeds = getSeedNodes.mkString(","),
        seedPort = 9301
      )

      val cw = CwConf(
        home = homeDir,
        clusterName = clusterName,
        dataCenter = dataCenter,
        hostName = aliases(0),
        resourceManager = wsAllocations,
        sName = "cw-start.sh",
        logLevel = WebserviceProps(this).LogLevel.getLogLevel,
        debug = deb,
        hostIp = host,
        minMembers = getMinMembers,
        seeds = getSeedNodes.mkString(","),
        seedPort = 9301
      )

      val ctrl = CtrlConf(
        home = homeDir,
        sName = "start.sh",
        seeds = ips.take(3).mkString(","),
        clusterName = clusterName,
        resourceManager = ctrlAllocations,
        singletonStarter = true,
        pingIp = aliases(0),
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
        pingIp = aliases(0),
        hostIp = host,
        minMembers = getMinMembers
      )

      val zookeeper =
        ZookeeperConf(
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

      val kibana = KibanaConf(
        hostIp = host,
        home = homeDir,
        listenPort = "9090",
        listenAddress = host,
        elasticsearchUrl = s"${aliases(0)}:9201"
      )

      val logstash = LogstashConf(
        clusterName = cn,
        elasticsearchUrl = s"${aliases(0)}:9201",
        home = homeDir,
        dir = "logstash",
        sName = "start.sh",
        subdivision = dataDirs.esDataDirs.size,
        hostIp = host
      )

      List(esMaster, web, cw, ctrl, dcConf, zookeeper, kafka, bg) ++ casSubDivs ++ esSubDivs ++ (if (withElk)
                                                                                                   List(
                                                                                                     logstash,
                                                                                                     kibana
                                                                                                   )
                                                                                                 else
                                                                                                   List
                                                                                                     .empty[ComponentConf])
    }
  }

}
