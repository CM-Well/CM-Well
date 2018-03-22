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
/*
import scala.collection.{GenMap, GenSeq}
import scala.util.Try


case class GridLocal(user : String ,
                     password : String ,
                     ipMappings : IpMappings,
                     inet : String,
                     clusterName : String,
                     dataCenter : String,
                     dataDirs : DataDirs,
                     instDirs : InstDirs,
                     esMasters : Int,
                     topology : NetTopology,
                     allocationPlan : ModuleAllocations,
                     rackNum : Int,
                     useAuthorization : Boolean,
                     deployJava : Boolean,
                     production : Boolean,
                     g1 : Boolean = false,
                     su : Boolean = true,
                     casRowCacheSize : Int = 0,
                     haProxy : Option[HaProxy] = None,
                     minMembers : Option[Int]
                      )
  extends Host(user, password, ipMappings, dataDirs.casDataDirs.size , inet, clusterName, dataCenter, dataDirs, instDirs, 2 , allocationPlan,useAuthorization,deployJava,production, su,minMembers = minMembers, haProxy = haProxy) {

  override def getElasticsearchMasters : Int = esMasters
  override def getCassandraHostIDs(host: String): String = ???

  override def getMode: String = "gridLocal"

  /*override def addNodes(ips: List[String]): Host = ???*/

  override def getSeedNodes : List[String] = {
    val m = topology.getTopologyMap(ipMappings)
    List(m.get(ips(0)).get(0), m.get(ips(0)).get(1),m.get(ips(0)).get(2))
  }



  override def startCassandra(h: GenSeq[String]): Unit = {
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}", h, false)

    (2 to dataDirs.casDataDirs.size).foreach{
      index =>
        command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript(s"./start$index.sh")}", h, false)
    }
  }

  override def startElasticsearch(h: GenSeq[String]): Unit = {
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur/; ${startScript("./start-master.sh")}", h, false)
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur/; ${startScript("./start.sh")}", h, false)
    (2 to dataDirs.esDataDirs.size).foreach{
      index =>
        command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur/; ${startScript(s"./start$index.sh")}", h, false)
    }
  }

  override def initElasticsearch(h: GenSeq[String]): Unit = {
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur/; ${startScript("./start-master.sh")}", h, false)
    Try(ElasticsearchLock().waitForModule(ips(0), 1))
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur/; ${startScript("./start3.sh")}", h, false)
    Try(ElasticsearchLock().waitForModule(ips(0), 2))
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur/; ${startScript("./start.sh")}", h, false)
    command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur/; ${startScript("./start2.sh")}", h, false)



    (4 to dataDirs.esDataDirs.size).foreach{
      index =>
        command(s"cd ${instDirs.globalLocation}/cm-well/app/es/cur/; ${startScript(s"./start$index.sh")}", h, false)
    }
  }

  override def initCassandra(h: GenSeq[String]): Unit = {
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start.sh")}", h, false)
    Try(CassandraLock().waitForModule(ips(0), 1))
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start3.sh")}", h, false)
    Try(CassandraLock().waitForModule(ips(0), 2))
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript("./start2.sh")}", h, false)



    (4 to dataDirs.casDataDirs.size).foreach{
      index =>
        command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur/; ${startScript(s"./start$index.sh")}", h, false)
    }
  }

  def addInetAliases(m : GenMap[String, List[String]], cidr : Int) {
    m.foreach {
      tuple =>
        var index = 0
        tuple._2.foreach {
          ip =>
            command(s"ifconfig $inet:$index $ip/$cidr up",tuple._1,true)
            index += 1
        }
    }
  }

  def removeInetAliases(r : Range, i : GenSeq[String] = ips.par) {
    r.foreach {
      index =>
        command(s"ifconfig $inet:$index down", i, true)
    }
  }


  override def prepareMachines(hosts : GenSeq[String] = ips.par, rootPass : String, userPass : String) {
    super.prepareMachines(hosts, rootPass, userPass)
    createNetwork(topology, false)
  }

  override def pingAddress = {
    val m = topology.getTopologyMap(ipMappings)
    m.get(ips(0)).get(0)
  }

  override def mkScripts(hosts : GenSeq[String] = ips.par) : GenSeq[ModuleConf] = {
    val aloc = allocationPlan.getJvmAllocations
    val casAllocations = aloc.cas//DefaultAlocations(4000,4000,1000,0)
    val esAllocations = aloc.es//DefaultAlocations(6000,6000,400,0)

    val esMasterAllocations = allocationPlan.getElasticsearchMasterAllocations

    val bgAllocations = aloc.bg//DefaultAlocations(1000,1000,512,0)
    val wsAllocations = aloc.ws
    val ctrlAllocations = aloc.ctrl
    val topologyMap = topology.getTopologyMap(ipMappings)
    val homeDir = s"${instDirs.globalLocation}/cm-well"
    hosts.flatMap{
      host =>
        val aliases = topologyMap(host)

        val cas = CassandraConf(homeDir, getSeedNodes.mkString(",") , clusterName, casAllocations, "GossipingPropertyFileSnitch" , "cas" , "cas",casRowCacheSize, 3, "cassandra.yaml", aliases(0), aliases(0) , "start.sh" ,1, IndexRackSelector(rackNum),g1, host)

        val casSubDivs = for(i <- 2 to dataDirs.casDataDirs.size)
        yield {
          CassandraConf(homeDir, getSeedNodes.mkString(",") , clusterName, casAllocations, "GossipingPropertyFileSnitch" , s"ccl$i" , s"cas$i",casRowCacheSize, 3, "cassandra.yaml", aliases(i-1), aliases(i-1) , s"start$i.sh",i,IndexRackSelector(rackNum),g1, host)
        }


        val es = ElasticsearchConf(clusterName, aliases(0) , dataDirs.esDataDirs.size,2, getSeedNodes.mkString(","), homeDir, esAllocations, "es","es-hub.yml", aliases(0),  esMasters, "start.sh",1,IndexRackSelector(rackNum),g1, host)
        val esSubDivs = for(i <- 2 to dataDirs.esDataDirs.size)
        yield {
          ElasticsearchConf(clusterName, aliases(i-1), dataDirs.esDataDirs.size,2, getSeedNodes.mkString(","), homeDir,  esAllocations, s"es$i","es.yml", aliases(i-1),  esMasters, s"start$i.sh",i,IndexRackSelector(rackNum),g1, host)
        }

        val esMaster = ElasticsearchConf(clusterName, s"${aliases(0)}-master", dataDirs.esDataDirs.size,2, getSeedNodes.mkString(","), homeDir, esMasterAllocations, "es-master","es-master.yml", aliases(0), esMasters, "start-master.sh",dataDirs.esDataDirs.size+1,IndexRackSelector(rackNum),g1, host)
        val bg = BatchConf(homeDir , clusterName , dataCenter , aliases(0) , bgAllocations, "start.sh", true ,Deployment.BatchProps.LogLevel.getLogLevel,deb , host)
        val web = WebConf(homeDir, clusterName, dataCenter, aliases(0), wsAllocations , "start.sh",useAuthorization,Deployment.WebserviceProps.LogLevel.getLogLevel,deb, host)
        val cw = CwConf(homeDir, clusterName, dataCenter, host, wsAllocations, "cw-start.sh", Deployment.WebserviceProps.LogLevel.getLogLevel, deb, host)
        val ctrl = CtrlConf(homeDir, "start.sh", host , clusterName,ctrlAllocations , true , host , user, deb, host)
        List(
          cas,es,esMaster,bg,web,cw,ctrl
        ) ++ casSubDivs ++esSubDivs
    }
  }

  override def getNewHostInstance(ipms: IpMappings): Host = ???
}
 */
