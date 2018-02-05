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
import scala.util.Success

/**
 * Created by michael on 2/3/15.
 */
class GridDocker(_clusterName : String,
                 _dataCenter : String ,
                  dirsMapping : Seq[(String,String,String,String,String,String)],
                 _allocationPlan : ModuleAllocations,
                 _useAuthorization : Boolean,
                 _deployJava : Boolean,
                 _production : Boolean,
                 _g1 : Boolean = false,
                 _su : Boolean = true,
                 _casRowCacheSize : Int = 256,
                 haProxy : Option[HaProxy] = None,
                 dcTarget : Option[String] = None,
                 minMembers : Option[Int] = None) extends Grid(
  "u" ,
  "" ,
  ipMappings = IpMappings(List("127.0.0.1")),
  "eth0",
  _clusterName,
  _dataCenter,
  DataDirs(Vector("/mnt/d1/cas"),Vector("/mnt/d1/ccl"),Vector("/mnt/d2/es"),List("/mnt/d2/tlog"), List("/mnt/d2/kafka"), "/mnt/d2/zookeeper" , "/mnt/d2/log"),
  InstDirs("/mnt/s1/cmwell-installation", "/opt"),
  3,
  _allocationPlan,
  _useAuthorization,
  _deployJava,
  _production,
  _g1,
  _su,
  _casRowCacheSize,
  haProxy = haProxy, dcTarget = dcTarget,
  minMembers = minMembers) with DockerFunctionallities
{

  private[this] var excludes : List[String] = List.empty[String]

  def exclude(ip : String) = excludes = excludes ++ List(ip)

  override def ips = getDockerIps.toList.diff(excludes)


  override def prepareMachines(hosts : GenSeq[String] = ips.par, sudoerName: String, sudoerPass : String, userPass : String): Unit = {
    ips.foreach {
      ip =>
        command(s"ssh-keygen -R $ip")
    }
    super.prepareMachines(hosts, sudoerName, sudoerPass, userPass)
  }


  override def prepareMachinesNonInteractive(sudoerName: String, sudoerPass : String = "said2000", uPass : String = "said2000", hosts : GenSeq[String] = ips.par) {
    ips.foreach {
      ip =>
        command(s"ssh-keygen -R $ip")
    }
    super.prepareMachinesNonInteractive(sudoerName, sudoerPass, uPass, hosts)
  }

  def initDocker {
    dirsMapping foreach {
      mapping =>
        spawnDockerNode(Some(mapping._1) ,Some(mapping._2),Some(mapping._3),Some(mapping._4),Some(mapping._5),Some(mapping._6))
    }
  }

  override def getNewHostInstance(ipms: IpMappings): Host = {
    super.copy(ipMappings = IpMappings(ips))
  }
}
