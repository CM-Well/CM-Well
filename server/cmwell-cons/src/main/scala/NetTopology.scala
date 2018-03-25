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
import scala.io.Source

abstract class NetTopology(amountPerMachine : Int = 4, cidr : Int = 16, val persistent : Boolean = true) {
  def getTopologyMap(mappings : IpMappings) : GenMap[String, List[String]] = {
    mappings.getMap
  }

  def getCidr = cidr


  def getNetMask : String = {
    val vec1 = for(i <- 1 to cidr) yield 1
    val vec0 = for(i <- 1 to 32 - cidr) yield 0
    val vec = vec1 ++ vec0

    val seg1 = Integer.parseInt(vec.take(8).mkString,2)
    val seg2 = Integer.parseInt(vec.drop(8).take(8).mkString,2)
    val seg3 = Integer.parseInt(vec.drop(16).take(8).mkString,2)
    val seg4 = Integer.parseInt(vec.drop(24).take(8).mkString,2)

    s"$seg1.$seg2.$seg3.$seg4"
  }

  def getBroadcastAddress(ip : String) {

  }
}

/*case class GenTopology(netSegment : String = "192.168" , amountPerMachine : Int = 4, cidr : Int = 16) extends NetTopology(amountPerMachine, cidr) {
  override def getTopologyMap(hosts : GenSeq[String]) : GenMap[String, List[String]] = {
    val hostsWithIndexes = hosts.zipWithIndex
    hostsWithIndexes.map{
      host =>
        host._1 -> (1 to amountPerMachine).toList.map(item => s"$netSegment.${host._2}.$item")
    }.toMap
  }
}*/

/*object AliasTopology {
  def apply(path : String, amountPerMachine : Int, cidr : Int) {
    AliasTopology(Source.fromFile(path).mkString.split("\n").toList,amountPerMachine,cidr)
  }
}*/

case class FixedTopology(amountPerMachine : Int, cidr : Int) extends NetTopology(amountPerMachine, cidr)
case class VLanTopology(tag : Int , amountPerMachine : Int = 4, cidr : Int = 16) extends NetTopology(amountPerMachine, cidr)