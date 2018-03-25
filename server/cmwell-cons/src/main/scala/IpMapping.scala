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


import java.io.File

  import scala.collection.{GenSeq, GenMap}
  import scala.io.Source
  import scala.util.parsing.json.JSON

/**
 * Created by michael on 10/7/14.
 */

case class IpMapping(ip : String, mappings : Option[Seq[String]])
case class IpMappings(m : List[IpMapping], filePath : Option[String] = None) {
  def getMap : Map[String, List[String]] = {
    m.map{
      mapping =>
        mapping.mappings match {
          case Some(mapValue) => mapping.ip -> mapValue.toList
          case None => mapping.ip -> List.empty
        }
    }.toMap
  }

  def getIps : List[String] = {
    m.map(mp => mp.ip)
  }

  //This is a smart combine that will keep the mapping where they were (e.g. do nothing). Only if it's a new mapping they will be added
  //This way the configuration of the added node will be the same as the old one, if existed.
  def combine(ipm : IpMappings) : IpMappings = {
    val originalIps = m.map(_.ip)
    val combinedMappings = m ++ ipm.m.filterNot(im => originalIps.contains(im.ip))
    IpMappings(combinedMappings ,filePath)
  }

  def remove(ips : List[String]) : IpMappings = {
    IpMappings(m.filterNot(im => ips.contains(im.ip)),filePath)
  }
}

object IpMappings {
  def apply(ips : String*) : IpMappings = {
    IpMappings(ips.toList)
  }

  def apply(ips : List[String]) : IpMappings = {
    IpMappings(ips.map(ip => IpMapping(ip, None)))
  }
}

object IpMappingController {


  def genMappings(ips : List[String], mappings : List[String], amountPerMachine : Int) : IpMappings = {
    val m = ips.zip(mappings.sliding(amountPerMachine,amountPerMachine).toList).toMap
    IpMappings(m.map{
      mapping =>
        IpMapping(mapping._1, Some(mapping._2))
    }.toList)
  }

  def genMappings(ips : List[String]) : IpMappings = {
    IpMappings(
      ips.map(ip => IpMapping(ip, None))
    )
  }

  def genMappings(ipsPath : String) : IpMappings = {
    val ips = Source.fromFile(ipsPath).mkString.split("\n").toList
    genMappings(ips)
  }

  def genMappings(ipsPath : String, mappingsPath : String, amountPerMachine : Int) : IpMappings = {
    val ips = Source.fromFile(ipsPath).mkString.split("\n").toList
    val mappings = Source.fromFile(mappingsPath).mkString.split("\n").toList

    genMappings(ips, mappings, amountPerMachine)
  }





  def getIps(path : String) : List[String] = {
    readMapping(path).getIps
  }

  def readMapping(path : String) : IpMappings = {
    val content = Source.fromFile(path).mkString
    val j = JSON.parseFull(content).asInstanceOf[Option[List[Map[String,Option[Any]]]]]
    j match {
      case Some(mappings) =>
        IpMappings(mappings.map{
          m =>
            val mappingsOpt = m.get("mappings") match {
              case Some(v) => Some(m("mappings").asInstanceOf[List[String]])
              case None => None
            }

            IpMapping(m("ip").asInstanceOf[String], mappingsOpt)
        },Some(path))
      case None => throw new Exception("Bad json")
    }
  }

  def createMappingJson(ipm : IpMappings) : String = {
    val content = ipm.m.map{
      mapping =>
        mapping.mappings match {
          case Some(m) => s""" {"ip":"${mapping.ip}", "mappings":[${m.mkString("\"","\",\"","\"")}]} """
          case None => s""" {"ip":"${mapping.ip}"} """
        }

    }
    s"[\n${content.mkString(",\n")}\n]"
  }

  private def writeToFile(path: String, content: String): Unit = {
    val pw = new java.io.PrintWriter(new File(path))
    try pw.write(content) finally pw.close()
  }

  def writeMapping(ipm : IpMappings, path : String) {
    val j = createMappingJson(ipm)
    writeToFile(path, j)
  }

}
