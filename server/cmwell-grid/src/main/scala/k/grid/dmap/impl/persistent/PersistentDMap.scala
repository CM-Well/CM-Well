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


package k.grid.dmap.impl.persistent


import java.io.{FileNotFoundException, PrintWriter, File}

import com.typesafe.scalalogging.LazyLogging
import k.grid.dmap.impl.persistent.json.PersistentDMapDataFileParsingException
import k.grid.{Grid, Config}
import k.grid.dmap.api._

import scala.util.{Failure, Success, Try}
import spray.json._
import scala.concurrent.duration._
import json.MapDataJsonProtocol._
import scala.concurrent.ExecutionContext.Implicits.global
/**
 * Created by michael on 5/29/16.
 */
object PersistentDMap extends DMapFacade {
  override def masterType: DMapActorInit = DMapActorInit(classOf[PersistentMaster], "PersistentMaster")

  override def slaveType: DMapActorInit = DMapActorInit(classOf[PersistentSlave], "PersistentSlave")
}

class PersistentMaster extends DMapMaster {
  override val facade: DMapFacade = PersistentDMap
  override def onStart: Unit = {

  }
}

class PersistentSlave extends DMapSlave with LazyLogging {

  Grid.system.scheduler.schedule(5.seconds, 1.second, self, WriteData)

  case class MapHolder(m : Map[String, SettingsValue], timestamp : Long)

  case object NewData extends DMapMessage {
    override def act: Unit = {
      hasNewData = true
    }
  }

  case object WriteData extends DMapMessage {
    override def act: Unit = {
      val m = facade.sm
      if(hasNewData){
        writeMap(MapData(m, lastTimestamp))
        hasNewData = false
      }

    }
  }

  var hasNewData : Boolean = false

  private val dataFile = new File(s"${Grid.persistentDmapDir}/${Config.clusterName}")

  def readMap : Option[MapData] = {
    val content = Try(scala.io.Source.fromFile(dataFile).getLines().mkString("\n").parseJson.convertTo[MapData]) match {
      case Success(c) => Some(c)
      case Failure(e) if e.isInstanceOf[FileNotFoundException] => None
      case Failure(e) => {
        logger.error(e.getMessage, e)
        None
      }
    }
    content
  }

  def writeMap(md : MapData) = {
    val content = md.toJson.toString
    new PrintWriter(dataFile) { write(content); close }
  }

  override val facade: DMapFacade = PersistentDMap
  override def onStart: Unit = {

    if(Grid.isController){
      import java.io.File
      logger.info(s" *** Will use data dir: ${Grid.persistentDmapDir}")
      Try(new File(Grid.persistentDmapDir).mkdir())

      val mdOpt = readMap

      mdOpt.foreach { md =>
        lastTimestamp = md.timestamp
        facade.sm = md.m
      }
    }
  }

  override protected def onUpdate(oldMap: Map[String, SettingsValue], newMap: Map[String, SettingsValue], timestamp: Long): Unit = {
    if(Grid.isController)
      self ! NewData
  }
}