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
package k.grid.dmap.impl.persistent

import java.io.{File, FileNotFoundException, PrintWriter}

import com.typesafe.scalalogging.LazyLogging
import k.grid.{Config, Grid}
import k.grid.dmap.api._
import play.api.libs.json.Json

import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._
import json.MapDataJsonProtocol._
import scala.concurrent.ExecutionContext.Implicits.global

object PersistentDMap extends DMapFacade {
  override def masterType: DMapActorInit = DMapActorInit(classOf[PersistentMaster], "PersistentMaster")

  override def slaveType: DMapActorInit = DMapActorInit(classOf[PersistentSlave], "PersistentSlave")
}

class PersistentMaster extends DMapMaster {
  override val facade: DMapFacade = PersistentDMap
  override def onStart: Unit = {}
}

class PersistentSlave extends DMapSlave with LazyLogging {

  Grid.system.scheduler.schedule(5.seconds, 1.second, self, WriteData)

  case class MapHolder(m: Map[String, SettingsValue], timestamp: Long)

  case object NewData extends DMapMessage {
    override def act: Unit = {
      hasNewData = true
    }
  }

  case object WriteData extends DMapMessage {
    override def act: Unit = {
      val m = facade.sm
      if (hasNewData) {
        writeMap(MapData(m, lastTimestamp))
        hasNewData = false
      }

    }
  }

  var hasNewData: Boolean = false

  private val dataFile = new File(s"${Grid.persistentDmapDir}/${Config.clusterName}")

  def readMap: Option[MapData] = {
    val content = Try {
      val src = scala.io.Source.fromFile(dataFile)
      val mData = Json.parse(src.getLines().mkString("\n")).as[MapData]
      src.close()
      mData
    } match {
      case Success(c)                                          => Some(c)
      case Failure(e) if e.isInstanceOf[FileNotFoundException] => None
      case Failure(e) => {
        logger.error(e.getMessage, e)
        None
      }
    }
    content
  }

  def writeMap(md: MapData) = {
    val content = Json.stringify(Json.toJson(md))
    new PrintWriter(dataFile) { write(content); close }
  }

  override val facade: DMapFacade = PersistentDMap
  override def onStart: Unit = {

    if (Grid.isController) {
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

  override protected def onUpdate(oldMap: Map[String, SettingsValue],
                                  newMap: Map[String, SettingsValue],
                                  timestamp: Long): Unit = {
    if (Grid.isController)
      self ! NewData
  }
}
