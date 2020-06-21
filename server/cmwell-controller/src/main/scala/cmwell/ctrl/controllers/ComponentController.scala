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
package cmwell.ctrl.controllers

import akka.actor.ActorSelection
import cmwell.ctrl.controllers.CassandraController._
import cmwell.ctrl.config.Config
import cmwell.ctrl.utils.ProcUtil
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid

import scala.concurrent.{blocking, Future}
import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 2/16/15.
  */
abstract class ComponentController(startScriptLocation: String, psIdentifier: String, dirIdentifier: Set[String]) {
  object ComponentControllerLogger extends LazyLogging {
    lazy val l = logger
  }

  protected val startScriptPattern: String = "start[0-9]*.sh"

  def getStartScriptLocation = startScriptLocation

  def getStartScripts(location: String): Set[String] = {
    ProcUtil.executeCommand(s"ls -1 $location/ | grep $startScriptPattern") match {
      case Success(str) =>
        str.trim.split("\n").toSet
      case Failure(err) => Set.empty[String]
    }
  }

  def getDataDirs(location: String, id: String): Set[String] = {
    ProcUtil.executeCommand(s"ls -1 $location | grep $id[0-9]*") match {
      case Success(str) =>
        str.trim.split("\n").toSet
      case Failure(err) => Set.empty[String]
    }
  }

  private def doStart: Unit = {
    getStartScripts(startScriptLocation).foreach { sScript =>
      val runScript = s"HAL=9000 $startScriptLocation/$sScript"
      ProcUtil.executeCommand(runScript)
    }
  }

  def start {
    blocking {
      Future {
        doStart
      }
    }
  }

  private def doStop(forceKill: Boolean = false, tries: Int = 5): Unit = {
    val cmd =
      s"ps aux | grep $psIdentifier | egrep -v 'grep|starter' | awk '{print $$2}' | xargs kill ${if (forceKill) "-9"
      else ""}"
    ComponentControllerLogger.l.info(s"executing $cmd")
    ProcUtil.executeCommand(cmd)
    val isDead =
      ProcUtil.executeCommand(s"ps aux | grep $psIdentifier | egrep -v 'grep|starter' | awk '{print $$2}'").get.isEmpty
    if (!isDead) {
      if (tries > 1) doStop(false, tries - 1) else doStop(true, tries - 1)
    }

  }
  def stop {
    Future {
      blocking {
        doStop()
      }
    }
  }

  def restart: Unit = {
    Future {
      blocking {
        doStop()
        doStart
      }
    }
  }

  def clearData {
    Future {
      blocking {
        dirIdentifier.foreach { id =>
          getDataDirs(s"${Config.cmwellHome}/data/", id).foreach { dir =>
            ProcUtil.executeCommand(s"rm -rf ${Config.cmwellHome}/data/$dir/")
          }
        }
      }
    }
  }
}
