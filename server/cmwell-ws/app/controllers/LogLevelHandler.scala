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


package controllers

import cmwell.ctrl.config.Jvms
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid
import k.grid.monitoring.{MonitorActor, SetNodeLogLevel}
import play.api.mvc.{AnyContent, Action, Controller, Request}
import security.AuthUtils
import javax.inject._
import scala.util.Try

@Singleton
class LogLevelHandler  @Inject()(authUtils: AuthUtils) extends Controller with LazyLogging {
  def handleSetLogLevel = Action { implicit req =>
    val tokenOpt = authUtils.extractTokenFrom(req)
    if (authUtils.isOperationAllowedForUser(security.Admin, tokenOpt))
      setLogLevel(req)
    else
      Forbidden("not authorized")
  }

  private def setLogLevel(req: Request[AnyContent]) = {
    val validLogLevels = SetNodeLogLevel.lvlMappings.keySet
    val roleMapping = Map("WEB" -> Jvms.WS, "BG" -> Jvms.BG, "CTRL" -> Jvms.CTRL, "CW" -> Jvms.CW, "DC" -> Jvms.DC)
    val validComponents = roleMapping.keySet
    val validHosts = Grid.availableMachines

    val lvlStr = req.getQueryString("lvl")
    val component = req.getQueryString("comp").map(_.toUpperCase())
    val host = req.getQueryString("host")
    val duration = if (req.getQueryString("duration").isEmpty) Some("10") else req.getQueryString("duration")

    (lvlStr, component, host, duration) match {
      case (Some(l), _, _, _) if !validLogLevels.contains(l.toUpperCase) => BadRequest(s"Bad log level provided, the valid log levels are ${validLogLevels.mkString(", ")}.")
      case (_, Some(c), _, _) if !validComponents.contains(c.toUpperCase) => BadRequest(s"Bad component provided, the valid components are ${validComponents.mkString(", ")}.")
      case (_, _, Some(h), _) if !validHosts.contains(h.toUpperCase) => BadRequest(s"Bad host provided, the valid hosts are ${validHosts.mkString(", ")}.")
      case (_, _, _, Some(d)) if Try(d.toInt).isFailure => BadRequest(s"Bad duration provided, please provide a positive int, or 0 if you wish to keep this log level indefinitely.")
      case (None, _, _, _) => BadRequest(s"No log level provided, the valid log levels are ${validLogLevels.mkString(", ")}")
      case _ =>
        val lvl = lvlStr.flatMap(SetNodeLogLevel.levelTranslator)

        lvl.foreach { level =>
          val members = {
            val f1 = host.map { h =>
              Grid.jvmsAll.filter(_.host == h)
            }.getOrElse(Grid.jvmsAll)

            val f2 = component.map( c => roleMapping(c)).map { c =>
              f1.filter( h =>  h.identity.isDefined && h.identity.get == c)
            }.getOrElse(f1)
            f2
          }

          logger.info(s"Changing the log level of [${members.mkString(", ")}] to $level")

          members.map {
            member =>
              Grid.selectActor(MonitorActor.name, member) ! SetNodeLogLevel(level, duration.map(_.toInt))
          }
        }
        Ok("Done!")
    }
  }
}
