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
package controllers

import cmwell.ws.Settings
import com.typesafe.scalalogging.LazyLogging
import javax.inject._
import k.grid.dmap.api.SettingsString
import k.grid.dmap.impl.persistent.PersistentDMap
import play.api.mvc.InjectedController
import security.AuthUtils

@Singleton
class BackPressureToggler @Inject()(authUtils: AuthUtils) extends InjectedController with LazyLogging {

  val BACKPRESSURE_TRIGGER = "cmwell.ws.pushbackpressure.trigger"

  def handleBackpressure = Action { implicit req =>
    val tokenOpt = authUtils.extractTokenFrom(req)
    if (authUtils.isOperationAllowedForUser(security.Admin, tokenOpt)) {
      val thresholdFactor = req.getQueryString("pbp")
      thresholdFactor.map(_.toLowerCase) match {
        case Some("enable") =>
          PersistentDMap.set(BACKPRESSURE_TRIGGER, SettingsString("enable"))
          Ok(s"Changed backpressure trigger to enable")
        case Some("disable") =>
          PersistentDMap.set(BACKPRESSURE_TRIGGER, SettingsString("disable"))
          Ok(s"Changed backpressure trigger to disable")
        case Some("block") =>
          PersistentDMap.set(BACKPRESSURE_TRIGGER, SettingsString("block"))
          Ok(s"Changed backpressure trigger to block")
        case None =>
          val curValOpt = PersistentDMap.get(BACKPRESSURE_TRIGGER).flatMap(_.as[String])
          curValOpt match {
            case Some(v) => Ok(s"Please provide the parameter 'pbp'. The current value is: '$v'")
            case None =>
              Ok(s"Please provide the parameter 'pbp'. No value is set; defaulting to ${Settings.pushbackpressure}")
          }
        case Some(unknown) => BadRequest(s"value [$unknown] is invalid. valid values are: [enable,disable,block]")
      }
    } else Forbidden("not authorized")
  }

  def get: String = PersistentDMap.get(BACKPRESSURE_TRIGGER).fold[String](Settings.pushbackpressure) {
    case SettingsString(v) => v
    case unknown           => s"invalid unknown state: $unknown"
  }
}
