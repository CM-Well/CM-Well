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


package trafficshaping

import com.typesafe.scalalogging.LazyLogging
import k.grid.dmap.impl.persistent.PersistentDMap
import play.api.mvc.{Action, Controller}
import DMapKeys._
import k.grid.dmap.api.SettingsLong
import security.AuthUtils
import javax.inject._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
/**
  * Created by michael on 8/4/16.
  */
@Singleton
class TrafficHandler @Inject()(authUtils: AuthUtils)(implicit ec: ExecutionContext) extends Controller with LazyLogging {
  // todo: remove this.
  def handleTimeout = Action.async { implicit originalRequest =>
    cmwell.util.concurrent.delayedTask(5.seconds)(Future.successful(Ok)).flatMap(identity)
  }

  def handleThresholdFactor = Action { implicit req =>
    val tokenOpt = authUtils.extractTokenFrom(req)
    if (authUtils.isOperationAllowedForUser(security.Admin, tokenOpt)) {
      val thresholdFactor = req.getQueryString("tf").map(_.toLong)
      thresholdFactor match {
        case Some(l) =>
          PersistentDMap.set(THRESHOLD_FACTOR, SettingsLong(l))
          Ok(s"Changed Threshold factor to $l")
        case None =>
          val curValOpt = PersistentDMap.get(THRESHOLD_FACTOR).flatMap(_.as[Long])
          curValOpt match {
            case Some(curVal) if curVal > 0L => Ok(s"""Please provide the parameter "tf". The current value is: $curVal""")
            case _ => Ok(s"""Traffic shaping is disabled. Please provide the parameter "tf" in order to activate it.""")
          }
      }
    }
    else Forbidden("not authorized")
  }
}
