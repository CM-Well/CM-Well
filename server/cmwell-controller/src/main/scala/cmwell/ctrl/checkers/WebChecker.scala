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
package cmwell.ctrl.checkers

import cmwell.ctrl.config.Config
import cmwell.ctrl.controllers.WebserverController
import cmwell.util.http.{SimpleHttpClient => Http}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

/**
  * Created by michael on 12/3/14.
  */
object WebChecker extends Checker with RestarterChecker with LazyLogging {

  override val storedStates: Int = 10

  override def restartAfter: FiniteDuration = 10.minutes

  override def doRestart: Unit = {
    WebserverController.restart
    logger.warn("Webservice was restarted.")
  }

  private val req = s"http://${Config.webAddress}:${Config.webPort}"

  override def check: Future[ComponentState] = {
    val res = Http.get(req)
    val startTime = System.currentTimeMillis()
    res
      .map { x =>
        val now = System.currentTimeMillis()
        if (x.status < 400 || x.status == 503) WebOk((now - startTime).toInt)
        else {
          logger.warn(s"WebChecker.check: got a bad response when GET $req. response = $x")
          WebBadCode(x.status, (now - startTime).toInt)
        }
      }
      .recover {
        case _: Throwable =>
          WebDown()
      }
  }
}
