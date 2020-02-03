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
package filters

import javax.inject._

import akka.stream.Materializer
import play.api.mvc.{Filter, RequestHeader, Result, Results}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Success, Try}
import cmwell.ws.Settings._
import trafficshaping._

/**
  * Created by michael on 6/29/16.
  */
class TrafficShapingFilter @Inject()(implicit override val mat: Materializer, ec: ExecutionContext) extends Filter {

  def reqType(req: RequestHeader): String = {
    val opOpt = req.getQueryString("op")
    val segs = req.path.split("/")
    val path = Try(req.path.split("/")(1))

    (opOpt, path) match {
      case (Some(op), _)                           => op
      case (None, Success(p)) if p.startsWith("_") => p
      case _                                       => "get"
    }
  }

  def collectData(resultFuture: Future[Result], ip: String, requestType: String, startTime: Long): Unit = {
    if (ip != "127.0.0.1") {
      resultFuture.foreach { r =>
        val requestDuration = System.currentTimeMillis() - startTime
        TrafficShaper.addRequest(ip, requestType, requestDuration)
      }

    }
  }

  def collectData(ip: String, requestType: String, startTime: Long): Unit = {
    if (ip != "127.0.0.1") {
      val requestDuration = System.currentTimeMillis() - startTime
      TrafficShaper.addRequest(ip, requestType, requestDuration)
    }
  }

  def isNeedTrafficShapping(ip: String, requestType: String): Boolean = {
    val untrackedRequests = Vector("_in", "_ow")
    !untrackedRequests.contains(requestType)
  }

  override def apply(next: (RequestHeader) => Future[Result])(request: RequestHeader): Future[Result] = {
    import Math._

    val ip = request.attrs(Attrs.UserIP)
    lazy val resultFuture = next(request)
    val startTime = request.attrs(Attrs.RequestReceivedTimestamp)
    val maxDurationMillis = maxRequestTimeSec * 1000
    val penalty = TrafficShaper.penalty(ip)
    val requestType = reqType(request)

    if (TrafficShaper.isEnabled && isNeedTrafficShapping(ip, requestType))
      penalty match {
        case NoPenalty =>
          collectData(resultFuture, ip, requestType, startTime)
          resultFuture
        case DelayPenalty =>
          collectData(resultFuture, ip, requestType, startTime)
          resultFuture.flatMap { res =>
            val currentTime = System.currentTimeMillis()
            val reqDurationMillis = currentTime - startTime
            val penalty = min(reqDurationMillis, maxDurationMillis - reqDurationMillis).max(0)
            cmwell.util.concurrent.delayedTask(penalty.millis) { res }
          }
        case FullBlockPenalty =>
          cmwell.util.concurrent.delayedTask(maxDurationMillis.millis) {
            collectData(ip, requestType, startTime)
            Results.ServiceUnavailable("Please reduce the amount of requests")
          }
      } else
      resultFuture
  }
}
