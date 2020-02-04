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
package trafficshaping

import akka.actor.Actor
import akka.actor.Actor.Receive
import cmwell.ws.Settings
import cmwell.ws.Settings._
import com.typesafe.scalalogging.LazyLogging
import k.grid.dmap.api.SettingsLong
import k.grid.dmap.impl.persistent.PersistentDMap

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by michael on 6/29/16.
  */
case object GetTrafficData
case class TrafficData(requestors: Map[String, RequestorCounter])

object CongestionAnalyzer {
  val name = "CongestionAnalyzer"
  var penalizeTopUsers = 3
}

class CongestionAnalyzer extends Actor with LazyLogging {
  import Settings._
  import DMapKeys._

  val numOfCpus = Runtime.getRuntime.availableProcessors()
  def getThresholdFactor: Long = {
    PersistentDMap
      .get(THRESHOLD_FACTOR)
      .map {
        case SettingsLong(l) => l
        case _               => 0L
      }
      .getOrElse(0L)
  }

  case object AnalyzeCongestion

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    context.system.scheduler.schedule(0.seconds, checkFrequency.seconds, self, AnalyzeCongestion)
  }

  override def receive: Receive = {
    case AnalyzeCongestion =>
      val thresholdFactor = getThresholdFactor
      val threshold = checkFrequency.seconds.toMillis * thresholdFactor

      TrafficShaper.lastRequests.toVector
        .sortBy(_._2.requestsTime)
        .takeRight(CongestionAnalyzer.penalizeTopUsers)
        .foreach {
          case (k, v) =>
            if (v.requestsTime > threshold) {
              v.penalty = v.penalty.next
              logger.info(s"The user $k is getting ${v.penalty}.")
            } else v.penalty = v.penalty.prev
            v.reset
        }

      TrafficShaper.lastRequests = TrafficShaper.lastRequests.filter {
        case (k, v) => v.penalty != NoPenalty || v.requestsTime > 0L
      }

    case GetTrafficData =>
      sender ! TrafficData(TrafficShaper.getRequestors)
  }
}
