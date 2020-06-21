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

import k.grid.Grid
import k.grid.dmap.api.SettingsLong
import k.grid.dmap.impl.persistent.PersistentDMap

/**
  * Created by michael on 6/29/16.
  */
case class RequestCounter(var counter: Long = 0L) {
  def inc(duration: Long) = counter += duration
}

object PenaltyStage {
  val severity = Vector(NoPenalty, DelayPenalty, FullBlockPenalty)
  def chooseHighest(p1: PenaltyStage, p2: PenaltyStage): PenaltyStage = {
    val i1 = severity.indexOf(p1)
    val i2 = severity.indexOf(p2)

    if (i1 >= i2) p1 else p2
  }
}

trait PenaltyStage {
  def next: PenaltyStage
  def prev: PenaltyStage
}

case object NoPenalty extends PenaltyStage {
  override def prev: PenaltyStage = NoPenalty
  override def next: PenaltyStage = DelayPenalty

  override def toString: String = "Pass through"
}

case object DelayPenalty extends PenaltyStage {
  override def prev: PenaltyStage = NoPenalty
  override def next: PenaltyStage = FullBlockPenalty

  override def toString: String = "Traffic shape"
}

case object FullBlockPenalty extends PenaltyStage {
  override def prev: PenaltyStage = DelayPenalty
  override def next: PenaltyStage = FullBlockPenalty

  override def toString: String = "Circuit break"
}

case class RequestorCounter(var penalty: PenaltyStage = NoPenalty,
                            var requestsCounters: Map[String, RequestCounter] = Map.empty[String, RequestCounter]) {

  def requestsTime: Long = {
    requestsCounters.values.map(_.counter).sum
  }

  def inc(request: String, duration: Long) = {
    requestsCounters.get(request) match {
      case Some(req) => req.inc(duration)
      case None =>
        val rc = new RequestCounter
        rc.inc(duration)
        requestsCounters = requestsCounters.updated(request, rc)
    }
  }

  def reset = {
//    requestsCounters = Map.empty[String, RequestCounter]
    requestsCounters = requestsCounters.map { rc =>
      rc._1 -> rc._2.copy(counter = (rc._2.counter * 0.7).toLong)
    }
  }
}

object TrafficShaper {
  // We by design use a mutable object in an immutable map.
  private[trafficshaping] var lastRequests = Map.empty[String, RequestorCounter]

  Grid.create(classOf[CongestionAnalyzer], CongestionAnalyzer.name)

  def isEnabled: Boolean = {
    import DMapKeys._
    PersistentDMap.get(THRESHOLD_FACTOR).flatMap(_.as[Long]) match {
      case Some(l) if l > 0L => true
      case _                 => false
    }
  }

  def penalty(ip: String) = {
    if (ip == "127.0.0.1") NoPenalty
    else
      lastRequests.get(ip) match {
        case Some(uInfo) => uInfo.penalty
        case None        => NoPenalty
      }
  }

  def addRequest(ip: String, reqType: String, duration: Long) {
    lastRequests.get(ip) match {
      case Some(uInfo) => uInfo.inc(reqType, duration)
      case None =>
        val uInfo = new RequestorCounter
        uInfo.inc(reqType, duration)
        lastRequests = lastRequests.updated(ip, uInfo)
    }
  }

  def getRequestors = lastRequests.map { case (k, v) => k -> v }
}
