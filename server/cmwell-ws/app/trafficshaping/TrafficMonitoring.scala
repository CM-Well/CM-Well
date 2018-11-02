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

import actions.{MarkdownTable, MarkdownTuple}
import akka.util.Timeout
import cmwell.ctrl.config.Jvms
import cmwell.domain.{FileContent, FileInfoton, VirtualInfoton}
import k.grid.Grid
import java.net.InetAddress
import scala.concurrent.Future
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

/**
  * Created by michael on 7/3/16.
  */
object TrafficMonitoring {

  implicit val timeout = Timeout(5.seconds)

  def mergeCounters(m1: Map[String, RequestCounter], m2: Map[String, RequestCounter]): Map[String, RequestCounter] = {
    val keyset = m1.keySet ++ m2.keySet

    keyset.map { key =>
      val v1 = m1.get(key).map(_.counter).getOrElse(0L)
      val v2 = m2.get(key).map(_.counter).getOrElse(0L)

      key -> RequestCounter(v1 + v2)
    }.toMap
  }

  def nslookup(ip: String): String = Try(InetAddress.getByName(ip)).map(_.getHostName).getOrElse("NA")

  def traffic(path: String, dc: String): Future[Option[VirtualInfoton]] = {
    val setFut = Grid.jvms(Jvms.WS).map { jvm =>
      (Grid.selectActor(CongestionAnalyzer.name, jvm) ? GetTrafficData).mapTo[TrafficData]
    }
    val futSet = cmwell.util.concurrent.successes(setFut)
    futSet.map { set =>
      val trafficData = set.foldLeft(TrafficData(Map.empty[String, RequestorCounter])) {
        case (r1, r2) =>
          val keyset = r1.requestors.keySet ++ r2.requestors.keySet
          val newMap = keyset.map { key =>
            val v1 = r1.requestors.getOrElse(key, RequestorCounter(NoPenalty, Map.empty[String, RequestCounter]))
            val v2 = r2.requestors.getOrElse(key, RequestorCounter(NoPenalty, Map.empty[String, RequestCounter]))

            key -> RequestorCounter(PenaltyStage.chooseHighest(v1.penalty, v2.penalty),
                                    mergeCounters(v1.requestsCounters, v2.requestsCounters))
          }.toMap
          TrafficData(newMap)
      }

      val reqMap = trafficData.requestors

      val reqTypes = reqMap.values.flatMap(_.requestsCounters.keySet).toSet.toSeq.sorted
      val header = MarkdownTuple("IP", "Host", "Proc Duration", "Plan").add(reqTypes)

      val reqTuples = reqMap.map { r =>
        val requestorCounters = r._2.requestsCounters
        val allCounters = reqTypes.map(t => t -> RequestCounter(0L)).toMap ++ requestorCounters
        val counters = allCounters.toSeq.sortBy(_._1).map(_._2.counter.toString)
        MarkdownTuple(r._1, nslookup(r._1), r._2.requestsTime.toString, r._2.penalty.toString).add(counters)
      }.toSeq

      val table = MarkdownTable(header, reqTuples)

      val statusText =
        if (TrafficShaper.isEnabled) "### Traffic shaping is enabled" else "### Traffic shaping is disabled"

      val content = statusText + "\n\n\n" + table.get

      Some(
        VirtualInfoton(FileInfoton(path, dc, None, content = Some(FileContent(content.getBytes, "text/x-markdown")), protocol = None))
      )
    }
  }
}
