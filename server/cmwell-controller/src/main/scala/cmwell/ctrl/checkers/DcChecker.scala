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

import akka.actor.Cancellable
import cmwell.ctrl.config.Config
import cmwell.stats.Stats.Settings
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.LazyLogging
import k.grid.{ClientActor, Grid, RestartJvm}
import k.grid.service.{KillService, LocalServiceManager}
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import cmwell.util.http.{SimpleHttpClient => Http}

case class ActiveDcSync(id: String, qp: Option[String], wh: Boolean, transformations: List[String], host: String)
case class InfotonDiff(me: Long, remote: Long) {
  def isEqual = me == remote
}

case class IndextimeDiff(me: Long, remote: Long) {
  def isEqual = me == remote
}

case class DcDiff(infotonDiff: InfotonDiff, indextimeDiff: IndextimeDiff, remoteHost: String, id: String)

object DcChecker extends Checker with RestarterChecker with LazyLogging {

  override val storedStates: Int = 10

  private def getActiveDcSyncs(host: String): Future[Set[ActiveDcSync]] = {

    Http.get(s"http://$host/meta/sys/dc/?op=search&format=json&with-data").map { r =>
      val json: JsValue = Json.parse(r.payload)
      val JsDefined(JsArray(infotons)) = json.\("results").\("infotons")
      val iterator = infotons.iterator

      var dcNodes = Set.empty[JsValue]
      while (iterator.hasNext) {
        val jsonNode = iterator.next
        dcNodes = dcNodes + jsonNode
      }

      dcNodes.map { dcNode =>
        val fields = dcNode.\("fields")
        val id = fields.\("id").\(0).as[String]
        val qp = fields.\("qp").\(0).toOption.flatMap(_.asOpt[String])
        val wh = fields.\("with-history").\(0).toOption.flatMap(_.asOpt[String]).getOrElse("true")
        val transformations = fields.\("transformations").asOpt[List[String]].getOrElse(List.empty)
        val location = fields.\("location").\(0).as[String]
        ActiveDcSync(id, qp, wh == "true", transformations, location)
      }
    }
  }

  private def getLastIndexTime(host: String, dc: String, qp: Option[String], withHistory: Boolean): Future[Long] = {
    val qpPart = qp.fold("?")(qp => s"?qp=$qp&")
    val whPart = if (withHistory) "with-history&" else ""
    Http.get(s"http://$host/proc/dc/$dc$qpPart${whPart}format=json&with-data").map { r =>
      val json: JsValue = Json.parse(r.payload)
      json.\("fields").\("lastIdxT").\(0).as[Long]
    }
  }

  /*
  def getNumberOfInfotons(host : String, dc : String): Future[Long] = {
      val path = s"http://$host/?op=search&qp=system.dc::$dc&format=json&with-data&length=0&recursive"
    HttpUtil.httpGet(path).map {
      r =>
        val json: JsonNode = Json.parse(r.content)
        json.get("results").get("total").asLong()
    }
  }
   */

  override def restartAfter: FiniteDuration = 30.minutes

  override def doRestart: Unit = {
    logger.info(s"Restarting DataCenterSyncManager since it failed for ${restartAfter.toString}")
    // todo: Do this instead of killing the whole jvm.
    //Grid.singletonRef("DataCenterSyncManager") ! KillSingleton
    LocalServiceManager
      .mapping("DataCenterSyncManager")
      .map(jvm => Grid.selectActor(ClientActor.name, jvm) ! RestartJvm)
  }

  private def transform(transformations: List[String], str: String): String = {
    val parsedTransformations = transformations.map(_.split("->") match { case Array(source, target) => (source, target)})
    parsedTransformations.foldLeft(str)((result, kv) => result.replace(kv._1, kv._2))
  }

  override def check: Future[ComponentState] = {
    val host = s"${Config.webAddress}:${Config.webPort}"
    val result = getActiveDcSyncs(host).flatMap { activeDcSync =>
      val futures = activeDcSync.map { dc =>
        val qpStr = dc.qp.fold("")(qp => s"qp=$qp")
        val whStr = if (dc.wh) "with-history" else ""
        val qpAndWhStr = (for (str <- List(qpStr, whStr) if str.nonEmpty) yield str).mkString("&")
        val qpAndWhStrFinal = if (qpAndWhStr.length == 0) "" else "?" + qpAndWhStr
        val transStr = if (dc.transformations.isEmpty) "" else s"&trans:${dc.transformations.mkString("[", ",", "]")}"
        val id = s"${dc.id}$qpAndWhStrFinal$transStr"
        val remoteHost = dc.host
        val remoteLastIndexTimeF = getLastIndexTime(remoteHost, dc.id, dc.qp, dc.wh).recover {
          case err: Throwable => -1L
        }
        val localLastIndexTimeF = getLastIndexTime(host, dc.id, dc.qp.map(transform(dc.transformations, _)), dc.wh).recover { case err: Throwable => -1L }
        val aggregated = for {
          remoteLastIndexTime <- remoteLastIndexTimeF
          localLastIndexTime <- localLastIndexTimeF
        } yield
          DcDiff(InfotonDiff(0L /*localNumberOfInfotons*/, 0L /*remoteNumberOfInfotons*/ ),
                 IndextimeDiff(localLastIndexTime, remoteLastIndexTime),
                 remoteHost,
                 id)
        aggregated
          .map { dcDiff =>
            if (dcDiff.indextimeDiff.remote != -1L)
              getLastStates(1).headOption match {
                case Some(sb: DcStatesBag) =>
                  val lastState = sb.states.get(id)
                  logger.debug(s"DcChecker: Last state for id [$id] was $lastState, remote index time: ${dcDiff.indextimeDiff.remote}")
                  val newState = lastState match {
                    case Some(DcSyncing(dcId, _dcDiff, ch, _)) =>
                      if (dcDiff.indextimeDiff.isEqual) DcSyncing(id, dcDiff, Config.listenAddress)
                      else if (dcDiff.indextimeDiff.me != _dcDiff.indextimeDiff.me)
                        DcSyncing(id, dcDiff, Config.listenAddress)
                      else DcNotSyncing(id, dcDiff, 1, Config.listenAddress)
                    case Some(DcNotSyncing(dcId, _dcDiff, nsc, ch, _)) =>
                      if (dcDiff.indextimeDiff.isEqual) DcSyncing(id, dcDiff, Config.listenAddress)
                      else if (dcDiff.indextimeDiff.me != _dcDiff.indextimeDiff.me)
                        DcSyncing(id, dcDiff, Config.listenAddress)
                      else DcNotSyncing(id, dcDiff, 1 + nsc, Config.listenAddress)
                    case _ => DcSyncing(id, dcDiff, Config.listenAddress)
                  }
                  id -> newState
                case _ => id -> DcSyncing(id, dcDiff, Config.listenAddress)
              } else {
              logger.debug("DcChecker: Couldn't retrieve remote last index time.")
              getLastStates(1).headOption match {
                case Some(sb: DcStatesBag) =>
                  sb.states.get(id) match {
                    case Some(s: DcCouldNotGetDcStatus) =>
                      logger.debug(s"DcChecker: Last state was $s")
                      id -> DcCouldNotGetDcStatus(id, dcDiff, s.errorCounter + 1, Config.listenAddress)
                    case st: Option[ComponentState] =>
                      logger.debug(s"DcChecker: Last state was $st")
                      id -> DcCouldNotGetDcStatus(id, dcDiff, 1, Config.listenAddress)
                  }
                case None => id -> DcCouldNotGetDcStatus(id, dcDiff, 1, Config.listenAddress)
                case _ =>
                  logger.error("Wrong ComponentStatus is in DcStates!")
                  id -> DcCouldNotGetDcStatus(id, dcDiff, 1, Config.listenAddress)
              }
            }
          }
          .recover { // Probably unreachable because of the recovers in indexTime retrievals, but just in case...
            case err: Throwable =>
              getLastStates(1).headOption match {
                case Some(s: DcCouldNotGetDcStatus) =>
                  id -> DcCouldNotGetDcStatus(id,
                                              DcDiff(InfotonDiff(0L, 0L), (IndextimeDiff(0L, 0L)), remoteHost, id),
                                              s.errorCounter + 1,
                                              Config.listenAddress)
                case _ =>
                  id -> DcCouldNotGetDcStatus(id,
                                              DcDiff(InfotonDiff(0L, 0L), (IndextimeDiff(0L, 0L)), remoteHost, id),
                                              1,
                                              Config.listenAddress)
              }

          }
      }
      cmwell.util.concurrent.successes(futures).map { set =>
        DcStatesBag(set.toMap, Config.listenAddress)
      }
    } /*.recover {
      case err : Throwable => DcCouldNotGetDcSyncList(Settings.host)
    }*/
    result
  }

}
