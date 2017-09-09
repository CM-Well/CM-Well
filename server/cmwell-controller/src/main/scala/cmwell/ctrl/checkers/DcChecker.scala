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


package cmwell.ctrl.checkers

import akka.actor.Cancellable
import cmwell.ctrl.checkers.BatchChecker._
import cmwell.ctrl.config.Config
import cmwell.ctrl.utils.{HttpUtil, ProcUtil}
import cmwell.stats.Stats.Settings
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.LazyLogging
import k.grid.{ClientActor, RestartJvm, Grid}
import k.grid.service.{KillService, LocalServiceManager}
import play.libs.Json

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

/**
 * Created by michael on 8/19/15.
 */

case class ActiveDcSync(id : String, qp : Option[String], host : String)
case class InfotonDiff(me : Long, remote : Long) {
  def isEqual = me == remote
}

case class IndextimeDiff(me : Long, remote : Long) {
  def isEqual = me == remote
}

case class DcDiff(infotonDiff : InfotonDiff, indextimeDiff : IndextimeDiff, remoteHost : String, id : String)

object DcChecker  extends Checker with RestarterChecker with LazyLogging {

  override val storedStates: Int = 10

  private def getActiveDcSyncs(host : String) : Future[Set[ActiveDcSync]] = {

     HttpUtil.httpGet(s"http://$host/meta/sys/dc/?op=search&qp=type::remote&format=json&with-data").map {
      r =>
        val json: JsonNode = Json.parse(r.content)
        val infotons = json.get("results").get("infotons")
        val iterator = infotons.iterator

        var dcNodes = Set.empty[JsonNode]
        while(iterator.hasNext) {
          val jsonNode = iterator.next
          dcNodes = dcNodes + jsonNode
        }

        dcNodes.map{
          dcNode =>
            val fields = dcNode.get("fields")
            val id = fields.get("id").iterator().next.asText()
            val qp = if (fields.has("qp")) Some(fields.get("qp").iterator().next.asText()) else None
            val location = fields.get("location").iterator().next.asText()
            ActiveDcSync(id, qp, location)
        }
    }
  }

  private def getLastIndexTime(host : String, dc : String, qp: Option[String]): Future[Long] = {
    val qpPart = qp.fold("?")(qp => s"?qp=$qp&")
    HttpUtil.httpGet(s"http://$host/proc/dc/$dc${qpPart}format=json&with-data").map {
      r =>
        val json: JsonNode = Json.parse(r.content)
        json.get("fields").get("lastIdxT").iterator().next().asLong()
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
    LocalServiceManager.mapping("DataCenterSyncManager").map(jvm => Grid.selectActor(ClientActor.name, jvm) ! RestartJvm)
  }

  override def check: Future[ComponentState] = {
    val host = s"${Config.webAddress}:${Config.webPort}"
    val result = getActiveDcSyncs(host).flatMap {
      activeDcSync =>
        val futures = activeDcSync.map {
          dc =>
            val id = s"${dc.id}${dc.qp.fold("")(qp => s"?qp=$qp")}"
            val remoteHost = dc.host
            val remoteLastIndexTimeF = getLastIndexTime(remoteHost, dc.id, dc.qp).recover{case err : Throwable => -1L}
            val localLastIndexTimeF = getLastIndexTime(host, dc.id, dc.qp).recover{case err : Throwable => -1L}
            val aggregated = for {
              remoteLastIndexTime <- remoteLastIndexTimeF
              localLastIndexTime <- localLastIndexTimeF
            } yield DcDiff(InfotonDiff(0L /*localNumberOfInfotons*/ , 0L /*remoteNumberOfInfotons*/), IndextimeDiff(localLastIndexTime, remoteLastIndexTime), remoteHost, id)
            aggregated.map {
              dcDiff =>
                if(dcDiff.indextimeDiff.remote != -1L)
                  getLastStates(1).headOption match {
                    case Some(sb : DcStatesBag) =>
                      val lastState = sb.states.get(id)
                      logger.debug(s"DcChecker: Last state was $lastState, remote index time: ${dcDiff.indextimeDiff.remote}")
                      val newState = lastState match {
                        case Some(DcSyncing(dcId, _dcDiff, ch, _)) =>
                          if(dcDiff.indextimeDiff.isEqual) DcSyncing(id, dcDiff, Config.listenAddress)
                          else if (dcDiff.indextimeDiff.me != _dcDiff.indextimeDiff.me) DcSyncing(id, dcDiff, Config.listenAddress)
                          else DcNotSyncing(id, dcDiff,1, Config.listenAddress)
                        case Some(DcNotSyncing(dcId, _dcDiff, nsc, ch, _)) =>
                          if(dcDiff.indextimeDiff.isEqual) DcSyncing(id, dcDiff, Config.listenAddress)
                          else if (dcDiff.indextimeDiff.me != _dcDiff.indextimeDiff.me) DcSyncing(id, dcDiff, Config.listenAddress)
                          else DcNotSyncing(id, dcDiff,1+nsc, Config.listenAddress)
                        case _ => DcSyncing(id, dcDiff, Config.listenAddress)
                      }
                      id -> newState
                    case _ => id -> DcSyncing(id, dcDiff, Config.listenAddress)
                  }
                else {
                  logger.debug("DcChecker: Couldn't retrieve remote last index time.")
                  getLastStates(1).headOption match {
                    case Some(sb : DcStatesBag) => sb.states.get(id) match {
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
            }.recover { // Probably unreachable because of the recovers in indexTime retrievals, but just in case...
              case err : Throwable =>
                getLastStates(1).headOption match {
                  case Some(s : DcCouldNotGetDcStatus) => id -> DcCouldNotGetDcStatus(id,DcDiff(InfotonDiff(0L, 0L), (IndextimeDiff(0L,0L)), remoteHost, id), s.errorCounter+1, Config.listenAddress)
                  case _ => id -> DcCouldNotGetDcStatus(id,DcDiff(InfotonDiff(0L, 0L), (IndextimeDiff(0L,0L)), remoteHost, id), 1, Config.listenAddress)
                }

            }
        }
        cmwell.util.concurrent.successes(futures).map {
          set =>
            DcStatesBag(set.toMap, Config.listenAddress)
        }
    }/*.recover {
      case err : Throwable => DcCouldNotGetDcSyncList(Settings.host)
    }*/
    result
  }

}
