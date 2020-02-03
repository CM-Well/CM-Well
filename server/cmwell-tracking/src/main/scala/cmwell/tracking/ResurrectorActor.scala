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
package cmwell.tracking

import akka.actor.{Actor, ActorIdentity, ActorRef, Cancellable, Identify, Terminated}
import cmwell.driver.Dao
import cmwell.zstore.ZStore
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/**
  * Created by yaakov on 3/15/17.
  */
class ResurrectorActor extends Actor with LazyLogging {
  import TrackingUtilImpl.actorIdFromActorPath

  private lazy val dao =
    Dao(Settings.irwServiceDaoClusterName, Settings.irwServiceDaoKeySpace2, Settings.irwServiceDaoHostName, 9042, initCommands = None)
  private lazy val zStore: ZStore = ZStore.apply(dao)

  override def receive: Receive = activelyWatch(Map.empty, Map.empty, Map.empty)

  def fetchDataFromZStore(trackingId: TrackingId): Future[Array[PathStatus]] = { // todo : Future[Box[Array[PathStatus]]]
    val dirtyDataFut = zStore.get(s"ta-${trackingId.actorId}").map { payload =>
      val s = new String(payload, "UTF-8")
      s.split('\n').map { pair =>
        val s = pair.split('\u0000')
        PathStatus(s(0), TrackingStatus(s(1)))
      }
    }

    val cleanDataFut =
      dirtyDataFut.flatMap(dirtyData => TrackingUtil().cleanDirtyData(dirtyData, trackingId.createTime).map(_.toArray))
    cleanDataFut
  }

  def activelyWatch(resurrectedRefs: Map[String, ActorRef],
                    sendersWaitingForRefs: Map[String, SenderAndCreateTime],
                    schedulingSubscriptions: Map[String, Cancellable]): Receive = {

    case Spawn(name, initialData, restoredData, createTime) =>
      log(s"Spawn($name, $initialData, $restoredData)")

      resurrectedRefs.get(name) match {
        case Some(actorRef) => sender ! actorRef
        case None => {
          val ta = Grid.create(classOf[TrackingActor], name, initialData, restoredData.nonEmpty, createTime)

          // todo: What if ta doesn't response to RestoredData or Ping? We should schedule this...

          if (restoredData.nonEmpty)
            ta ! RestoredData(restoredData)
          else
            ta ! Ping

          val actorId = actorIdFromActorPath(ta)
          context.become(
            activelyWatch(resurrectedRefs,
                          sendersWaitingForRefs.updated(actorId, SenderAndCreateTime(sender(), createTime)),
                          schedulingSubscriptions)
          )
        }
      }

    case TrackingActorSynAck(actorId) =>
      log(s"TrackingActorSynAck($actorId) BTW, sendersWaitingForRefs = $sendersWaitingForRefs")
      val ta = sender()
      context.watch(ta)
      sendersWaitingForRefs.get(actorId).foreach(_.sender ! ta)
      schedulingSubscriptions.get(actorId).foreach(_.cancel())
      context.become(
        activelyWatch(resurrectedRefs.updated(actorId, ta),
                      sendersWaitingForRefs - actorId,
                      schedulingSubscriptions - actorId)
      )

    case ActorData(actorId, data, createTime) =>
      log(s"ActorData($actorId, $data)")
      val paths = data.view.map(_.path).to(Set)
      self ! Spawn(actorId, paths, data, createTime)

    case Resurrect(TrackingId(actorId, createTime)) =>
      log(s"Resurrect($actorId)")
      resurrectedRefs
        .get(actorId)
        .fold[Unit] {
          context.actorSelection(actorId) ! Identify(actorId)
          val cancellable = context.system.scheduler.schedule(3.seconds, 3.seconds, self, RetryPathIdentify(actorId))
          context.become(
            activelyWatch(resurrectedRefs,
                          sendersWaitingForRefs.updated(actorId, SenderAndCreateTime(sender, createTime)),
                          schedulingSubscriptions.updated(actorId, cancellable))
          )
        }(ref => sender.tell(Some(ref), Actor.noSender))

    case ActorIdentity(path: String, Some(ref)) =>
      log(s"ActorIdentity($path,Some($ref))")
      context.watch(ref)
      val actorId = actorIdFromActorPath(path)
      sendersWaitingForRefs.get(actorId).foreach(_.sender ! Some(ref))
      schedulingSubscriptions.get(actorId).foreach(_.cancel())
      context.become(
        activelyWatch(resurrectedRefs.updated(actorId, ref),
                      sendersWaitingForRefs - actorId,
                      schedulingSubscriptions - actorId)
      )

    case ActorIdentity(path: String, None) =>
      log(s"ActorIdentity($path,None)")
      val actorId = actorIdFromActorPath(path)
      schedulingSubscriptions.get(actorId).foreach(_.cancel())
      val createTime = sendersWaitingForRefs(actorId).createTime
      fetchDataFromZStore(TrackingId(actorId, createTime)).onComplete {
        case Success(data) => self ! ActorData(actorId, data, createTime)
        //todo: use Box from zStore to distinguish between zStore successfully fetched nothing and zStore failed to fetch
        case Failure(_) => sendersWaitingForRefs.get(actorId).foreach(_.sender ! None)
      }
      context.become(activelyWatch(resurrectedRefs, sendersWaitingForRefs, schedulingSubscriptions - actorId))

    case Terminated(ref) =>
      log(s"Terminated($ref)")
      context.unwatch(ref)
      val actorId = actorIdFromActorPath(ref)
      context.become(activelyWatch(resurrectedRefs - actorId, sendersWaitingForRefs, schedulingSubscriptions))

    case RetryPathIdentify(path) if schedulingSubscriptions.contains(path) =>
      log(s"RetryPathIdentify($path) && schedulingSubscriptions.contains")
      context.actorSelection(path) ! Identify(path)
      val cancellable = context.system.scheduler.schedule(3.seconds, 3.seconds, self, RetryPathIdentify(path))
      val actorId = actorIdFromActorPath(path)
      context.become(
        activelyWatch(resurrectedRefs, sendersWaitingForRefs, schedulingSubscriptions.updated(actorId, cancellable))
      )

  }

  case class RetryPathIdentify(path: String)
  case class SenderAndCreateTime(sender: ActorRef, createTime: Long)

  private def log(s: String) = logger.debug(s"Tracking: Resurrector: Got $s")
}

case class Spawn(name: String, initialData: Set[String], restoredData: Seq[PathStatus], createTime: Long)
case class Resurrect(trackingId: TrackingId)
case class ActorData(actorId: String, data: Seq[PathStatus], createTime: Long)
case class TrackingActorSynAck(path: String)
