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


package cmwell.tracking

import akka.actor.{Actor, ActorRef, PoisonPill}
import akka.pattern.pipe
import cmwell.tracking.TrackingUtil.zStore
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.{Map => MMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
  * Created by yaakov on 3/15/17.
  */
class TrackingActor(paths: Set[String], restored: Boolean, createTime: Long) extends Actor with LazyLogging {

  case object SelfPersist
  case object SelfCheck
  case object SelfDeadline

  private val selfPersistInterval  = 10.seconds
  private val selfCheckInterval    = 5.seconds
  private val selfDeadlineInterval = 15.minutes

  private def startSchedulers(): Unit = {
    context.system.scheduler.schedule(selfPersistInterval, selfPersistInterval, self, SelfPersist)(executor = context.system.dispatcher)
    context.system.scheduler.scheduleOnce(selfCheckInterval, self, SelfCheck)(executor = context.system.dispatcher)
    context.system.scheduler.scheduleOnce(selfDeadlineInterval, self, SelfDeadline)(executor = context.system.dispatcher)
  }

  private def log(s: String): Unit = logger.debug(s"TrackingActor[${context.self.path.name}]: $s")

  private val myId = TrackingUtil().actorIdFromActorPath(context.self)
  private val imok = TrackingActorSynAck(myId)

  private def isDone = data.values.forall(TrackingStatus.isFinal)

  log(s"CTOR(paths = ${paths.mkString(",")}, restored = $restored)")

  override def preStart(): Unit = {
    if(restored)
      context.become(zombie)
    else
      startSchedulers()

    super.preStart()
  }

  val data: MMap[String, TrackingStatus] = {
    val m = MMap[String, TrackingStatus]()
    if(!restored)
      paths.foreach { path => m += path -> InProgress }
    m
  }

  def dataToExpose: Seq[PathStatus] = data.map {
    case (p, s) if !TrackingStatus.isFinal(s) => PathStatus(p, InProgress) // user should never see PartialDone(m,n)
    case (p, s) => PathStatus(p, s)
  }.toSeq

  def zombie: Receive = {
    case RestoredData(pathStatuses) =>
      log("as a zombie, got RestoredData")
      pathStatuses.foreach { case PathStatus(p, s) => data += p -> s }
      startSchedulers()
      sender() ! imok
      context.become(receive)
  }

  override def receive: Receive = receiveWithSubscriberOpt(None)

  def receiveWithSubscriberOpt(subscriber: Option[ActorRef]): Receive = {
    case Ping =>
      sender ! imok

    case PathStatus(path, PartialDone(1, 1)) =>
      log("Got PartialDone(1, 1)")
      data += path -> Done
      pub(subscriber)

    case PathStatus(path, pd@PartialDone(completed, total)) =>
      log(s"Got $pd")
      data += path -> data.get(path).fold[TrackingStatus](pd) {
        case InProgress => pd
        case PartialDone(c, t) =>
          if (t != total) log(s"oops! for path $path got PartialDone($completed,$total) but current data says PartialDone($c,$t)")
          if (completed + c == t) Done else PartialDone(completed + c, t)
        case other =>
          log(s"$other was not expected")
          pd
      }
      pub(subscriber)

    case PathStatus(path, status) =>
      log("update")
      data += path -> status
      pub(subscriber)

    case Read =>
      log("read")
      sender ! dataToExpose

    case RestoredData(pathStatuses) =>
      log("not a zombie, but got RestoredData")
      pathStatuses.foreach { case PathStatus(p, s) => data += p -> s }
      pub(subscriber)

    case SelfCheck if isDone =>
      log(s"SelfCheck && isDone - will die in $selfCheckInterval...")
      context.system.scheduler.scheduleOnce(selfCheckInterval, self, PoisonPill)(executor = context.system.dispatcher)

    case SelfCheck =>
      log("SelfCheck")
      TrackingUtil().cleanDirtyData(dataToExpose, createTime).map(RestoredData) pipeTo self
      context.system.scheduler.scheduleOnce(selfCheckInterval, self, SelfCheck)(executor = context.system.dispatcher)

    case SelfDeadline if isDone =>
      log("Deadline && isDone")
      self ! PoisonPill

    case SelfDeadline => // in future will be sent to zombies pool instead of suicide?
      log("Deadline")
      self ! PoisonPill


    case SelfPersist =>
      log("SelfPersist")
      zStore.put(s"ta-$myId", serializedSelf, secondsToLive = selfDeadlineInterval.toSeconds.toInt, true)

    case SubscribeToDone =>
      log(s"TrackingActor: got BlockUntilDone")
      context.become(receiveWithSubscriberOpt(Some(sender())))

    case msg => log(msg.toString) // for debugging
  }

  private def serializedSelf = data.map { case (p, s) => s"$p\0$s" }.mkString("\n").getBytes("UTF-8")

  private def pub(sub: Option[ActorRef]) = if(isDone) sub.foreach(_ ! dataToExpose)
}

case class RestoredData(data: Seq[PathStatus])
case object Ping
case object Read
case object SubscribeToDone
