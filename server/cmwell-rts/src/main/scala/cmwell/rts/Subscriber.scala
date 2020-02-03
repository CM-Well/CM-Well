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
package cmwell.rts

import java.nio.charset.StandardCharsets

import akka.actor.{ReceiveTimeout, _}
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp, UnreachableMember, _}
import akka.cluster.{Cluster, Member}
import akka.pattern.ask
import akka.util.Timeout
import cmwell.domain.Infoton
import cmwell.formats.FormatType
import cmwell.util.string.Base64
import com.typesafe.scalalogging.LazyLogging
import k.grid.{Grid, GridJvm, JvmJoinedEvent, JvmLeftEvent}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by markz on 7/10/14.
  */
case class Subscribe(subscriber: String, rule: Rule, transmitType: TransmitType)
case class UnSubscribe(subscriber: String)
case class NotifyUnSubscribe(subscriber: String)

case class NotifyAddRule(publisherActorMap: Map[GridJvm, ActorSelection], subscriber: String, rule: Rule)
case class NotifyRemoveRule(subscriberActorMap: Map[GridJvm, ActorSelection], subscriber: String)

case object PullData

class SubscriberPushActor(sendData: (Seq[String]) => Unit, bulkSize: Int = 100) extends Actor with LazyLogging {
  //val cluster = Cluster(context.system)
  var data = new mutable.ListBuffer[String]()
  var timestamp = System.currentTimeMillis()
  var count: Int = 0
  context.setReceiveTimeout(1 seconds)

  override def preStart(): Unit = {}

  def receive = {

    case NotifyAddRule(publisherActorMap: Map[GridJvm, ActorSelection], subscriber: String, rule: Rule) =>
      logger.info(
        s"NotifyAddRule($publisherActorMap : Map[Member , ActorSelection] , $subscriber : String , $rule : Rule)"
      )
      logger.info(s"publisherActorMap: $publisherActorMap")

      publisherActorMap.foreach { case (m, a) => a ! AddSubscriber(subscriber, rule) }

    case NotifyRemoveRule(subscriberActorMap: Map[GridJvm, ActorSelection], subscriber: String) =>
      logger.info(s"NotifyRemoveRule($subscriberActorMap : Map[Member , ActorSelection] , $subscriber : String)")
      subscriberActorMap.foreach { case (m, a) => a ! RemoveSubscriber(subscriber) }

    case ReceiveTimeout =>
      // if got timeout and there is data in the buffer send & empty buffer
      if (!data.isEmpty) {
        sendData(data.toSeq)
        data = ListBuffer.empty[String]
      }

    case msg @ PublishOne(i: String) =>
      logger.info(s"received $msg from $sender")
      // here we need to make the sensor
//      val deltaTimestamp = System.currentTimeMillis() - timestamp
//      if ( deltaTimestamp <= 1000 && count >= 100 )
//          // we need to call Unsubscribe.
//        logger.info(s"call Unsubscribe delta [${deltaTimestamp}] count [${count}}]" )
//      else {
//        if ( deltaTimestamp > 1000 ) {
//          // create new timestamp && zero count
//          timestamp = System.currentTimeMillis()
//          count = 0
//        }
//        count += 1
      // accumulate data
      data.append(i)
      // send data if needed according bulk size
      if (data.length == bulkSize) {
        sendData(data.toSeq)
        // empty data buffer
        data = ListBuffer.empty[String]
      }
//      }
    case _ =>
  }

}

object PullDataContainer {
  def apply(overflowFlag: Boolean, data: Vector[String], format: FormatType): PullDataContainer =
    new PullDataContainer(overflowFlag, data, format)
}

class PullDataContainer(val overflowFlag: Boolean, val data: Vector[String], val format: FormatType)
    extends Serializable

class SubscriberPullActor(format: FormatType, bulkSize: Int = 100) extends Actor with LazyLogging {

  var data = new mutable.ListBuffer[String]()
  var overflowFlag: Boolean = false
  var timestamp = System.currentTimeMillis()
  var count: Int = 0

  override def preStart(): Unit = {}

  def receive = {

    case NotifyAddRule(publisherActorMap: Map[GridJvm, ActorSelection], subscriber: String, rule: Rule) =>
      logger.info(
        s"NotifyAddRule($publisherActorMap : Map[Member , ActorSelection] , $subscriber : String , $rule : Rule)"
      )
      publisherActorMap.foreach { case (m, a) => a ! AddSubscriber(subscriber, rule) }

    case NotifyRemoveRule(subscriberActorMap: Map[GridJvm, ActorSelection], subscriber: String) =>
      logger.info(s"NotifyRemoveRule($subscriberActorMap : Map[Member , ActorSelection] , $subscriber : String)")
      subscriberActorMap.foreach { case (m, a) => a ! RemoveSubscriber(subscriber) }

    case PublishOne(i: String) =>
      // here we need to make the sensor
      val deltaTimestamp = System.currentTimeMillis() - timestamp
      if (deltaTimestamp <= 1000 && count >= 100)
        // we need to call Unsubscribe.
        logger.info(s"call Unsubscribe delta [${deltaTimestamp}] count [${count}}]")
      else {
        if (deltaTimestamp > 1000) {
          // create new timestamp && zero count
          timestamp = System.currentTimeMillis()
          count = 0
        }
        count += 1
        if (data.size == bulkSize) {
          data.remove(0)
          overflowFlag = true
        } else {
          overflowFlag = false
        }
        data.append(i)
      }
    case PullData =>
      // here send the data
      val tmp = Vector.empty[String] ++ data
      data = ListBuffer.empty[String]
      val d = PullDataContainer(overflowFlag, tmp, format)
      // send data back
      sender ! d
    case _ =>
  }

}

class SubscriberAgent extends Actor with LazyLogging {

  val cluster = Cluster(context.system)

  // this the map of all publishers

  var publisherActorMap: Map[GridJvm, ActorSelection] = Map.empty[GridJvm, ActorSelection]
  // this the map of all subscribers

  var subscriberActorMap: Map[GridJvm, ActorSelection] = Map.empty[GridJvm, ActorSelection]

  var rulesMap: Map[String, Rule] = Map.empty[String, Rule]

  var localSubscriberActorMap: Map[String, ActorRef] = Map.empty[String, ActorRef]

  override def preStart(): Unit = {
    logger.info("SubscriberAgent: preStart")
    Grid.subscribeForGridEvents(self)

  }

  def receive = {

    case Subscribe(subscriber: String, rule: Rule, transmitType: TransmitType) =>
      val ar = transmitType match {
        case Pull(format) =>
          logger.info(s"Create a pull actor")
          val props = Props(new SubscriberPullActor(format))
          Grid.create(props, subscriber)
        case Push(sendData) =>
          logger.info(s"Create a push actor [$sendData]")
          val props = Props(new SubscriberPushActor(sendData))
          Grid.create(props, subscriber)
      }
      // got new subscriber
      logger.info(s"Subscribe [$ar]")
      rulesMap += (subscriber -> rule)
      localSubscriberActorMap += (subscriber -> ar)
      ar ! NotifyAddRule(publisherActorMap, subscriber, rule)
      // return actor string to creator
      val name = s"${Grid.me}/user/$subscriber"
      logger.info(s"Subscribe Actor location [$name]")
      sender ! name

    case NotifyUnSubscribe(subscriber: String) =>
      subscriberActorMap.foreach { case (m, a) => a ! UnSubscribe(subscriber) }

    case UnSubscribe(subscriber: String) =>
      // remove
      logger.info(s"UnSubscribe msg ${localSubscriberActorMap}")
      localSubscriberActorMap.get(subscriber) match {
        case Some(ar) =>
          logger.info("subscriber found remove from all publishers")
          val ar = localSubscriberActorMap(subscriber)
          rulesMap -= (subscriber)
          localSubscriberActorMap -= (subscriber)
          ar ! NotifyRemoveRule(publisherActorMap, subscriber)
        case None => // not found in here
      }

    case JvmJoinedEvent(jvm) => {
      if (jvm.hasLabel("publisher")) {
        val a = Grid.selectActor("publisher", jvm)
        publisherActorMap += (jvm -> a)
        rulesMap.map {
          case (subscriber, rule) =>
            //a ! AddRule(s,r,localSubscriberActorMap.get(s).get)
            val ar = localSubscriberActorMap.get(subscriber).get
            //TODO: send only the publisher that have joined now
            ar ! NotifyAddRule(publisherActorMap, subscriber, rule)
            logger.info(s" publisherActorMap $publisherActorMap")
        }
      }

      if (jvm.hasLabel("subscriber")) {
        val a = Grid.selectActor("subscriber", jvm)
        subscriberActorMap += (jvm -> a)
        logger.info(s" subscriberActorMap $subscriberActorMap")
      }
    }

    case JvmLeftEvent(jvm) => {
      logger.info(s"$self jvm left $jvm")
      if (jvm.hasLabel("publisher")) {
        publisherActorMap -= jvm
      }

      if (jvm.hasLabel("subscriber")) {
        subscriberActorMap -= jvm
      }
    }

    case unknown =>
    //logger.warn(s"Got unknown message $unknown")
  }

}

object Subscriber {
  val subscriberAgentActor: ActorRef = Grid.create(classOf[SubscriberAgent], "subscriber")
  val s = new Subscriber(subscriberAgentActor)
  def init: Unit = {}
  def subscribe(subscriber: String, rule: Rule, transmitType: TransmitType) =
    s.subscribe(subscriber, rule, transmitType)
  def unsubscribe(subscriber: String) = s.unsubscribe(subscriber)
  def pull(subscriber: String): Future[PullDataContainer] = s.pull(subscriber)
}

class Subscriber(val subscriberAgent: ActorRef) extends LazyLogging {
  implicit val timeout = Timeout(5 seconds)
  def subscribe(subscriber: String, rule: Rule, transmitType: TransmitType): Future[String] = {
    val futureSubscribe = subscriberAgent ? Subscribe(subscriber, rule, transmitType)
    futureSubscribe.map(d => Base64.encodeBase64URLSafeString(d.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)))
  }

  // TODO: this call can be made from any node we need to lookup the actor and send
  def unsubscribe(subscriber: String) {
    // first lets lookup the subscriber agent
    val k = Base64.decodeBase64String(subscriber, "UTF-8")
    val l = k.split("/")
    logger.info(s"unsubscribe [$subscriber] to [$k] splited $l")
    val sub = l(l.length - 1)
    val t = (l(l.length - 3)).split("@")
    val location = t(1)
    // lets locate
    logger.info(s"location [$location] sub [$sub]")
    // TODO : need to build the actor and than send NotifyUnSubscribe
    val f = Grid.selectActor("subscriber", GridJvm(location))
    f ! NotifyUnSubscribe(sub)
  }

  def pull(subscriber: String): Future[PullDataContainer] = {
    val k = Base64.decodeBase64String(subscriber, "UTF-8")
    val f = Grid.selectByPath(k)
    val ff = f ? PullData
    ff.mapTo[PullDataContainer]
  }
}
