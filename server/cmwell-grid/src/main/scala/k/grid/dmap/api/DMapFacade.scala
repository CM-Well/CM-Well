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


package k.grid.dmap.api

import akka.actor.{Actor, ActorRef, ActorSelection, PoisonPill}
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid
import k.grid.dmap.impl.inmem.InMemDMap
import k.grid.dmap.impl.persistent.PersistentDMap

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try
/**
 * Created by michael on 5/26/16.
 */

case class DMapActorInit(classType : Class[_ <: DMapActor], name : String, args: Any*)

object DMapMessage {
  def currentTimestamp = System.currentTimeMillis()
}

trait DMapMessage {
  def act {}
}

object SettingsValue {
  trait SettingsValueExtractor[T] {
    def get(sv: SettingsValue): Option[T]
  }

  implicit val stringSettingsValueExtractor = new SettingsValueExtractor[String] {
    override def get(sv: SettingsValue): Option[String] = sv match {
      case SettingsString(v) => Some(v)
      case _ => None
    }
  }

  implicit val booleanSettingsValueExtractor = new SettingsValueExtractor[Boolean] {
    override def get(sv: SettingsValue): Option[Boolean] = sv match {
      case SettingsBoolean(v) => Some(v)
      case _ => None
    }
  }

  implicit val stringSetSettingsValueExtractor = new SettingsValueExtractor[Set[String]] {
    override def get(sv: SettingsValue): Option[Set[String]] = sv match {
      case SettingsSet(v) => Some(v)
      case _ => None
    }
  }

  implicit val longSettingsValueExtractor = new SettingsValueExtractor[Long] {
    override def get(sv: SettingsValue): Option[Long] = sv match {
      case SettingsLong(v) => Some(v)
      case _ => None
    }
  }
}
sealed trait SettingsValue extends Serializable {
    def as[T : SettingsValue.SettingsValueExtractor]: Option[T] =
      implicitly[SettingsValue.SettingsValueExtractor[T]].get(this)
}
case class SettingsString(str : String) extends SettingsValue
case class SettingsBoolean(str : Boolean) extends SettingsValue
case class SettingsSet(set : Set[String]) extends SettingsValue
case class SettingsLong(lng : Long) extends SettingsValue

case object DMapSlaveJoined extends DMapMessage
case object DMapMasterJoined extends DMapMessage
case object GetDMap extends DMapMessage
case object GetLeaderHost extends DMapMessage
case class MapData(m : Map[String, SettingsValue], timestamp : Long = DMapMessage.currentTimestamp) extends DMapMessage
case class HandShake(m : Map[String, SettingsValue], timestamp : Long)

case class SetValue(key : String , value : SettingsValue, timestamp : Long = DMapMessage.currentTimestamp) extends DMapMessage
case class AggregateValue(key : String , value : String, timestamp : Long = DMapMessage.currentTimestamp) extends DMapMessage
case class SubtractValue(key : String, value : String, timestamp : Long = DMapMessage.currentTimestamp) extends DMapMessage
case class GetValue(key : String) extends DMapMessage


object DMaps extends LazyLogging {
  private[this] var dmaps = Set.empty[DMapFacade]
  private[grid] def register(dmap : DMapFacade) = {
    logger.info(s" *** Registrating $dmap")
    dmaps = dmaps + dmap
  }
  private[grid] def get = dmaps

  register(InMemDMap)
  register(PersistentDMap)
}


/**
 * DMapFacade is the trait that defines the api that the user uses to interact with the distributed map.
 * When defining DMapFacade the DMapMaster and DMapSlave types should be provided.
 */
trait DMapFacade {
  implicit val timeout = Timeout(20.seconds)
  def masterType : DMapActorInit
  def slaveType : DMapActorInit




  private[this] val _m = masterType
  private[this] val _s = slaveType

  private[grid] var mm = Map.empty[String, SettingsValue]
  private[grid] var sm = Map.empty[String, SettingsValue]

  var slave : ActorRef = _

  def salveName = _s.name
  def master = Grid.selectSingleton(_m.name, None, Grid.clusterProxy)




  private[grid] def initMaster: Unit = {

    Grid.createSingleton(_m.classType, _m.name, None, _m.args:_*)
    Grid.selectSingleton(_m.name, None)
    initSlave
  }

  private[grid] def initSlave: Unit = {
    slave = Grid.create(_s.classType, _s.name, _s.args:_*)
  }

  private[grid] def shutdownSlave: Unit = {
    if(slave != null) slave ! PoisonPill
  }


  /**
   * Returns the current map
   * @return
   */
  def getMap = sm

  /**
   * Aggregate is applicable only for SettingsSet. 'value' will be added to 'key' set.
   * @param key
   * @param value
   */
  def aggregate(key : String, value : String) {
    master ! AggregateValue(key, value)
  }

  /**
   * Subtract is applicable only for SettingsSet. 'value' will be removed from 'key' set.
   * @param key
   * @param value
   */
  def subtract(key : String, value : String) {
    master ! SubtractValue(key, value)
  }

  /**
   * set adds or changes the 'value' of 'key'.
   * @param key
   * @param value
   */
  def set(key : String, value : SettingsValue) {
    master ! SetValue(key, value)
  }

  /**
   * get returns the 'value' of 'key'.
   * @param key
   * @return
   */
  def get(key : String) : Option[SettingsValue] = {
    //Await.result((localSlave ? GetValue(key)).mapTo[Option[SettingsValue]], 20 seconds)
    //Await.result((ref ? GetValue(key)).mapTo[Option[SettingsValue]], 20 seconds)
    sm.get(key)
  }


}

trait DMapActor extends Actor

trait DMapMaster extends DMapActor with LazyLogging {
  implicit val timeout = Timeout(20.seconds)
  val facade : DMapFacade
  def currentTimestamp : Long = System.currentTimeMillis / 1000

  def onStart: Unit = {

  }
  protected[this] var lastTimestamp = 0L
  private[this] def slaves : Set[ActorSelection] = Grid.jvmsAll.map(jvm => Grid.selectActor(facade.salveName,jvm))

  protected def propagateMsgToSlaves(dmMsg : DMapMessage) {
    logger.debug(s"Master - propagating $dmMsg to slaves ($slaves}).")
    slaves.foreach(_ ! dmMsg)
  }

  protected def askSlaves(dmMsg : DMapMessage) : Future[Set[Any]] = {
    val ff = slaves.map(_ ? dmMsg)
    cmwell.util.concurrent.successes(ff)
  }

  override def preStart(): Unit = {
    facade.mm = Map.empty[String, SettingsValue]
    lastTimestamp = 0L

    logger.info("Starting DMap Master")
    logger.info("Requesting initial data from local slave")


    context.system.scheduler.schedule(60.seconds, 60.seconds) {
      propagateMsgToSlaves(MapData(facade.mm, lastTimestamp))
    }

    onStart
    //Await.result(f, 20 seconds)
  }

  override def receive: Receive = {
    case SetValue(key, value, timestamp) =>
      logger.debug(s"Master - Setting value $key -> $value")
      facade.mm = facade.mm.updated(key, value)
      propagateMsgToSlaves(SetValue(key, value, timestamp))
      lastTimestamp = timestamp
    case AggregateValue(key, value, timestamp) =>
      logger.debug(s"Master - Aggregating value $key -> $value")
      facade.mm.get(key) match {
        case Some(sv) =>
          sv match {
            case SettingsSet(set) =>
              //if(!set.contains(value)){
              facade.mm = facade.mm.updated(key, SettingsSet(set + value))
              propagateMsgToSlaves(AggregateValue(key, value, timestamp))
              lastTimestamp = timestamp
            //}
            case SettingsString(str) => //
            case SettingsBoolean(bool) => //
            case SettingsLong(lng) => //
          }
        case None =>
          facade.mm = facade.mm.updated(key, SettingsSet(Set(value)))
          propagateMsgToSlaves(AggregateValue(key, value, timestamp))
          lastTimestamp = timestamp
      }

    case SubtractValue(key, value, timestamp) =>
      facade.mm.get(key) match {
        case Some(sv) =>
          sv match {
            case SettingsSet(set) =>
              //if(!set.contains(value)){
              facade.mm = facade.mm.updated(key, SettingsSet(set - value))
              propagateMsgToSlaves(SubtractValue(key, value, timestamp))
              lastTimestamp = timestamp
            //}
            case SettingsString(str) => //
            case SettingsLong(lng) => //
            case SettingsBoolean(bool) => //
          }
        case None =>
      }

    case GetDMap => sender ! MapData(facade.mm, lastTimestamp)

    case HandShake(md, timestamp) =>
      if(timestamp > lastTimestamp) {
        facade.mm = md
        lastTimestamp = timestamp
        propagateMsgToSlaves(MapData(facade.mm, lastTimestamp))
      } else {
        sender ! MapData(facade.mm, lastTimestamp)
      }


    case CurrentClusterState(members,unreachable,seenBy,leader,roleLeaderMap) => //this.members = members.map(_.address)

    case dmm : DMapMessage => dmm.act
    case _ => // Do nothing

  }
}

trait DMapSlave extends DMapActor with LazyLogging {
  implicit val timeout = Timeout(20.seconds)
  val facade : DMapFacade
  //private[this] var m = Map.empty[String, SettingsValue]


  protected def onStart {}
  protected def onUpdate(oldMap : Map[String, SettingsValue] , newMap : Map[String, SettingsValue], timestamp : Long): Unit = {

  }

  protected var lastTimestamp = 0L

//  context.system.scheduler.schedule(Duration.Zero, 10.seconds ) {
    //logger.debug(s"current slave state: ${facade.sm}")
//  }

  override def preStart(): Unit = {
    facade.sm = Map.empty[String, SettingsValue]
    lastTimestamp = 0L

    onStart
    facade.master ! HandShake(facade.sm, lastTimestamp)
  }

  override def postStop(): Unit = {
    logger.info("DMapSlave stopped")
  }

  override def receive: Receive = {
    case SetValue(key , value , timestamp) =>
      logger.debug(s"Slave - Setting value $key -> $value")
      lastTimestamp = timestamp

      val old = facade.sm
      facade.sm = facade.sm.updated(key, value)

      onUpdate(old, facade.sm, timestamp)

    case SubtractValue(key, value, timestamp) =>
      facade.sm.get(key) match {
        case Some(sv) =>
          sv match {
            case SettingsSet(set) =>
              val old = facade.sm
              facade.sm = facade.sm.updated(key, SettingsSet(set - value))
              lastTimestamp = timestamp
              onUpdate(old, facade.sm, timestamp)
            case SettingsString(str) => //
            case SettingsBoolean(_) => //
            case SettingsLong(_) => //
          }
        case None =>
      }

    case AggregateValue(key, value, timestamp) =>
      logger.debug(s"Slave - Aggregating value $key -> $value")
      lastTimestamp = timestamp
      val old = facade.sm
      facade.sm.get(key) match {
        case Some(sv) =>
          sv match {
            case SettingsSet(set) =>
              if(!set.contains(value)){
                facade.sm = facade.sm.updated(key, SettingsSet(set + value))
                lastTimestamp = timestamp
                onUpdate(old, facade.sm, timestamp)

              }
            case SettingsString(str) => //
            case SettingsBoolean(_) => //
            case SettingsLong(lng) => //
          }
        case None =>
          facade.sm = facade.sm.updated(key, SettingsSet(Set(value)))
          lastTimestamp = timestamp
          onUpdate(old, facade.sm, timestamp)
      }

    case MapData(m, timestamp) =>
      lastTimestamp = timestamp
      val old = facade.sm
      facade.sm = m
      onUpdate(old, facade.sm, timestamp)

    case GetValue(key) => sender ! facade.sm.get(key)

    case GetDMap => sender ! MapData(facade.sm, lastTimestamp)

    case dmm : DMapMessage => dmm.act
  }
}
