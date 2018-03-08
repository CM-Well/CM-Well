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


package k.grid.service

import akka.actor._
import akka.actor.Actor.Receive
import akka.pattern.{ask, pipe}
import com.typesafe.scalalogging.LazyLogging
import k.grid.{GridJvm, Grid}
import k.grid.service.messages._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by michael on 2/10/16.
 */

case class ServiceInitInfo(ServiceType : Class[_ <: Actor], preferredJVM:Option[GridJvm], args : Any*)


case class ServiceTypes(m : Map[String, ServiceInitInfo] = Map.empty[String, ServiceInitInfo]) {

  def addLocal(name : String, clazz: Class[_ <: Actor], args : Any*) : ServiceTypes = {
    val initInfo = ServiceInitInfo(clazz, Some(Grid.thisMember), args:_*)
    this.copy(m = m.updated(name, initInfo))
  }

  def add(name : String, clazz: Class[_ <: Actor], args : Any*) : ServiceTypes = {
    val initInfo = ServiceInitInfo(clazz, None, args:_*)
    this.copy(m = m.updated(name, initInfo))
  }

  def add(mm : Map[String, ServiceInitInfo]) : ServiceTypes = {
    this.copy(m ++ mm)
  }
}


case object KillService
// todo: add handling for router death.
class ServiceRouter(routee : String) extends Actor with LazyLogging {
  implicit val timeout = akka.util.Timeout(24.hours)

  private[this] var printWarnOnResolvingErrorsTimestamp = 0L

  override def receive: Actor.Receive = {
    case KillService => {
      val jvmOpt = LocalServiceManager.getServiceJvm(routee)
      jvmOpt match {
        case Some(jvm) => Grid.selectActor(s"${LocalServiceManager.name}/${routee}", jvm) ! PoisonPill
        case None => // Do nothing
      }
    }
    case msg => {
      val jvmOpt = LocalServiceManager.getServiceJvm(routee)
      jvmOpt match {
        case Some(jvm) =>
          logger.trace(s"[ServiceRouter] routing message($msg) to $routee on $jvm")
          val s = sender()
          (Grid.selectActor(s"${LocalServiceManager.name}/${routee}", jvm) ? msg) foreach {
            f =>
              logger.trace(s"[ServiceRouter] received msg from $routee, piping to sender")
              s ! f
          }
        case None => {
          // don't pollute the logs more than once per routee every 2 minutes
          val now = System.currentTimeMillis()
          if (now - printWarnOnResolvingErrorsTimestamp > 120000) {
            logger.warn(s"[ServiceRouter] couldn't find Service $routee instance!")
            printWarnOnResolvingErrorsTimestamp = now
          }
        }
      }
    }
  }
}

object LocalServiceManager extends LazyLogging {
  implicit val timeout = akka.util.Timeout(15.seconds)
  private[this] var routers = Map.empty[String, ActorRef]

  val name = "LocalServiceManager"

  def init(st: ServiceTypes): Unit = {
    Grid.create(classOf[LocalServiceManager], LocalServiceManager.name, st)
  }

  // todo: hide mapping!!
  var mapping = Map.empty[String, Option[GridJvm]]

  def getServiceJvm(name: String) = mapping.get(name).flatten

  def serviceRef(name: String): ActorRef = scala.concurrent.blocking {
    synchronized {
      routers.get(name) match {
        case Some(ac) => ac
        case None =>
          val newRouter = Grid.create(classOf[ServiceRouter], s"$name-router", name)
          routers = routers.updated(name, newRouter)
          newRouter
      }
    }
  }
}
class LocalServiceManager(st : ServiceTypes) extends Actor with LazyLogging{
  implicit val timeout = akka.util.Timeout(15.seconds)
  private[this] lazy val coordinator = {
    val seed = Grid.seedMembers.head
    Grid.selectSingleton(ServiceCoordinator.name, None, seed)
  }

  override def receive: Receive = {
    case ServiceMapping(m) =>
      logger.debug(s"[ServiceMapping] current mapping: $m")
      LocalServiceManager.mapping = m
    case RegisterServices(gm) => {
      Grid.singletonJvm = gm
      logger.debug(s"[RegisterServices] registering [${st.m.keySet.mkString(",")}]")
      val statuses = st.m.map {
        serviceType =>
          val isRunning = context.child(serviceType._1).isDefined
          logger.debug(s"[RegisterServices] the Service ${serviceType._1} running: $isRunning")
          ServiceStatus(serviceType._1, isRunning, serviceType._2.preferredJVM)
      }.toSet
      sender ! ServiceInstantiationRequest(Grid.thisMember, statuses)
    }
    case RunService(name) => {
      if(context.child(name).isDefined) {
        logger.warn(s"[RunService] the Service $name is already running.")
      } else {
        logger.info(s"[RunService] will run $name, known Services: ${st.m.keySet.mkString("[",",","]")}")
        st.m.get(name).foreach {
          t =>
            val a = context.actorOf(Props(t.ServiceType, t.args: _*), name)
            logger.debug(s"[RunService] the path of the actor is: ${a.path}")
        }
      }
    }
    case StopService(name) => {
      logger.info(s"[StopService] $name")
      context.child(name).foreach( _ ! PoisonPill )
    }
  }

}
