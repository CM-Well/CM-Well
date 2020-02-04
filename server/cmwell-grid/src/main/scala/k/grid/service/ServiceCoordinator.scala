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
package k.grid.service

import akka.actor.Actor
import akka.actor.Actor.Receive
import akka.pattern.ask
import com.typesafe.scalalogging.LazyLogging
import k.grid.{Grid, GridJvm}
import k.grid.service.messages._
import scala.concurrent.duration._
import cmwell.util.concurrent._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 2/9/16.
  */
object ServiceCoordinator {
  val name = "ServiceCoordinator"
  def init = Grid.createSingleton(classOf[ServiceCoordinator], ServiceCoordinator.name, None)
}

class ServiceCoordinator extends Actor with LazyLogging {
  implicit val timeout = akka.util.Timeout(15.seconds)
  private[this] case class UpdateServiceMapping(name: String, ogm: Option[GridJvm])
  private[this] case object SendRegistrations
  private[this] case object BroadcastServiceMapping

  private[this] var mapping = Map.empty[String, Option[GridJvm]]

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    Grid.system.scheduler.schedule(0.seconds, 30.seconds, self, SendRegistrations)
  }

  override def receive: Receive = {
    case UpdateServiceMapping(name, ogm) => mapping = mapping.updated(name, ogm)
    case BroadcastServiceMapping =>
      logger.debug(s"[BroadcastServiceMapping] current mapping: $mapping")
      val jvms = Grid.jvmsAll
      jvms.foreach { jvm =>
        Grid.selectActor(LocalServiceManager.name, jvm) ! ServiceMapping(mapping)
      }
    case SendRegistrations => {
      val jvms = Grid.jvmsAll
      logger.debug(s"[SendRegistrations] currentJvms: $jvms")
      val futures = jvms.map { jvm =>
        (Grid.selectActor(LocalServiceManager.name, jvm) ? RegisterServices(Grid.thisMember))
          .mapTo[ServiceInstantiationRequest]
      }

      val future = successes(futures)
      future.foreach { stat =>
        val candidatesSet = stat.flatMap { cc =>
          cc.s.map(_ -> cc.member)
        }
        val candidatesMap = candidatesSet.groupBy(_._1.name).map(t => t._1 -> t._2)
        candidatesMap.foreach {
          case (serviceName, ssg)
              if ssg.count(_._1.isRunning) == 0 => // No one is running the service. Will choose one candidate to run it.
            logger.warn(
              s"[ServiceInstantiation] No one is running the service $serviceName. Will choose one candidate to run it"
            )
            val winner = ssg.find(_._1.preferredJVM.isDefined) match {
              case Some((ServiceStatus(_, _, Some(preferredJvm)), _)) if (stat.exists(_.member == preferredJvm)) =>
                logger.info(s"[ServiceInstantiation] choosing preferred JVM as target for service")
                preferredJvm
              case _ =>
                logger.info(s"[ServiceInstantiation] choosing random JVM as target for service ")
                val vec = ssg.toVector
                val candIndex = Random.nextInt(vec.size)
                vec(candIndex)._2
            }
            logger.info(s"[ServiceInstantiation] will run $serviceName on $winner")
            Grid.selectActor(LocalServiceManager.name, winner) ! RunService(serviceName)
            // We will update that currently no one is running the service, we will know if it runs only in the next sample.
            self ! UpdateServiceMapping(serviceName, None)
          case (serviceName, ssg)
              if ssg.count(_._1.isRunning) > 1 => // We have more then one service for some reason. We will keep only one.
            logger.warn(
              s"[ServiceInstantiation] We have more then one $serviceName service for some reason. We will keep only one."
            )
            ssg.collect { case e @ (ServiceStatus(_, true, Some(pJVM)), rJVM) if pJVM == rJVM => e } match {
              // if one of them is running on its preffered JVM, keep it and stop the rest of them
              case runningOnPref if runningOnPref.size > 0 =>
                val keep = runningOnPref.head
                val running = ssg.filter(_._1.isRunning)
                val stop = running - keep
                logger.info(s"[ServiceInstantiation] keeping preferred JVM and stopping the rest")
                stop.foreach { m =>
                  Grid.selectActor(LocalServiceManager.name, m._2) ! StopService(serviceName)
                }
                self ! UpdateServiceMapping(serviceName, Some(keep._2))
              // otherwise, keep one and stop all rest
              case _ =>
                logger.info(s"[ServiceInstantiation] keeping the first JVM and stopping the rest")
                ssg
                  .filter(_._1.isRunning)
                  .tail
                  .foreach(m => Grid.selectActor(LocalServiceManager.name, m._2) ! StopService(serviceName))
                self ! UpdateServiceMapping(serviceName, Some(ssg.head._2))
            }

          case (serviceName, ssg) if ssg.count(_._1.isRunning) == 1 => // All is good!
            logger.debug(s"[ServiceInstantiation] All is good! There is one instance of $serviceName")
            // we know that the get will succeed
            val runner = ssg.find(_._1.isRunning).get

            // if runner is not preferred and preferred jvm is available
            if (runner._1.preferredJVM.isEmpty && candidatesMap(serviceName).exists(_._1.preferredJVM.isDefined)) {
              val preferredJvm = candidatesMap(serviceName).collect {
                case (ServiceStatus(_, _, Some(preferred)), _) => preferred
              }.head
              logger.warn(
                s"we've found that service: ${runner._1.name} is not running on its preferred JVM stopping it. "
              )
              Grid.selectActor(LocalServiceManager.name, runner._2) ! StopService(serviceName)
              // We will update that currently no one is running the service, we will know if it runs only in the next sample.
              self ! UpdateServiceMapping(serviceName, None)
            } else {
              if (mapping.get(runner._1.name).flatten != Some(runner._2))
                self ! UpdateServiceMapping(serviceName, ssg.find(_._1.isRunning).map(_._2))
            }
        }
        // will broadcast the current service mapping to the rest of the members.
        self ! BroadcastServiceMapping
      }
    }
  }
}
