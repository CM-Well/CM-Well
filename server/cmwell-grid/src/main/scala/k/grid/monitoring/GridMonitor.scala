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
package k.grid.monitoring

import akka.util.Timeout
import cmwell.util.concurrent._
import akka.actor.Actor
import akka.actor.Actor.Receive
import akka.pattern.ask
import k.grid._
import k.grid.service.LocalServiceManager
import k.grid.service.messages.{RegisterServices, ServiceInstantiationRequest}
import scala.concurrent.Future
import scala.concurrent.duration._
import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 3/2/16.
  */
case object GetMembersInfo
case class MembersInfo(m: Map[GridJvm, JvmInfo])

object GridMonitor extends LazyLogging {
  implicit val timeout = Timeout(15.seconds)
  lazy private val sel = Grid.selectSingleton(GridMonitor.name, None, Grid.seedMembers.head)
  def name = "GridMonitor"
  def init = Grid.createSingleton(classOf[GridMonitor], name, None)
  def getMembersInfo: Future[MembersInfo] = {
    logger.info("[GridMonitor] getMembersInfo")
    (sel ? GetMembersInfo).mapTo[MembersInfo]
  }
}

case class MemInfoKey(host: String, name: String)
class GridMonitor extends Actor with LazyLogging {
  private implicit val timeout = Timeout(15.seconds)

  private[this] var membersInfo = Map.empty[MemInfoKey, (GridJvm, JvmInfo)]
  private case object SendInfoRequests
  private case class UpdateMembersInfoMap(m: Map[GridJvm, JvmInfo])

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    context.system.scheduler.schedule(0.seconds, 10.seconds, self, SendInfoRequests)

  }

  override def receive: Receive = {
    case UpdateMembersInfoMap(m) =>
      membersInfo = membersInfo ++ m.map {
        case (k, v) => MemInfoKey(k.host, k.identity.map(_.name).getOrElse("")) -> (k, v)
      }
    case SendInfoRequests => {
      logger.info("SendInfoRequests")
      val jvms = Grid.jvmsAll
      val futures = jvms.map { jvm =>
        ((Grid.selectActor(ClientActor.name, jvm) ? GetClientInfo).mapTo[JvmInfo].map(jvm -> _)).recover {
          case _ => {
            val inf = membersInfo.get(MemInfoKey(jvm.host, jvm.identity.map(_.name).getOrElse(""))) match {
              case Some((gridJvm, jvmInfo)) => jvmInfo.copy(status = Stopped)
              case None                     => JvmInfo(ClientMember, Stopped, -1, 0L, Set.empty[MemoryInfo], Set.empty[GcInfo], "NA", "")
            }
            jvm -> inf
          }
        }
      }

      val future = successes(futures).map(_.toMap)
      future.foreach(m => self ! UpdateMembersInfoMap(m))
    }
    case GetMembersInfo =>
      logger.info("Got GetMembersInfo")
      sender ! MembersInfo(membersInfo.map { case (k1, (k2, v)) => k2 -> v })
  }
}
