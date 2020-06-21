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
package k.grid

import akka.actor.Actor.Receive
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.actor.ActorLogging
import akka.actor.Actor
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import k.grid.monitoring._
import k.grid.registration.LocalRegistrationManager

import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor._
import akka.cluster._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

/**
  * Created by markz on 5/14/14.
  */
case class ShutdownNode(address: Address)
case object CheckIsolation
case object BroadcastGridInfo

case class MonitorMessage(from: String)

case class Hosts(h: Set[String])

class NodeActor extends Actor with LazyLogging {
  private[this] var shutdownQueue = Map.empty[Address, Cancellable]

  val cluster = Cluster(context.system)
  var leader: Option[Address] = None
  val system = Grid.system

  var hosts = Set.empty[String]

  val failsAllowed = 3
  var failCounter = 0

  private var lastMonitorMessageTime = Long.MaxValue

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    //#subscribe
    cluster.subscribe(self, classOf[ClusterDomainEvent])
    cluster.subscribe(self, classOf[LeaderChanged])
    system.scheduler.schedule(30.seconds, 30.seconds, self, CheckIsolation)
    system.scheduler.schedule(0.seconds, 1.seconds, self, BroadcastGridInfo)

  }
  override def postStop(): Unit = cluster.unsubscribe(self)
  implicit val timeout = Timeout(5.seconds)

  def membershipReceive: Receive = {
    case MemberUp(member) =>
      logger.info("[GridListener] Member is Up: {}", member.address)
      hosts = hosts + member.address.host.get
    case ReachableMember(member) =>
      logger.info("[GridListener] Member detected as reachable: {}", member)
      //      shutdownQueue.get(member.address).map(c => c.cancel())
      //      shutdownQueue = shutdownQueue.filterNot(t => t._1 != member.address)
      hosts = hosts + member.address.host.get
    case UnreachableMember(member) =>
      logger.info("[GridListener] Member detected as unreachable: {}", member)
      hosts = hosts - member.address.host.get
    //shutdownQueue = shutdownQueue.updated(member.address, context.system.scheduler.scheduleOnce(10 seconds, self, ShutdownNode(member.address)))
    case MemberRemoved(member, previousStatus) =>
      logger.info("[GridListener] Member is Removed: {} after {}", member.address, previousStatus)
      hosts = hosts - member.address.host.get
    case MemberExited(member) =>
      logger.info("[GridListener] Member is Exited: {}", member.address)
      hosts = hosts - member.address.host.get
    case LeaderChanged(address) =>
      logger.info(s"[GridListener] leader changed: $address")
      leader = address
  }

  def receive = membershipReceive.orElse {
    case ShutdownNode(addr) =>
      logger.info(s"[GridListener] Downing node $addr")
      cluster.leave(addr)
      shutdownQueue = shutdownQueue.filterNot(t => t._1 != addr)

    case CheckIsolation =>
      val isCluster = Grid.hostName != "127.0.0.1"
      val ctrls =
        LocalRegistrationManager.jvms.count(_.identity.map(_.name).getOrElse("") == Config.controllerMemberName)
      val regFailure = LocalRegistrationManager.registrationFailure
      val isIsolated = ctrls < Config.minMembers

      if (isCluster && (isIsolated || regFailure)) {
        logger.warn(
          s"IsolationStat: minMembers=${Config.minMembers}, ctrls=$ctrls, failCounter=$failCounter, failsAllowed=$failsAllowed, regFail=$regFailure"
        )
        if (failCounter >= failsAllowed) {
          logger.warn(
            s"IsolationStat: This node was recognized as isolated and will restart its actor system. ($ctrls)"
          )
          // Grid.rejoin
          sys.exit(1)
        } else failCounter += 1
      } else failCounter = 0

    case BroadcastGridInfo =>
      val machines = Grid.members.map(_.host.getOrElse(""))
      for (host <- Grid.jvmsOnThisHost) {
        Grid.selectActor("ClientActor", host) ! Hosts(machines)
      }
    case _: MemberEvent => // ignore
  }
}
