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
import akka.actor.{Actor, ActorRef}
import akka.actor.Actor.Receive
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory
import scala.collection.JavaConverters._

/**
  * Created by michael on 2/7/16.
  */
object GridData {
  var allKnownHosts = Set.empty[String]
  var upHosts = Set.empty[String]
  var downHosts = Set.empty[String]

}

object ClientActor {
  val name = "ClientActor"
}

case class EventSubscription(subscriber: ActorRef)
case class NodeJoined(host: String)
case class NodeLeft(host: String)

trait GridRole
case object ClientMember extends GridRole
case object GridMember extends GridRole
case object Controller extends GridRole

trait GridMemberStatus
case object Running extends GridMemberStatus
case object Stopped extends GridMemberStatus

case object GetClientInfo
case class JvmInfo(role: GridRole,
                   status: GridMemberStatus,
                   pid: Int,
                   uptime: Long,
                   memInfo: Set[MemoryInfo],
                   gcInfo: Set[GcInfo],
                   logLevel: String,
                   extraData: String,
                   sampleTime: Long = System.currentTimeMillis())

case class MemoryInfo(name: String, used: Long, max: Long) {
  def usedPercent: Int = (used.toDouble / max.toDouble * 100.0).toInt
}
case class GcInfo(name: String, gcCount: Long, gcTime: Long)

case class JvmMembershipReport(joined: Set[GridJvm], left: Set[GridJvm])

case class JvmLeftEvent(jvm: GridJvm)
case class JvmJoinedEvent(jvm: GridJvm)

case object RestartJvm

class ClientActor extends Actor with LazyLogging {
  private val upSince = System.currentTimeMillis()
  private lazy val mpools = ManagementFactory.getMemoryPoolMXBeans()
  private lazy val gcbeans = ManagementFactory.getGarbageCollectorMXBeans()
  private[this] var subscribers = Set.empty[ActorRef]

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {}

  private def getGcInfo: Set[GcInfo] = {
    gcbeans.asScala.map { gc =>
      GcInfo(gc.getName, gc.getCollectionCount, gc.getCollectionTime)
    }.toSet
  }

  private def getMemUsage: Set[MemoryInfo] = {
    mpools.asScala.map { pool =>
      val usage = pool.getUsage
      MemoryInfo(pool.getName, usage.getUsed, if (usage.getMax <= 0) usage.getCommitted else usage.getMax)
    }.toSet
  }

  private def getMyPid: Int = {
    val vmName = ManagementFactory.getRuntimeMXBean().getName()
    val p = vmName.indexOf("@")
    val pid = vmName.substring(0, p)
    pid.toInt
  }

  lazy val pid = getMyPid

  override def receive: Receive = {
    case Hosts(machines) => {
      GridData.allKnownHosts = GridData.allKnownHosts.union(machines)
      logger.debug(s"[Hosts] current machines: $machines")
      logger.debug(s"Grid upHosts: ${GridData.upHosts}")

      val prevDownHosts = GridData.downHosts
      val prevUpHosts = GridData.upHosts

      GridData.downHosts = GridData.upHosts -- machines
      GridData.upHosts = machines

      val newDownHosts = prevUpHosts -- prevDownHosts -- machines
      val newUpHosts = machines -- prevUpHosts

      for {
        downHost <- newDownHosts
        subscriber <- subscribers
      } {
        subscriber ! NodeLeft(downHost)
      }

      for {
        upHost <- newUpHosts
        subscriber <- subscribers
      } {
        subscriber ! NodeJoined(upHost)
      }

    }

    case JvmMembershipReport(joined, left) => {
      for {
        jvm <- joined
        subscriber <- subscribers
      } {
        subscriber ! JvmJoinedEvent(jvm)
      }

      for {
        jvm <- left
        subscriber <- subscribers
      } {
        subscriber ! JvmLeftEvent(jvm)
      }
    }

    case GetClientInfo => {
      logger.debug("Got GetClientInfo")
      val uptime = System.currentTimeMillis() - upSince
      val lvl = LoggerFactory.getLogger("ROOT").asInstanceOf[ch.qos.logback.classic.Logger].getLevel.toString

      val role = {
        if (Grid.isController && Grid.isSingletonJvm) Controller
        else if (Grid.isController && !Grid.isSingletonJvm) GridMember
        else ClientMember
      }

      sender ! JvmInfo(role, Running, pid, uptime, getMemUsage, getGcInfo, lvl, Grid.extraDataCollector())
    }
    case EventSubscription(subscriber) => {
      subscribers = subscribers + subscriber
    }
    case RestartJvm => {
      logger.warn("This jvm was intentionally restarted.")
      System.exit(1)
    }
  }
}
