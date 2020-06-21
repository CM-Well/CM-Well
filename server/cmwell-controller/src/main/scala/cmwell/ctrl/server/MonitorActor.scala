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
package cmwell.ctrl.server

import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{ClusterDomainEvent, MemberRemoved, MemberUp, UnreachableMember}
import cmwell.ctrl.checkers._
import cmwell.ctrl.client.CtrlClient

import cmwell.ctrl.hc._
import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import k.grid.{Grid, GridReceives}
import akka.pattern.pipe
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.language.postfixOps

/**
  * Created by michael on 12/25/14.
  */
case class RestartPolicy(restartAfter: Duration)
case object SendNotification
case class UpdateCheckerState(cs: ComponentState)
case object ReleaseBusy

case class ScheduleRestart(d: FiniteDuration, rch: RestarterChecker)
case class DoRestart(rch: RestarterChecker)
case object CancelRestart
abstract class MonitorActor(var checker: Checker,
                            hostName: String,
                            start: Int,
                            interval: Int,
                            clusterListener: Boolean = false)
    extends Actor
    with LazyLogging {

  private val system = Grid.system

  private[this] var restartCancellable: Cancellable = null
  private[this] var scheduleCancellable: Cancellable = null

  var busy = false

  override def preStart(): Unit = {
    scheduleCancellable = system.scheduler.schedule(interval milli, interval milli, self, SendNotification)
    logger.info(s"started monitor actor $self")
  }

  override def postStop(): Unit = {
    logger.info(s"postStop of actor $self")
    scheduleCancellable.cancel()
    super.postStop()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.info(s"preRestart of actor $self reason: $reason when processing $message")
    super.preRestart(reason, message)
  }

  override def receive: Receive = GridReceives.monitoring(sender).orElse {
    case SendNotification =>
      if (!busy) {
        busy = true
        checker.check.map(self ! _).recover {
          case err: Throwable =>
            logger.error(s"Failed to check component", err)
            self ! ReleaseBusy
        }
      } else {
        logger.warn(s"Skipping check iteration, last result: ${checker.getLastStates(1).headOption}")
      }
    case cs: ComponentState =>
      checker match {
        case rch: RestarterChecker => {
          if (cs.getColor != GreenStatus && restartCancellable == null) {
            self ! ScheduleRestart(rch.restartAfter, rch)
          } else if (cs.getColor == GreenStatus && restartCancellable != null) self ! CancelRestart
        }
        case _ => // Do nothing
      }

      logger.debug(s"Storing $cs in using actor $self in path ${self.path} states store.")
      checker.storeState(cs)
      createUpdateState(cs).foreach { us =>
        HealthActor.ref ! us

      }
      HealthActor.ref ! PingGot(s"!!!!! ping from $hostName !!!!!")
      self ! ReleaseBusy

    case ReleaseBusy => busy = false

    case ScheduleRestart(d, rch) => {
      restartCancellable = Grid.system.scheduler.scheduleOnce(d, self, DoRestart(rch))
    }
    case DoRestart(rch) => {
      rch.doRestart
      restartCancellable = null
    }
    case CancelRestart => {
      if (restartCancellable != null) restartCancellable.cancel()
      restartCancellable = null
    }

    case message => logger.warn(s"received message: $message")
  }

  def createUpdateState(cs: ComponentState): Set[UpdateStat]

}

class CassandraMonitorActor(hostName: String, start: Int, interval: Int)
    extends MonitorActor(CassandraChecker, hostName, start, interval) {
  override def createUpdateState(cs: ComponentState): Set[UpdateStat] = Set(UpdateCasStat(hostName, cs))
}

class ElasticsearchMonitorActor(hostName: String, start: Int, interval: Int)
    extends MonitorActor(ElasticsearchChecker, hostName, start, interval) {
  override def createUpdateState(cs: ComponentState): Set[UpdateStat] = Set(UpdateEsStat(hostName, cs))
}

class WebMonitorActor(hostName: String, start: Int, interval: Int)
    extends MonitorActor(WebChecker, hostName, start, interval) {
  override def createUpdateState(cs: ComponentState): Set[UpdateStat] = Set(UpdateWsStat(hostName, cs))
}

class BgHealthMonitorActor(hostName: String, start: Int, interval: Int)
    extends MonitorActor(BgChecker, hostName, start, interval) {
  override def createUpdateState(cs: ComponentState): Set[UpdateStat] = Set(UpdateBgStat(hostName, cs))
}

class DcMonitorActor(hostName: String, start: Int, interval: Int)
    extends MonitorActor(DcChecker, hostName, start, interval) {
  override def createUpdateState(cs: ComponentState): Set[UpdateStat] =
    cs match {
      case DcStatesBag(states, ch, genTime) => states.map(dcStat => UpdateDcStat(dcStat._1, dcStat._2)).toSet
    }
}

class ZookeeperMonitorActor(hostName: String, start: Int, interval: Int)
    extends MonitorActor(ZookeeperChecker, hostName, start, interval) {
  override def createUpdateState(cs: ComponentState): Set[UpdateStat] = Set(UpdateZookeeperStat(hostName, cs))
}

class KafkaMonitorActor(hostName: String, start: Int, interval: Int)
    extends MonitorActor(KafkaChecker, hostName, start, interval) {
  override def createUpdateState(cs: ComponentState): Set[UpdateStat] = Set(UpdateKafkaStat(hostName, cs))
}

class SystemMonitorActor(hostName: String, start: Int, interval: Int)
    extends MonitorActor(SystemChecker, hostName, start, interval) {
  override def createUpdateState(cs: ComponentState): Set[UpdateStat] = Set(UpdateSystemStat(hostName, cs))
}
