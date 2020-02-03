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
package cmwell.ctrl.hc

import java.net.InetAddress

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.pattern.ask
import akka.util.Timeout
import cmwell.ctrl.checkers._
import cmwell.ctrl.commands.{RestartWebserver, StartElasticsearchMaster}
import cmwell.ctrl.config.Config._
import cmwell.ctrl.config.{Config, Jvms}
import cmwell.ctrl.controllers.ZookeeperController
import cmwell.ctrl.ddata.DData
import cmwell.ctrl.server.CommandActor
import cmwell.ctrl.tasks._
import cmwell.ctrl.utils.AlertReporter
import cmwell.util.resource._
import com.typesafe.scalalogging.LazyLogging
import k.grid._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{blocking, Future}
import scala.language.postfixOps
import scala.util.Success

/**
  * Created by michael on 12/14/14.
  */
case class NodeJoinRequest(host: String)
trait JoinResponse
case object JoinOk extends JoinResponse
case object JoinBootComponents extends JoinResponse
case object JoinShutdown extends JoinResponse

case object GetClusterStatus
case object GetClusterDetailedStatus
case object GetCassandraStatus
case object GetCassandraDetailedStatus
case object GetElasticsearchStatus
case object GetElasticsearchDetailedStatus
case object GetBgStatus
case object GetWebStatus
case object GetDcStatus

case object GetGcStatus

case class ClusterStatus(members: Set[String],
                         casStat: ComponentState,
                         esStat: ComponentState,
                         wsStat: (Map[String, ComponentState], StatusColor),
                         bgStat: (Map[String, ComponentState], StatusColor),
                         zookeeperStat: (Map[String, ComponentState], StatusColor),
                         kafkaStat: (Map[String, ComponentState], StatusColor),
                         controlNode: String,
                         esMasters: Set[String])

case class ClusterStatusDetailed(casStat: Map[String, ComponentState],
                                 esStat: Map[String, ComponentState],
                                 wsStat: Map[String, ComponentState],
                                 bgStat: Map[String, ComponentState],
                                 zkStat: Map[String, ComponentState],
                                 kafkaStat: Map[String, ComponentState],
                                 healthHost: String)

case class RemoveNode(ip: String)

case class RemoveFromDownNodes(ip: String)

case class SetKnownHosts(hosts: Set[String])

case class ReSpawnMasters(downHosts: Set[String])

case object GetActiveNodes
case class ActiveNodes(an: Set[String])

case object CommandReceived

trait UpdateStat
case class UpdateCasStat(ccr: (String, ComponentState)) extends UpdateStat
case class UpdateEsStat(ecr: (String, ComponentState)) extends UpdateStat
case class UpdateWsStat(wcr: (String, ComponentState)) extends UpdateStat
case class UpdateBgStat(bcr: (String, ComponentState)) extends UpdateStat
case class UpdateDcStat(dcr: (String, ComponentState)) extends UpdateStat
case class UpdateZookeeperStat(zkr: (String, ComponentState)) extends UpdateStat
case class UpdateKafkaStat(kafr: (String, ComponentState)) extends UpdateStat
case class UpdateSystemStat(sstat: (String, ComponentState)) extends UpdateStat

case class GcStats(hostName: String, pid: Int, roles: Set[String], timeInGc: Long, amountOfGc: Long, gcInterval: Long)

case class HeakupLatency(hostName: String,
                         pid: Int,
                         roles: Set[String],
                         p25: Long,
                         p50: Long,
                         p75: Long,
                         p90: Long,
                         p99: Long,
                         p99_5: Long,
                         max: Long)

case class UpdateHostName(ip: String, hostName: String)
case class PingGot(message: String)
case object CheckElasticsearchMasterStatus
case object CheckZookeeperStatus

case object CheckForWsCrisis

object HealthActor {
  //lazy val ref = Grid.selectSingleton(Singletons.health)
  lazy val ref = Grid.serviceRef("HealthActor")
}

class HealthActor extends Actor with LazyLogging with AlertReporter {
  implicit val timeout = Timeout(3 seconds)

  alert(s"Started health actor on $host", host = Some(host))

  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    //cluster.subscribe(self, classOf[ClusterDomainEvent])
    Grid.subscribeForGridEvents(self)
    DData.setPingIp(Config.pingIp)

    system.scheduler.schedule(30 seconds, 30 seconds, self, CheckElasticsearchMasterStatus)

    system.scheduler.scheduleOnce(5.minutes, self, CheckForWsCrisis)
    system.scheduler.scheduleOnce(5.seconds, self, CheckZookeeperStatus)

    val downHosts = Grid.allKnownHosts -- Grid.upHosts

    log.info(s"Down nodes: $downHosts")

    if (downHosts.nonEmpty) {
      self ! DownNodesDetected(downHosts)
      self ! ReSpawnMasters(downHosts)
    }
  }

  def log = logger

  def currentTimestamp: Long = System.currentTimeMillis / 1000

  case object CheckIdles

  val system = Grid.system

  private[this] val casStat = new CassandraGridStatus()
  private[this] val esStat = new ElasticsearchGridStatus()
  private[this] val wsStat = new WebGridStatus()
  private[this] val bgStat = new BgGridStatus()
  private[this] val dcStat = new DcGridStatus()
  private[this] val zkStat = new ZkGridStatus()
  private[this] val kafkaStat = new KafkaGridStatus()

  private[this] var hostIps: Map[String, Vector[String]] = Map.empty

  private[this] var gcStats: Map[(String, Int), (Set[String], Long, Long)] = Map.empty
  private[this] var heakupLatencyStats: Map[(String, Int), (Set[String], Long)] = Map.empty

  private[this] var hostNames: Map[String, String] = Map.empty

  private[this] val cancellableScheduler = system.scheduler.schedule(30 seconds, 30 seconds) {
    self ! CheckIdles
  }

  override def postStop(): Unit = {
    log.info("Health actor unsubscribed")
    cluster.unsubscribe(self)
    logger.info("HealthActor died. Cancelling the scheduler")
    cancellableScheduler.cancel()
  }

  def getReachableCassandraNode: Future[String] = {
    Future {
      blocking {
        val s = hostIps.keySet ++ hostIps.values.flatten.toSet
        CassandraChecker.getReachableHost(s) match {
          case Some(host) => host
          case None       => ""
        }
      }
    }
  }

  private def isReachable(host: String): Boolean = {
    InetAddress.getByName(host).isReachable(3000)
  }

  private def getCasDetailed = {
    val detailedRes = hostIps.map { hip =>
      casStat.getState match {
        case CassandraOk(m, _, _) => (hip._1, CassandraOk(m.filter(stats => hip._2.contains(stats._1))))
        case CassandraDown(gt)    => (hip._1, CassandraDown(gt))
        case ReportTimeout(gt)    => (hip._1, CassandraDown(gt))
      }
    }
    detailedRes
  }

  override def receive: Receive =
    GridReceives.monitoring(sender).orElse {
      //case WhoAreYou => sender ! WhoIAm(Grid.me.toString)
      case LeaderChanged(leader) => log.info(s"Leader changed: $leader")

      case CheckIdles =>
        val now = currentTimestamp
        if (casStat.getState != null && now - casStat.getState.genTime > idleWaitSeconds) {
          log.warn(s"Cass diff: ${now - casStat.getState.genTime}")
          self ! UpdateCasStat("", ReportTimeout())
        }

        esStat.getStatesMap.foreach { estat =>
          if (now - estat._2.genTime > idleWaitSeconds) {
            log.warn(s"Elastic ${estat._1} diff ${now - estat._2.genTime}")
            self ! UpdateEsStat(estat._1 -> ReportTimeout())
          }
        }

        wsStat.getStatesMap.foreach { wstat =>
          if (now - wstat._2.genTime > idleWaitSeconds) {
            log.warn(s"Web ${wstat._1} diff ${now - wstat._2.genTime}")
            self ! UpdateWsStat(wstat._1 -> ReportTimeout())
          }
        }

        bgStat.getStatesMap.foreach {
          case (host, cs) =>
            if (now - cs.genTime > idleWaitSeconds) {
              log.warn(s"bg $host diff ${now - cs.genTime}")
              self ! UpdateBgStat(host -> ReportTimeout())
            }
        }

        dcStat.getStatesMap.foreach { dcstat =>
          if (now - dcstat._2.genTime > idleWaitSeconds) {
            log.warn(s"Dc ${dcstat._1} diff ${now - dcstat._2.genTime}")
            self ! UpdateDcStat(dcstat._1 -> ReportTimeout())
          }
        }

        zkStat.getStatesMap.foreach { zkS =>
          if (now - zkS._2.genTime > idleWaitSeconds) {
            log.warn(s"Zookeeper ${zkS._1} diff ${now - zkS._2.genTime}")
            self ! UpdateZookeeperStat(zkS._1 -> ReportTimeout())
          }
        }

        kafkaStat.getStatesMap.foreach { kafS =>
          if (now - kafS._2.genTime > idleWaitSeconds) {
            log.warn(s"Kafka ${kafS._1} diff ${now - kafS._2.genTime}")
            self ! UpdateKafkaStat(kafS._1 -> ReportTimeout())
          }
        }

      case us: UpdateStat =>
        logger.debug(s"Received UpdateStat: $us from actor $sender")
        //AlertReporter.alert(us)
        us match {
          case UpdateCasStat(ccr) =>
            ccr._2 match {
              case co: CassandraOk =>
              case _ =>
                getReachableCassandraNode.onComplete {
                  case Success(host) if host != "" => DData.setPingIp(host)
                  case _                           => // do nothing
                }
            }
            casStat.update("", ccr._2)
          case UpdateEsStat(ecr) =>
            esStat.update(ecr._1, ecr._2)
//          ecr._2 match {
//            case eState : ElasticsearchState if eState.hasMaster=> {
//              val currentMasters = DData.getEsMasters
//              if(currentMasters.size == Config.esMasters && !currentMasters.contains(ecr._1)){
//                logger.info(s"Stopping Elasticsearch master on ${ecr._1}")
//                CommandActor.select(ecr._1) ! StopElasticsearchMaster
//              } else {
//                DData.addEsMaster(ecr._1)
//              }
//            }
//            case _ => // Do nothing
//          }
          case UpdateWsStat(wcr) =>
            wsStat.update(wcr._1, wcr._2)
          case UpdateBgStat(bcr) =>
            bgStat.update(bcr._1, bcr._2)
          case UpdateDcStat(dcr) =>
            dcStat.update(dcr._1, dcr._2)

          case UpdateZookeeperStat(zkr) =>
            zkStat.update(zkr._1, zkr._2)

          case UpdateKafkaStat(kafr) =>
            kafkaStat.update(kafr._1, kafr._2)

          case UpdateSystemStat(sstat) =>
            log.debug(s"received cluster info from ${sstat._1} : ${sstat._2}")
            sstat._2 match {
              case SystemResponse(ips, shortName, _) =>
                hostIps = hostIps.updated(sstat._1, ips)
                updateIps(sstat._1, ips)
                updateShortName(sstat._1, shortName)

              //HealthActor.addKnownNode(sstat._1)
              case _ =>
            }
        }

      case GcStats(hostName, pid, roles, timeInGc, amountOfGc, gcInterval) =>
        log.debug(s"got gc stats: $hostName $pid")
        gcStats = gcStats.updated((hostName, pid), (roles, timeInGc, amountOfGc))

      case HeakupLatency(hostName, pid, roles, p25, p50, p75, p90, p99, p99_5, max) =>
        log.debug(s"GotHeakupLatencyStats $hostName $pid ${roles.toSeq(1)} $p25 $p50 $p75 $p90 $p99 $p99_5 $max")
        heakupLatencyStats = heakupLatencyStats.updated((hostName, pid), (roles, max))
      case GetActiveNodes => sender ! ActiveNodes(hostIps.keySet)
      case GetClusterStatus =>
        sender ! ClusterStatus(
          Grid.availableMachines,
          casStat.getState,
          esStat.getStatesMap.headOption.getOrElse("" -> ElasticsearchDown())._2,
          (wsStat.getStatesMap.map(t => (t._1, t._2)), wsStat.getColor),
          (bgStat.getStatesMap.map(t => (t._1, t._2)), bgStat.getColor),
          (zkStat.getStatesMap.map(t => (t._1, t._2)), zkStat.getColor),
          (kafkaStat.getStatesMap.map(t => (t._1, t._2)), kafkaStat.getColor),
          Config.listenAddress,
          esStat.getStatesMap.filter(_._2.asInstanceOf[ElasticsearchState].hasMaster).keySet
        )

      case GetCassandraStatus     => sender ! casStat.getState
      case GetElasticsearchStatus => sender ! esStat.getStatesMap.head._2
      case GetWebStatus           => sender ! (wsStat.getStatesMap.map(t => (t._1, t._2)), wsStat.getColor)
      case GetBgStatus            => sender ! (bgStat.getStatesMap.map(t => (t._1, t._2)), bgStat.getColor)
      case GetCassandraDetailedStatus =>
        sender ! getCasDetailed
      case GetElasticsearchDetailedStatus =>
        sender ! esStat

      case GetClusterDetailedStatus =>
        sender ! ClusterStatusDetailed(getCasDetailed,
                                       esStat.getStatesMap,
                                       wsStat.getStatesMap,
                                       bgStat.getStatesMap,
                                       zkStat.getStatesMap,
                                       kafkaStat.getStatesMap,
                                       listenAddress)
      case GetGcStatus => sender ! gcStats

      case GetDcStatus => sender ! dcStat.getStatesMap

      case RemoveFromDownNodes(ip) =>
        logger.info(s"Removing $ip from down nodes")
        DData.setDownNodes(DData.getDownNodes -- Set(ip))
        logger.info(s"down nodes list after removing: ${DData.getDownNodes}")

      case msg @ RemoveNode(ip) =>
        logger.info(s"received $msg")
        sender ! CommandReceived
        esStat.remove(ip)
        bgStat.remove(ip)
        wsStat.remove(ip)
        kafkaStat.remove(ip)
        zkStat.remove(ip)
        hostIps = hostIps.filterNot(_._1 == ip)
        gcStats = gcStats.filterNot(_._1._1 == ip)
        heakupLatencyStats = heakupLatencyStats.filterNot(_._1._1 == ip)
        DData.addDownNode(ip)
        logger.info("Sending EndOfGraceTime")
        Grid.system.scheduler.scheduleOnce(20.seconds) {
          self ! EndOfGraceTime
        }

      case NodeJoined(host) => {
        logger.info(s"Node $host has joined.")
        self ! NodesJoinedDetected(Set(host))
      }
      case NodeLeft(host) => {
        logger.info(s"Node $host has left.")
        self ! DownNodesDetected(Set(host))
      }

      case ReSpawnMasters(downHosts) =>
        val masters = DData.getEsMasters
        val known = DData.getKnownNodes

        val upMasters = masters -- downHosts
        val upNotMasters = known -- masters -- downHosts
        if (upMasters.size < Config.esMasters) {
          val numOfDownMasters = Config.esMasters - upMasters.size
          upNotMasters.take(numOfDownMasters).foreach { host =>
            logger.info(s"Spawning Elasticsearch master on $host")
            CommandActor.select(host) ! StartElasticsearchMaster
          }
          (masters -- upMasters).foreach(DData.removeEsMaster(_))
        }

      case r: ClusterEvent =>
        ClusterState.newState(r)

      case NodeJoinRequest(node) =>
        log.info(
          s"Join request from: $node , current state: ${ClusterState.getCurrentState} , down nodes: ${DData.getDownNodes}"
        )
        val res = ClusterState.getCurrentState match {
          case Stable =>
            if (DData.getDownNodes.contains(node)) {
              (None, JoinShutdown)
            } else {
              (Some(NodesJoinedDetected(Set(node))), JoinOk)
            }
          case DownNodes(nodes) =>
            if (nodes.contains(node)) {
              (Some(NodesJoinedDetected(Set(node))), JoinBootComponents)
            } else if (DData.getDownNodes.contains(node)) {
              (None, JoinShutdown)
            } else {
              (Some(NodesJoinedDetected(Set(node))), JoinOk)
            }

        }
        sender ! res._2
        if (res._1.isDefined) self ! res._1.get
        log.info(s"Join verdict: $res")
        alert(node, res._2)

        val fHname =
          blocking {
            Future {
              val addr = InetAddress.getByName(node)
              addr.getHostName()
            }
          }

        fHname.map(hostName => self ! UpdateHostName(node, hostName))

      case ce: ComponentEvent =>
        val hostName = hostNames.get(ce.id)
        alert(ce, hostName)

      case UpdateHostName(ip, hostName) => hostNames = hostNames.updated(ip, hostName)

      case PingGot(message) => log.debug(s"Got: $message")

      case str: String => log.info(s"Got: $str")

      case CheckElasticsearchMasterStatus => esStat.normalizeMasters()

      case CheckZookeeperStatus if ZookeeperUtils.isZkNode =>
        ZookeeperController.start

      case t: Task => {
        t match {
          case an @ AddNode(node) =>
            implicit val timeout = Timeout(24.hours)

            val ar = Grid.create(clazz = classOf[TaskExecutorActor],
                                 name = TaskExecutorActor.name + "AddNode" + System.currentTimeMillis().toString)

            val s = sender()
            //ar.ask(an)(implicitly[akka.util.Timeout],sender()) - equivalent besides the log print
            val f = (ar ? an).mapTo[TaskResult]
            f.foreach { ts =>
              logger.info(s"Task status is: $ts")
              s ! ts
            }

          case cn @ ClearNode(node) =>
            implicit val timeout = Timeout(1.hours)
            val included = Grid.jvms(Jvms.CTRL).filterNot(_.host == node)
            val ar =
              Grid.createSomewhere(white = included,
                                   black = Set.empty[GridJvm],
                                   clazz = classOf[TaskExecutorActor],
                                   name = TaskExecutorActor.name + "ClearNode" + System.currentTimeMillis().toString)

            val s = sender()
            val f = (ar ? cn).mapTo[TaskResult]
            f.foreach { ts =>
              logger.info(s"Task status is: $ts")
              s ! ts
            }

          case _ => // do nothing
        }
      }

      case CheckForWsCrisis =>
        logger.debug("Checking for web service crisis")

        val (greens, notGreens) = wsStat.getStatesMap.partition(_._2.getColor == GreenStatus)
        if (greens.size <= 2 && notGreens.size > 0) {
          logger.warn(s"web services are not responding, restarting ${notGreens.map(_._1).mkString(", ")}.")
          notGreens.foreach(comp => CommandActor.select(comp._1) ! RestartWebserver)
          system.scheduler.scheduleOnce(10.minutes, self, CheckForWsCrisis)
        } else {
          system.scheduler.scheduleOnce(30.seconds, self, CheckForWsCrisis)
        }
      case message => {
        logger.warn(s"unexcpected message $message")
      }
    }
}

object ZookeeperUtils {
  def isZkNode: Boolean = zkServers(Config.listenAddress)

  def zkServers = using(scala.io.Source.fromFile(s"${Config.cmwellHome}/conf/zookeeper/zoo.cfg")) { source =>
    source.getLines().filter(_.startsWith("server.")).map(_.dropWhile(_ != '=').takeWhile(_ != ':').substring(1)).toSet
  }
}
