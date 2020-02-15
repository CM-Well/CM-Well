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

import cmwell.ctrl.checkers._
import cmwell.ctrl.commands._
import cmwell.ctrl.config.Config._
import cmwell.ctrl.server.CommandActor
import cmwell.ctrl.utils.AlertReporter
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid

import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by michael on 2/26/15.
  */
trait ComponentGridState {
  protected var states: Map[String, ComponentState] = Map.empty[String, ComponentState]
  def name: String

  def update(host: String, cs: ComponentState) {
    val previousColor = Try(getColor)
    val previousState = states.getOrElse(host, null)
    val currentState = getCurrentState(host, previousState, cs)
    states = states.updated(host, currentState)
    afterUpdate
    raiseEvents(host)(previousState, currentState).foreach(state => HealthActor.ref ! state)
    val currentColor = Try(getColor)

    if (currentColor.isSuccess && previousColor.isSuccess && currentColor.get != previousColor.get) {
      HealthActor.ref ! ComponentChangedColorEvent(name, currentColor.get)
    }

    handleState(host)(cs)
  }

  def afterUpdate = {}

  def getCurrentState(host: String, previousState: ComponentState, receivedState: ComponentState): ComponentState =
    receivedState

  def raiseEvents(ip: String): PartialFunction[(ComponentState, ComponentState), Set[ComponentEvent]]

  def handleState(ip: String): PartialFunction[ComponentState, Unit] = {
    case _ => //
  }

  def remove(host: String) {
    val previousColor = Try(getColor)
    states = states.filterNot(x => x._1 == host)
    afterUpdate
    val currentColor = Try(getColor)

    if (currentColor.isSuccess && previousColor.isSuccess && currentColor.get != previousColor.get) {
      HealthActor.ref ! ComponentChangedColorEvent(name, currentColor.get)
    }
  }

  def getStatesMap = states

  def getColor: StatusColor
}

class WebGridStatus extends ComponentGridState with AlertReporter {

  override def name: String = "Web"

  private[this] var wsColor: StatusColor = GreenStatus

  private[this] def updateWsColor() {
    val colors = states.values.map(_.getColor).toList
    val reds = colors.count(RedStatus.==)
    val yellows = colors.count(YellowStatus.==)
    val greens = colors.count(GreenStatus.==)

    if (greens < colors.size) {
      logger.warn(s"updateWsColor: HC detects some non-greens ws, states = $states")
    }

    if (greens >= reds + yellows) wsColor = GreenStatus
    else if (greens > 1) wsColor = YellowStatus
    else wsColor = RedStatus
  }

  override def afterUpdate: Unit = updateWsColor()

  override def getColor: StatusColor = wsColor

  override def raiseEvents(ip: String): PartialFunction[(ComponentState, ComponentState), Set[ComponentEvent]] = {
    case (s1: ReportTimeout, s2: ReportTimeout) => Set.empty[ComponentEvent]
    case (s1: WebOk, s2: WebOk)                 => Set.empty[ComponentEvent]
    case (s1: WebDown, s2: WebDown)             => Set.empty[ComponentEvent]
    case (s1: WebBadCode, s2: WebBadCode)       => Set.empty[ComponentEvent]

    case (_, s2: WebOk)         => Set(WebNormalEvent(ip))
    case (_, s2: WebBadCode)    => Set(WebBadCodeEvent(ip, s2.code))
    case (_, s2: WebDown)       => Set(WebDownEvent(ip))
    case (_, s2: ReportTimeout) => Set(ReportTimeoutEvent(ip))
  }

  val malFuncWss = scala.collection.mutable.Map.empty[String, Int]

  def checkRestart(ip: String): Unit = {
    val c = malFuncWss.getOrElse(ip, 0)
    logger.debug(s"The Web server on node $ip malfunctioned ${c + 1} times.")
    if (c >= webFailCount) {
      logger.debug(s"Selecting CommandActor on node $ip")
      CommandActor.select(ip) ! RestartWebserver

      malFuncWss.update(ip, 0)
      logger.warn(s"The Web server on node $ip malfunctioned $webFailCount times and therefore was restarted.")
      alert(s"The Web server on node $ip malfunctioned $webFailCount times and therefore was restarted.",
            YELLOW,
            Some(ip),
            Some("Web"),
            Some(ip))
    } else {
      malFuncWss.update(ip, c + 1)
    }
  }

  override def handleState(ip: String): PartialFunction[ComponentState, Unit] = {
    case s: WebOk =>
    //malFuncWss.update(ip, 0)
    case s: WebBadCode =>
    //checkRestart(ip)

    case s: WebDown =>
    //checkRestart(ip)

    case s: ReportTimeout =>
    case _                => //
  }
}

class DcGridStatus extends ComponentGridState with LazyLogging {
  private[this] var dcColor: StatusColor = GreenStatus

  override def name: String = "Dc"

  override def getColor: StatusColor = dcColor

  override def raiseEvents(ip: String): PartialFunction[(ComponentState, ComponentState), Set[ComponentEvent]] = {
    case (s1: ReportTimeout, s2: ReportTimeout) => Set.empty[ComponentEvent]

    case (s1: DcCouldNotGetDcStatus, s2: DcCouldNotGetDcStatus) =>
      logger.debug(
        s"DcGridStatus: s1.getColor=${s1.getColor}(${s1.errorCounter}), s2.getColor=${s2.getColor}(${s2.errorCounter})"
      )
      if (s1.getColor == GreenStatus && s2.getColor == YellowStatus) Set(DcCouldNotGetDcEvent(s2.dcId, s2.checkerHost))
      else if (s1.getColor == YellowStatus && s2.getColor == RedStatus)
        Set(DcCouldNotGetDcLongEvent(s2.dcId, s2.checkerHost))
      else Set.empty[ComponentEvent]

    case (s1: DcNotSyncing, s2: DcNotSyncing) if (s1.getColor == YellowStatus && s2.getColor == RedStatus) =>
      logger.debug(
        s"DcGridStatus: case (s1 : DcNotSyncing, s2 : DcNotSyncing) if (s1.getColor == YellowStatus && s2.getColor == RedStatus )"
      )
      logger.debug(s"DcGridStatus: s1.getColor=${s1.getColor}, s2.getColor=${s2.getColor}")
      Set(DcLostConnectionEvent(s1.dcId, s1.checkerHost))
    case (s1: DcNotSyncing, s2: DcNotSyncing) if (s1.getColor == GreenStatus && s2.getColor == YellowStatus) =>
      logger.debug(
        s"DcGridStatus: case (s1 : DcNotSyncing, s2 : DcNotSyncing) if (s1.getColor == GreenStatus && s2.getColor == YellowStatus )"
      )
      logger.debug(s"DcGridStatus: s1.getColor=${s1.getColor}, s2.getColor=${s2.getColor}")
      Set(DcNotSyncingEvent(s1.dcId, s1.checkerHost))

    case (s1: DcState, s2: DcSyncing) if s1.getColor == GreenStatus => Set.empty[ComponentEvent]
    case (_, s2: DcSyncing)                                         => Set(DcNormalEvent(s2.dcId, s2.checkerHost))
    case (_, s2: DcNotSyncing)                                      => Set.empty[ComponentEvent]
    case (_, s2: DcCouldNotGetDcStatus)                             => Set.empty[ComponentEvent]
    case (_, s2: ReportTimeout)                                     => Set(ReportTimeoutEvent(ip))
  }

  override def handleState(ip: String): PartialFunction[ComponentState, Unit] = {
    case s: DcNotSyncing => // todo: add poison pill functionality
    case _               => //
  }
}

class ZkGridStatus extends ComponentGridState with LazyLogging {
  override def name: String = "Zookeeper"

  override def getColor: StatusColor = {
    val numOfOk = states.count {
      case (_, _: ZookeeperOk) => true
      case _                   => false
    }
    val numOfRo = states.count {
      case (_, _: ZookeeperReadOnly) => true
      case _                         => false
    }
    val totalMachines = states.size
    if (totalMachines < 3) {
      if (numOfOk == 1) GreenStatus else RedStatus
    } else {
      if (numOfOk == 3) GreenStatus else if (numOfOk == 2) YellowStatus else RedStatus
    }
  }

  override def raiseEvents(ip: String): PartialFunction[(ComponentState, ComponentState), Set[ComponentEvent]] = {
    case (s1: ReportTimeout, s2: ReportTimeout)                     => Set.empty[ComponentEvent]
    case (s1: ZookeeperOk, s2: ZookeeperOk)                         => Set.empty[ComponentEvent]
    case (s1: ZookeeperReadOnly, s2: ZookeeperReadOnly)             => Set.empty[ComponentEvent]
    case (s1: ZookeeperNotOk, s2: ZookeeperNotOk)                   => Set.empty[ComponentEvent]
    case (s1: ZookeeperNotRunning, s2: ZookeeperNotRunning)         => Set.empty[ComponentEvent]
    case (s1: ZookeeperSeedNotRunning, s2: ZookeeperSeedNotRunning) => Set.empty[ComponentEvent]
    case (_, s2: ZookeeperOk)                                       => Set(ZkOkEvent(ip))
    case (_, s2: ZookeeperReadOnly)                                 => Set(ZkReadOnlyEvent(ip))
    case (_, s2: ZookeeperNotOk)                                    => Set(ZkNotOkEvent(ip))
    case (_, s2: ZookeeperNotRunning)                               => Set(ZkNotRunningEvent(ip))
    case (_, s2: ZookeeperSeedNotRunning)                           => Set(ZkSeedNotRunningEvent(ip))
    case (_, s2: ReportTimeout)                                     => Set(ReportTimeoutEvent(ip))

  }
}

class KafkaGridStatus extends ComponentGridState with LazyLogging {
  override def name: String = "Kafka"

  override def getColor: StatusColor = {
    val numOfOks = states.count {
      case (_, _: KafkaOk) => true
      case _               => false
    }
    val totalMachines = states.size
    if (numOfOks == totalMachines) GreenStatus else RedStatus
  }

  override def raiseEvents(ip: String): PartialFunction[(ComponentState, ComponentState), Set[ComponentEvent]] = {
    case (s1: ReportTimeout, s2: ReportTimeout) => Set.empty[ComponentEvent]
    case (s1: KafkaOk, s2: KafkaOk)             => Set.empty[ComponentEvent]
    case (s1: KafkaNotOk, s2: KafkaNotOk)       => Set.empty[ComponentEvent]
    case (_, s2: KafkaOk)                       => Set(KafkaOkEvent(ip))
    case (_, s2: KafkaNotOk)                    => Set(KafkaNotOkEvent(ip))
    case (_, s2: ReportTimeout)                 => Set(ReportTimeoutEvent(ip))
  }
}

class BgGridStatus extends ComponentGridState with LazyLogging {
  override def name: String = "Bg"

  override def getColor: StatusColor = {
    val numOfOks = states.count {
      case (_, _: BgOk) => true
      case _            => false
    }
    val totalMachines = states.size
    if (numOfOks == totalMachines) GreenStatus else RedStatus
  }

  override def raiseEvents(ip: String): PartialFunction[(ComponentState, ComponentState), Set[ComponentEvent]] = {
    case (s1: ReportTimeout, s2: ReportTimeout) => Set.empty[ComponentEvent]
    case (s1: BgOk, s2: BgOk)                   => Set.empty[ComponentEvent]
    case (s1: BgNotOk, s2: BgNotOk)             => Set.empty[ComponentEvent]
    case (_, s2: BgOk)                          => Set(BgOkEvent(ip))
    case (_, s2: BgNotOk)                       => Set(BgNotOkEvent(ip))
    case (_, s2: ReportTimeout)                 => Set(ReportTimeoutEvent(ip))
  }
}

class CassandraGridStatus extends ComponentGridState {
  override def name: String = "Cassandra"
  private[this] var state: ComponentState = _

  override def getColor: StatusColor = state.getColor

  override def update(host: String, cs: ComponentState): Unit = {
    val previousColor = Try(getColor)
    raiseEvents(host)(state, cs).foreach(state => HealthActor.ref ! state)
    state = cs
    val currentColor = Try(getColor)

    if (currentColor.isSuccess && previousColor.isSuccess && currentColor.get != previousColor.get) {
      HealthActor.ref ! ComponentChangedColorEvent(name, currentColor.get)
    }
  }

  override def raiseEvents(ip: String): PartialFunction[(ComponentState, ComponentState), Set[ComponentEvent]] = {
    case (s1: ReportTimeout, s2: ReportTimeout) => Set.empty[ComponentEvent]
    case (s1: CassandraOk, s2: CassandraOk) =>
      val partitionPrev = s1.m.partition(item => item._2 == "UN")
      val partitionNow = s2.m.partition(item => item._2 == "UN")
      (partitionNow._1.keySet -- partitionPrev._1.keySet)
        .map(ip => CassandraNodeNormalEvent(ip)) ++ (partitionNow._2.keySet -- partitionPrev._2.keySet)
        .map(ip => CassandraNodeDownEvent(ip))
    case (_, s2: CassandraOk) =>
      val partition = s2.m.partition(item => item._2 == "UN")
      partition._1.keySet.map(ip => CassandraNodeNormalEvent(ip)) ++ partition._2.keySet
        .map(ip => CassandraNodeDownEvent(ip))
    case (s1: CassandraOk, s2: CassandraDown) =>
      s1.m.keySet.map(ip => CassandraNodeUnavailable(ip))
    case (_, s2: CassandraDown) => Set.empty[ComponentEvent]
    case (_, s2: ReportTimeout) => Set(ReportTimeoutEvent(ip))
  }

  def getState = if (state != null) state else CassandraDown()
}

class ElasticsearchGridStatus extends ComponentGridState with LazyLogging {
  import cmwell.util.concurrent.delayedTask

  private lazy val seedNodes = ZookeeperUtils.zkServers // this is ugly. will be refactored Some(Day)
  private def availableSeedNodes = seedNodes & Grid.availableMachines

  def normalizeMasters() = { // AKA taking the horses back to the dovecote
    val runningMasters = {
      val esStates = states.map(x => (x._1, x._2.asInstanceOf[ElasticsearchState]))
      esStates.collect { case esState if esState._2.hasMaster => esState._1 }.toSet
    }

    val seedNodesThatShouldStartMaster = availableSeedNodes -- runningMasters
    val nodesThatShouldStopMaster = {
      val runningMastersNotOnSeeds = runningMasters -- availableSeedNodes
      // always take 1 (if the set is empty of course take nothing) to put down master nodes slowly.
      // On the next iteration on other node will be stopped until no master elastic on non seed node
      runningMastersNotOnSeeds.take(1)
    }

    seedNodesThatShouldStartMaster.foreach(CommandActor.select(_) ! StartElasticsearchMaster)
    nodesThatShouldStopMaster.foreach(
      node => delayedTask(15.seconds) { CommandActor.select(node) ! StopElasticsearchMaster }
    )

    val updatedMasters = runningMasters ++ seedNodesThatShouldStartMaster -- nodesThatShouldStopMaster

    val replacementNewMasters = { // replacing MIA seed nodes
      val availableNonMasterNodes = Grid.availableMachines -- updatedMasters
      availableNonMasterNodes.take(3 - updatedMasters.size)
    }
    replacementNewMasters.foreach(CommandActor.select(_) ! StartElasticsearchMaster)

    // report to logfile what just happen:
    val p: Set[_] => String = _.mkString(",")
    if (seedNodesThatShouldStartMaster.size + nodesThatShouldStopMaster.size + replacementNewMasters.size == 0)
      logger.debug("Normalizing ES Masters: Nothing to do (all ES-Masters are in the right places)")
    else
      logger.info(
        s"Normalizing ES Masters: seedNodesThatShouldStartMaster[${p(seedNodesThatShouldStartMaster)}], " +
          s"nodesThatShouldStopMaster[${p(nodesThatShouldStopMaster)}], " +
          s"replacementNewMasters[${p(replacementNewMasters)}]"
      )
  }

  override def name: String = "Elasticsearch"

  override def getColor: StatusColor = states.values.head.getColor

  override def raiseEvents(ip: String): PartialFunction[(ComponentState, ComponentState), Set[ComponentEvent]] = {
    case (s1: ReportTimeout, s2: ReportTimeout)               => Set.empty[ComponentEvent]
    case (s1: ElasticsearchGreen, s2: ElasticsearchGreen)     => Set.empty[ComponentEvent]
    case (s1: ElasticsearchYellow, s2: ElasticsearchYellow)   => Set.empty[ComponentEvent]
    case (s1: ElasticsearchRed, s2: ElasticsearchRed)         => Set.empty[ComponentEvent]
    case (s1: ElasticsearchBadCode, s2: ElasticsearchBadCode) => Set.empty[ComponentEvent]
    case (s1: ElasticsearchDown, s2: ElasticsearchDown)       => Set.empty[ComponentEvent]

    case (_, s2: ElasticsearchGreen)   => Set(ElasticsearchNodeGreenEvent(ip))
    case (_, s2: ElasticsearchYellow)  => Set(ElasticsearchNodeYellowEvent(ip))
    case (_, s2: ElasticsearchRed)     => Set(ElasticsearchNodeRedEvent(ip))
    case (_, s2: ElasticsearchBadCode) => Set(ElasticsearchNodeBadCodeEvent(ip, s2.code))
    case (_, s2: ElasticsearchDown)    => Set(ElasticsearchNodeDownEvent(ip))
    case (_, s2: ReportTimeout)        => Set(ReportTimeoutEvent(ip))
  }

  override def afterUpdate: Unit = {
    //spawnMasters
  }
}
