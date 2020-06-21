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
package cmwell.ctrl.checkers

import cmwell.ctrl.config.Config._
import cmwell.ctrl.hc.ZookeeperUtils

/**
  * Created by michael on 12/4/14.
  */
trait StatusColor
case object GreenStatus extends StatusColor {
  override def toString: String = "green"
}

case object YellowStatus extends StatusColor {
  override def toString: String = "yellow"
}

case object RedStatus extends StatusColor {
  override def toString: String = "red"
}

trait ComponentState {
  def getColor: StatusColor
  def genTime: Long
  //def raiseEvent(id : String, previousState : ComponentState) : Set[ComponentEvent]

//  def event(id : String) : ComponentEvent
//
//  def getEvent(cs : ComponentState, id : String) : Option[ComponentEvent] = {
//    if(cs != this) Some(event(id)) else None
//  }
}

object ComponentState {
  def currentTimestamp: Long = System.currentTimeMillis / 1000

}

trait WebState extends ComponentState {}

case class WebOk(responseTime: Int, genTime: Long = ComponentState.currentTimestamp) extends WebState {
  override def getColor: StatusColor = GreenStatus

}

case class WebBadCode(code: Int, responseTime: Int, genTime: Long = ComponentState.currentTimestamp) extends WebState {
  override def getColor: StatusColor = YellowStatus

}

case class WebDown(genTime: Long = ComponentState.currentTimestamp) extends WebState {
  override def getColor: StatusColor = RedStatus

}

trait CassandraState extends ComponentState
case class CassandraOk(m: Map[String, String],
                       rackMapping: Map[String, Iterable[String]] = Map.empty,
                       genTime: Long = ComponentState.currentTimestamp)
    extends CassandraState {
  def getTotal = m.values.size
  def getUnCount = m.values.filter(_ == "UN").size
  def getHost(ip: String): String = {
    rackMapping.find(_._2.toVector.contains(ip)).map(_._1).getOrElse("NA")
  }

  private def decideColor(mappings: Map[String, String]): StatusColor = {
    val total = mappings.values.size
    val un = mappings.values.filter(_ == "UN").size
    val dn = total - un

    if (dn < 2 && un > dn) GreenStatus
    else if (dn > 3 || un < dn) RedStatus
    else YellowStatus
  }

  override def getColor: StatusColor = {
    if (rackMapping.isEmpty) {
      decideColor(m)
    } else {
      val rackMap = rackMapping.map { rack =>
        val rackStat =
          if (rack._2.map(innerIp => m.getOrElse(innerIp, "DN")).toVector.filterNot(_ == "UN").size > 0) "DN" else "UN"
        rack._1 -> rackStat
      }
      decideColor(rackMap)
    }
  }
}

case class CassandraDown(genTime: Long = ComponentState.currentTimestamp) extends CassandraState {
  override def getColor: StatusColor = RedStatus
}

trait ElasticsearchState extends ComponentState {
  def hasMaster: Boolean
}
case class ElasticsearchGreen(n: Int,
                              d: Int,
                              p: Int,
                              s: Int,
                              hasMaster: Boolean = false,
                              genTime: Long = ComponentState.currentTimestamp)
    extends ElasticsearchState {
  override def getColor: StatusColor = GreenStatus
}

case class ElasticsearchYellow(n: Int,
                               d: Int,
                               p: Int,
                               s: Int,
                               hasMaster: Boolean = false,
                               genTime: Long = ComponentState.currentTimestamp)
    extends ElasticsearchState {
  override def getColor: StatusColor = YellowStatus
}

case class ElasticsearchRed(n: Int,
                            d: Int,
                            p: Int,
                            s: Int,
                            hasMaster: Boolean = false,
                            genTime: Long = ComponentState.currentTimestamp)
    extends ElasticsearchState {
  override def getColor: StatusColor = RedStatus
}

case class ElasticsearchBadCode(code: Int, hasMaster: Boolean = false, genTime: Long = ComponentState.currentTimestamp)
    extends ElasticsearchState {
  override def getColor: StatusColor = RedStatus
}

case class ElasticsearchDown(hasMaster: Boolean = false, genTime: Long = ComponentState.currentTimestamp)
    extends ElasticsearchState {
  override def getColor: StatusColor = RedStatus
}

trait DcState extends ComponentState

case class DcStatesBag(states: Map[String, DcState],
                       checkerHost: String,
                       genTime: Long = ComponentState.currentTimestamp)
    extends DcState {
  override def getColor: StatusColor = {
    if (states.exists(_._2.getColor == RedStatus)) RedStatus
    else if (states.exists(_._2.getColor == YellowStatus)) YellowStatus
    else GreenStatus
  }
}

case class DcSyncing(dcId: String, dcDiff: DcDiff, checkerHost: String, genTime: Long = ComponentState.currentTimestamp)
    extends DcState {
  override def getColor: StatusColor = GreenStatus
}

case class DcNotSyncing(dcId: String,
                        dcDiff: DcDiff,
                        notSyncingCounter: Int,
                        checkerHost: String,
                        genTime: Long = ComponentState.currentTimestamp)
    extends DcState {
  override def getColor: StatusColor = {
    if (notSyncingCounter > 20) RedStatus
    else if (notSyncingCounter > 3) YellowStatus
    else GreenStatus
  }
}

//case class DcSyncStatus(dcId : String, dcDiff : DcDiff, notSyncingCounter : Int, genTime : Long = ComponentState.currentTimestamp) extends DcState {
//  def syncing = notSyncingCounter > 3
//  def notSyncing = !syncing
//  override def getColor: StatusColor = {
//    if(notSyncingCounter > 3) YellowStatus
//    else if(notSyncingCounter > 20) RedStatus
//    else GreenStatus
//  }
//}

case class DcCouldNotGetDcStatus(dcId: String,
                                 dcDiff: DcDiff,
                                 errorCounter: Int,
                                 checkerHost: String,
                                 genTime: Long = ComponentState.currentTimestamp)
    extends DcState {
  override def getColor: StatusColor = {
    if (errorCounter > 20) RedStatus
    else if (errorCounter > 3) YellowStatus
    else GreenStatus
  }
}

trait ZookeeperState extends ComponentState

case class ZookeeperOk(genTime: Long = ComponentState.currentTimestamp) extends ZookeeperState {
  override def getColor: StatusColor = GreenStatus
}

case class ZookeeperReadOnly(genTime: Long = ComponentState.currentTimestamp) extends ZookeeperState {
  override def getColor: StatusColor = YellowStatus
}

case class ZookeeperNotRunning(genTime: Long = ComponentState.currentTimestamp) extends ZookeeperState {
  override def getColor: StatusColor = GreenStatus
}

case class ZookeeperSeedNotRunning(genTime: Long = ComponentState.currentTimestamp) extends ZookeeperState {
  override def getColor: StatusColor = RedStatus
}

case class ZookeeperNotOk(genTime: Long = ComponentState.currentTimestamp) extends ZookeeperState {
  override def getColor: StatusColor = RedStatus
}

trait KafkaState extends ComponentState

case class KafkaOk(genTime: Long = ComponentState.currentTimestamp) extends KafkaState {
  override def getColor: StatusColor = GreenStatus
}

case class KafkaNotOk(genTime: Long = ComponentState.currentTimestamp) extends KafkaState {
  override def getColor: StatusColor = RedStatus
}

trait BgState extends ComponentState

case class BgOk(genTime: Long = ComponentState.currentTimestamp) extends BgState {
  override def getColor: StatusColor = GreenStatus
}

case class BgNotOk(genTime: Long = ComponentState.currentTimestamp) extends BgState {
  override def getColor: StatusColor = RedStatus
}

trait SystemState extends ComponentState

case class SystemResponse(interfaces: Vector[String],
                          shortName: String,
                          genTime: Long = ComponentState.currentTimestamp)
    extends SystemState {
  override def getColor: StatusColor = GreenStatus
}

case class ReportTimeout(genTime: Long = ComponentState.currentTimestamp) extends ComponentState {
  override def getColor: StatusColor = RedStatus
}
