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
package cmwell.ctrl.utils

import cmwell.ctrl.checkers._
import cmwell.ctrl.config.Config
import cmwell.ctrl.hc._
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsString, Json}

/**
  * Created by michael on 2/24/15.
  */
trait AlertReporter extends LazyLogging {
  private[this] var shortNames = Map.empty[String, String]
  private[this] var hostIps = Map.empty[String, Vector[String]]

  def updateShortName(ip: String, sn: String) = {
    val currentVal = shortNames.get(ip)
    currentVal match {
      case Some(v) => if (v != sn) shortNames = shortNames.updated(ip, sn)
      case None    => shortNames = shortNames.updated(ip, sn)
    }
  }

  def updateIps(host: String, ips: Vector[String]) = {
    hostIps = hostIps.updated(host, ips)
  }

  def getHostNameFromIp(ip: String): Option[String] = {
    hostIps.find(_._2.contains(ip)).map(_._1)
  }

  val alertsLogger = LoggerFactory.getLogger("alerts")
  object AlertColor {
    def fromStatusColor(sc: StatusColor) = {
      sc match {
        case GreenStatus  => GREEN
        case YellowStatus => YELLOW
        case RedStatus    => RED
      }
    }
  }
  trait AlertColor {
    def severity: String
  }
  case object GREEN extends AlertColor {
    override def severity: String = "CLEAR"
  }
  case object YELLOW extends AlertColor {
    override def severity: String = "MINOR"
  }
  case object RED extends AlertColor {
    override def severity: String = "CRITICAL"
  }

//  def alert(msg : String, color : AlertColor = GREEN, host : Option[String] = None, eventKey : Option[String] = None, node : Option[String] = None) {
//    logger.info(s"""CM-WELL-ALERT alert_status=$color severity=${color.severity} alert_message='$msg' EventSource=ctrl${if(host.isDefined)
//    s" alert_host=${host.get}" else ""}${if(eventKey.isDefined) s" EventKey=${eventKey.get}" else ""}${if(node.isDefined) s" Node=${node.get}" else ""}""")
//  }

  case class Alert(msg: String,
                   color: AlertColor = GREEN,
                   host: Option[String] = None,
                   eventKey: Option[String] = None,
                   node: Option[String] = None) {
    def json: String = {
      val fields = Map(
        "message" -> msg,
        "host" -> host.flatMap(shortNames.get(_)).getOrElse(""),
        "group" -> "CM-WELL",
        "key" -> eventKey.getOrElse(""),
        "severity" -> color.severity,
        "status" -> color.toString,
        "node" -> node.getOrElse("")
      ).filter(_._2.nonEmpty)

      Json
        .obj(
          "event" -> JsString("trams.alert"),
          "environment" -> JsString(Config.clusterName),
          "trams" -> Json.obj(
            "alert" -> Json.toJson(fields)
          )
        )
        .toString
    }
  }

  def alert(msg: String,
            color: AlertColor = GREEN,
            host: Option[String] = None,
            eventKey: Option[String] = None,
            node: Option[String] = None) {
    val alertObj = Alert(msg, color, host, eventKey, node)

    logger.info(
      s"""CM-WELL-ALERT alert_status=$color severity=${color.severity} alert_message='$msg' EventSource=ctrl${if (host.isDefined)
        s" alert_host=${host.get}"
      else ""}${if (eventKey.isDefined) s" EventKey=${eventKey.get}" else ""}${if (node.isDefined) s" Node=${node.get}"
      else ""}"""
    )
    alertsLogger.info(alertObj.json)
  }

  def alert(clusterEvent: ClusterEvent) {
    clusterEvent match {
      case NodesJoinedDetected(nodes) => alert(s"The nodes ${nodes.mkString(",")} joined the cluster.", GREEN)
      case DownNodesDetected(nodes)   => alert(s"The nodes ${nodes.mkString(",")} are down.", RED)
      case EndOfGraceTime             => alert("End of grace time.", YELLOW)
    }
  }

  def alert(host: String, joinResponse: JoinResponse) {
    joinResponse match {
      case JoinOk => alert(s"$host was trying to join and was accepted.", GREEN, Some(host))
      case JoinBootComponents =>
        alert(
          s"$host was trying to return to the cluster and was accepted. It will start cm-well components on this machine.",
          GREEN,
          Some(host)
        )
      case JoinShutdown =>
        alert(s"$host was trying to return to the cluster but was rejected because to much time passed.",
              YELLOW,
              Some(host))
    }
  }

  def alert(ce: ComponentEvent, hostName: Option[String]) {
    ce match {
      case WebNormalEvent(ip) => alert(s"Web is normal at $ip", GREEN, Some(ip), Some("Web"), hostName)
      case WebBadCodeEvent(ip, code) =>
        alert(s"Web is returning code $code at $ip", YELLOW, Some(ip), Some("Web"), hostName)
      case WebDownEvent(ip) => alert(s"Web is down at $ip", RED, Some(ip), Some("Web"), hostName)

      case BgOkEvent(ip)    => alert(s"Bg is normal at $ip", GREEN, Some(ip), Some("Bg"), hostName)
      case BgNotOkEvent(ip) => alert(s"Bg is not ok (stuck?) at $ip", RED, Some(ip), Some("Bg"), hostName)

      case CassandraNodeNormalEvent(ip) =>
        alert(s"Cassandra is normal at $ip", GREEN, getHostNameFromIp(ip), Some("Cassandra"), hostName)
      case CassandraNodeDownEvent(ip) =>
        alert(s"Cassandra is down at $ip", YELLOW, getHostNameFromIp(ip), Some("Cassandra"), hostName)
      case CassandraNodeUnavailable(ip) =>
        alert(s"Cassandra is unavailable at $ip", YELLOW, getHostNameFromIp(ip), Some("Cassandra"), hostName)

      case ElasticsearchNodeGreenEvent(ip) =>
        alert(s"Elasticsearch is green at $ip", GREEN, Some(ip), Some("Elasticsearch"), hostName)
      case ElasticsearchNodeYellowEvent(ip) =>
        alert(s"Elasticsearch is yellow at $ip", YELLOW, Some(ip), Some("Elasticsearch"), hostName)
      case ElasticsearchNodeRedEvent(ip) =>
        alert(s"Elasticsearch is red at $ip", RED, Some(ip), Some("Elasticsearch"), hostName)
      case ElasticsearchNodeBadCodeEvent(ip, code) =>
        alert(s"Elasticsearch is returning code $code at $ip", YELLOW, Some(ip), Some("Elasticsearch"), hostName)
      case ElasticsearchNodeDownEvent(ip) =>
        alert(s"Elasticsearch is down at $ip", RED, Some(ip), Some("Elasticsearch"), hostName)

      case DcNormalEvent(dc, checker) =>
        alert(s"Syncing data from data center $dc.", GREEN, Some(checker), Some("DataCenter"))
      case DcNotSyncingEvent(dc, checker) =>
        alert(s"Couldn't sync from data center $dc.", YELLOW, Some(checker), Some("DataCenter"))
      case DcLostConnectionEvent(dc, checker) =>
        alert(s"Couldn't sync from data center $dc for very long time.", RED, Some(checker), Some("DataCenter"))
      case DcCouldNotGetDcEvent(dc, checker) =>
        alert(s"Couldn't get meta data from the data center $dc.", YELLOW, Some(checker), Some("DataCenter"))
      case DcCouldNotGetDcLongEvent(dc, checker) =>
        alert(s"Couldn't get meta data from the data center $dc for long time.", RED, Some(checker), Some("DataCenter"))
      case DcCouldNotGetDataEvent(checker) =>
        alert(s"Couldn't get local data center meta data.", RED, Some(checker), Some("DataCenter"))

      case ComponentChangedColorEvent(component, color) =>
        alert(s"$component changed to $color", AlertColor.fromStatusColor(color), eventKey = Some(component))

      case _ => //
    }
  }
}
