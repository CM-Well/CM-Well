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
package actions

import actions.Color._
import actions.GridMonitoring.{Active, All, CsvPretty}
import akka.pattern.ask
import cmwell.ctrl.checkers._
import cmwell.ctrl.client.CtrlClient
import cmwell.domain._
import cmwell.fts.FieldFilter
import cmwell.util.FullBox
import cmwell.util.build.BuildInfo
import cmwell.util.os.Props
import cmwell.util.string.Hash.crc32
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Settings.esTimeout
import cmwell.ws.util.DateParser._
import cmwell.ws.util.FieldFilterParser
import cmwell.ws.{BGMonitorActor, GetOffsetInfo, OffsetsInfo, PartitionOffsetsInfo, PartitionStatus, Settings, Green => _, Red => _, Yellow => _}
import com.typesafe.scalalogging.LazyLogging
import javax.inject._
import k.grid.Grid
import logic.CRUDServiceFS
import org.joda.time._
import trafficshaping.TrafficMonitoring
import wsutil._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.math.min
import scala.util.{Success, Try}

object BgMonitoring {
  lazy val monitor = Grid.serviceRef(classOf[BGMonitorActor].getName)
}

@Singleton
class ActiveInfotonGenerator @Inject()(
                                        backPressureToggler: controllers.BackPressureToggler,
                                        crudServiceFS: CRUDServiceFS,
                                        dashBoard: DashBoard,
                                        cmwellRDFHelper: CMWellRDFHelper
                                      ) extends LazyLogging {

  import BgMonitoring.{monitor => bgMonitor}
  import dashBoard._
  lazy val typesCache = crudServiceFS.passiveFieldTypesCache

  /**
    * @return /proc/node infoton fields map
    */
  private[this] def nodeValFields: FieldsOpt = {
    val esColor =
      Try(
        Await.result(dashBoard.getElasticsearchStatus(), esTimeout)._1.toString
      ).getOrElse("grey")
    Some(
      Map[String, Set[FieldValue]](
        "pbp" -> Set(FString(backPressureToggler.get)),
        "cm-well_release" -> Set(FString(BuildInfo.release)),
        "cm-well_version" -> Set(FString(BuildInfo.version)),
        "git_commit_version" -> Set(FString(BuildInfo.gitCommitVersion)),
        "scala_version" -> Set(FString(BuildInfo.scalaVersion)),
        "sbt_version" -> Set(FString(BuildInfo.sbtVersion)),
        "cassandra_version" -> Set(FString(BuildInfo.cassandraVersion)),
        "elasticsearch_version" -> Set(FString(BuildInfo.elasticsearchVersion)),
        //        "es_cluster_name" -> Set(FString(cmwell.fts.Settings.clusterName)),
        "build_machine" -> Set(FString(BuildInfo.buildMachine)),
        "build_time" -> Set(FDate(BuildInfo.buildTime)),
        "encoding_version" -> Set(FString(BuildInfo.encodingVersion)),
        "java_runtime_name" -> Set(
          FString(System.getProperty("java.runtime.name"))
        ),
        "java_runtime_version" -> Set(
          FString(System.getProperty("java.runtime.version"))
        ),
        "java_vendor" -> Set(FString(System.getProperty("java.vendor"))),
        "java_version" -> Set(FString(System.getProperty("java.version"))),
        "java_vm_name" -> Set(FString(System.getProperty("java.vm.name"))),
        "java_vm_version" -> Set(
          FString(System.getProperty("java.vm.version"))
        ),
        "os_arch" -> Set(FString(System.getProperty("os.arch"))),
        "os_name" -> Set(FString(System.getProperty("os.name"))),
        "os_version" -> Set(FString(System.getProperty("os.version"))),
        "user_timezone" -> Set(FString(System.getProperty("user.timezone"))),
        "machine_name" -> Set(FString(Props.machineName)),
        "es_color" -> Set(FString(esColor)),
        "use_auth" -> Set(
          FBoolean(java.lang.Boolean.getBoolean("use.authorization"))
        )
      )
    )
  }

  def getColoredStatus(clr: Color): String = {
    val (color, status) = Map(
      Green -> ("green", "Green"),
      Yellow -> ("yellow", "Yellow"),
      Red -> ("red", "Red"),
      Grey -> ("grey", "N/A")
    )(clr)
    coloredHtmlString(color, status)
  }

  def coloredHtmlString(color: String, status: String) =
    s"""<span style='color:$color'>$status</span>"""

  private[this] def colorAdapter(c: StatusColor): Color.Color = {
    val r = c match {
      case GreenStatus  => Green
      case YellowStatus => Yellow
      case RedStatus    => Red
    }
    r
  }

  private[this] def getWsHealth(it: Iterable[ComponentState],
                                c: StatusColor) = {
    val wsColor = colorAdapter(c)
    val p = it.map(_.getColor).partition(_ == GreenStatus)
    val wsTxt = s"${p._1.size} Up / ${it.size}"
    val d = new DateTime(it.head.genTime * 1000L)
    val ws = ((wsColor, wsTxt), d)
    ws
  }

  private[this] def getElasticsearchHealth(cr: ComponentState) = {
    val dt = new DateTime(cr.genTime * 1000L)
    cr match {
      case ElasticsearchGreen(n, d, p, s, _, _) =>
        ((Green, s"n:$n,d:$d,p:$p,s:$s"), dt)
      case ElasticsearchYellow(n, d, p, s, _, _) =>
        ((Yellow, s"n:$n,d:$d,p:$p,s:$s"), dt)
      case ElasticsearchRed(n, d, p, s, _, _) =>
        ((Red, s"n:$n,d:$d,p:$p,s:$s"), dt)
      case ElasticsearchDown(_, _) =>
        ((Red, s"Elasticsearch is down"), dt)
      case ElasticsearchBadCode(code, _, _) =>
        ((Red, s"Elasticsearch check has produced http code: $code"), dt)
      case ReportTimeout(_) =>
        ((Red, "Couldn't retrieve Elasticsearch status"), dt)

    }
  }

  private[this] def getCassandraHealth(cr: ComponentState) = {
    cr match {
      case co: CassandraOk =>
        val color = colorAdapter(co.getColor)
        val total = co.getTotal
        val un = co.getUnCount
        val casTxt = s"$un Up / $total"
        ((color, casTxt), new DateTime(cr.genTime * 1000L))
      case CassandraDown(_) =>
        ((Red, "Cassandra is down"), new DateTime(cr.genTime * 1000L))
      case ReportTimeout(_) =>
        (
          (Red, "Couldn't retrieve Cassandra status"),
          new DateTime(cr.genTime * 1000L)
        )
    }
  }

  private[this] def getZookeeperHealth(cr: Iterable[ComponentState],
                                       c: StatusColor) = {
    val color = colorAdapter(c)
    val oks = cr.count(_.isInstanceOf[ZookeeperOk])
    val notOks = cr.count(_.isInstanceOf[ZookeeperNotOk])
    val notRunning =
      cr.count(state => state.isInstanceOf[ZookeeperSeedNotRunning])
    val readOnly = cr.count(_.isInstanceOf[ZookeeperReadOnly])
    val stats = s"ok: $oks, ro: $readOnly, noks: $notOks, nr: $notRunning"
    val genTime = Try(cr.head.genTime).getOrElse(0L)
    ((color, s"$stats"), new DateTime(genTime * 1000L))
  }

  private[this] def getKafkaHealth(cr: Iterable[ComponentState],
                                   c: StatusColor) = {
    val color = colorAdapter(c)
    val oks = cr.count(_.isInstanceOf[KafkaOk])
    val notOks = cr.count(_.isInstanceOf[KafkaNotOk])
    val stats = s"$oks Up / ${cr.size}"
    val genTime = Try(cr.head.genTime).getOrElse(0L)
    ((color, s"$stats"), new DateTime(genTime * 1000L))
  }

  private[this] def getClusterHealth: HealthTimedDataWithBG = {
    val r = CtrlClient.getClusterStatus.map { cs =>
      val ws = getWsHealth(cs.wsStat._1.values, cs.wsStat._2)
      val bg = {
        val data = Await
          .result(getDetailedBgStatus(), timeOut)
          .values
          .flatMap(_.flatMap(_._2))
        val pss = data.map(_.partitionStatus).toSeq
        val msg = {
          val lags = data.map(i => i.writeOffset - i.readOffset).toSeq
          s"Avg. Lag: ${if (lags.isEmpty) 0 else lags.sum / lags.length}"
        }
        (aggrPartitionColorToColor(pss) -> msg) -> DateTime.now()
      }
      val es = getElasticsearchHealth(cs.esStat)
      val cas = getCassandraHealth(cs.casStat)
      val zk =
        getZookeeperHealth(cs.zookeeperStat._1.values, cs.zookeeperStat._2)
      val kaf = getKafkaHealth(cs.kafkaStat._1.values, cs.kafkaStat._2)
      (ws, bg, es, cas, zk, kaf, cs.controlNode, cs.esMasters)
    }
    try {
      Await.result(r, timeOut)
    } catch {
      case t: Throwable =>
        (
          ((Red, "NA"), new DateTime(0L)),
          ((Red, "NA"), new DateTime(0L)),
          ((Red, "NA"), new DateTime(0L)),
          ((Red, "NA"), new DateTime(0L)),
          ((Red, "NA"), new DateTime(0L)),
          ((Red, "NA"), new DateTime(0L)),
          "",
          Set.empty[String]
        )
    }
  }

  private[this] def getClusterDetailedHealth: DetailedHealthTimedData = {
    val r = CtrlClient.getClusterDetailedStatus.map { cs =>
      val keys = cs.wsStat.keySet ++ cs.bgStat.keySet ++ cs.esStat.keySet ++ cs.casStat.keySet

      val m = keys.map { k =>
        (if (k == cs.healthHost) s"${k}*" else k) -> (colorAdapter(
          cs.wsStat.getOrElse(k, WebDown()).getColor
        ),
          colorAdapter(cs.bgStat.getOrElse(k, BgNotOk()).getColor),
          colorAdapter(cs.casStat.getOrElse(k, CassandraDown()).getColor),
          colorAdapter(cs.esStat.getOrElse(k, ElasticsearchDown()).getColor))
      }.toMap
      (m, new DateTime(cs.wsStat.head._2.genTime * 1000L))
    }
    try {
      Await.result(r, timeOut)
    } catch {
      case t: Throwable => (Map.empty, new DateTime(0L))
    }
  }

  private[this] def getClusterDetailedHealthNew: Map[String,
    ((String, Color),
      (String, Color),
      (String, Color),
      (String, Color),
      (String, Color),
      (String, Color))] = {
    val bgStatsFut = getDetailedBgStatus()

    val r = CtrlClient.getClusterDetailedStatus.flatMap { cs =>
      bgStatsFut.map { bgStat =>
        val keys = cs.wsStat.keySet ++ bgStat.keySet ++ cs.esStat.keySet ++ cs.casStat.keySet ++ cs.zkStat.keySet ++ cs.kafkaStat.keySet
        keys.map { k =>
          val wsMessage = cs.wsStat.getOrElse(k, WebDown()) match {
            case wr: WebOk =>
              (
                s"Ok<br>Response time: ${wr.responseTime} ms",
                colorAdapter(wr.getColor)
              )
            case wr: WebBadCode =>
              (
                s"Web is returning code: ${wr.code}<br>Response time: ${wr.responseTime} ms",
                colorAdapter(wr.getColor)
              )
            case wr: WebDown => (s"Web is down", colorAdapter(wr.getColor))
            case wr: ReportTimeout =>
              ("Can't retrieve Web status", colorAdapter(wr.getColor))
          }

          val bgMessage = {
            val data = bgStat.getOrElse(k, Set.empty)
            data
              .map {
                case (par, infos) =>
                  s"Par$par: ${infos.map(_.toShortInfoString).toSeq.sorted.mkString(" ")}"
              }
              .mkString("<br>") -> aggrPartitionColorToColor(
              data.flatMap(_._2).map(_.partitionStatus).toSeq
            )
          }

          def generateCasLine(casIns: (String, String)) : String = {
            val msg = s"${casIns._2}"
            if (casIns._2 == "UN") msg
            else s"""<font color="red"><b>$msg</b></font>"""
          }

          val casMessage = cs.casStat.getOrElse(k, CassandraDown()) match {
            case cr: CassandraOk =>
              (
                cr.m.map(generateCasLine).mkString("<br>"),
                colorAdapter(cr.getColor)
              )
            case cr: CassandraDown =>
              ("Cassandra is down", colorAdapter(cr.getColor))
            case cr: ReportTimeout =>
              (
                "Can't retraggrPartitionStatusieve Cassandra status",
                colorAdapter(cr.getColor)
              )
          }

          val zkMessage = cs.zkStat.getOrElse(k, ZookeeperNotOk()) match {
            case zr: ZookeeperOk => ("", colorAdapter(zr.getColor))
            case zr: ZookeeperReadOnly =>
              ("ZooKeeper is in read-only mode", colorAdapter(zr.getColor))
            case zr: ZookeeperNotOk =>
              ("ZooKeeper is not ok", colorAdapter(zr.getColor))
            case zr: ZookeeperNotRunning =>
              ("ZooKeeper is not running", colorAdapter(zr.getColor))
            case zr: ZookeeperSeedNotRunning =>
              ("ZooKeeper is not running", colorAdapter(zr.getColor))
            case zr: ReportTimeout =>
              ("Can't retrieve ZooKeeper status", colorAdapter(zr.getColor))
          }

          val kafMessage = cs.kafkaStat.getOrElse(k, KafkaNotOk()) match {
            case kafr: KafkaOk => ("", colorAdapter(kafr.getColor))
            case kafr: KafkaNotOk =>
              ("Kafka is not running", colorAdapter(kafr.getColor))
            case kafr: ReportTimeout =>
              ("Can't retrieve Kafka status", colorAdapter(kafr.getColor))
          }

          val esMessage = cs.esStat.getOrElse(k, ElasticsearchDown()) match {
            case er: ElasticsearchGreen =>
              val txt = if (er.hasMaster) "*" else ""
              (txt, colorAdapter(er.getColor))
            case er: ElasticsearchYellow =>
              val txt = if (er.hasMaster) "*" else ""
              (txt, colorAdapter(er.getColor))
            case er: ElasticsearchRed =>
              val txt = if (er.hasMaster) "*" else ""
              (txt, colorAdapter(er.getColor))
            case er: ElasticsearchDown =>
              val txt = if (er.hasMaster) "*" else ""
              (s"${txt}<br>Elasticsearch is down", colorAdapter(er.getColor))
            case er: ElasticsearchBadCode =>
              val txt = if (er.hasMaster) "*" else ""
              (
                s"${txt}<br>Elasticsearch is returning code: ${er.code}",
                colorAdapter(er.getColor)
              )
            case er: ReportTimeout =>
              ("Can't retrieve Elasticsearch status", colorAdapter(er.getColor))
          }
          (if (k == cs.healthHost) s"${k}*" else k) -> (wsMessage, bgMessage, casMessage, esMessage, zkMessage, kafMessage)
        }.toMap
      }
    }
    try {
      Await.result(r, timeOut)
    } catch {
      case t: Throwable => Map.empty
    }
  }

  private[this] def getDetailedBgStatus()
  : Future[Map[String, Set[(String, Iterable[PartitionOffsetsInfo])]]] = {
    val offsetInfoFut =
      ask(bgMonitor, GetOffsetInfo)(10.seconds).mapTo[OffsetsInfo]
    val partitionsByLocations = Grid.getSingletonsInfo
      .filter(sd => sd.name.startsWith("BGActor") && sd.location.contains(":"))
      .groupBy(sd => sd.location.substring(0, sd.location.indexOf(':')))
      .map { case (loc, sd) => loc -> sd.map(_.name.replace("BGActor", "")) }
    offsetInfoFut.map { offsetInfo =>
      val partitionsInfos =
        offsetInfo.partitionsOffsetInfo.values.groupBy(_.partition).map {
          case (partition, infos) =>
            partition.toString -> infos
        }
      partitionsByLocations.map {
        case (location, partitions) =>
          location -> partitions.map(p => p -> partitionsInfos.get(p)).collect {
            case (partition, Some(poi)) => partition -> poi
          }
      }
    }
  }

  private[this] def aggrPartitionColorToColor(
                                               ps: Seq[PartitionStatus]
                                             ): Color = {
    if (ps.forall(_ == cmwell.ws.Green)) Green
    else if (ps.forall(_ == cmwell.ws.Red)) Red
    else Yellow
  }

  private[this] def generateHealthFields: FieldsOpt = {
    val (ws, bg, es, ca, zk, kaf, controlNode, masters) = getClusterHealth
    val (wClr, wMsg) = ws._1
    val (bClr, bMsg) = bg._1
    val (eClr, eMsg) = es._1
    val (cClr, cMsg) = ca._1
    val (zkClr, zkMsg) = zk._1
    val (kafClr, kafMsg) = kaf._1
    Some(
      Map[String, Set[FieldValue]](
        "ws_color" -> Set(FString(wClr.toString)),
        "ws_message" -> Set(FString(wMsg)),
        "ws_generation_time" -> Set(FDate(fdf(ws._2))),
        "bg_color" -> Set(FString(bClr.toString)),
        "bg_message" -> Set(FString(bMsg)),
        "bg_generation_time" -> Set(FDate(fdf(bg._2))),
        "es_color" -> Set(FString(eClr.toString)),
        "es_message" -> Set(FString(eMsg)),
        "es_generation_time" -> Set(FDate(fdf(es._2))),
        "cas_color" -> Set(FString(cClr.toString)),
        "cas_message" -> Set(FString(cMsg)),
        "zk_color" -> Set(FString(zkClr.toString)),
        "zk_message" -> Set(FString(zkMsg)),
        "kf_color" -> Set(FString(kafClr.toString)),
        "kf_message" -> Set(FString(kafMsg)),
        "cas_generation_time" -> Set(FDate(fdf(ca._2))),
        "control_node" -> Set(FString(controlNode)),
        "masters" -> masters.map(FString(_))
      )
    )
  }

  /**
    * @return health markdown string
    */
  private[this] def generateHealthMarkdown(now: DateTime): String = {

    val (
      (ws, wsTime),
      (bg, bgTime),
      (es, esTime),
      (ca, caTime),
      (zk, zkTime),
      (kf, kfTime),
      controlNode,
      masters
      ) = getClusterHealth

    def determineCmwellColorBasedOnComponentsColor(
                                                    components: Seq[Color]
                                                  ): Color = {
      val all = components.filter(_ != Grey)
      if (all.forall(_ == Green)) Green
      else if (all.contains(Red)) Red
      else Yellow
    }

    val colorWithoutKafka = determineCmwellColorBasedOnComponentsColor(
      Seq(ws._1, bg._1, ca._1, es._1, zk._1)
    )
    val color = getColoredStatus(
      if (colorWithoutKafka == Green && kf._1 != Green) Yellow
      else colorWithoutKafka
    )
    val wsClr = getColoredStatus(ws._1)
    val bgClr = getColoredStatus(bg._1)
    val esClr = getColoredStatus(es._1)
    val caClr = getColoredStatus(ca._1)
    val zkClr = getColoredStatus(zk._1)
    val kfClr = getColoredStatus(kf._1)
    s"""
##### Current time: ${fdf(now)}
# CM-Well Health
### CM-Well cluster is $color.
#### Cluster name: ${Settings.clusterName}
| **Component** | **Status** | **Message** |  **Timestamp**  |
|---------------|------------|-------------|-----------------|
| WS            | $wsClr     | ${ws._2}    | ${fdf(wsTime)}  |
| BG            | $bgClr     | ${bg._2}    | ${fdf(bgTime)}  |
| ES            | $esClr     | ${es._2}    | ${fdf(esTime)}  |
| CAS           | $caClr     | ${ca._2}    | ${fdf(caTime)}  |
| ZK            | $zkClr     | ${zk._2}    | ${fdf(zkTime)}  |
| KAFKA         | $kfClr     | ${kf._2}    | ${fdf(kfTime)}  |
"""
  }

  private[this] def generateHealthDetailedFields: FieldsOpt = {
    //val (xs, timeStamp) = DashBoardCache.cacheAndGetDetailedHealthData
    val res = getClusterDetailedHealthNew
    Some(
      res.toSeq
        .sortBy(_._1)
        .view
        .flatMap {
          case (host, (ws, bg, ca, es, zk, kf)) =>
            List(
              s"ws@$host" -> Set[FieldValue](FString(ws._2.toString)),
              s"bg@$host" -> Set[FieldValue](FString(bg._2.toString)),
              s"cas@$host" -> Set[FieldValue](FString(ca._2.toString)),
              s"es@$host" -> Set[FieldValue](FString(es._2.toString)),
              s"zk@$host" -> Set[FieldValue](FString(zk._2.toString)),
              s"kafka@$host" -> Set[FieldValue](FString(kf._2.toString))
            )
        }.to(Map)
    )
  }

  def generateDetailedHealthCsvPretty(): String = {
    val title = s"${Settings.clusterName} - Health Detailed"
    val csvTitle = "Node,WS,BG,CAS,ES,ZK,KF"
    val csvData = generateDetailedHealthCsvData()
    views.html
      .csvPretty(title, s"$csvTitle\\n${csvData.replace("\n", "\\n")}")
      .body
  }

  def generateDetailedHealthMarkdown(now: DateTime): String = {
    val csvData = generateDetailedHealthCsvData()
    s"""
##### Current time: ${fdf(now)}
#### Cluster name: ${Settings.clusterName}
### Data was generated on:
| **Node** | **WS** | **BG** | **CAS** | **ES** | **ZK** | **KF** |
|----------|--------|--------|---------|--------|--------|--------|
${csvToMarkdownTableRows(csvData)}
"""
  }

  private def generateDetailedHealthCsvData(): String = {
    def zkStatusString(zkTuple: (String, Color)): String = {
      if (zkTuple._1 == "ZooKeeper is not running" && zkTuple._2 == Green) ""
      else s"${getColoredStatus(zkTuple._2)}<br>${zkTuple._1}"
    }
    val clusterHealth = getClusterDetailedHealthNew
    clusterHealth
      .map(
        r =>
          s"${r._1},${getColoredStatus(r._2._1._2)}<br>${r._2._1._1},${getColoredStatus(
            r._2._2._2
          )}<br>${r._2._2._1},${getColoredStatus(r._2._3._2)}<br>${r._2._3._1},${getColoredStatus(
            r._2._4._2
          )}<br>${r._2._4._1},${zkStatusString(r._2._5)},${getColoredStatus(
            r._2._6._2
          )}<br>${r._2._6._1}"
      )
      .mkString("\n")
  }

  private def csvToMarkdownTableRows(csvData: String): String =
    csvData
      .split("\\n")
      .map(_.split(",").mkString("|", "|", "|"))
      .mkString("\n")

  def generateBgData: Future[Map[String, Set[FieldValue]]] = {
    ask(bgMonitor, GetOffsetInfo)(10.seconds).mapTo[OffsetsInfo].map {
      offsetInfo =>
        offsetInfo.partitionsOffsetInfo
          .foldLeft(Map.empty[String, Set[FieldValue]]) {
            case (fields, pOffsetInfo) => {
              val topic = pOffsetInfo._2.topic
              val w = pOffsetInfo._2.writeOffset
              val r = pOffsetInfo._2.readOffset
              val l = w - r
              val s = pOffsetInfo._2.partitionStatus.toString
              val wField = topic + "_write_offset"
              val rField = topic + "_read_offset"
              val lField = topic + "_lag"
              val sField = topic + "_status"
              val wfv = FLong(
                w,
                Some(s"cmwell://meta/sys#partition_${pOffsetInfo._2.partition}")
              )
              val rfv = FLong(
                r,
                Some(s"cmwell://meta/sys#partition_${pOffsetInfo._2.partition}")
              )
              val lfv = FLong(
                l,
                Some(s"cmwell://meta/sys#partition_${pOffsetInfo._2.partition}")
              )
              val sfv =
                FString(
                  s,
                  None,
                  Some(
                    s"cmwell://meta/sys#partition_${pOffsetInfo._2.partition}"
                  )
                )
              val wSet =
                fields.get(wField).fold[Set[FieldValue]](Set(wfv))(_ + wfv)
              val rSet =
                fields.get(rField).fold[Set[FieldValue]](Set(rfv))(_ + rfv)
              val lSet =
                fields.get(lField).fold[Set[FieldValue]](Set(lfv))(_ + lfv)
              val sSet =
                fields.get(sField).fold[Set[FieldValue]](Set(sfv))(_ + sfv)
              fields
                .updated(wField, wSet)
                .updated(rField, rSet)
                .updated(lField, lSet)
                .updated(sField, sSet)
            }
          }
    }
  }

  def generateBgMarkdown(): String = {

    val offsetsInfo = ask(bgMonitor, GetOffsetInfo)(10.seconds).mapTo[OffsetsInfo].map { offsetInfo =>
      val sb = new StringBuilder
      sb ++= "##### Current time: "
      sb ++= fdf(offsetInfo.timeStamp)
      sb += '\n'
      sb ++= "# Bg Status in cluster "
      sb ++= Settings.clusterName
      sb += '\n'
      sb ++= "| **Partition** | **persist_topic write offset** | **persist_topic read offset** | **persist_topic lag** " +
        "| **persist_topic status** | **index_topic write offset** | **index_topic read offset** | **index_topic lag** | **index_topic status** |\n"
      sb ++= "|---------------|------------------|-----------------|---------|---------|------------------|-----------------|---------|---------|"
      offsetInfo.partitionsOffsetInfo.values
        .groupBy(_.partition)
        .toSeq
        .sortBy(_._1)
        .foreach {
          case (partition, offsetsInfo) =>
            val persistOffsetInfo =
              offsetsInfo.find(_.topic == "persist_topic").get
            val persistWriteOffset =
              coloredHtmlString(
                "green",
                s"${persistOffsetInfo.writeOffset}"
              )
            val persistReadOffset =
              coloredHtmlString("green", s"${persistOffsetInfo.readOffset}")
            val persistLag = persistOffsetInfo.writeOffset - persistOffsetInfo.readOffset
            val persistLagStr = coloredHtmlString("green", s"$persistLag")
            val persistStatus: String = {
              val color = persistOffsetInfo.partitionStatus.toString
              coloredHtmlString(color.toLowerCase, color)
            }

            val persistOffsetInfoPriority =
              offsetsInfo.find(_.topic == "persist_topic.priority").get
            val persistWriteOffsetPriority =
              coloredHtmlString(
                "blue",
                s"${persistOffsetInfoPriority.writeOffset}"
              )
            val persistReadOffsetPriority =
              coloredHtmlString(
                "blue",
                s"${persistOffsetInfoPriority.readOffset}"
              )
            val persistLagPriority = persistOffsetInfoPriority.writeOffset - persistOffsetInfoPriority.readOffset
            val persistLagPriorityStr =
              coloredHtmlString("blue", s"${persistLagPriority}")
            val persistStatusPriority: String = {
              val color = persistOffsetInfoPriority.partitionStatus.toString
              coloredHtmlString(color.toLowerCase, color)
            }

            val indexOffsetInfo =
              offsetsInfo.find(_.topic == "index_topic").get
            val indexWriteOffset =
              coloredHtmlString("green", s"${indexOffsetInfo.writeOffset}")
            val indexReadOffset =
              coloredHtmlString("green", s"${indexOffsetInfo.readOffset}")
            val indexLag = indexOffsetInfo.writeOffset - indexOffsetInfo.readOffset
            val indexLagStr = coloredHtmlString("green", s"${indexLag}")
            val indexStatus = {
              val color = indexOffsetInfo.partitionStatus.toString
              coloredHtmlString(color.toLowerCase, color)
            }

            val indexOffsetInfoPriority =
              offsetsInfo.find(_.topic == "index_topic.priority").get
            val indexWriteOffsetPriority =
              coloredHtmlString(
                "blue",
                s"${indexOffsetInfoPriority.writeOffset}"
              )
            val indexReadOffsetPriority =
              coloredHtmlString(
                "blue",
                s"${indexOffsetInfoPriority.readOffset}"
              )
            val indexLagPriority = indexOffsetInfoPriority.writeOffset - indexOffsetInfoPriority.readOffset
            val indexLagPriorityStr =
              coloredHtmlString("blue", s"${indexLagPriority}")
            val indexStatusPriority = {
              val color = indexOffsetInfoPriority.partitionStatus.toString
              coloredHtmlString(color.toLowerCase, color)
            }
            sb += '\n'
            sb += '|'
            sb ++= partition.toString
            sb += '|'
            sb ++= persistWriteOffset
            sb += '/'
            sb ++= persistWriteOffsetPriority
            sb += '|'
            sb ++= persistReadOffset
            sb += '/'
            sb ++= persistReadOffsetPriority
            sb += '|'
            sb ++= persistLagStr
            sb += '/'
            sb ++= persistLagPriorityStr
            sb += '|'
            sb ++= persistStatus
            sb += '/'
            sb ++= persistStatusPriority
            sb += '|'
            sb ++= indexWriteOffset
            sb += '/'
            sb ++= indexWriteOffsetPriority
            sb += '|'
            sb ++= indexReadOffset
            sb += '/'
            sb ++= indexReadOffsetPriority
            sb += '|'
            sb ++= indexLagStr
            sb += '/'
            sb ++= indexLagPriorityStr
            sb += '|'
            sb ++= indexStatus
            sb += '/'
            sb ++= indexStatusPriority
            sb += '|'
        }
      sb.result()
    }
    Await.result(offsetsInfo, 30 seconds) //TODO: Be Async...
  }

  def generateIteratorMarkdown: String = {
    val hostsToSessions = crudServiceFS.countSearchOpenContexts
    val lines = hostsToSessions.map {
      case (host, sessions) => s"|$host|$sessions|"
    } :+ s"| **Total** | ${hostsToSessions.foldLeft(0L) {
      case (sum, (_, add)) => sum + add
    }} |"
    s"""
# Open search contexts in cluster
| **Host** | **Open Contexts** |
|----------|-------------------|
${lines.mkString("\n")}
"""
  }

  import VirtualInfoton._

  import scala.language.implicitConversions

  def generateInfoton(
                       host: String,
                       path: String,
                       now: Long,
                       length: Int = 0,
                       offset: Int = 0,
                       isRoot: Boolean = false,
                       isAdmin: Boolean = false,
                       withHistory: Boolean,
                       fieldFilters: Option[FieldFilter],
                       timeContext: Option[Long]
                     ): Future[Option[VirtualInfoton]] = {

    val d: DateTime = new DateTime(now)

    implicit def iOptAsFuture(
                               iOpt: Option[VirtualInfoton]
                             ): Future[Option[VirtualInfoton]] =
      Future.successful(iOpt)

    def compoundDC = crudServiceFS.getListOfDC.map { seq =>
    {
      val dcKids: Seq[Infoton] =
        seq
          .map(
            dc =>
              VirtualInfoton(
                ObjectInfoton(SystemFields(s"/proc/dc/$dc", d, "VirtualInfoton", dc, None, "", "http"))
              ).getInfoton
          )
      Some(
        VirtualInfoton(
          CompoundInfoton(SystemFields(
            "/proc/dc",
            d,
            "VirtualInfoton",
            dc,
            None,
            "",
            "http"
          ),
            None,
            dcKids.slice(offset, offset + length),
            offset,
            length,
            dcKids.size
          )
        )
      )
    }
    }

    def getDcInfo(path: String) = {
      val dcId = path.drop("/proc/dc/".length)
      crudServiceFS
        .getInfotonByPathAsync(s"/meta/sys/dc/$dcId")
        .flatMap {
          case FullBox(ObjectInfoton(_,Some(fields))) =>
            // the user just gave the id (e.g. from the ui) and there should be an active sync of it.
            // Give the information according to the sync currently running.
            val id = fields("id").headOption.collect {
              case FString(x, _, _) => x
            }.get
            val wh = fields
              .get("with-history")
              .flatMap(_.headOption.collect { case FString(x, _, _) => x })
              .getOrElse("true") == "true"
            val qp = fields
              .get("qp")
              .flatMap(_.headOption.collect { case FString(x, _, _) => x })
            qp.fold(Success(None): Try[Option[RawFieldFilter]])(
              FieldFilterParser.parseQueryParams(_).map(Some.apply)
            )
              .map { qpOpt =>
                val fieldsFiltersFut = qpOpt.fold[Future[Option[FieldFilter]]](
                  Future.successful(Option.empty[FieldFilter])
                )(
                  rff =>
                    RawFieldFilter
                      .eval(rff, typesCache, cmwellRDFHelper, timeContext)
                      .map(Some.apply)
                )
                fieldsFiltersFut
              }
              .get
              .map(fieldFilter => (id, wh, fieldFilter))
          case _ =>
            //There is no matching dc infoton in the list - use the parameters got from the user
            Future.successful((dcId, withHistory, fieldFilters))
        }
        .flatMap {
          case (id, wh, fieldFilter) =>
            crudServiceFS.getLastIndexTimeFor(id, wh, fieldFilter)
        }
    }

    path.dropTrailingChars('/') match {
      case "/proc" => {
        val pk = procKids
        Some(
          VirtualInfoton(
            CompoundInfoton(SystemFields(
              path,
              d,
              "VirtualInfoton",
              dc,
              None,
              "",
              "http"),
              None,
              pk.slice(offset, offset + length),
              offset,
              min(pk.drop(offset).size, length),
              pk.size
            )
          )
        )
      }
      case "/proc/node" =>
        Some(VirtualInfoton(ObjectInfoton(SystemFields(path, d, "VirtualInfoton", dc, None, "", "http"), nodeValFields)))
      case "/proc/dc"                     => compoundDC
      case p if p.startsWith("/proc/dc/") => getDcInfo(p)
      case "/proc/fields"                 => crudServiceFS.getESFieldsVInfoton.map(Some.apply)
      case "/proc/health" =>
        Some(
          VirtualInfoton(ObjectInfoton(SystemFields(path, d, "VirtualInfoton", dc, None, "", "http"), generateHealthFields))
        )
      case "/proc/health.md" =>
        Some(
          VirtualInfoton(
            FileInfoton(SystemFields(path, d, "VirtualInfoton", dc, None, "", "http"),
              content = Some(
                FileContent(
                  generateHealthMarkdown(d).getBytes,
                  "text/x-markdown"
                )
              )
            )
          )
        )
      case "/proc/health-detailed" =>
        Some(
          VirtualInfoton(
            ObjectInfoton(SystemFields(path, d, "VirtualInfoton", dc, None, "", "http"),generateHealthDetailedFields)
          )
        )
      case "/proc/health-detailed.md" =>
        Some(
          VirtualInfoton(
            FileInfoton(SystemFields(path, d, "VirtualInfoton", dc, None, "", "http"),
              None,
              content = Some(
                FileContent(
                  generateDetailedHealthMarkdown(d).getBytes,
                  "text/x-markdown"
                )
              )
            )
          )
        )
      case "/proc/health-detailed.csv" =>
        Some(
          VirtualInfoton(
            FileInfoton(SystemFields(path, d, "VirtualInfoton", dc, None, "", "http"),
              None,
              content = Some(
                FileContent(
                  generateDetailedHealthCsvPretty().getBytes,
                  "text/html"
                )
              )
            )
          )
        )
      case "/proc/bg.md" =>
        Some(
          VirtualInfoton(
            FileInfoton(SystemFields(path, d, "VirtualInfoton", dc, None, "", "http"),
              None,
              content = Some(
                FileContent(generateBgMarkdown.getBytes, "text/x-markdown")
              )
            )
          )
        )
      case "/proc/bg" =>
        generateBgData.map(
          fields =>
            Some(VirtualInfoton(ObjectInfoton(SystemFields(path, d, "VirtualInfoton", dc, None, "", "http"), fields)))
        )
      case "/proc/search-contexts.md" =>
        Some(
          VirtualInfoton(
            FileInfoton(SystemFields(path, d, "VirtualInfoton", dc, None, "", "http"),
              None,
              content = Some(
                FileContent(
                  generateIteratorMarkdown.getBytes,
                  "text/x-markdown"
                )
              )
            )
          )
        )
      case "/proc/members-active.md" =>
        GridMonitoring.members(path, dc, Active, isRoot)
      case "/proc/members-all.md" =>
        GridMonitoring.members(path, dc, All, isRoot)
      case "/proc/members-all.csv" =>
        GridMonitoring.members(
          path,
          dc,
          All,
          isRoot,
          format = CsvPretty,
          contentTranformator = payload =>
            views.html
              .csvPretty(
                s"${Settings.clusterName} - Members-All",
                payload.replace("\n", "\\n")
              )
              .body
        )
      case "/proc/singletons.md"  => GridMonitoring.singletons(path, dc)
      case "/proc/actors.md"      => GridMonitoring.actors(path, dc)
      case "/proc/actors-diff.md" => GridMonitoring.actorsDiff(path, dc)
      case "/proc/requests.md"    => RequestMonitor.requestsInfoton(path, dc)
      case "/proc/dc-health.md"   => DcMonitor.dcHealthInfotonMD(path, dc)
      case "/proc/dc-distribution.md" =>
        DcMonitor.dcDistribution(path, dc, crudServiceFS)
      case "/proc/stp.md" =>
        SparqlTriggeredProcessorMonitor.generateTables(path, dc, isAdmin)
      case "/proc/traffic.md" => TrafficMonitoring.traffic(path, dc)
      case s if s.startsWith("/meta/ns/") => {
        val sysOrNn = s.drop("/meta/ns/".length)
        val url = s"http://$host/meta/$sysOrNn#"
        Some(
          VirtualInfoton(
            ObjectInfoton(SystemFields("/meta/ns/sys", new DateTime(), "VirtualInfoton", dc, None, "", "http"),
              Some(
                Map(
                  "url" -> Set[FieldValue](FString(url)),
                  "url_hash" -> Set[FieldValue](FString(crc32(url)))
                )
              )
            )
          )
        )
      }
      case _ => None
    }
  }

  private[this] def procKids: Vector[Infoton] = {
    val md = new DateTime()
    Vector(
      VirtualInfoton(ObjectInfoton(SystemFields("/proc/node", md, "VirtualInfoton", dc, None, "", "http"))),
      VirtualInfoton(ObjectInfoton(SystemFields("/proc/dc", md, "VirtualInfoton", dc, None, "", "http"))),
      VirtualInfoton(ObjectInfoton(SystemFields("/proc/bg", md, "VirtualInfoton", dc, None, "", "http"))),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/bg.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(ObjectInfoton(SystemFields("/proc/fields", md, "VirtualInfoton", dc, None, "", "http"))),
      VirtualInfoton(ObjectInfoton(SystemFields("/proc/health", md, "VirtualInfoton", dc, None, "", "http"))),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/health.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(ObjectInfoton(SystemFields("/proc/health-detailed", md, "VirtualInfoton", dc, None, "", "http"))),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/health-detailed.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/health-detailed.csv", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/html", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/search-contexts.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/members-active.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/members-all.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/members-all.csv", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/html", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/singletons.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/actors.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/actors-diff.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/requests.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/dc-health.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/dc-distribution.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/stp.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      ),
      VirtualInfoton(
        FileInfoton(SystemFields("/proc/traffic.md", md, "VirtualInfoton", dc, None, "", "http"),
          None,
          Some(FileContent("text/x-markdown", -1L))
        )
      )
    )
  }

  val dc = Settings.dataCenter
}
