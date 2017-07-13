/**
  * Copyright 2015 Thomson Reuters
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

import Color._
import actions.GridMonitoring.{Active, All, CsvPretty}
import akka.pattern.ask
import cmwell.ctrl.checkers._
import cmwell.ctrl.client.CtrlClient
import cmwell.domain._
import cmwell.util.build.BuildInfo
import cmwell.util.os.Props
import cmwell.util.string.Hash.crc32
import cmwell.ws.{BGMonitorActor, GetOffsetInfo, OffsetsInfo, Settings}
import cmwell.ws.Settings.esTimeout
import cmwell.ws.util.DateParser._
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid
import logic.CRUDServiceFS
import org.joda.time._
import trafficshaping.TrafficMonitoring
import wsutil._

import javax.inject._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.math.min
import scala.util.Try


object BgMonitoring {
  lazy val monitor = Grid.serviceRef(classOf[BGMonitorActor].getName)
}

@Singleton
class ActiveInfotonGenerator @Inject() (backPressureToggler: controllers.BackPressureToggler, crudServiceFS: CRUDServiceFS, dashBoard: DashBoard) extends LazyLogging {

  import BgMonitoring.{monitor => bgMonitor}
  import dashBoard._

  /**
   * @return /proc/node infoton fields map
   */
  private[this] def nodeValFields: FieldsOpt = {
    val (uwh,urh,iwh,irh) = BatchStatus.tlogStatus
    val esColor = Try(Await.result(dashBoard.getElasticsearchStatus(), esTimeout)._1.toString).getOrElse("grey")
    Some(Map[String,Set[FieldValue]](
      "pbp" -> Set(FString(backPressureToggler.get)),
      "nbg" -> Set(FBoolean(crudServiceFS.newBG)),
      "search_contexts_limit" -> Set(FLong(Settings.maxSearchContexts)),
      "cm-well_release" -> Set(FString(BuildInfo.release)),
      "cm-well_version" -> Set(FString(BuildInfo.version)),
      "git_commit_version" -> Set(FString(BuildInfo.gitCommitVersion)),
      "scala_version" -> Set(FString(BuildInfo.scalaVersion)),
      "sbt_version" -> Set(FString(BuildInfo.sbtVersion)),
      "cassandra_version" -> Set(FString(BuildInfo.cassandraVersion)),
      "elasticsearch_version" -> Set(FString(BuildInfo.elasticsearchVersion)),
      "es_cluster_name" -> Set(FString(cmwell.fts.Settings.clusterName)),
      "build_machine" -> Set(FString(BuildInfo.buildMachine)),
      "build_time" -> Set(FDate(BuildInfo.buildTime)),
      "encoding_version" -> Set(FString(BuildInfo.encodingVersion)),
      "java_runtime_name" -> Set(FString(System.getProperty("java.runtime.name"))),
      "java_runtime_version" -> Set(FString(System.getProperty("java.runtime.version"))),
      "java_vendor" -> Set(FString(System.getProperty("java.vendor"))),
      "java_version" -> Set(FString(System.getProperty("java.version"))),
      "java_vm_name" -> Set(FString(System.getProperty("java.vm.name"))),
      "java_vm_version" -> Set(FString(System.getProperty("java.vm.version"))),
      "os_arch" ->  Set(FString(System.getProperty("os.arch"))),
      "os_name" ->  Set(FString(System.getProperty("os.name"))),
      "os_version" ->  Set(FString(System.getProperty("os.version"))),
      "user_timezone" ->  Set(FString(System.getProperty("user.timezone"))),
      "machine_name" ->  Set(FString(Props.machineName)),
      "tlog_update_write_head" -> Set(FLong(uwh)),
      "tlog_update_read_head" -> Set(FLong(urh)),
      "tlog_index_write_head" -> Set(FLong(iwh)),
      "tlog_index_read_head" -> Set(FLong(irh)),
      "batch_color" -> Set(FString(BatchStatus.batchColor.toString)),
      "es_color" -> Set(FString(esColor))
    ))
  }

  def getColoredStatus(clr: Color): String = {
    val (color, status) = Map(
      Green -> ("green", "Green"),
      Yellow ->  ("yellow", "Yellow"),
      Red ->  ("red", "Red"),
      Grey ->  ("grey", "N/A"))(clr)
    coloredHtmlString(color, status)
  }

  def coloredHtmlString(color: String, status: String) = s"""<span style='color:$color'>$status</span>"""

  private[this] def colorAdapter(c : StatusColor) : Color.Color = {
    val r = c match {
      case GreenStatus => Green
      case YellowStatus => Yellow
      case RedStatus => Red
    }
    r
  }

  private[this] def getWsHealth(it : Iterable[ComponentState], c : StatusColor) = {
    val wsColor = colorAdapter(c)
    val p = it.map(_.getColor).partition(_ == GreenStatus)
    val wsTxt = s"${p._1.size} Up / ${it.size}"
    val d = new DateTime(it.head.genTime * 1000L)
    val ws = ((wsColor, wsTxt), d)
    ws
  }

  private[this] def getBatchHealth(it : Iterable[ComponentState], c : StatusColor) = {
    val bgColor = colorAdapter(c)
    val impSum = it.map{
      case b : BatchState => b.getImpDiff
      case _ => 0L
    }.fold(0L)(_ + _)

    val indexerSum = it.map{
      case b : BatchState => b.getIndexerDiff
      case _ => 0L
    }.fold(0L)(_ + _)

    val notIndexing = it.map{
      case BatchNotIndexing(v1,v2,v3,v4,_,_,_) => 1
      case _ => 0
    }.fold(0)(_ + _)

    val down = it.map{
      case b : BatchDown => 1
      case b : ReportTimeout => 1 // todo: deside what should happen here.
      case _ => 0
    }.fold(0)(_ + _)

    val bgTxt = s"imp diff: $impSum, idx diff: $indexerSum (down: $down, not indexing: $notIndexing)"
    val d = new DateTime(it.head.genTime * 1000L)
    val bg = ((bgColor, bgTxt),d)
    bg
  }

  private[this] def getElasticsearchHealth(cr : ComponentState) = {
    val dt = new DateTime(cr.genTime * 1000L)
    cr match {
      case ElasticsearchGreen(n, d, p, s, _,_) =>
        ((Green, s"n:$n,d:$d,p:$p,s:$s"), dt)
      case ElasticsearchYellow(n, d, p, s, _,_) =>
        ((Yellow, s"n:$n,d:$d,p:$p,s:$s"), dt)
      case ElasticsearchRed(n, d, p, s, _,_) =>
        ((Red, s"n:$n,d:$d,p:$p,s:$s"), dt)
      case ElasticsearchDown(_,_) =>
        ((Red, s"Elasticsearch is down"), dt)
      case ElasticsearchBadCode(code, _,_) =>
        ((Red, s"Elasticsearch check has produced http code: $code"), dt)
      case ReportTimeout(_) =>
        ((Red, "Couldn't retrieve Elasticsearch status"),dt)

    }
  }

  private[this] def getCassandraHealth(cr : ComponentState) = {
    cr match {
      case co : CassandraOk =>
        val color = colorAdapter(co.getColor)
        val total = co.getTotal
        val un = co.getUnCount
        val casTxt = s"$un Up / $total"
        ((color, casTxt), new DateTime(cr.genTime * 1000L))
      case CassandraDown(_) =>
        ((Red, "Cassandra is down"), new DateTime(cr.genTime * 1000L))
      case ReportTimeout(_) =>
        ((Red, "Couldn't retrieve Cassandra status"), new DateTime(cr.genTime * 1000L))
    }
  }

  private[this] def getZookeeperHealth(cr : Iterable[ComponentState], c : StatusColor) = {
    val color = colorAdapter(c)
    val oks = cr.count(_.isInstanceOf[ZookeeperOk])
    val notOks = cr.count(_.isInstanceOf[ZookeeperNotOk])
    val notRunning = cr.count(state => state.isInstanceOf[ZookeeperSeedNotRunning])
    val readOnly = cr.count(_.isInstanceOf[ZookeeperReadOnly])
    val stats = s"ok: $oks, ro: $readOnly, noks: $notOks, nr: $notRunning"
    val genTime = Try(cr.head.genTime).getOrElse(0L)
    ((color, s"$stats"), new DateTime(genTime * 1000L))
  }

  private[this] def getKafkaHealth(cr : Iterable[ComponentState], c : StatusColor) = {
    val color = colorAdapter(c)
    val oks = cr.count(_.isInstanceOf[KafkaOk])
    val notOks = cr.count(_.isInstanceOf[KafkaNotOk])
    val stats = s"$oks Up / ${cr.size}"
    val genTime = Try(cr.head.genTime).getOrElse(0L)
    ((color, s"$stats"), new DateTime(genTime * 1000L))
  }

  private[this] def getBatchDetailedHealth(t : BgType) = {
    val baseFut = t match {
      case Batch => CtrlClient.getBatchStatus
      case Bg => CtrlClient.getBgStatus
    }

    baseFut.map {
      bs =>
        bs._1.map {
          m =>
            m._2 match {
              case BatchOk(impSize, impLocation, indexerSize, indexerLocation, _,_,_) => m._1 -> Some((impSize, impLocation, indexerSize, indexerLocation))
              case BatchNotIndexing(impSize, impLocation, indexerSize, indexerLocation, _,_,_) => m._1 -> Some((impSize, impLocation, indexerSize, indexerLocation))
              case b : BatchDown => m._1 -> None
              case _ => m._1 -> None
            }
        }
    }
  }


  private[this] def getClusterHealth : HealthTimedData = {
    val r = CtrlClient.getClusterStatus.map{
      cs =>
        val ws = getWsHealth(cs.wsStat._1.values, cs.wsStat._2)
        val bg = getBatchHealth(cs.batchStat._1.values, cs.batchStat._2)
        val es = getElasticsearchHealth(cs.esStat)
        val cas = getCassandraHealth(cs.casStat)
        val zk = getZookeeperHealth(cs.zookeeperStat._1.values, cs.zookeeperStat._2)
        val kaf = getKafkaHealth(cs.kafkaStat._1.values, cs.kafkaStat._2)
        (ws,bg,es,cas,zk,kaf,cs.controlNode,cs.esMasters)
    }
    try {
      Await.result(r,timeOut)
    } catch {
      case t : Throwable =>
        (((Red, "NA"), new DateTime(0L)),((Red, "NA"), new DateTime(0L)),((Red, "NA"), new DateTime(0L)),((Red, "NA"), new DateTime(0L)),((Red, "NA"), new DateTime(0L)),((Red, "NA"), new DateTime(0L)),"",Set.empty[String])
    }
  }

  private[this] def getClusterDetailedHealth : DetailedHealthTimedData = {
    val r = CtrlClient.getClusterDetailedStatus.map{
      cs =>
        val keys = cs.wsStat.keySet ++ cs.batchStat.keySet ++ cs.esStat.keySet ++ cs.casStat.keySet

        val m = keys.map{
          k =>
            (if(k == cs.healthHost) s"${k}*" else k) -> (colorAdapter(cs.wsStat.getOrElse(k, WebDown()).getColor),
              colorAdapter(cs.batchStat.getOrElse(k,BatchDown(0,0,0,0)).getColor),
              colorAdapter(cs.casStat.getOrElse(k,CassandraDown()).getColor),
              colorAdapter(cs.esStat.getOrElse(k,ElasticsearchDown()).getColor))
        }.toMap
        (m,new DateTime(cs.wsStat.head._2.genTime * 1000L))
    }
    try{
      Await.result(r,timeOut)
    } catch {
      case t : Throwable => (Map.empty, new DateTime(0L))
    }
  }

  private[this] def getClusterDetailedHealthNew : Map[String, ((String, Color),(String, Color),(String, Color),(String, Color),(String, Color),(String, Color))] = {
    val r = CtrlClient.getClusterDetailedStatus.map{
      cs =>
        val keys = cs.wsStat.keySet ++ cs.batchStat.keySet ++ cs.esStat.keySet ++ cs.casStat.keySet ++ cs.zkStat.keySet ++ cs.kafkaStat.keySet
        keys.map{
          k =>
            val wsMessage = cs.wsStat.getOrElse(k, WebDown()) match {
              case wr : WebOk => (s"Ok<br>Response time: ${wr.responseTime} ms", colorAdapter(wr.getColor))
              case wr : WebBadCode => (s"Web is returning code: ${wr.code}<br>Response time: ${wr.responseTime} ms", colorAdapter(wr.getColor))
              case wr : WebDown => (s"Web is down", colorAdapter(wr.getColor))
              case wr : ReportTimeout => ("Can't retrieve Web status", colorAdapter(wr.getColor))
            }

            val batchMessage = cs.batchStat.getOrElse(k,BatchDown(0,0,0,0)) match {
              case br : BatchOk => (s"Ok<br>imp rate: ${br.impRate} b/s <br>idx rate: ${br.indexerRate} b/s", colorAdapter(br.getColor))
              case br : BatchNotIndexing => ("Batch isn't indexing", colorAdapter(br.getColor))
              case br : BatchDown => ("Batch is down", colorAdapter(br.getColor))
              case br : ReportTimeout => ("Can't retrieve Batch status", colorAdapter(br.getColor))
            }

            val casMessage = cs.casStat.getOrElse(k,CassandraDown()) match {
              case cr : CassandraOk => (cr.m.map(r => s"${r._1} -> ${r._2}").mkString("<br>"), colorAdapter(cr.getColor))
              case cr : CassandraDown => ("Cassandra is down", colorAdapter(cr.getColor))
              case cr : ReportTimeout => ("Can't retrieve Cassandra status", colorAdapter(cr.getColor))
            }

            val zkMessage = cs.zkStat.getOrElse(k, ZookeeperNotOk()) match {
              case zr : ZookeeperOk => ("", colorAdapter(zr.getColor))
              case zr : ZookeeperReadOnly => ("ZooKeeper is in read-only mode", colorAdapter(zr.getColor))
              case zr : ZookeeperNotOk => ("ZooKeeper is not ok", colorAdapter(zr.getColor))
              case zr : ZookeeperNotRunning => ("ZooKeeper is not running", colorAdapter(zr.getColor))
              case zr : ZookeeperSeedNotRunning => ("ZooKeeper is not running", colorAdapter(zr.getColor))
              case zr : ReportTimeout => ("Can't retrieve ZooKeeper status", colorAdapter(zr.getColor))
            }

            val kafMessage = cs.kafkaStat.getOrElse(k, KafkaNotOk()) match {
              case kafr : KafkaOk => ("", colorAdapter(kafr.getColor))
              case kafr : KafkaNotOk => ("Kafka is not running", colorAdapter(kafr.getColor))
              case kafr : ReportTimeout => ("Can't retrieve Kafka status", colorAdapter(kafr.getColor))
            }

            val esMessage = cs.esStat.getOrElse(k,ElasticsearchDown()) match {
              case er : ElasticsearchGreen =>
                val txt = if(er.hasMaster) "*" else ""
                (txt,colorAdapter(er.getColor))
              case er : ElasticsearchYellow =>
                val txt = if(er.hasMaster) "*" else ""
                (txt,colorAdapter(er.getColor))
              case er : ElasticsearchRed =>
                val txt = if(er.hasMaster) "*" else ""
                (txt,colorAdapter(er.getColor))
              case er : ElasticsearchDown =>
                val txt = if(er.hasMaster) "*" else ""
                (s"${txt}<br>Elasticsearch is down", colorAdapter(er.getColor))
              case er : ElasticsearchBadCode =>
                val txt = if(er.hasMaster) "*" else ""
                (s"${txt}<br>Elasticsearch is returning code: ${er.code}", colorAdapter(er.getColor))
              case er : ReportTimeout => ("Can't retrieve Elasticsearch status", colorAdapter(er.getColor))
            }
            (if(k == cs.healthHost) s"${k}*" else k) -> (wsMessage, batchMessage, casMessage, esMessage, zkMessage, kafMessage)
        }.toMap
    }
    try{
      Await.result(r,timeOut)
    } catch {
      case t : Throwable => Map.empty
    }
  }

  private[this] def generateHealthFields: FieldsOpt = {
    val (ws,bg,es,ca,zk,kaf,controlNode,masters) = getClusterHealth
    val (wClr,wMsg) = ws._1
    val (bClr,bMsg) = bg._1
    val (eClr,eMsg) = es._1
    val (cClr,cMsg) = ca._1
    val (zkClr,zkMsg) = zk._1
    val (kafClr,kafMsg) = kaf._1
    Some(Map[String,Set[FieldValue]](
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
      "masters" -> masters.map(FString(_)))
    )
  }

  /**
   * @return health markdown string
   */
  private[this] def generateHealthMarkdown: String = {

    logger.info("in generateHealthMarkdown")

    val ((ws,wsTime),(bg,bgTime),(es,esTime),(ca,caTime),(zk, zkTime),(kf, kfTime),controlNode, masters) = getClusterHealth

    def determineCmwellColorBasedOnComponentsColor(components: Seq[Color]): Color = {
      val all = components.filter(_ != Grey)
      if(all.forall(_ == Green)) Green
      else if(all.exists(_ == Red)) Red
      else Yellow
    }

    val colorWithoutKafka = determineCmwellColorBasedOnComponentsColor(Seq(ws._1,bg._1,ca._1,es._1,zk._1))
    val color = getColoredStatus(if (colorWithoutKafka == Green && kf._1 != Green) Yellow else colorWithoutKafka)
    val wsClr = getColoredStatus(ws._1)
    val bgClr = getColoredStatus(bg._1)
    val esClr = getColoredStatus(es._1)
    val caClr = getColoredStatus(ca._1)
    val zkClr = getColoredStatus(zk._1)
    val kfClr = getColoredStatus(kf._1)
    s"""
##### Current time: ${fdf(new DateTime(System.currentTimeMillis()))}
# CM-Well Health
### CM-Well cluster is $color.
#### Cluster name: ${Settings.clusterName}
| **Component** | **Status** | **Message** |  **Timestamp**  |
|---------------|------------|-------------|-----------------|
| WS            | ${wsClr}   | ${ws._2}    | ${fdf(wsTime)}  |
| BG            | ${bgClr}   | ${bg._2}    | ${fdf(bgTime)}  |
| ES            | ${esClr}   | ${es._2}    | ${fdf(esTime)}  |
| CAS           | ${caClr}   | ${ca._2}    | ${fdf(caTime)}  |
| ZK            | $zkClr     | ${zk._2}    | ${fdf(zkTime)}  |
| KF            | $kfClr     | ${kf._2}    | ${fdf(kfTime)}  |
"""
  }

  private[this] def generateHealthDetailedFields: FieldsOpt = {
    //val (xs, timeStamp) = DashBoardCache.cacheAndGetDetailedHealthData
    val res = getClusterDetailedHealthNew
    Some(res.toSeq.sortBy(_._1).map {
      case (host,(ws,bg,ca,es,zk,kf)) => List(
        (s"ws@$host"  -> Set[FieldValue](FString(ws._2.toString))),
        (s"bg@$host"  -> Set[FieldValue](FString(bg._2.toString))),
        (s"cas@$host" -> Set[FieldValue](FString(ca._2.toString))),
        (s"es@$host"  -> Set[FieldValue](FString(es._2.toString))),
        (s"zk@$host"  -> Set[FieldValue](FString(zk._2.toString))),
        (s"kf@$host"  -> Set[FieldValue](FString(kf._2.toString))))
    }.flatten.toMap)
  }


  def generateDetailedHealthCsvPretty(): String = {
    val title = s"${Settings.clusterName} - Health Detailed"
    val csvTitle =  "Node,WS,BG,CAS,ES,ZK,KF"
    val csvData = generateDetailedHealthCsvData()
    views.html.csvPretty(title, s"$csvTitle\\n${csvData.replace("\n","\\n")}").body
  }

  def generateDetailedHealthMarkdown: String = {
    val csvData = generateDetailedHealthCsvData()
    s"""
##### Current time: ${fdf(new DateTime(System.currentTimeMillis()))}
#### Cluster name: ${Settings.clusterName}
### Data was generated on:
| **Node** | **WS** | **BG** | **CAS** | **ES** | **ZK** | **KF** |
|----------|--------|--------|---------|--------|--------|--------|
${csvToMarkdownTableRows(csvData)}
"""
  }

  private def generateDetailedHealthCsvData(): String = {
    def zkStatusString(zkTuple: (String, Color)): String = {
      if (zkTuple._1 == "ZooKeeper is not running" && zkTuple._2 == Green) "" else s"${getColoredStatus(zkTuple._2)}<br>${zkTuple._1}"
    }
    val clusterHealth = getClusterDetailedHealthNew
    clusterHealth.map(r => s"${r._1},${getColoredStatus(r._2._1._2)}<br>${r._2._1._1},${getColoredStatus(r._2._2._2)}<br>${r._2._2._1},${getColoredStatus(r._2._3._2)}<br>${r._2._3._1},${getColoredStatus(r._2._4._2)}<br>${r._2._4._1},${zkStatusString(r._2._5)},${getColoredStatus(r._2._6._2)}<br>${r._2._6._1}").mkString("\n")
  }

  private def csvToMarkdownTableRows(csvData: String): String =
    csvData.split("\\n").map(_.split(",").mkString("|","|","|")).mkString("\n")

  trait BgType
  case object Bg extends BgType
  case object Batch extends BgType


  /**
   * @return batch markdown string
   */
  def generateBatchMarkdown(t : BgType): String = {

    val resFut = getBatchDetailedHealth(t).map{ set =>
      val lines = set.toList.sortBy(_._1).map{
        case (host,Some((uw,ur,iw,ir))) => s"|$host|$uw|$ur|${uw-ur}|$iw|$ir|${iw-ir}|"
        case (host, None) => s"|$host| &#x20E0; | &#x20E0; | &#x20E0; | &#x20E0; | &#x20E0; | &#x20E0; |"
      }

      s"""
##### Current time: ${fdf(new DateTime(System.currentTimeMillis()))}
# Batch Status in cluster ${Settings.clusterName}
| **Node** | UpdateTlog write pos | UpdateTlog read pos | **UpdateTlog diff**| IndexTlog write pos | IndexTlog read pos | **IndexTlog diff** |
|----------|----------------------|---------------------|--------------------|---------------------|--------------------|--------------------|
${lines.mkString("\n")}
"""
    }
    Await.result(resFut, 30 seconds) //TODO: Be Async...
  }

  def generateBgData: Future[Map[String,Set[FieldValue]]] = {
    ask(bgMonitor, GetOffsetInfo)(10.seconds).mapTo[OffsetsInfo].map { offsetInfo =>
      offsetInfo.partitionsOffsetInfo.foldLeft(Map.empty[String, Set[FieldValue]]) {
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
          val wfv = FLong(w, Some(s"cmwell://meta/sys#partition_${pOffsetInfo._2.partition}"))
          val rfv = FLong(r, Some(s"cmwell://meta/sys#partition_${pOffsetInfo._2.partition}"))
          val lfv = FLong(l, Some(s"cmwell://meta/sys#partition_${pOffsetInfo._2.partition}"))
          val sfv = FString(s, None, Some(s"cmwell://meta/sys#partition_${pOffsetInfo._2.partition}"))
          val wSet = fields.get(wField).fold[Set[FieldValue]](Set(wfv))(_ + wfv)
          val rSet = fields.get(rField).fold[Set[FieldValue]](Set(rfv))(_ + rfv)
          val lSet = fields.get(lField).fold[Set[FieldValue]](Set(lfv))(_ + lfv)
          val sSet = fields.get(sField).fold[Set[FieldValue]](Set(sfv))(_ + sfv)
          fields
            .updated(wField, wSet)
            .updated(rField, rSet)
            .updated(lField, lSet)
            .updated(sField, sSet)
        }
      }
    }
  }

  def generateBgMarkdown(t : BgType): String = {

    val offsetsInfo = ask(bgMonitor, GetOffsetInfo)(10.seconds).mapTo[OffsetsInfo].map{ offsetInfo =>

      s"""
##### Current time: ${fdf(offsetInfo.timeStamp)}
# Bg Status in cluster ${Settings.clusterName}
| **Partition** | **persist_topic write offset** | **persist_topic read offset** | **persist_topic lag** | **persist_topic status** | **index_topic write offset** | **index_topic read offset** | **index_topic lag** | **index_topic status** |
|---------------|------------------|-----------------|---------|---------|------------------|-----------------|---------|---------|
${
    offsetInfo.partitionsOffsetInfo.values.groupBy(_.partition).toSeq.sortBy(_._1).map { case (partition, offsetsInfo) =>
      val persistOffsetInfo = offsetsInfo.find(_.topic == "persist_topic").get
      val persistWriteOffset = persistOffsetInfo.writeOffset
      val persistReadOffset = persistOffsetInfo.readOffset
      val persistLag = persistWriteOffset - persistReadOffset
      val persistStatus:String = {
        val color = persistOffsetInfo.partitionStatus.toString
        coloredHtmlString(color.toLowerCase,color)
      }

      val indexOffsetInfo = offsetsInfo.find(_.topic == "index_topic").get
      val indexWriteOffset = indexOffsetInfo.writeOffset
      val indexReadOffset = indexOffsetInfo.readOffset
      val indexLag = indexWriteOffset - indexReadOffset
      val indexStatus = {
        val color = indexOffsetInfo.partitionStatus.toString
        coloredHtmlString(color.toLowerCase,color)
      }
      s"|$partition|$persistWriteOffset|$persistReadOffset|$persistLag|$persistStatus|$indexWriteOffset|$indexReadOffset|$indexLag|$indexStatus|"
    }.mkString("\n")
 }
"""
    }

    Await.result(offsetsInfo, 30 seconds) //TODO: Be Async...
  }


  def generateIteratorMarkdown: String = {
    val hostsToSessions = crudServiceFS.countSearchOpenContexts()
    val lines = hostsToSessions.map{
      case (host,sessions) => s"|$host|$sessions|"
    } :+ s"| **Total** | ${(0L /: hostsToSessions){case (sum,(_,add)) => sum + add}} |"
    s"""
# Open search contexts in cluster
| **Host** | **Open Contexts** |
|----------|-------------------|
${lines.mkString("\n")}
"""
  }

  import VirtualInfoton._

  import scala.language.implicitConversions


  def generateInfoton(host: String, path: String, md: DateTime = new DateTime(), length: Int = 0, offset: Int = 0, isRoot : Boolean = false, nbg: Boolean = false): Future[Option[VirtualInfoton]] = {

    implicit def iOptAsFuture(iOpt: Option[VirtualInfoton]): Future[Option[VirtualInfoton]] = Future.successful(iOpt)

    def compoundDC = crudServiceFS.getListOfDC().map{
      seq => {
        val dcKids: Seq[Infoton] = seq.map(dc => VirtualInfoton(ObjectInfoton(s"/proc/dc/$dc", dc, None, md, None)).getInfoton)
        Some(VirtualInfoton(CompoundInfoton("/proc/dc",dc,None,md,None,dcKids.slice(offset,offset+length),offset,length,dcKids.size)))
      }
    }

    path.dropTrailingChars('/') match {
      case "/proc" => {val pk = procKids; Some(VirtualInfoton(CompoundInfoton(path, dc, None, md,None,pk.slice(offset,offset+length),offset,min(pk.drop(offset).size,length),pk.size)))}
      case "/proc/node" => Some(VirtualInfoton(ObjectInfoton(path, dc, None, md,nodeValFields)))
      case "/proc/dc" => compoundDC
      case p if p.startsWith("/proc/dc/") => crudServiceFS.getLastIndexTimeFor(p.drop("/proc/dc/".length))
      case "/proc/fields" => crudServiceFS.getESFieldsVInfoton(nbg).map(Some.apply)
      case "/proc/health" => Some(VirtualInfoton(ObjectInfoton(path, dc, None, md, generateHealthFields)))
      case "/proc/health.md" => Some(VirtualInfoton(FileInfoton(path, dc, None, content = Some(FileContent(generateHealthMarkdown.getBytes, "text/x-markdown")))))
      case "/proc/health-detailed" => Some(VirtualInfoton(ObjectInfoton(path, dc, None, md, generateHealthDetailedFields)))
      case "/proc/health-detailed.md" => Some(VirtualInfoton(FileInfoton(path, dc, None, content = Some(FileContent(generateDetailedHealthMarkdown.getBytes, "text/x-markdown")))))
      case "/proc/health-detailed.csv" => Some(VirtualInfoton(FileInfoton(path, dc, None, content = Some(FileContent(generateDetailedHealthCsvPretty().getBytes, "text/html")))))
      case "/proc/batch.md" =>  Some(VirtualInfoton(FileInfoton(path, dc, None, content = Some(FileContent(generateBatchMarkdown(Batch).getBytes, "text/x-markdown")))))
      case "/proc/bg.md" =>  Some(VirtualInfoton(FileInfoton(path, dc, None, content = Some(FileContent(generateBgMarkdown(Bg).getBytes, "text/x-markdown")))))
      case "/proc/bg" =>  generateBgData.map(fields => Some(VirtualInfoton(ObjectInfoton(path, dc, None, md, fields))))
      case "/proc/search-contexts.md" =>  Some(VirtualInfoton(FileInfoton(path, dc, None, content = Some(FileContent(generateIteratorMarkdown.getBytes, "text/x-markdown")))))
      case "/proc/members-active.md" => GridMonitoring.members(path, dc, Active, isRoot)
      case "/proc/members-all.md" => GridMonitoring.members(path, dc, All, isRoot)
      case "/proc/members-all.csv" => GridMonitoring.members(path, dc, All, isRoot, format = CsvPretty, contentTranformator = payload => views.html.csvPretty(s"${Settings.clusterName} - Members-All", payload.replace("\n","\\n")).body)
      case "/proc/singletons.md" => GridMonitoring.singletons(path, dc)
      case "/proc/actors.md" => GridMonitoring.actors(path, dc)
      case "/proc/actors-diff.md" => GridMonitoring.actorsDiff(path, dc)
      case "/proc/requests.md" => RequestMonitor.requestsInfoton(path, dc)
      case "/proc/dc-health.md" => DcMonitor.dcHealthInfotonMD(path, dc)
      case "/proc/dc-distribution.md" => DcMonitor.dcDistribution(path, dc, crudServiceFS)
      case "/proc/stp.md" => SparqlTriggeredProcessorMonitor.generateTables(path, dc)
      case "/proc/traffic.md" => TrafficMonitoring.traffic(path, dc)
      case s if s.startsWith("/meta/ns/") => {
        val sysOrNn = s.drop("/meta/ns/".length)
        val url = s"http://$host/meta/$sysOrNn#"
        Some(VirtualInfoton(ObjectInfoton("/meta/ns/sys", dc, None, new DateTime(), Some(Map("url"->Set[FieldValue](FString(url)),"url_hash"->Set[FieldValue](FString(crc32(url))))))))
      }
      case _ => None
    }
  }

  private[this] def procKids: Vector[Infoton] = {
    val md = new DateTime()
    Vector(VirtualInfoton(ObjectInfoton("/proc/node", dc, None,md) ),
           VirtualInfoton(ObjectInfoton("/proc/dc", dc, None,md) ),
           VirtualInfoton(ObjectInfoton("/proc/bg", dc, None,md) ),
           VirtualInfoton(FileInfoton("/proc/bg.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L)))),
           VirtualInfoton(FileInfoton("/proc/batch.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L)))),
           VirtualInfoton(ObjectInfoton("/proc/fields", dc, None,md) ),
           VirtualInfoton(ObjectInfoton("/proc/health", dc, None,md) ),
           VirtualInfoton(FileInfoton("/proc/health.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(ObjectInfoton("/proc/health-detailed", dc, None,md) ),
           VirtualInfoton(FileInfoton("/proc/health-detailed.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/health-detailed.csv", dc, None,md,None , Some(FileContent("text/html",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/search-contexts.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/members-active.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/members-all.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/members-all.csv", dc, None,md,None , Some(FileContent("text/html",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/singletons.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/actors.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/actors-diff.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/requests.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/dc-health.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/dc-distribution.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/stp.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) ),
           VirtualInfoton(FileInfoton("/proc/traffic.md", dc, None,md,None , Some(FileContent("text/x-markdown",-1L))) )
    )
  }

  val dc = Settings.dataCenter
}
