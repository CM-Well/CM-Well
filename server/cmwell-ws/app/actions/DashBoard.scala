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

import javax.inject._

import akka.actor.ActorSystem
import akka.stream.Materializer
import controllers.HealthUtils
import logic.CRUDServiceFS
import cmwell.util.http.{SimpleHttpClient => Http, _}
import cmwell.syntaxutils.!!!
import cmwell.domain.{SimpleResponse => _, _}
import cmwell.util.concurrent.SimpleScheduler
import scala.util._
import scala.concurrent.{Await, ExecutionContext, Future, Promise, duration}
import duration._
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.ws._
import play.api.libs.json._
import play.api.Play.current
import org.joda.time._

import scala.language.postfixOps

object Color extends Enumeration {
  type Color = Value
  type ColorTuple4 = Tuple4[Color,Color,Color,Color]
  val Green, Yellow, Red, Grey = Value

  def apply(color: String): Color = color.toLowerCase match {
    case "green" => Green
    case "yellow" => Yellow
    case "red" => Red
    case _ => Grey
  }
}

import Color._

@Singleton
class DashBoard @Inject()(crudServiceFS: CRUDServiceFS)(implicit ec: ExecutionContext, sys: ActorSystem, mat: Materializer) extends LazyLogging { dashBoard =>

  val timeOut = cmwell.ws.Settings.cacheTimeout.seconds

  type FutureColorTuple4 = (Future[Color],Future[Color],Future[Color],Future[Color])
  type Ratio = (Int,Int)
  type TlogDiffOnHost = Set[(String, Long,Long)]
  type TlogState = (Long,Long,Long,Long)
  type TlogStatus = Map[String,TlogState]
  type GenStatus = (Int,String,List[CassandraStatusExtracted],String)
  type Fields = Map[String,Set[FieldValue]]
  type FieldsOpt = Option[Fields]
  type ComponentHealth = (Color,String)
  type ComponentHealthFutureTuple4 = (Future[ComponentHealth],Future[ComponentHealth],Future[ComponentHealth],Future[ComponentHealth])
  type TimedHealth = (ComponentHealth,DateTime)
  type HealthTimedData = (TimedHealth,TimedHealth,TimedHealth,TimedHealth,TimedHealth,TimedHealth,String,Set[String])
  type DetailedHealthData = Map[String,ColorTuple4]
  type DetailedHealthTimedData = (DetailedHealthData, DateTime)

  object HostWithPort extends scala.util.matching.Regex("""(.+):(\d+)""")
  object CassandraStatus1 extends scala.util.matching.Regex("""\s*(\S+)\s+(\d+.\d+.\d+.\d+)\s+(\d+.?\d*\s*\S+)\s+(\d+.?\d*%)\s+(\S+)\s+(-?\d+)\s+(\S+)\s*""")
  object CassandraStatus2 extends scala.util.matching.Regex("""\s*(\S+)\s+(\d+.\d+.\d+.\d+)\s+(\d+.?\d*\s*\S+)\s+(-?\d+)\s+(\d+.?\d*%)\s+(\S+)\s+(\S+)\s*""")
  object CassandraStatus3 extends scala.util.matching.Regex("""\s*(\S+)\s+(\d+.\d+.\d+.\d+)\s+(\d+.?\d*\s*\S+)\s+(-?\d+)\s+\?\s+(\S+)\s+(\S+)\s*""")
  object CassandraStatus4 extends scala.util.matching.Regex("""\s*(\S+)\s+(\d+.\d+.\d+.\d+)\s+(\d+.?\d*\s*\S+)\s+\?\s+(\S+)\s+(-?\d+)\s+(\S+)\s*""")

  case class CassandraStatusExtracted(status: String,ip: String,load: String,owns: String,id: String,token: String,rack: String)

  object Hosts {
    @volatile private[this] var hosts: Set[String] = Set.empty[String]

    private[this] val hostsDaemonCancellable = {
      import scala.concurrent.duration.DurationInt

      SimpleScheduler.scheduleAtFixedRate(0 seconds, 1 minutes) {
        crudServiceFS.getInfoton("/meta/sys",None,None).map{
          case None => logger.error("could not retrieve data from local CRUDService")
          case Some(Everything(metaSys)) => metaSys.fields match {
            case None => logger.error("/meta/sys retrieved from local CRUDService, but does not contain fields")
            case Some(m: Fields) => {
              hosts = m("known-cmwell-hosts").collect{
                case FString(host,_,_) => host
              }
            }
          }
          case Some(UnknownNestedContent(_)) => !!! //  not reachable! case written to silence compilation warnings
        }
      }
    }

    def getCachedHostsSet = hosts
    def foreachHost[T](doWithHost: String => T): Future[Set[T]] = Future(hosts map doWithHost)
  }

  import Hosts.foreachHost

  def getCmwellWSRatio: Future[Either[String,(Ratio,String)]] = {
    val p = Promise[Either[String,(Ratio,String)]]()

    val fsfes: Future[Set[Future[Either[String,(String,Int)]]]] = foreachHost{ host =>
      Http.get(s"http://$host/?format=json").map{
        wsr => Right(host -> wsr.status)
      }.recover{
        case ex: Throwable => {
          logger.error("getCmwellWSRatio: future failed",ex)
          Left(s"GET for host $host failed with message: ${ex.getMessage}")
        }
      }
    }

    val ffsew = fsfes.map[Future[Set[Either[String,(String,Int)]]]](Future.sequence(_)) //TODO: report scala bug? `Future.sequence(_)` should be equivalent to `Future.sequence`, but infered type is `Nothing`?
    ffsew.onComplete{
      case Success(fsw) => {
        fsw.foreach{s =>
          val sb = new StringBuilder
          val ratio = s.foldLeft(0 -> 0) {
            case ((n, d), Right((_, 200))) => (n + 1, d + 1)
            case ((n, d), Right((host,i))) => sb.append(s"\n$host returned $i status code."); (n, d + 1)
            case ((n, d), Left(err)) => sb.append(s"\n$err"); (n, d + 1)
          }
          p.success(Right(ratio -> sb.mkString))
        }
      }
      case Failure(err) => {
        logger.error("getCmwellWSRatio: future seq failed",err)
        p.success(Left(err.getMessage))
      }
    }
    p.future
  }


  def getStatusForAll = {
    val fSet = foreachHost{ host =>
      val ws = Http.get(s"http://$host/?format=json").map(_.status)
      val (bg,es) = {
        val ff = Http.get(s"http://$host/proc/node?format=json").map(r => Json.parse(r.payload).\("fields").get).recover{
          case ex: java.net.ConnectException => {
            logger.error(s"could not retrieve data from $host/proc/node",ex)
            val grey = JsArray(Seq(JsString("grey")))
            JsObject(Seq("batch_color" -> grey, "es_color" -> grey))
          }
        }
        ff.map(_.\("batch_color")(0).as[String]) -> ff.map(_.\("es_color")(0).as[String])
      }
      val ca = Http.get(s"http://$host/health/cas")(SimpleResponse.Implicits.UTF8StringHandler,ec,sys,mat).map(_.payload.lines.collect{
        case CassandraStatus1(status,ip,load,owns,id,token,rack) => CassandraStatusExtracted(status,ip,load,owns,id,token,rack)
        case CassandraStatus2(status,ip,load,token,owns,id,rack) => CassandraStatusExtracted(status,ip,load,owns,id,token,rack)
        case CassandraStatus3(status,ip,load,token,id,rack) => CassandraStatusExtracted(status,ip,load,"NO DATA",id,token,rack)
        case CassandraStatus4(status,ip,load,token,id,rack) => CassandraStatusExtracted(status,ip,load,"NO DATA",id,token,rack)
      }.toList)
      host -> (ws,bg,ca,es)
    }
    fSet
  }

  object BatchStatus {

    @volatile private[this] var currentState: TlogState = (-1L,-1L,-1L,-1L)
    @volatile private[this] var previousState: TlogState = (-1L,-1L,-1L,-1L)

    private[this] val tlogStateDaemonCancellable = {
      import scala.concurrent.duration.DurationInt
      cmwell.util.concurrent.SimpleScheduler.scheduleAtFixedRate(0 seconds, 30 seconds){
        Try(tlogStatus).foreach { ts: TlogState =>
          previousState = currentState
          currentState = ts
        }
      }
    }

    def tlogStatus: TlogState = {
      val updatesWriteHead = crudServiceFS.updatesTlog.size
      val updatesReadHead = crudServiceFS.impState.position
      val indexesWriteHead = crudServiceFS.uuidsTlog.size
      val indexesReadHead = crudServiceFS.indexerState.position
      (updatesWriteHead, updatesReadHead, indexesWriteHead, indexesReadHead)
    }

    def get: Tuple2[TlogState,TlogState] = previousState -> currentState

    def getAll: Future[Set[(String,Option[TlogState])]] = {
      val fsft = foreachHost { host =>
        host -> getBatchStatusForHost(host)
      }
      fsft.map(_.map{
        case (host, bgStts) => host -> Try(Await.result(bgStts, timeOut)).toOption
      })
    }

    def getAggregatedBatchColor: Future[(String,String)] = {
      val fsfs = foreachHost { host =>
        host -> Http.get(s"http://$host/proc/node?format=json").map { wsr =>
          val fields = Json.parse(wsr.payload) \ "fields"
          val batchColor = fields.\("batch_color")(0).as[String]
          val uw = fields.\("tlog_update_write_head")(0).as[Long]
          val ur = fields.\("tlog_update_read_head")(0).as[Long]
          val iw = fields.\("tlog_index_write_head")(0).as[Long]
          val ir = fields.\("tlog_index_read_head")(0).as[Long]
          (batchColor, (uw - ur, iw - ir))
        }
      }

      val fss = fsfs.map(sfs => {
        sfs.map {
          case (h, fs) => h -> Try(Await.result(fs, timeOut)).toOption
        }
      })

      fss.map(ss => {
        val delta = ss.foldLeft(0L -> 0L) {
          case ((ud, id), (_, Some((_, (cud, cid))))) => (ud + cud, id + cid)
          case ((ud, id), (_, None)) => (ud, id)
        }
        val diff = s"imp diff: ${delta._1}, idx diff: ${delta._2}"
        val (grey, notGrey) = ss.partition(_._2.isEmpty)
        val gs = grey.size
        def msg(m: String, b: Boolean) = if (gs > 0 && b) s" ($m, $gs bg instances are N/A)"
        else if (b) s" ($m)"
        else if (gs > 0) s" ($gs bg instances are N/A)"
        else ""
        if (notGrey.isEmpty) "Grey" -> "all bg are N/A"
        else if (notGrey.forall(_._2.get._1.toLowerCase == "green")) "Green" -> s"$diff${msg("idle", delta ==(0L, 0L))}"
        else if (notGrey.exists(_._2.get._1.toLowerCase == "red")) "Red" -> ss.filter(_._2.get._1.toLowerCase == "red").map(_._1).mkString("The following nodes:", ", ", s" are red!${msg(diff, true)}")
        else "Yellow" -> notGrey.filter(_._2.get._1.toLowerCase == "yellow").map(_._1).mkString("The following nodes:", ", ", s" are yellow.${msg(diff, true)}")
      })
    }

    //TODO: return cached values on failures, recover future with cached values etc'...
    def getBatchStatusForHost(host: String = "localhost:9000"): Future[TlogState] = {
      Http.get(s"http://$host/proc/node?format=json").map{
        case wsr if wsr.status == 200 => {
          val fields = Json.parse(wsr.payload) \ "fields"
          val uwh: Long = (fields \ "tlog_update_write_head")(0).as[Long]
          val urh: Long = (fields \ "tlog_update_read_head")(0).as[Long]
          val iwh: Long = (fields \ "tlog_index_write_head")(0).as[Long]
          val irh: Long = (fields \ "tlog_index_read_head")(0).as[Long]
          (uwh,urh,iwh,irh)
        }
        case _ => (-1L,-1L,-1L,-1L)
      }
    }

    private[this] def getBatchStatus: Future[TlogDiffOnHost] = {
      val p = Promise[TlogDiffOnHost]()
      val fsfw = foreachHost { host =>
        getBatchStatusForHost(host).map{
          case (uwh,urh,iwh,irh) => (host,uwh-urh,iwh-irh)
        }
      }

      val ffs = fsfw.map(Future.sequence(_))
      ffs.onComplete{
        case Failure(err) => p.failure(err)
        case Success(fs) => fs.onComplete{
          case Success(st) => p.success(st)
          case Failure(ex) => p.failure(ex)
        }
      }
      p.future
    }

    def batchColor = {
      val (prv,cur) = get
      def isOk(pw: Long, pr: Long, cw: Long, cr: Long) = cw == cr || cr > pr
      val updatesOk = isOk(prv._1,prv._2,cur._1,cur._2)
      val indexesOk = isOk(prv._3,prv._4,cur._3,cur._4)
      if(updatesOk && indexesOk) Green
      else if(updatesOk) Yellow
      else if(indexesOk) Yellow
      else Red
    }
  }

  def getElasticsearchStatus(host: String = HealthUtils.ip): Future[(String,String)] = {
    cmwell.util.http.SimpleHttpClient.get(s"http://$host:9200/_cluster/health").map { res =>
      val j: JsValue = Json.parse(res.payload)
      val color: String = j.\("status").as[String]
      val n: Int = j.\("number_of_nodes").as[Int]
      val d: Int = j.\("number_of_data_nodes").as[Int]
      val p: Int = j.\("active_primary_shards").as[Int]
      val s: Int = j.\("active_shards").as[Int]
      color -> s"n:$n,d:$d,p:$p,s:$s"
    }.recover {
      case ex: Throwable => "grey" -> ex.getMessage
    }
  }

  def getCassandraStatus: Future[List[CassandraStatusExtracted]] = {
    Future{controllers.HealthUtils.CassNodetoolStatus}.map{
      _.lines.collect{
        case CassandraStatus1(status,ip,load,owns,id,token,rack) => CassandraStatusExtracted(status,ip,load,owns,id,token,rack)
        case CassandraStatus2(status,ip,load,token,owns,id,rack) => CassandraStatusExtracted(status,ip,load,owns,id,token,rack)
        case CassandraStatus3(status,ip,load,token,id,rack) => CassandraStatusExtracted(status,ip,load,"NO DATA",id,token,rack)
        case CassandraStatus4(status,ip,load,token,id,rack) => CassandraStatusExtracted(status,ip,load,"NO DATA",id,token,rack)
      }.toList
    }
  }

  def generateHealthData: ComponentHealthFutureTuple4 = {
    val wsTuple = dashBoard.getCmwellWSRatio.map {
      case Right(((n, d),msg)) => {
        val clr = if (n + 1 >= d) Green
        else {
          logger.warn(msg)
          if (n > 1) Yellow
          else Red
        }
        (clr -> s"$n Up / $d")
      }
      case Left(err) => (Red -> err)
    }

    val bgTuple = BatchStatus.getAggregatedBatchColor.map {
      case (clr,msg) => Color(clr) -> msg
    }

    val esTuple = dashBoard.getElasticsearchStatus().map(t => (Color(t._1) -> t._2))

    val caTuple = dashBoard.getCassandraStatus.map { xs =>
      val upNormal = xs.filter(_.status.toUpperCase == "UN")
      val clr = cassandraStatusToColor(xs)
      (clr, s"${upNormal.size} Up / ${xs.size}")
    }

    wsTuple.onFailure{
      case ex: Throwable => logger.error("data could not be retrieved for ws.", ex)
    }
    bgTuple.onFailure{
      case ex: Throwable => logger.error("data could not be retrieved for bg.", ex)
    }
    esTuple.onFailure{
      case ex: Throwable => logger.error("data could not be retrieved for es.", ex)
    }
    caTuple.onFailure{
      case ex: Throwable => logger.error("data could not be retrieved for ca.", ex)
    }

    (wsTuple,bgTuple,esTuple,caTuple)
  }

  def cassandraStatusToColor(lcse: List[CassandraStatusExtracted]): Color = {
    val (upNormal,badStatus) = lcse.partition(_.status.toUpperCase == "UN")
    if(badStatus.size < 2 && upNormal.size > badStatus.size) Green
    else if(badStatus.size > 3 || upNormal.size < badStatus.size) Red
    else Yellow
  }

  /**
   * @return health-detailed data
   */
  def generateDetailedHealthData: Future[List[(String,FutureColorTuple4)]] = {
    val f = dashBoard.getStatusForAll.map { stf =>
      stf.map {
        case (host,(wsResponse, batchColor, cassStats, esColor)) => {
          val ws = wsResponse.map(r => if(r == 200) Green else Yellow).recover{case _ => Red}
          val bg = batchColor.map(Color(_))
          val ca = cassStats.map(cassandraStatusToColor(_)).recover{
            case ex: java.net.ConnectException => {
              logger.error(s"could not retrieve data from $host/health/cas",ex)
              Grey
            }
          }
          val es = esColor.map(Color(_))
          host -> (ws, bg, ca, es)
        }
      }.toList.sortBy(_._1)
    }
    f
  }

  object DashBoardCache {

    @volatile private[this] var wsData = (Grey, "Undefined")
    @volatile private[this] var wsTime = new DateTime(0L)
    @volatile private[this] var bgData = (Grey, "Undefined")
    @volatile private[this] var bgTime = new DateTime(0L)
    @volatile private[this] var esData = (Grey, "Undefined")
    @volatile private[this] var esTime = new DateTime(0L)
    @volatile private[this] var caData = (Grey, "Undefined")
    @volatile private[this] var caTime = new DateTime(0L)
    @volatile private[this] var detail: DetailedHealthData = Map.empty[String,ColorTuple4]
    @volatile private[this] var detailTime = new DateTime(0L)
    @volatile private[this] var batch: Map[String,TlogState] = Map.empty[String,TlogState]
    @volatile private[this] var batchTime = new DateTime(0L)

    def init: Future[(DetailedHealthData,TlogStatus)] = {
      val f = foreachHost{host =>
        val detailedHealth: (String,ColorTuple4) = host -> (Grey,Grey,Grey,Grey)
        val batchStatus: (String,TlogState) = host -> (0L,0L,0L,0L)
        detailedHealth -> batchStatus
        }.map(_.toList.sortBy(_._1).unzip)
        f.onComplete{
        case Success(v) => detail = v._1.toMap ; batch = v._2.toMap
        case Failure(e) => logger.error("init DashBoardCache failed.", e)
      }
      f.map(t => t._1.toMap -> t._2.toMap)
    }

    def cacheAndGetHealthData: HealthTimedData = {
      val (ws,bg,es,ca) = generateHealthData

      //make promises that will eventually hold valid data if successful
      val wsp = Promise[TimedHealth]()
      val bgp = Promise[TimedHealth]()
      val esp = Promise[TimedHealth]()
      val cap = Promise[TimedHealth]()

      //update cache on success & fill promises with successful data
      ws.onComplete{
        case Success(v) => {wsData = v; wsTime = new DateTime(); wsp.success(v -> wsTime)}
        case Failure(e) => wsp.failure(e)
      }
      bg.onComplete{
        case Success(v) => {bgData = v; bgTime = new DateTime(); bgp.success(v -> bgTime)}
        case Failure(e) => bgp.failure(e)
      }
      es.onComplete{
        case Success(v) => {esData = v; esTime = new DateTime(); esp.success(v -> esTime)}
        case Failure(e) => esp.failure(e)
      }
      ca.onComplete{
        case Success(v) => {caData = v; caTime = new DateTime(); cap.success(v -> caTime)}
        case Failure(e) => cap.failure(e)
      }

      //always have a successful future with valid data
      val wsr: Future[TimedHealth] = wsp.future.recover{case _ => wsData -> wsTime}
      val bgr: Future[TimedHealth] = bgp.future.recover{case _ => bgData -> bgTime}
      val esr: Future[TimedHealth] = esp.future.recover{case _ => esData -> esTime}
      val car: Future[TimedHealth] = cap.future.recover{case _ => caData -> caTime}

      //make sure to complete successfully within the specified timeout
      val deadline = timeOut.fromNow
      val wsth = Try(Await.result(wsr,deadline.timeLeft)).getOrElse(wsData -> wsTime)
      val bgth = Try(Await.result(bgr,deadline.timeLeft)).getOrElse(bgData -> bgTime)
      val esth = Try(Await.result(esr,deadline.timeLeft)).getOrElse(esData -> esTime)
      val cath = Try(Await.result(car,deadline.timeLeft)).getOrElse(caData -> caTime)

      (wsth,bgth,esth,cath,null,null,"", Set.empty[String])
    }

    def cacheAndGetDetailedHealthData: DetailedHealthTimedData = {

      def updateColor(opt: Option[ColorTuple4], c1: Option[Color], c2: Option[Color], c3: Option[Color], c4: Option[Color]) = opt match {
        case Some((o1,o2,o3,o4)) =>  (c1.getOrElse(o1),c2.getOrElse(o2),c3.getOrElse(o3),c4.getOrElse(o4))
        case None => (c1.getOrElse(Grey),c2.getOrElse(Grey),c3.getOrElse(Grey),c4.getOrElse(Grey))
      }

      val deadline = timeOut.fromNow
      val f = generateDetailedHealthData.map(_.toMap)
      val xs: Map[String,FutureColorTuple4] = Try(Await.result(f,timeOut)).getOrElse[Map[String,FutureColorTuple4]](Hosts.getCachedHostsSet.map(
        h => {
          val fGrey = Future.successful(Grey)
          val tOpt: Option[FutureColorTuple4] = detail.get(h).map(c => (Future.successful(c._1),Future.successful(c._2),Future.successful(c._3),Future.successful(c._4)))
          val t: FutureColorTuple4 = tOpt.getOrElse((fGrey,fGrey,fGrey,fGrey))
          h -> t
        }).toMap)
      xs.foreach {
        case (h, (c1, c2, c3, c4)) => {
          c1.onSuccess { case c: Color => {
            val clrTup = updateColor(detail.get(h), Some(c), None, None, None)
            detail = detail.updated(h, clrTup)
            detailTime = new DateTime()
          }}
          c2.onSuccess { case c: Color => {
            val clrTup = updateColor(detail.get(h), None, Some(c), None, None)
            detail = detail.updated(h, clrTup)
            detailTime = new DateTime()
          }}
          c3.onSuccess { case c: Color => {
            val clrTup = updateColor(detail.get(h), None, None, Some(c), None)
            detail = detail.updated(h, clrTup)
            detailTime = new DateTime()
          }}
          c4.onSuccess { case c: Color => {
            val clrTup = updateColor(detail.get(h), None, None, None, Some(c))
            detail = detail.updated(h, clrTup)
            detailTime = new DateTime()
          }}
        }
      }

      val ys = xs.toList.flatMap{case (_,(f1,f2,f3,f4)) => Seq(f1,f2,f3,f4)}
      val fSeq = Future.sequence(ys)
      Try(Await.ready(fSeq, deadline.timeLeft)).toOption match {
        case _ => detail -> detailTime
      }
    }

    def cacheAndGetBatchStatus: (TlogStatus,DateTime) = {
      val f = BatchStatus.getAll.map(_.collect{case (h,Some(ts)) => h -> ts})
      f.onSuccess{case s => batch = s.toMap; batchTime = new DateTime()}
      Try(Await.result(f,timeOut).toMap -> new DateTime()).getOrElse(batch -> batchTime)
    }
  }
}
