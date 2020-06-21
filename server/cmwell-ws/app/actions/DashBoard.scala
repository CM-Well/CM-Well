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

import javax.inject._

import akka.actor.ActorSystem
import akka.stream.Materializer
import controllers.HealthUtils
import logic.CRUDServiceFS
import cmwell.util.http.{SimpleHttpClient => Http, _}
import cmwell.syntaxutils.!!!
import cmwell.domain.{SimpleResponse => _, _}
import cmwell.util.concurrent.SimpleScheduler
import cmwell.ws.Settings

import scala.util._
import scala.concurrent.{duration, Await, ExecutionContext, Future, Promise}
import duration._
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.ws._
import play.api.libs.json._
import play.api.Play.current
import org.joda.time._

import scala.language.postfixOps

object Color extends Enumeration {
  type Color = Value
  type ColorTuple4 = Tuple4[Color, Color, Color, Color]
  val Green, Yellow, Red, Grey = Value

  def apply(color: String): Color = color.toLowerCase match {
    case "green"  => Green
    case "yellow" => Yellow
    case "red"    => Red
    case _        => Grey
  }
}

import Color._

@Singleton
class DashBoard @Inject()(crudServiceFS: CRUDServiceFS)(implicit ec: ExecutionContext,
                                                        sys: ActorSystem,
                                                        mat: Materializer)
    extends LazyLogging { dashBoard =>

  val timeOut = cmwell.ws.Settings.cacheTimeout.seconds

  type FutureColorTuple4 = (Future[Color], Future[Color], Future[Color], Future[Color])
  type Ratio = (Int, Int)
  type GenStatus = (Int, String, List[CassandraStatusExtracted], String)
  type Fields = Map[String, Set[FieldValue]]
  type FieldsOpt = Option[Fields]
  type ComponentHealth = (Color, String)
  type ComponentHealthFutureTuple = (Future[ComponentHealth], Future[ComponentHealth], Future[ComponentHealth])
  type TimedHealth = (ComponentHealth, DateTime)
  type HealthTimedData = (TimedHealth, TimedHealth, TimedHealth, TimedHealth, TimedHealth, String, Set[String])
  type HealthTimedDataWithBG =
    (TimedHealth, TimedHealth, TimedHealth, TimedHealth, TimedHealth, TimedHealth, String, Set[String])
  type DetailedHealthData = Map[String, ColorTuple4]
  type DetailedHealthTimedData = (DetailedHealthData, DateTime)

  object HostWithPort extends scala.util.matching.Regex("""(.+):(\d+)""")
  object CassandraStatus1
      extends scala.util.matching.Regex(
        """\s*(\S+)\s+(\d+.\d+.\d+.\d+)\s+(\d+.?\d*\s*\S+)\s+(\d+.?\d*%)\s+(\S+)\s+(-?\d+)\s+(\S+)\s*"""
      )
  object CassandraStatus2
      extends scala.util.matching.Regex(
        """\s*(\S+)\s+(\d+.\d+.\d+.\d+)\s+(\d+.?\d*\s*\S+)\s+(-?\d+)\s+(\d+.?\d*%)\s+(\S+)\s+(\S+)\s*"""
      )
  object CassandraStatus3
      extends scala.util.matching.Regex(
        """\s*(\S+)\s+(\d+.\d+.\d+.\d+)\s+(\d+.?\d*\s*\S+)\s+(-?\d+)\s+\?\s+(\S+)\s+(\S+)\s*"""
      )
  object CassandraStatus4
      extends scala.util.matching.Regex(
        """\s*(\S+)\s+(\d+.\d+.\d+.\d+)\s+(\d+.?\d*\s*\S+)\s+\?\s+(\S+)\s+(-?\d+)\s+(\S+)\s*"""
      )

  case class CassandraStatusExtracted(status: String,
                                      ip: String,
                                      load: String,
                                      owns: String,
                                      id: String,
                                      token: String,
                                      rack: String)

  object Hosts {
    @volatile private[this] var hosts: Set[String] = Set.empty[String]

    private[this] val hostsDaemonCancellable = {
      import scala.concurrent.duration.DurationInt

      SimpleScheduler.scheduleAtFixedRate(0 seconds, 1 minutes) {
        crudServiceFS.getInfoton("/meta/sys", None, None).map {
          case None => logger.error("could not retrieve data from local CRUDService")
          case Some(Everything(metaSys)) =>
            metaSys.fields match {
              case None => logger.error("/meta/sys retrieved from local CRUDService, but does not contain fields")
              case Some(m: Fields) => {
                hosts = m("known-cmwell-hosts").collect {
                  case FString(host, _, _) => host
                }
              }
            }
          case Some(UnknownNestedContent(_)) => !!! //  not reachable! case written to silence compilation warnings
        }
      }
    }

    def getCachedHostsSet = hosts
    def foreachHost[T](doWithHost: String => T): Future[Set[T]] = Future(hosts.map(doWithHost))
  }

  import Hosts.foreachHost

  def getCmwellWSRatio: Future[Either[String, (Ratio, String)]] = {
    val p = Promise[Either[String, (Ratio, String)]]()

    val fsfes: Future[Set[Future[Either[String, (String, Int)]]]] = foreachHost { host =>
      Http
        .get(s"http://$host/?format=json")
        .map { wsr =>
          Right(host -> wsr.status)
        }
        .recover {
          case ex: Throwable => {
            logger.error("getCmwellWSRatio: future failed", ex)
            Left(s"GET for host $host failed with message: ${ex.getMessage}")
          }
        }
    }

    //TODO: report scala bug? `Future.sequence(_)` should be equivalent to `Future.sequence`, but infered type is `Nothing`?
    val ffsew = fsfes.map[Future[Set[Either[String, (String, Int)]]]](Future.sequence(_))
    ffsew.onComplete {
      case Success(fsw) => {
        fsw.foreach { s =>
          val sb = new StringBuilder
          val ratio = s.foldLeft(0 -> 0) {
            case ((n, d), Right((_, 200)))  => (n + 1, d + 1)
            case ((n, d), Right((host, i))) => sb.append(s"\n$host returned $i status code."); (n, d + 1)
            case ((n, d), Left(err))        => sb.append(s"\n$err"); (n, d + 1)
          }
          p.success(Right(ratio -> sb.mkString))
        }
      }
      case Failure(err) => {
        logger.error("getCmwellWSRatio: future seq failed", err)
        p.success(Left(err.getMessage))
      }
    }
    p.future
  }

  def getStatusForAll = {
    val fSet = foreachHost { host =>
      val ws = Http.get(s"http://$host/?format=json").map(_.status)
      val (bg, es) = {
        val ff =
          Http.get(s"http://$host/proc/node?format=json").map(r => Json.parse(r.payload).\("fields").get).recover {
            case ex: java.net.ConnectException => {
              logger.error(s"could not retrieve data from $host/proc/node", ex)
              val grey = JsArray(Seq(JsString("grey")))
              JsObject(Seq("batch_color" -> grey, "es_color" -> grey))
            }
          }
        ff.map(_.\("batch_color")(0).as[String]) -> ff.map(_.\("es_color")(0).as[String])
      }
      val ca = Http
        .get(s"http://$host/health/cas")(SimpleResponse.Implicits.UTF8StringHandler, ec, sys, mat)
        .map(_.payload.lines.collect {
          case CassandraStatus1(status, ip, load, owns, id, token, rack) =>
            CassandraStatusExtracted(status, ip, load, owns, id, token, rack)
          case CassandraStatus2(status, ip, load, token, owns, id, rack) =>
            CassandraStatusExtracted(status, ip, load, owns, id, token, rack)
          case CassandraStatus3(status, ip, load, token, id, rack) =>
            CassandraStatusExtracted(status, ip, load, "NO DATA", id, token, rack)
          case CassandraStatus4(status, ip, load, token, id, rack) =>
            CassandraStatusExtracted(status, ip, load, "NO DATA", id, token, rack)
        }.toList)
      host -> (ws, bg, ca, es)
    }
    fSet
  }

  def getElasticsearchStatus(host: String = HealthUtils.ip): Future[(String, String)] = {
    cmwell.util.http.SimpleHttpClient
      .get(s"http://$host:9200/_cluster/health")
      .map { res =>
        val j: JsValue = Json.parse(res.payload)
        val color: String = j.\("status").as[String]
        val n: Int = j.\("number_of_nodes").as[Int]
        val d: Int = j.\("number_of_data_nodes").as[Int]
        val p: Int = j.\("active_primary_shards").as[Int]
        val s: Int = j.\("active_shards").as[Int]
        color -> s"n:$n,d:$d,p:$p,s:$s"
      }
      .recover {
        case ex: Throwable => "grey" -> ex.getMessage
      }
  }

  def getCassandraStatus: Future[List[CassandraStatusExtracted]] = {
    Future { controllers.HealthUtils.CassNodetoolStatus }.map {
      _.lines.collect {
        case CassandraStatus1(status, ip, load, owns, id, token, rack) =>
          CassandraStatusExtracted(status, ip, load, owns, id, token, rack)
        case CassandraStatus2(status, ip, load, token, owns, id, rack) =>
          CassandraStatusExtracted(status, ip, load, owns, id, token, rack)
        case CassandraStatus3(status, ip, load, token, id, rack) =>
          CassandraStatusExtracted(status, ip, load, "NO DATA", id, token, rack)
        case CassandraStatus4(status, ip, load, token, id, rack) =>
          CassandraStatusExtracted(status, ip, load, "NO DATA", id, token, rack)
      }.toList
    }
  }

  def generateHealthData: ComponentHealthFutureTuple = {
    val wsTuple = dashBoard.getCmwellWSRatio.map {
      case Right(((n, d), msg)) => {
        val clr =
          if (n + 1 >= d) Green
          else {
            logger.warn(msg)
            if (n > 1) Yellow
            else Red
          }
        clr -> s"$n Up / $d"
      }
      case Left(err) => Red -> err
    }

    val esTuple = dashBoard.getElasticsearchStatus().map(t => (Color(t._1) -> t._2))

    val caTuple = dashBoard.getCassandraStatus.map { xs =>
      val upNormal = xs.filter(_.status.toUpperCase == "UN")
      val clr = cassandraStatusToColor(xs)
      (clr, s"${upNormal.size} Up / ${xs.size}")
    }

    wsTuple.failed.foreach {
      case ex: Throwable => logger.error("data could not be retrieved for ws.", ex)
    }
    esTuple.failed.foreach {
      case ex: Throwable => logger.error("data could not be retrieved for es.", ex)
    }
    caTuple.failed.foreach {
      case ex: Throwable => logger.error("data could not be retrieved for ca.", ex)
    }

    (wsTuple, esTuple, caTuple)
  }

  def cassandraStatusToColor(lcse: List[CassandraStatusExtracted]): Color = {
    val (upNormal, badStatus) = lcse.partition(_.status.toUpperCase == "UN")
    if (badStatus.size < 2 && upNormal.size > badStatus.size) Green
    else if (badStatus.size > 3 || upNormal.size < badStatus.size) Red
    else Yellow
  }

  /**
    * @return health-detailed data
    */
  def generateDetailedHealthData: Future[List[(String, FutureColorTuple4)]] = {
    val f = dashBoard.getStatusForAll.map { stf =>
      stf
        .map {
          case (host, (wsResponse, bgColor, cassStats, esColor)) => {
            val ws = wsResponse.map(r => if (r == 200) Green else Yellow).recover { case _ => Red }
            val bg = bgColor.map(Color(_))
            val ca = cassStats.map(cassandraStatusToColor).recover {
              case ex: java.net.ConnectException => {
                logger.error(s"could not retrieve data from $host/health/cas", ex)
                Grey
              }
            }
            val es = esColor.map(Color(_))
            host -> (ws, bg, ca, es)
          }
        }
        .toList
        .sortBy(_._1)
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
    @volatile private[this] var detail: DetailedHealthData = Map.empty[String, ColorTuple4]
    @volatile private[this] var detailTime = new DateTime(0L)

    def cacheAndGetHealthData: HealthTimedData = {
      val (ws, es, ca) = generateHealthData

      //make promises that will eventually hold valid data if successful
      val wsp = Promise[TimedHealth]()
      val esp = Promise[TimedHealth]()
      val cap = Promise[TimedHealth]()

      //update cache on success & fill promises with successful data
      ws.onComplete {
        case Success(v) => { wsData = v; wsTime = new DateTime(); wsp.success(v -> wsTime) }
        case Failure(e) => wsp.failure(e)
      }
      es.onComplete {
        case Success(v) => { esData = v; esTime = new DateTime(); esp.success(v -> esTime) }
        case Failure(e) => esp.failure(e)
      }
      ca.onComplete {
        case Success(v) => { caData = v; caTime = new DateTime(); cap.success(v -> caTime) }
        case Failure(e) => cap.failure(e)
      }

      //always have a successful future with valid data
      val wsr: Future[TimedHealth] = wsp.future.recover { case _ => wsData -> wsTime }
      val esr: Future[TimedHealth] = esp.future.recover { case _ => esData -> esTime }
      val car: Future[TimedHealth] = cap.future.recover { case _ => caData -> caTime }

      //make sure to complete successfully within the specified timeout
      val deadline = timeOut.fromNow
      val wsth = Try(Await.result(wsr, deadline.timeLeft)).getOrElse(wsData -> wsTime)
      val esth = Try(Await.result(esr, deadline.timeLeft)).getOrElse(esData -> esTime)
      val cath = Try(Await.result(car, deadline.timeLeft)).getOrElse(caData -> caTime)

      (wsth, esth, cath, null, null, "", Set.empty[String])
    }

    def cacheAndGetDetailedHealthData: DetailedHealthTimedData = {

      def updateColor(opt: Option[ColorTuple4],
                      c1: Option[Color],
                      c2: Option[Color],
                      c3: Option[Color],
                      c4: Option[Color]) = opt match {
        case Some((o1, o2, o3, o4)) => (c1.getOrElse(o1), c2.getOrElse(o2), c3.getOrElse(o3), c4.getOrElse(o4))
        case None                   => (c1.getOrElse(Grey), c2.getOrElse(Grey), c3.getOrElse(Grey), c4.getOrElse(Grey))
      }

      val deadline = timeOut.fromNow
      val f = generateDetailedHealthData.map(_.toMap)
      val xs: Map[String, FutureColorTuple4] = Try(Await.result(f, timeOut)).getOrElse[Map[String, FutureColorTuple4]](
        Hosts.getCachedHostsSet
          .map(h => {
            val fGrey = Future.successful(Grey)
            val tOpt: Option[FutureColorTuple4] = detail
              .get(h)
              .map(
                c =>
                  (Future.successful(c._1), Future.successful(c._2), Future.successful(c._3), Future.successful(c._4))
              )
            val t: FutureColorTuple4 = tOpt.getOrElse((fGrey, fGrey, fGrey, fGrey))
            h -> t
          })
          .toMap
      )
      xs.foreach {
        case (h, (c1, c2, c3, c4)) => {
          c1.foreach {
            c: Color => {
              val clrTup = updateColor(detail.get(h), Some(c), None, None, None)
              detail = detail.updated(h, clrTup)
              detailTime = new DateTime()
            }
          }
          c2.foreach {
            c: Color => {
              val clrTup = updateColor(detail.get(h), None, Some(c), None, None)
              detail = detail.updated(h, clrTup)
              detailTime = new DateTime()
            }
          }
          c3.foreach {
            c: Color => {
              val clrTup = updateColor(detail.get(h), None, None, Some(c), None)
              detail = detail.updated(h, clrTup)
              detailTime = new DateTime()
            }
          }
          c4.foreach {
            c: Color => {
              val clrTup = updateColor(detail.get(h), None, None, None, Some(c))
              detail = detail.updated(h, clrTup)
              detailTime = new DateTime()
            }
          }
        }
      }

      val ys = xs.toList.flatMap { case (_, (f1, f2, f3, f4)) => Seq(f1, f2, f3, f4) }
      val fSeq = Future.sequence(ys)
      Try(Await.ready(fSeq, deadline.timeLeft)).toOption match {
        case _ => detail -> detailTime
      }
    }
  }
}
