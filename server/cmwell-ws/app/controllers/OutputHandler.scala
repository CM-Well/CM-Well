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


package controllers

import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}

import actions.RequestMonitor
import cmwell.domain._
import cmwell.formats.{FormatExtractor, _}
import cmwell.util.formats.JsonEncoder
import cmwell.ws.util.RequestHelpers._
import cmwell.ws.util.TypeHelpers
import com.typesafe.scalalogging.LazyLogging
import logic.CRUDServiceFS
import org.joda.time.DateTimeZone
import org.joda.time.format.ISODateTimeFormat
import play.api.mvc._
import security.{AuthUtils, PermissionLevel}
import wsutil._
import javax.inject._

import akka.stream.scaladsl.Flow
import cmwell.util.FullBox
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.util.TypeHelpers.asBoolean
import ld.cmw.{NbgPassiveFieldTypesCache, ObgPassiveFieldTypesCache}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * Created with IntelliJ IDEA.
 * User: gilad
 * Date: 11/17/13
 * Time: 9:03 AM
 * To change this template use File | Settings | File Templates.
 */
@Singleton
class OutputHandler  @Inject()(crudServiceFS: CRUDServiceFS,
                               authUtils: AuthUtils,
                               tbg: NbgToggler,
                               cmwellRDFHelper: CMWellRDFHelper,
                               formatterManager: FormatterManager) extends Controller with LazyLogging with TypeHelpers {

  val fullDateFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC)

  def typesCache(nbg: Boolean) = if(nbg || tbg.get) crudServiceFS.nbgPassiveFieldTypesCache else crudServiceFS.obgPassiveFieldTypesCache

  def overrideMimetype(default: String, req: Request[AnyContent]): (String, String) = req.getQueryString("override-mimetype") match {
    case Some(mimetype) => (CONTENT_TYPE, mimetype)
    case _ => (CONTENT_TYPE, default)
  }

  def internetDateFormatter = {
    val ret = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss 'GMT'", Locale.US)
    ret.setTimeZone(TimeZone.getTimeZone("UTC"))
    ret
  }

  def getNoCacheHeaders(): List[(String, String)] = {
    val d = internetDateFormatter.format(new Date(System.currentTimeMillis()))
    List("Expires" -> d,
      "Date" -> d,
      "Cache-Control" ->
        "no-cache, private, no-store",
      "Pragma" -> "no-cache")
  }

  private def getPathsVector(body: String, baseUrl: String): Vector[String] = {
    if (body.contains("\n"))
      body.split("\n").collect { case s: String if !s.forall(_.isWhitespace) => getCannonicalRepresentation(s, baseUrl)}.toVector
    else
      Vector(getCannonicalRepresentation(body, baseUrl))
  }

  /**
   * convert any path to a cmwell root path:
   * http://example.org/p/a/t/h  => /example.org/p/a/t/h
   * https://example.org/p/a/t/h => /s.example.org/p/a/t/h
   * cmwell://p/a/t/h            => /p/a/t/h
   * http://connext/p/a/t/h      => /p/a/t/h
   * /p/a/t/h/                   => /p/a/t/h
   * p/a/t/h                     => /p/a/t/h
   * @param path
   * @return
   */
  private def getCannonicalRepresentation(path: String, baseUrl: String): String = {
    def trimWhitspaces(s: String, trim: String => String = _.dropWhile(_.isWhitespace)): String = trim(trim(s).reverse).reverse
    val tPath = trimWhitspaces(path)
    val http = "http://"
    val https = "https://"
    val cmwell = "cmwell://"
    if (tPath.startsWith(baseUrl)) tPath.drop(baseUrl.length)
    else if (tPath.startsWith(http)) s"/${tPath.drop(http.length)}"
    else if (tPath.startsWith(https)) s"/https.${tPath.drop(https.length)}"
    else if (tPath.startsWith(cmwell)) s"/${tPath.drop(cmwell.length)}"
    else if (tPath.startsWith("/")) tPath
    else s"/$tPath"
  }

  def handleWebSocket(format: String) = WebSocket.accept[String,String] { request =>
    val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
    val formatter = format match {
      case FormatExtractor(formatType) =>
        formatterManager.getFormatter(
          format = formatType,
          host = request.host,
          uri = request.uri,
          pretty = request.queryString.keySet("pretty"),
          callback = request.queryString.get("callback").flatMap(_.headOption),
          withData = request.getQueryString("with-data"),
          nbg = nbg)
    }
    Flow[String].mapAsync(cmwell.ws.Streams.parallelism){ msg =>
      msg.take(4) match {
        case "/ii/" => crudServiceFS.getInfotonByUuidAsync(msg.drop(4))
        case _ => crudServiceFS.getInfotonByPathAsync(msg)
      }
    }.collect {
      case FullBox(i) =>formatter.render(i)
    }
  }

//  def handleWebSocketOLD(format: String) = WebSocket.using[String] { request =>
//    val (out, channel) = Concurrent.broadcast[String]
//    //log the message to stdout and send response back to client
//    val in = Iteratee.foreach[String] {
//      msg =>
//        //the Enumerator returned by Concurrent.broadcast subscribes to the channel and will
//        //receive the pushed messages
//        val fBag = CRUDServiceFS.getInfotons(Vector(msg)) //why ask for a vector of 1 infoton & return a bag? would'nt it be better to get a single infoton?
//      val formatter = format match {
//          case FormatExtractor(formatType) =>
//            FormatterManager.getFormatter(
//              format = formatType,
//              host = request.host,
//              uri = request.uri,
//              pretty = request.queryString.keySet("pretty"),
//              callback = request.queryString.get("callback").flatMap(_.headOption),
//              withData = request.getQueryString("with-data"))
//        }
//        fBag.onComplete {
//          case Success(b) => channel.push(formatter.render(b))
//          case Failure(e) => channel.end(e)
//        }
//    }
//    (in, out)
//  }

  def handlePost(format: String = "") = Action.async { implicit req =>
    if(req.contentType.getOrElse("").contains("json"))
      RequestMonitor.add("out",req.path, req.rawQueryString, req.body.asJson.getOrElse("").toString)
    else
      RequestMonitor.add("out",req.path, req.rawQueryString, req.body.asText.getOrElse(""))

    val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
    val fieldsMaskFut = extractFieldsMask(req,typesCache(nbg),cmwellRDFHelper, nbg)

    val formatType = format match {
      case FormatExtractor(ft) => ft
      case _ => RdfType(N3Flavor)
    }

    val formatter = formatterManager.getFormatter(
      format = formatType,
      host = req.host,
      uri = req.uri,
      pretty = req.queryString.keySet("pretty"),
      callback = req.queryString.get("callback").flatMap(_.headOption),
      withData = req.getQueryString("with-data"),
      nbg = nbg)

    val either: Either[SimpleResponse, Vector[String]] = req.body.asJson match {
      case Some(json) => {
        JsonEncoder.decodeInfotonPathsList(json) match {
          case Some(infotonPaths: InfotonPaths) => Right(infotonPaths.paths.toVector)
          case _ => Left(SimpleResponse(false, Some("not a valid bag of infotons request.")))
        }
      }
      case None => req.body.asText match {
        case Some(text) => Right(getPathsVector(text, cmWellBase))
        case None => Left(SimpleResponse(false, Some("unknown content (you may want to change content-type)")))
      }
    }
    val mimetype = overrideMimetype(formatter.mimetype, req)._2
    either match {
      case Left(badReqSimpleResponse) => Future.successful(BadRequest(formatter.render(badReqSimpleResponse)).as(mimetype))
      case Right(vector) => {
        val t = futureRetrievablePathsFromPathsAsLines(vector, req)
        fieldsMaskFut.flatMap { fieldsMask =>
          t.map {
            case (ok, s) if ok => Ok(formatter.render(s.masked(fieldsMask))).as(mimetype)
            case (_, s) => InsufficientStorage(formatter.render(s)).as(mimetype)
          }.recover(PartialFunction(wsutil.exceptionToResponse))
        }
      }
    }
  }

  def futureRetrievablePathsFromPathsAsLines(text: String, baseUrl: String, req: Request[AnyContent]): Future[(Boolean,RetrievablePaths)] = futureRetrievablePathsFromPathsAsLines(getPathsVector(text, baseUrl), req)

  def futureRetrievablePathsFromPathsAsLines(pathsVector: Vector[String], req: Request[AnyContent]) = {

    val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)

    val (byUuid, byPath) = pathsVector.partition(_.startsWith("/ii/")) match {
      case (xs, ys) => (xs.map(_.drop(4)), ys) // "/ii/".length = 4
    }

    crudServiceFS.getInfotonsByPathOrUuid(byPath, byUuid).flatMap {
      case BagOfInfotons(coreInfotons) => {
        val eInfotons = req.getQueryString("yg") match {
          case None => Future.successful(true -> coreInfotons)
          case Some(ygp) => Try(wsutil.pathExpansionParser(ygp, coreInfotons, req.getQueryString("yg-chunk-size").flatMap(asInt).getOrElse(10),cmwellRDFHelper,typesCache(nbg),nbg)) match {
            case Success(f) => f
            case Failure(e) => Future.failed(e)
          }
        }

        val fInfotons = eInfotons.flatMap {
          case (true,ygExpandedInfotons) => req.getQueryString("xg") match {
            case None => Future.successful(true -> ygExpandedInfotons)
            case Some(xgp) => Try(wsutil.deepExpandGraph(xgp, ygExpandedInfotons,cmwellRDFHelper,typesCache(nbg),nbg)) match {
              case Success(f) => f
              case Failure(e) => Future.failed(e)
            }
          }
          case t => Future.successful(t)
        }

        fInfotons.map { case (ok,infotons) =>
          val irretrievableUuids = byUuid.filterNot(uuid => coreInfotons.exists(_.uuid == uuid)).map(uuid => s"/ii/$uuid")
          val irretrievablePaths = byPath.filterNot(path => coreInfotons.exists(_.path == path))

          val notAllowedPaths = authUtils.filterNotAllowedPaths(infotons.map(_.path), PermissionLevel.Read, authUtils.extractTokenFrom(req)).toVector
          val allowedInfotons = infotons.filterNot(i => notAllowedPaths.contains(i.path))

          ok -> RetrievablePaths(allowedInfotons, irretrievableUuids ++ irretrievablePaths ++ notAllowedPaths)
        }
      }
    }
  }
}
