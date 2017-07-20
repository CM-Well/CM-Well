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

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.Locale
import javax.inject._

import actions._
import akka.NotUsed
import akka.pattern.AskTimeoutException
import akka.stream.scaladsl.Source
import akka.util.{ByteString, Timeout}
import cmwell.common.file.MimeTypeIdentifier
import cmwell.domain.{BagOfInfotons, CompoundInfoton, DeletedInfoton, FString, PaginationInfo, SearchResponse, SearchResults, _}
import cmwell.formats._
import cmwell.fts._
import cmwell.rts.{Key, Pull, Push, Subscriber}
import cmwell.util.concurrent.{Combiner, SingleElementLazyAsyncCache}
import cmwell.util.formats.Encoders
import cmwell.util.http.SimpleHttpClient
import cmwell.util.loading.ScalaJsRuntimeCompiler
import cmwell.util.{Box, BoxedFailure, EmptyBox, FullBox}
import cmwell.web.ld.exceptions.UnsupportedURIException
import cmwell.ws.adt.request.{CMWellRequest, CreateConsumer, Search}
import cmwell.ws.adt.{BulkConsumeState, ConsumeState, SortedConsumeState}
import cmwell.ws.util.RequestHelpers._
import cmwell.ws.util.TypeHelpers._
import cmwell.ws.util._
import cmwell.ws.{Settings, Streams}
import com.typesafe.scalalogging.LazyLogging
import k.grid.{ClientActor, Grid, GridJvm, RestartJvm}
import ld.cmw.{NbgPassiveFieldTypesCache, ObgPassiveFieldTypesCache}
import ld.exceptions.BadFieldTypeException
import logic.{CRUDServiceFS, InfotonValidator}
import markdown.MarkdownFormatter
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import play.api.http.MediaType
import play.api.libs.json.{JsArray, JsBoolean, JsNumber, JsObject, JsString, _}
import play.api.mvc.{ResponseHeader, Result, _}
import play.utils.UriEncoding
import security.PermissionLevel.PermissionLevel
import security._
import wsutil.{asyncErrorHandler, errorHandler, _}
import cmwell.syntaxutils.!!!
import cmwell.web.ld.cmw.CMWellRDFHelper

import scala.collection.mutable.{HashMap, MultiMap}
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scala.xml.Utility

object ApplicationUtils {
  /**
    * returns a boolean future determining weather deletion is allowed, and if it's forbidden, also returns a message with the reason.
    *
    * @param path
    * @return
    */
  def infotonPathDeletionAllowed(path: String, recursive: Boolean, crudServiceFS: CRUDServiceFS, nbg: Boolean)(implicit ec: ExecutionContext): Future[Either[String, Seq[String]]] = {
    def numOfChildren(p: String): Future[SearchResults] = {
      val pathFilter = if (p.length > 1) Some(PathFilter(p, true)) else None
      crudServiceFS.search(pathFilter, None, Some(DatesFilter(None, None)), PaginationParams(0, 500), false, false, SortParam.empty, false, false, nbg)
    }

    if (!InfotonValidator.isInfotonNameValid(path)) Future.successful {
      Left(s"the path $path is not valid for deletion. ")
    }
    else if (recursive) numOfChildren(path).map {
      case sr if sr.total > 500 => Left("recursive DELETE is forbidden for large (>500 descendants) infotons. DELETE descendants first.")
      case sr => Right(sr.infotons.map(_.path) :+ path)
    }
    else Future.successful(Right(Seq(path)))
  }

  def getNoCacheHeaders(): List[(String, String)] = {
    val d = ResponseHeader.httpDateFormat.print(System.currentTimeMillis())
    List("Expires"       -> d,
         "Date"          -> d,
         "Cache-Control" -> "no-cache, private, no-store",
         "Pragma"        -> "no-cache")
  }
}

@Singleton
class Application @Inject()(bulkScrollHandler: BulkScrollHandler,
                            activeInfotonGenerator: ActiveInfotonGenerator,
                            cachedSpa: CachedSpa,
                            crudServiceFS: CRUDServiceFS,
                            tbg: NbgToggler,
                            streams: Streams,
                            authUtils: AuthUtils,
                            cmwellRDFHelper: CMWellRDFHelper,
                            formatterManager: FormatterManager)(implicit ec: ExecutionContext) extends Controller with FileInfotonCaching with LazyLogging {

  import ApplicationUtils._

  lazy val nCache: NbgPassiveFieldTypesCache = crudServiceFS.nbgPassiveFieldTypesCache
  lazy val oCache: ObgPassiveFieldTypesCache = crudServiceFS.obgPassiveFieldTypesCache
  val fullDateFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC)

  def typesCache(nbg: Boolean) = if(nbg || tbg.get) nCache else oCache
  def typesCache(req: Request[_]) = if(req.getQueryString("nbg").flatMap(asBoolean).getOrElse(false) || tbg.get) nCache else oCache
  def isReactive[A](req: Request[A]): Boolean = req.getQueryString("reactive").fold(false)(!_.equalsIgnoreCase("false"))

  //TODO: validate query params

  private def requestSlashValidator[A](request: Request[A]): Try[Request[A]] = Try{
    if (request.path.toUpperCase(Locale.ENGLISH).contains("%2F"))
      throw new UnsupportedURIException("%2F is illegal in the path part of the URI!")
    else {
      val decodedPath = UriEncoding.decodePath(request.path, "UTF-8")
      val requestHeader = request.copy(path = decodedPath, uri = decodedPath + request.uri.drop(request.path.length))
      Request(requestHeader, request.body)
    }
  }

  def handleTypesCacheGet = Action(r => Ok(typesCache(r).getState))

  def handleGET(path:String) = Action.async { implicit originalRequest =>

    val op = originalRequest.getQueryString("op").getOrElse("read")
    RequestMonitor.add(op,path, originalRequest.rawQueryString, "")
    requestSlashValidator(originalRequest) match {
      case Failure(e) => asyncErrorHandler(e)
      case Success(request) => request match {
        case Operation.search() => handleSearch(request)
        case Operation.aggregate() => handleAggregate(request)
        case Operation.startScroll() => handleStartScroll(request)
        case Operation.scroll() => handleScroll(request)
        case Operation.createConsumer() => handleCreateConsumerRequest(request)
        case Operation.consume() => handleConsume(request)
        case Operation.subscribe() => handleSubscribe(request)
        case Operation.unsubscribe() => handleUnsubscribe(request)
        case Operation.pull() => handlePull(request)
        case Operation.stream() => handleStream(request)
        case Operation.multiStream() => handleBoostedStream(request)
        case Operation.superStream() => handleSuperStream(request)
        case Operation.queueStream() => handleQueueStream(request)
        case Operation.bulkConsumer() => bulkScrollHandler.handle(request)
        case Operation.fix() => handleFix(request)
        case Operation.info() => handleInfo(request)
        case Operation.verify() => handleVerify(request)
        case Operation.fixDc() => handleFixDc(request)
        case Operation.purgeAll() => handlePurgeAll(request)
        case Operation.purgeHistory() => handlePurgeHistory(request)
        case Operation.purgeLast() | Operation.rollback() => handlePurgeLast(request)
        case Operation.read() | _ => handleRead(request)
      }
    }
  }

  def handlePull(req: Request[AnyContent]): Future[Result] = {

    val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
    extractFieldsMask(req,typesCache(nbg), cmwellRDFHelper, nbg).flatMap { fieldsMask =>

      req.getQueryString("sub") match {
        case Some(sub) => {
          val p = Promise[Unit]()
          val source = Source.unfoldAsync[String, ByteString](sub) { subscription =>
            Subscriber.pull(subscription).flatMap(d => d.data match {
//              //IS FIRST CASE EVEN REACHABLE?!?!?!?
//              case _ if p.isCompleted => Future.successful(None)
              case v if v.isEmpty => cmwell.util.concurrent.SimpleScheduler.schedule[Option[(String, ByteString)]](3.seconds)(Some(subscription -> cmwell.ws.Streams.endln))
              case _ => {
                val formatter = formatterManager.getFormatter(
                  d.format,
                  req.host,
                  req.uri,
                  req.queryString.keySet("pretty"),
                  req.queryString.get("callback").flatMap(_.headOption),
                  nbg = nbg
                )

                val f = crudServiceFS.getInfotonsByPathOrUuid(uuids = d.data).map { bag =>
                  val data = ByteString(bag.infotons.map(i => formatter.render(i.masked(fieldsMask))).mkString("", "\n", "\n"), StandardCharsets.UTF_8)
                  Some(subscription -> data)
                }
                //                val f = Future(Some(subscription -> d.data.map(CRUDServiceFS.getInfotonByUuid).collect{
                //                  case oi : Option[Infoton] if oi.isDefined => oi.get.masked(fieldsMask)
                //                }.map(formatter.render).mkString("","\n","\n").getBytes("UTF-8")))
                f.recover {
                  case e => {
                    logger.error("future failed in handlePull", e)
                    Option.empty[(String, ByteString)]
                  }
                }
              }
            })
          }

          Future.successful(Ok.chunked(source))
        }
        case None =>
          Future.successful(BadRequest("missing sub param."))
      }
    }
  }

  /*
    for subscribe

    pull
    http://[host:port]/[path]?op=subscribe&method=pull&qp=?

    push
    http://[host:port]/[path]?op=subscribe&method=push&callback=http://[host]/&qp=?

op=< subscribe | unsubscribe >
format=< json | yaml | n3 | ttl | rdfxml | ... >
method=< push | pull >
bulk-size=< [#infotons] >
subscription-key=< [KEY] >
qp=< [query predicate] >
callback=< [URL] >

   */
  def handleSubscribe(request: Request[AnyContent]): Future[Result] = Try {
    val path = normalizePath(request.path)
    val name = cmwell.util.os.Props.machineName
    val sub = cmwell.util.string.Hash.crc32(s"${System.currentTimeMillis().toString}#$name")
    //    val bulkSize = request.getQueryString("bulk-size").flatMap(asInt).getOrElse(50)
    request.getQueryString("qp")
      .map(RTSQueryPredicate.parseRule(_, path))
      .getOrElse(Right(cmwell.rts.PathFilter(new cmwell.rts.Path(path, true)))) match {
      case Left(error) => Future.successful(BadRequest(s"bad syntax for qp: $error"))
      case Right(rule) => request.getQueryString("format").getOrElse("json") match {
        case FormatExtractor(format) => request.getQueryString("method") match {
          case Some(m) if m.equalsIgnoreCase("pull") => Subscriber.subscribe(sub, rule, Pull(format)).map(Ok(_))
          case Some(m) if m.equalsIgnoreCase("push") => request.getQueryString("callback") match {
            case Some(url) => Subscriber.subscribe(sub, rule, Push(getHandlerFor(format, url, tbg.get))).map(Ok(_))
            case None => Future.successful(BadRequest("missing callback for method push"))
          }
          case _ => Future.successful(BadRequest("unsupported or missing method for real time search "))
        }
        case _ => Future.successful(BadRequest(s"un-recognized type: ${request.headers("format")}"))
      }
    }
  }.recover(asyncErrorHandler).get

  def getHandlerFor(format: FormatType, url: String, nbg: Boolean): (Seq[String]) => Unit = {
    uuids => {

      uuids.foreach {
        uuid =>
          logger.info(s"Sending $uuid to $url.")
      }
      val infotonsFut = crudServiceFS.getInfotonsByPathOrUuid(uuids = uuids.toVector)
      //TODO: probably not the best host to provide a formatter. is there a way to get the original host the subscription was asked from?
      val formatter = formatterManager.getFormatter(format, s"http://${cmwell.util.os.Props.machineName}:9000",nbg = nbg)
      val futureRes = infotonsFut.flatMap { bag =>
        import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
        SimpleHttpClient.post[String](url,formatter.render(bag),Some(formatter.mimetype))
      }
      futureRes.onComplete {
        case Success(wsr) => if (wsr.status != 200)
          logger.warn(s"bad response: ${wsr.status}, ${wsr.payload}")
        case Failure(t) => logger.error(s"post to $url failed", t)
      }
    }
  }

  def handleUnsubscribe(request: Request[AnyContent]): Future[Result] = {
    request.getQueryString("sub") match {
      case Some(sub) =>
        Subscriber.unsubscribe(sub)
        Future.successful(Ok(s"unsubscribe $sub"))
      case None =>
        Future.successful(BadRequest("missing sub param. "))
    }
  }

  def handleMetaQuad(base64OfPartition: String) = Action.async { req =>
    val underscorePartition = cmwell.util.string.Base64.decodeBase64String(base64OfPartition,"UTF-8") // e.g: "_2"
    val partition = underscorePartition.tail.toInt
    val iOpt = Some(VirtualInfoton(ObjectInfoton(
      s"/meta/quad/Y213ZWxsOi8vbWV0YS9zeXMjcGFydGl0aW9u$base64OfPartition",
      Settings.dataCenter,
      None,
      Map[String,Set[FieldValue]]("alias" -> Set(FString(s"partition_$partition")), "graph" -> Set(FReference(s"cmwell://meta/sys#partition_$partition"))))))
    infotonOptionToReply(req, iOpt.map(VirtualInfoton.v2i))
  }

  def handleProcGET(path: String) = Action.async {
    implicit req => {
      val iPath: String = {
        val noTrailingSlashes: String = path.dropTrailingChars('/')
        if (noTrailingSlashes == "/proc" || noTrailingSlashes.isEmpty) "/proc" else "/proc/" + noTrailingSlashes
      }
      handleGetForActiveInfoton(req, iPath)
    }
  }

  private def handleGetForActiveInfoton(req: Request[AnyContent], path: String) = Try {
    // todo: fix this.. should check that the token is valid...
    val tokenOpt = authUtils.extractTokenFrom(req)
    val isRoot = authUtils.isValidatedAs(tokenOpt, "root")
    val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)

    val length = req.getQueryString("length").flatMap(asInt).getOrElse(if (req.getQueryString("format").isEmpty) 13 else 0) // default is 0 unless 1st request for the ajax app
    val offset = req.getQueryString("offset").flatMap(asInt).getOrElse(0)
    //    infotonOptionToReply(req,ActiveInfotonGenerator.generateInfoton(req.host, path, new DateTime(),length,offset))

    activeInfotonGenerator
      .generateInfoton(req.host, path, new DateTime(), length, offset, isRoot, nbg)
      .flatMap(iOpt => infotonOptionToReply(req, iOpt.map(VirtualInfoton.v2i)))
  }.recover(asyncErrorHandler).get

  def handleZzGET(key: String) = Action.async {
    implicit req => {
      val allowed = authUtils.isOperationAllowedForUser(security.Admin, authUtils.extractTokenFrom(req), evenForNonProdEnv = true)

      def mapToResp(task: Future[_]): Future[Result] = {
        val p = Promise[Result]
        task.onComplete {
          case Success(_) => p.success(Ok("""{"success":true}"""))
          case Failure(e) => p.success(Ok(s"""{"success":false,"message":"${Option(e.getMessage).getOrElse(e.getCause.getMessage)}"}"""))
        }
        p.future
      }

      req.getQueryString("op") match {
        case _ if !allowed => Future.successful(Forbidden("Not allowed to use zz"))
        case Some("purge") =>
          mapToResp(crudServiceFS.zStore.remove(key))
        case Some("list") =>
          (req.getQueryString("limit") match {
            case Some(limit) => crudServiceFS.zStore.ls(limit.toInt)
            case None => crudServiceFS.zStore.ls()
          }).map(lsRes => Ok(lsRes.mkString("\n")))
        case Some("put") =>
          val value = req.getQueryString("payload").getOrElse("").getBytes("UTF-8")
          mapToResp(req.getQueryString("ttl") match {
            case Some(ttl) => crudServiceFS.zStore.put(key, value, ttl.toInt, false)
            case None => crudServiceFS.zStore.put(key, value)
          })
        case None =>
          val p = Promise[Result]
          crudServiceFS.zStore.get(key).onComplete {
            case Success(payload) => p.success(Ok(
              if (req.getQueryString("format").contains("text")) new String(payload, "UTF-8")
              else payload.mkString(",")
            ))
            case Failure(e) => e match {
              case _: NoSuchElementException => p.success(NotFound("zz item not found"))
              case _ => p.success(Ok(s"""{"success":false,"message":"${Option(e.getMessage).getOrElse(e.getCause.getMessage)}"}"""))
            }
          }
          p.future
      }
    }
  }

  def handleUuidGET(uuid: String) = Action.async {
    implicit req => {
      def allowed(infoton: Infoton, level: PermissionLevel = PermissionLevel.Read) = authUtils.filterNotAllowedPaths(Seq(infoton.path), level, authUtils.extractTokenFrom(req)).isEmpty
      val isPurgeOp = req.getQueryString("op").contains("purge")
      def fields = req.getQueryString("fields").map(FieldNameConverter.toActualFieldNames)
      val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)

      if (!uuid.matches("^[a-f0-9]{32}$"))
        Future.successful(BadRequest("not a valid uuid format"))
      else {
        crudServiceFS.getInfotonByUuidAsync(uuid).flatMap {
          case FullBox(infoton) if isPurgeOp && allowed(infoton, PermissionLevel.Write) =>
            val formatter = getFormatter(req, formatterManager, "json", nbg)

            req.getQueryString("index") match {
              case Some(index) =>
                crudServiceFS.purgeUuidFromIndex(uuid,index,nbg).map(_ =>
                  Ok(formatter.render(SimpleResponse(success = true, Some(s"Note: $uuid was only purged from $index but not from CAS!")))).as(overrideMimetype(formatter.mimetype, req)._2)
                )
              case None =>
                crudServiceFS.getInfotons(Seq(infoton.path),nbg).flatMap { boi =>
                  if (infoton.uuid == boi.infotons.head.uuid) Future.successful(BadRequest("This specific version of CM-Well does not support this operation for the last version of the Infoton."))
                  else {
                    crudServiceFS.purgeUuid(infoton).map { _ =>
                      Ok(formatter.render(SimpleResponse(success = true, None))).as(overrideMimetype(formatter.mimetype, req)._2)
                    }
                  }
                }
            }
          case FullBox(infoton) if isPurgeOp => Future.successful(Forbidden("Not authorized"))

          case FullBox(infoton) if allowed(infoton) => extractFieldsMask(req,typesCache(nbg),cmwellRDFHelper, nbg).flatMap(fm => infotonOptionToReply(req, Some(infoton), fieldsMask = fm))
          case FullBox(infoton) => Future.successful(Forbidden("Not authorized"))

          case EmptyBox => infotonOptionToReply(req, None)
          case BoxedFailure(e) => asyncErrorHandler(e)
        }
      }
    }
  }

  def handleTrack(trackingId: String): Action[AnyContent] = Action.async { implicit request =>
    import cmwell.tracking.{InProgress, TrackingId, TrackingUtil}

    def getDataFromActor(trackingId: String) = trackingId match {
      case TrackingId(tid) => logger.debug(s"Tracking: Trying to get data from $tid"); TrackingUtil().readStatus(tid)
      case _ => Future.failed(new IllegalArgumentException(s"Invalid trackingID"))
    }

    val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
    val resultsFut = getDataFromActor(trackingId)
    val formatter = getFormatter(request, formatterManager, defaultFormat = "ntriples", nbg, withoutMeta = true)
    def errMsg(msg: String) = s"""{"success":false,"error":"$msg"}"""

    resultsFut.map { results =>
      val trackingHeaders = if(results.forall(_.status != InProgress)) Seq("X-CM-WELL-TRACKING" -> "Done") else Seq.empty[(String,String)]
      val response = BagOfInfotons(results map pathStatusAsInfoton)
      Ok(formatter.render(response)).withHeaders(trackingHeaders:_*).as(overrideMimetype(formatter.mimetype, request)._2)
    }.recover {
      // not using errorHandler in order to hide from user the exception message.
      case _: IllegalArgumentException => BadRequest(errMsg("Invalid trackingID"))
      case _: NoSuchElementException   => Gone(errMsg("This tracking ID was never created or its tracked request has been already completed"))
      case _: AskTimeoutException      => ServiceUnavailable(errMsg("Tracking is currently unavailable"))
      case _                           => InternalServerError(errMsg("An unexpected error has occurred"))
    }
  }

  def handleDELETE(path:String) = Action.async(parse.raw) { implicit originalRequest =>
    requestSlashValidator(originalRequest) match {
      case Failure(e) => asyncErrorHandler(e)
      case Success(request) => {
        val normalizedPath = normalizePath(request.path)
        if (!InfotonValidator.isInfotonNameValid(normalizedPath))
          Future.successful(BadRequest(Json.obj("success" -> false, "message" -> """you can't delete from "proc" / "ii" / or any path starting with "_" (service paths...)""")))
        //    else if(Settings.authSystemVersion==1 && !security.RSAAuthorizationService.authorize(normalizedPath,request.headers.get("X-CM-WELL-TOKEN")))
        //      Future.successful(Forbidden(Json.obj("success" -> false, "message" -> """authorization failed!""")))
        else {
          val nbg = originalRequest.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
          //deleting values based on json
          request.getQueryString("data") match {
            case Some(jsonStr) =>
              jsonToFields(jsonStr.getBytes("UTF-8")) match {
                case Success(fields) =>
                  crudServiceFS.deleteInfoton(normalizedPath, Some(fields)).map { _ => Ok(Json.obj("success" -> true)) }
                case Failure(exception) => asyncErrorHandler(exception)
              }
            case None => {
              val fields: Option[Map[String, Set[FieldValue]]] = request.getQueryString("field") match {
                case Some(field) =>
                  val value = request.getQueryString("value") match {
                    case Some(value) => FString(value)
                    case _ => FString("*")
                  }
                  Some(Map(field -> Set(value)))
                case _ => None
              }

              val p = Promise[Result]()
              for {
                either <- infotonPathDeletionAllowed(normalizedPath, request.getQueryString("recursive").getOrElse("true").toBoolean,crudServiceFS,nbg)
              } {
                (fields.isDefined, either) match {
                  case (true, _) => crudServiceFS.deleteInfoton(normalizedPath, fields)
                    .onComplete {
                      case Success(b) => p.success(Ok(Json.obj("success" -> b)))
                      case Failure(e) => p.success(InternalServerError(Json.obj("success" -> false, "message" -> e.getMessage)))
                    }
                  case (false, Right(paths)) => crudServiceFS.deleteInfotons(paths.map(_ -> None).toList)
                    .onComplete {
                      case Success(b) => p.success(Ok(Json.obj("success" -> b)))
                      case Failure(e) => p.success(InternalServerError(Json.obj("success" -> false, "message" -> e.getMessage)))
                    }
                  case (false, Left(msg)) => p.success(BadRequest(Json.obj("success" -> false, "message" -> msg)))
                }
              }
              p.future
            }
          }
        }
      }
    }
  }

  private def handleStartScroll(request: Request[AnyContent]): Future[Result] = request
    .getQueryString("qp")
    .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
    .map { qpOpt =>

      if (crudServiceFS.countSearchOpenContexts().map(_._2).sum > Settings.maxSearchContexts)
        Future.successful(BadRequest(Json.obj("success" -> false, "message" -> "Too many open iterators. wait and try later.")))
      else {
        val normalizedPath = normalizePath(request.path)
        val from = DateParser.parseDate(request.getQueryString("from").getOrElse(""), FromDate).toOption
        val to = DateParser.parseDate(request.getQueryString("to").getOrElse(""), ToDate).toOption
        val length = request.getQueryString("length").flatMap(asInt).getOrElse(10)
        val offset = request.getQueryString("offset").flatMap(asInt).getOrElse(0)
        val scrollTtl = request.getQueryString("session-ttl").flatMap(asInt).getOrElse(15).min(60)
        val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
        val withHistory = request.queryString.keySet("with-history")
        val withDeleted = request.queryString.keySet("with-deleted")
        val pathFilter = Some(PathFilter(normalizedPath, withDescendants))
        val withData = request.getQueryString("with-data")
        val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
        val fieldsFiltersFut = qpOpt.fold(Future.successful(Option.empty[FieldFilter]))(rff => RawFieldFilter.eval(rff,typesCache(nbg), cmwellRDFHelper,nbg).map(Some.apply))
        fieldsFiltersFut.flatMap { fieldFilter =>

          val formatter = request.getQueryString("format").getOrElse("json") match {
            case FormatExtractor(formatType) =>
              formatterManager.getFormatter(format = formatType,
                host = request.host,
                uri = request.uri,
                pretty = request.queryString.keySet("pretty"),
                callback = request.queryString.get("callback").flatMap(_.headOption),
                fieldFilters = fieldFilter,
                offset = Some(offset.toLong),
                length = Some(length.toLong),
                withData = withData,
                nbg = nbg)
          }

          val fmFut = extractFieldsMask(request,typesCache(nbg),cmwellRDFHelper, nbg)
          crudServiceFS.startScroll(
            pathFilter,
            fieldFilter,
            Some(DatesFilter(from, to)),
            PaginationParams(offset, length),
            scrollTtl,
            withHistory,
            withDeleted,
            debugInfo = request.queryString.keySet("debug-info"),nbg).flatMap { startScrollResult =>
            val rv = createScrollIdDispatcherActorFromIteratorId(startScrollResult.iteratorId, withHistory, (scrollTtl + 5).seconds)
            fmFut.map { fm =>
              Ok(formatter.render(startScrollResult.copy(iteratorId = rv).masked(fm))).as(formatter.mimetype)
            }
          }
        }.recover(errorHandler)
      }
    }.recover(asyncErrorHandler).get

  private def createScrollIdDispatcherActorFromIteratorId(id: String, withHistory: Boolean, ttl: FiniteDuration): String = {
    val ar = Grid.createAnon(classOf[IteratorIdDispatcher], id, withHistory, ttl)
    val rv = Key.encode(ar.path.toSerializationFormatWithAddress(Grid.me))
    logger.info(s"created actor with id = $rv")
    rv
  }

  //TODO: can improve runtime on the expense of generating another temporary collection, i.e:
  /*
   * val m = fromES.collect{case i if i.indexTime.isDefined => i.uuid -> i.indexTime.get}.toMap
   * fromCassandra.map{
   *   case i:<infoton type> => i.copy(indexTime = m.get(i.uuid))
   * }
   */
  private def addIndexTime(fromCassandra: Seq[Infoton], uuidToindexTime: Map[String,Long]): Seq[Infoton] = fromCassandra.map {
    case i:ObjectInfoton  if i.indexTime.isEmpty => i.copy(indexTime = uuidToindexTime.get(i.uuid))
    case i:FileInfoton    if i.indexTime.isEmpty => i.copy(indexTime = uuidToindexTime.get(i.uuid))
    case i:LinkInfoton    if i.indexTime.isEmpty => i.copy(indexTime = uuidToindexTime.get(i.uuid))
    case i:DeletedInfoton if i.indexTime.isEmpty => i.copy(indexTime = uuidToindexTime.get(i.uuid))
    case i => i
  }

  /**
   * answers with a stream, which is basically a chunked response,
   * where each chunk is a reply from a scroll session, and there
   * are multiple scroll sessions (1 per index in ES).
   *
   * @param request
   * @return
   */
  private def handleBoostedStream(request: Request[AnyContent]): Future[Result] = request
    .getQueryString("qp")
    .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
    .map { qpOpt =>

      if (crudServiceFS.countSearchOpenContexts().map(_._2).sum > Settings.maxSearchContexts)
        Future.successful(BadRequest(Json.obj("success" -> false, "message" -> "Too many open search contexts. wait and try later.")))
      else {
        val normalizedPath = normalizePath(request.path)
        val from = DateParser.parseDate(request.getQueryString("from").getOrElse(""), FromDate).toOption
        val to = DateParser.parseDate(request.getQueryString("to").getOrElse(""), ToDate).toOption
        val offset = 0 //request.getQueryString("offset").flatMap(asInt).getOrElse(0)
        val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
        val withHistory = request.queryString.keySet("with-history")
        val withDeleted = request.queryString.keySet("with-deleted")
        val withMeta = request.queryString.keySet("with-meta")
        val length = request.getQueryString("length").flatMap(asLong)
        val pathFilter = Some(PathFilter(normalizedPath, withDescendants))
        val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
        val fieldsMaskFut = extractFieldsMask(request,typesCache(nbg),cmwellRDFHelper, nbg)
        val (withData, format) = {
          val wd = request.getQueryString("with-data")
          val frmt = request.getQueryString("format").getOrElse({
            if (wd.isEmpty) "text" else "nt"
          })
          if (Set("nt", "ntriples", "nq", "nquads").exists(frmt.equalsIgnoreCase) || frmt.toLowerCase.startsWith("json")) {
            Some("text") -> frmt
          }
          else {
            None -> frmt
          }
        }
        format match {
          case f if !Set("text", "path", "tsv", "tab", "nt", "ntriples", "nq", "nquads")(f.toLowerCase) && !f.toLowerCase.startsWith("json") => Future.successful(BadRequest(Json.obj("success" -> false, "message" -> "not a streamable type (use 'text','tsv','ntriples', 'nquads', or any json)")))
          case FormatExtractor(formatType) => {
            val fieldsFiltersFut = qpOpt.fold(Future.successful(Option.empty[FieldFilter]))(rff => RawFieldFilter.eval(rff,typesCache(nbg),cmwellRDFHelper,nbg).map(Some.apply))
            fieldsFiltersFut.flatMap { fieldFilter =>
              fieldsMaskFut.flatMap { fieldsMask =>

                /* RDF types allowed in mstream are: ntriples, nquads, jsonld & jsonldq
                 * since, the jsons are not realy RDF, just flattened json of infoton per line,
                 * there is no need to tnforce subject uniquness. but ntriples, and nquads
                 * which split infoton into statements (subject-predicate-object triples) per line,
                 * we don't want different versions to "mix" and we enforce uniquness only in this case
                 */
                val forceUniqueness: Boolean = withHistory && (formatType match {
                  case RdfType(NquadsFlavor) => true
                  case RdfType(NTriplesFlavor) => true
                  case _ => false
                })
                val formatter = formatterManager.getFormatter(format = formatType,
                  host = request.host,
                  uri = request.uri,
                  pretty = false,
                  callback = request.queryString.get("callback").flatMap(_.headOption),
                  fieldFilters = fieldFilter,
                  offset = Some(offset.toLong),
                  length = Some(500L),
                  withData = withData,
                  withoutMeta = !withMeta,
                  filterOutBlanks = true,
                  forceUniqueness = forceUniqueness,
                  nbg = nbg) //cleanSystemBlanks set to true, so we won't output all the meta information we usaly output. it get's messy with streaming. we don't want each chunk to show the "document context"

                val datesFilter = {
                  if (from.isEmpty && to.isEmpty) None
                  else Some(DatesFilter(from, to))
                }
                streams.multiScrollSource(
                  pathFilter = pathFilter,
                  fieldFilter = fieldFilter,
                  datesFilter = datesFilter,
                  withHistory = withHistory,
                  withDeleted = withDeleted,
                  nbg = nbg).map {
                  case (source, hits) => {
                    val s = streams.scrollSourceToByteString(source, formatter, withData.isDefined, withHistory, length, fieldsMask, nbg)
                    Ok.chunked(s).as(overrideMimetype(formatter.mimetype, request)._2).withHeaders("X-CM-WELL-N" -> hits.toString)
                  }
                }.recover(errorHandler)
              }.recover(errorHandler)
            }.recover(errorHandler)
          }
        }
      }
    }.recover(asyncErrorHandler).get

  private def handleSuperStream(request: Request[AnyContent]): Future[Result] = request
    .getQueryString("qp")
    .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
    .map { qpOpt =>

      if (crudServiceFS.countSearchOpenContexts().map(_._2).sum > Settings.maxSearchContexts)
        Future.successful(BadRequest(Json.obj("success" -> false, "message" -> "Too many open search contexts. wait and try later.")))
      else {
        val normalizedPath = normalizePath(request.path)
        val from = DateParser.parseDate(request.getQueryString("from").getOrElse(""), FromDate).toOption
        val to = DateParser.parseDate(request.getQueryString("to").getOrElse(""), ToDate).toOption
        val offset = 0
        //request.getQueryString("offset").flatMap(asInt).getOrElse(0)
        val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
        val withHistory = request.queryString.keySet("with-history")
        val withDeleted = request.queryString.keySet("with-deleted")
        val withMeta = request.queryString.keySet("with-meta")
        val length = request.getQueryString("length").flatMap(asLong)
        val pathFilter = Some(PathFilter(normalizedPath, withDescendants))
        val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
        val fieldsMaskFut = extractFieldsMask(request,typesCache(nbg),cmwellRDFHelper, nbg)
        val (withData, format) = {
          val wd = request.getQueryString("with-data")
          val frmt = request.getQueryString("format").getOrElse({
            if (wd.isEmpty) "text" else "nt"
          })
          if (Set("nt", "ntriples", "nq", "nquads").exists(frmt.equalsIgnoreCase) || frmt.toLowerCase.startsWith("json")) {
            Some("text") -> frmt
          }
          else {
            None -> frmt
          }
        }
        format match {
          case f if !Set("text", "path", "tsv", "tab", "nt", "ntriples", "nq", "nquads")(f.toLowerCase) && !f.toLowerCase.startsWith("json") => Future.successful(BadRequest(Json.obj("success" -> false, "message" -> "not a streamable type (use 'text','tsv','ntriples', 'nquads', or any json)")))
          case FormatExtractor(formatType) => {
            val fieldsFiltersFut = qpOpt.fold(Future.successful(Option.empty[FieldFilter]))(rff => RawFieldFilter.eval(rff,typesCache(nbg),cmwellRDFHelper,nbg).map(Some.apply))
            fieldsFiltersFut.flatMap { fieldFilter =>
              fieldsMaskFut.flatMap { fieldsMask =>
                /* RDF types allowed in mstream are: ntriples, nquads, jsonld & jsonldq
                 * since, the jsons are not realy RDF, just flattened json of infoton per line,
                 * there is no need to tnforce subject uniquness. but ntriples, and nquads
                 * which split infoton into statements (subject-predicate-object triples) per line,
                 * we don't want different versions to "mix" and we enforce uniquness only in this case
                 */
                val forceUniqueness: Boolean = withHistory && (formatType match {
                  case RdfType(NquadsFlavor) => true
                  case RdfType(NTriplesFlavor) => true
                  case _ => false
                })
                val formatter = formatterManager.getFormatter(format = formatType,
                  host = request.host,
                  uri = request.uri,
                  pretty = false,
                  callback = request.queryString.get("callback").flatMap(_.headOption),
                  fieldFilters = fieldFilter,
                  offset = Some(offset.toLong),
                  length = Some(500L),
                  withData = withData,
                  withoutMeta = !withMeta,
                  filterOutBlanks = true,
                  forceUniqueness = forceUniqueness,
                  nbg = nbg) //cleanSystemBlanks set to true, so we won't output all the meta information we usaly output. it get's messy with streaming. we don't want each chunk to show the "document context"

                streams.superScrollSource(nbg,
                  pathFilter = pathFilter,
                  fieldFilter = fieldFilter,
                  datesFilter = Some(DatesFilter(from, to)),
                  paginationParams = PaginationParams(offset, 500),
                  withHistory = withHistory,
                  withDeleted = withDeleted).map { case (src, hits) =>

                  val s = streams.scrollSourceToByteString(src, formatter, withData.isDefined, withHistory, length, fieldsMask,nbg)
                  Ok.chunked(s).as(overrideMimetype(formatter.mimetype, request)._2).withHeaders("X-CM-WELL-N" -> hits.toString)
                }
              }
            }
          }
        }
      }
    }.recover(asyncErrorHandler).get

  private def handleStream(request: Request[AnyContent]): Future[Result] = request
    .getQueryString("qp")
    .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
    .map { qpOpt =>

      if (crudServiceFS.countSearchOpenContexts().map(_._2).sum > Settings.maxSearchContexts)
        Future.successful(BadRequest(Json.obj("success" -> false, "message" -> "Too many open search contexts. wait and try later.")))
      else {
        val normalizedPath = normalizePath(request.path)
        val from = DateParser.parseDate(request.getQueryString("from").getOrElse(""), FromDate).toOption
        val to = DateParser.parseDate(request.getQueryString("to").getOrElse(""), ToDate).toOption
        val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
        val withHistory = request.queryString.keySet("with-history")
        val withDeleted = request.queryString.keySet("with-deleted")
        val withMeta = request.queryString.keySet("with-meta")
        val length = request.getQueryString("length").flatMap(asLong)
        val pathFilter = Some(PathFilter(normalizedPath, withDescendants))
        val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
        val fieldsMaskFut = extractFieldsMask(request,typesCache(nbg),cmwellRDFHelper, nbg)
        val (withData, format) = {
          val wd = request.getQueryString("with-data")
          val frmt = request.getQueryString("format").getOrElse({
            if (wd.isEmpty) "text" else "nt"
          })
          if (Set("nt", "ntriples", "nq", "nquads").exists(frmt.equalsIgnoreCase) || frmt.toLowerCase.startsWith("json")) {
            Some("text") -> frmt
          }
          else {
            None -> frmt
          }
        }
        format match {
          case f if !Set("text", "path", "tsv", "tab", "nt", "ntriples", "nq", "nquads")(f.toLowerCase) && !f.toLowerCase.startsWith("json") => Future.successful(BadRequest(Json.obj("success" -> false, "message" -> "not a streamable type (use any json, or one of: 'text','tsv','ntriples', or 'nquads')")))
          case FormatExtractor(formatType) => {
            val fieldsFiltersFut = qpOpt.fold(Future.successful(Option.empty[FieldFilter]))(rff => RawFieldFilter.eval(rff,typesCache(nbg),cmwellRDFHelper,nbg).map(Some.apply))
            fieldsFiltersFut.flatMap { fieldFilter =>
              fieldsMaskFut.flatMap { fieldsMask =>
                /* RDF types allowed in stream are: ntriples, nquads, jsonld & jsonldq
                 * since, the jsons are not realy RDF, just flattened json of infoton per line,
                 * there is no need to tnforce subject uniquness. but ntriples, and nquads
                 * which split infoton into statements (subject-predicate-object triples) per line,
                 * we don't want different versions to "mix" and we enforce uniquness only in this case
                 */
                val forceUniqueness: Boolean = withHistory && (formatType match {
                  case RdfType(NquadsFlavor) => true
                  case RdfType(NTriplesFlavor) => true
                  case _ => false
                })
                val formatter = formatterManager.getFormatter(format = formatType,
                  host = request.host,
                  uri = request.uri,
                  pretty = false,
                  callback = request.queryString.get("callback").flatMap(_.headOption),
                  fieldFilters = fieldFilter,
                  withData = withData,
                  withoutMeta = !withMeta,
                  filterOutBlanks = true,
                  forceUniqueness = forceUniqueness,
                  nbg = nbg)


                streams.scrollSource(nbg,
                  pathFilter = pathFilter,
                  fieldFilters = fieldFilter,
                  datesFilter = Some(DatesFilter(from, to)),
                  paginationParams = PaginationParams(0, 500),
                  withHistory = withHistory,
                  withDeleted = withDeleted).map { case (src, hits) =>

                  val s = streams.scrollSourceToByteString(src, formatter, withData.isDefined, withHistory, length, fieldsMask,nbg)
                  Ok.chunked(s).as(overrideMimetype(formatter.mimetype, request)._2).withHeaders("X-CM-WELL-N" -> hits.toString)
                }
              }
            }
          }
        }
      }.recover(errorHandler)
    }.recover(asyncErrorHandler).get

  def generateSortedConsumeFieldFilters(qpOpt: Option[String],
                                        path: String,
                                        withDescendants: Boolean,
                                        withHistory: Boolean,
                                        withDeleted: Boolean,
                                        indexTime: Long,
                                        nbg: Boolean): Future[SortedConsumeState] = {
    val pOpt = {
      if (path == "/" && withDescendants) None
      else Some(path)
    }

    qpOpt.fold[Future[Option[FieldFilter]]](Future.successful(None)) { qp =>
      FieldFilterParser.parseQueryParams(qp) match {
        case Failure(err) => Future.failed(err)
        case Success(rff) => RawFieldFilter.eval(rff,typesCache(nbg),cmwellRDFHelper,nbg).map(Some.apply)
      }
    }.map { ffOpt =>
      SortedConsumeState(indexTime, pOpt, withHistory, withDeleted, withDescendants, ffOpt)
    }
  }

  private def transformFieldFiltersForConsumption(fieldFilters: Option[FieldFilter], timeStamp: Long): FieldFilter =
    fieldFilters match {
      case None => MultiFieldFilter(Must, List(
        FieldFilter(Must, GreaterThan, "system.indexTime", timeStamp.toString),
        FieldFilter(Must, LessThan, "system.indexTime", (System.currentTimeMillis() - 10000).toString)))
      case Some(ff) => MultiFieldFilter(Must, List(ff,
        FieldFilter(Must, GreaterThan, "system.indexTime", timeStamp.toString),
        FieldFilter(Must, LessThan, "system.indexTime", (System.currentTimeMillis() - 10000).toString)))
    }

  private def handleQueueStream(request: Request[AnyContent]): Future[Result] = {

    val indexTime: Long = request.getQueryString("index-time").flatMap(asLong).getOrElse(0L)
    val withMeta = request.queryString.keySet("with-meta")
    //TODO: length should determine the overall length (cutoff iteratee)
    //val length = request.getQueryString("length").flatMap(asLong)
    val lengthHint = request.getQueryString("length-hint").flatMap(asInt).getOrElse(100)
    val normalizedPath = normalizePath(request.path)
    val qpOpt = request.getQueryString("qp")
    val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
    //deprecated!
//    val from = request.getQueryString("from")
//    val to = request.getQueryString("to")
    val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
    val withHistory = request.queryString.keySet("with-history")
    val withDeleted = request.queryString.keySet("with-deleted")
    val (withData, format) = {
      val wd = request.getQueryString("with-data")
      val frmt = request.getQueryString("format").getOrElse({
        if (wd.isEmpty) "text" else "nt"
      })
      if (Set("nt", "ntriples", "nq", "nquads").exists(frmt.equalsIgnoreCase) || frmt.toLowerCase.startsWith("json"))
        Some("text") -> frmt
      else None -> frmt
    }

    format match {
      case f if !Set("text", "path", "tsv", "tab", "nt", "ntriples", "nq", "nquads")(f.toLowerCase) && !f.toLowerCase.startsWith("json") => Future.successful(BadRequest(Json.obj("success" -> false, "message" -> "not a streamable type (use any json, or one of: 'text','tsv','ntriples', or 'nquads')")))
      case FormatExtractor(formatType) => {
        /* RDF types allowed in stream are: ntriples, nquads, jsonld & jsonldq
         * since, the jsons are not realy RDF, just flattened json of infoton per line,
         * there is no need to enforce subject uniquness. but ntriples, and nquads
         * which split infoton into statements (subject-predicate-object triples) per line,
         * we don't want different versions to "mix" and we enforce uniquness only in this case
         */
        val forceUniqueness: Boolean = withHistory && (formatType match {
          case RdfType(NquadsFlavor) => true
          case RdfType(NTriplesFlavor) => true
          case _ => false
        })

        generateSortedConsumeFieldFilters(
          qpOpt = qpOpt,
          path = normalizedPath,
          withDescendants = withDescendants,
          withHistory = withHistory,
          withDeleted = withDeleted,
          indexTime = indexTime,
          nbg = nbg
        ).map {
          case SortedConsumeState(firstTimeStamp, path, history, deleted, descendants, fieldFilters) => {

            val formatter = formatterManager.getFormatter(
              format = formatType,
              host = request.host,
              uri = request.uri,
              pretty = false,
              fieldFilters = fieldFilters,
              withData = withData,
              withoutMeta = !withMeta,
              filterOutBlanks = true,
              forceUniqueness = forceUniqueness,
              nbg = nbg)

            import cmwell.ws.Streams._

            val src = streams.qStream(firstTimeStamp,path,history,deleted,descendants,lengthHint,fieldFilters,nbg)

//            if (!withData) source.flatMapConcat(identity)
//            //we already retry internally in IRW, but apparently, sometimes it's not enough: http://gitlab:8082/cm-well/cm-well/issues/136
//            else source.flatMapConcat(_.mapAsync(getAvailableProcessors)(thinfoton => retry(8,50.millis,2)(CRUDServiceFS.getInfotonByUuidAsync(thinfoton.uuid).map {
//              case FullBox(i) => i.indexTime.fold(cmwell.domain.addIndexTime(i, Some(thinfoton.indexTime)))(_ => i)
//              case EmptyBox => throw new NoSuchElementException(s"could not retrieve uuid [${thinfoton.uuid}] from cassandra.")
//              case BoxedFailure(e) => throw new Exception(s"could not retrieve uuid [${thinfoton.uuid}] from cassandra.",e)
//            })))

            val ss: Source[ByteString,NotUsed] = {
              if (withData.isEmpty) src.via(Flows.searchThinResultToByteString(formatter))
              else src.via(Flows.searchThinResultToFatInfoton(nbg,crudServiceFS)).via(Flows.infotonToByteString(formatter))
            }

            val contentType = {
              if (formatType.mimetype.startsWith("application/json"))
                overrideMimetype("application/json-seq;charset=UTF8", request)._2
              else
                overrideMimetype(formatType.mimetype, request)._2
            }

            Ok.chunked(ss).as(contentType) //TODO: `.withHeaders("X-CM-WELL-N" -> total.toString)`
          }
        }.recover(errorHandler)
      }
    }
  }

  def getQueryString(k: String)(implicit m: Map[String,Seq[String]]): Option[String] = m.get(k).flatMap(_.headOption)

  case class ExtractURLFormEnc(path: String) extends Action[Either[CMWellRequest,RawBuffer]] {

    def apply(request: Request[Either[CMWellRequest,RawBuffer]]): Future[Result] = Try {
      request.body match {
        case Right(b) => handlePutInfoton(path)(request.map(_ => b))
        case Left(CreateConsumer(path,qp)) => handleCreateConsumer(path)(qp)
        case Left(Search(path,base,host,uri,qp)) => handleSearch(path,base,host,uri)(qp)
      }
    }.recover {
      case e: Throwable => Future.successful(InternalServerError(e.getMessage + "\n" + cmwell.util.exceptions.stackTraceToString(e)))
    }.get

    lazy val parser = parse.using(rh => rh.mediaType match {
      case Some(MediaType("application", "x-www-form-urlencoded", _)) if rh.getQueryString("op").contains("create-consumer") => parse.urlFormEncoded.map(m => Left(CreateConsumer(rh,m)))
      case Some(MediaType("application", "x-www-form-urlencoded", _)) if rh.getQueryString("op").contains("search") => parse.urlFormEncoded.map(m => Left(Search(rh,m)))
      case _ => parse.raw.map(Right.apply)
    })
  }

  def handlePost(path: String) = ExtractURLFormEnc(path)

  private def handleCreateConsumerRequest(request: Request[AnyContent]): Future[Result] =
    handleCreateConsumer(request.path)(request.queryString)

  private def handleCreateConsumer(path: String)(createConsumerParams: Map[String,Seq[String]]): Future[Result] = Try {
    val indexTime = createConsumerParams.get("index-time").flatMap(_.headOption.flatMap(asLong))
    val normalizedPath = normalizePath(path)
    val qpOpt = createConsumerParams.get("qp").flatMap(_.headOption)
    val withDescendants = createConsumerParams.contains("with-descendants") || createConsumerParams.contains("recursive")
    val withHistory = createConsumerParams.contains("with-history")
    val withDeleted = createConsumerParams.contains("with-deleted")
    val lengthHint = createConsumerParams.get("length-hint").flatMap(_.headOption.flatMap(asLong))
    val nbg = createConsumerParams.get("nbg").flatMap(_.headOption.flatMap(asBoolean)).getOrElse(tbg.get)
    val consumeStateFut: Future[ConsumeState] = {
      val f = generateSortedConsumeFieldFilters(qpOpt, normalizedPath, withDescendants, withHistory, withDeleted, indexTime.getOrElse(0L),nbg)
      lengthHint.fold[Future[ConsumeState]](f)(lh => f.map(_.asBulk(lh)))
    }
    consumeStateFut.map { scs =>
      val id = ConsumeState.encode(scs)
      Ok("").withHeaders("X-CM-WELL-POSITION" -> id)
    }.recover(errorHandler)
  }.recover(asyncErrorHandler).get

  // This method is to enable to access handleConsume from routes.
  def handleConsumeRoute = Action.async { implicit request =>
    if (request.queryString.isEmpty) Future.successful(Ok(views.txt._consume(request)))
    else handleConsume(request)
  }
  // This method is to enable to access BulkScrollHandler.handle from routes.
  def handleBulkConsumeRoute = Action.async { implicit request =>
    if (request.queryString.isEmpty) Future.successful(Ok(views.txt._bulkConsume(request)))
    else bulkScrollHandler.handle(request)
  }

  private[controllers] def handleConsume(request: Request[AnyContent]): Future[Result] = Try {
    val sortedIteratorID = request.getQueryString("position").getOrElse(throw new IllegalArgumentException("`position` parameter is required"))

    //properties that needs to be re-sent every iteration
    val xg = request.getQueryString("xg")
    val (yg,ygChunkSize) = request.getQueryString("yg").fold(Option.empty[String] -> 0)(Some(_) -> request.getQueryString("yg-chunk-size").flatMap(asInt).getOrElse(10))
    val (requestedFormat,withData) = {
      val (f,b) = extractInferredFormatWithData(request,"json")
      f -> (b || yg.isDefined || xg.isDefined) //infer `with-data` implicitly, and don't fail the request
    }

    def wasSupplied(queryParamKey: String) = request.queryString.keySet(queryParamKey)

    if (wasSupplied("qp"))
      Future.successful(BadRequest("you can't specify `qp` together with `position` (`qp` is meant to be used only in the first iteration request. after that, continue iterating using the received `position`)"))
    else if (wasSupplied("from") || wasSupplied("to"))
      Future.successful(BadRequest("`from`/`to` is determined in the beginning of the iteration. can't specify together with `position`"))
    else if (wasSupplied("indexTime"))
      Future.successful(BadRequest("`indexTime` is determined in the beginning of the iteration. can't specify together with `position`"))
    else if (wasSupplied("with-descendants") || wasSupplied("recursive"))
      Future.successful(BadRequest("`with-descendants`/`recursive` is determined in the beginning of the iteration. can't specify together with `position`"))
    else if (wasSupplied("with-history"))
      Future.successful(BadRequest("`with-history` is determined in the beginning of the iteration. can't specify together with `position`"))
    else if (wasSupplied("with-deleted"))
      Future.successful(BadRequest("`with-deleted` is determined in the beginning of the iteration. can't specify together with `position`"))
    else {

      val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
      val sortedIteratorStateTry = ConsumeState.decode[SortedConsumeState](sortedIteratorID)
      val lengthHint = request
        .getQueryString("length-hint")
        .flatMap(asInt)
        .orElse(ConsumeState.decode[BulkConsumeState](sortedIteratorID).toOption.flatMap {
          case b if b.threshold <= Settings.maxLength => Some(b.threshold.toInt)
          case _ => None
        })
        .getOrElse(100)

      val debugInfo = request.queryString.keySet("debug-info")

      sortedIteratorStateTry.map {
        case sortedConsumeState@SortedConsumeState(timeStamp, path, history, deleted, descendants, fieldFilters) => {

          val pf = path.map(PathFilter(_, descendants))
          val ffs = transformFieldFiltersForConsumption(fieldFilters, timeStamp)
          val pp  = PaginationParams(0, lengthHint)
          val fsp = FieldSortParams(List("system.indexTime" -> Asc))

          val future = crudServiceFS.thinSearch(
            pathFilter       = pf,
            fieldFilters     = Some(ffs),
            datesFilter      = None,
            paginationParams = pp,
            withHistory      = history,
            withDeleted      = deleted,
            fieldSortParams  = fsp,
            debugInfo        = debugInfo,
            nbg              = nbg)

          val (contentType, formatter) = requestedFormat match {
            case FormatExtractor(formatType) =>
              val f = formatterManager.getFormatter(
                format = formatType,
                host = request.host,
                uri = request.uri,
                pretty = request.queryString.keySet("pretty"),
                callback = request.queryString.get("callback").flatMap(_.headOption),
                fieldFilters = fieldFilters,
                nbg = nbg)

              val m =
                if (formatType.mimetype.startsWith("application/json"))
                  overrideMimetype("application/json-seq;charset=UTF8", request)._2
                else
                  overrideMimetype(formatType.mimetype, request)._2

              (m, f)
          }

          future.flatMap {
            case sr: SearchThinResults if sr.thinResults.isEmpty => {
              if(debugInfo) {
                logger.info(s"""will emit 204 for search params:
                              |pathFilter       = $pf,
                              |fieldFilters     = $ffs,
                              |paginationParams = $pp,
                              |withHistory      = $history,
                              |fieldSortParams  = $fsp""".stripMargin)
              }
              val result = new Status(204).as(contentType).withHeaders("X-CM-WELL-POSITION" -> sortedIteratorID, "X-CM-WELL-N-LEFT" -> "0")
              Future.successful(result)
            }
            case SearchThinResults(total, _, _, results, _) if results.nonEmpty => {

              val idxT = results.maxBy(_.indexTime).indexTime //infotons.maxBy(_.indexTime.getOrElse(0L)).indexTime.getOrElse(0L)
              require(idxT > 0, "all infotons in iteration must have a valid indexTime defined")

              // last chunk
              if (results.length >= total)
                expandSearchResultsForSortedIteration(results, sortedConsumeState.copy(from = idxT), total, formatter, contentType, xg, yg, ygChunkSize, nbg)
              //regular chunk with more than 1 indexTime
              else if (results.exists(_.indexTime != idxT)) {
                val newResults = results.filter(_.indexTime < idxT)
                val id = sortedConsumeState.copy(from = idxT - 1)
                //expand the infotons with yg/xg, but only after filtering out the infotons with the max indexTime
                expandSearchResultsForSortedIteration(newResults, id, total, formatter, contentType, xg, yg, ygChunkSize, nbg)
              }
              //all the infotons in current chunk have the same indexTime
              else {

                val ffs2 = {
                  val eqff = FieldFilter(Must, Equals, "system.indexTime", idxT.toString)
                  fieldFilters.fold[FieldFilter](eqff) { ff =>
                    MultiFieldFilter(Must, List(ff,eqff))
                  }
                }

                val scrollFuture = streams.scrollSource(
                  pathFilter = pf,
                  fieldFilters = Some(ffs2),
                  withHistory = history,
                  withDeleted = deleted,
                  nbg = nbg)

                scrollFuture.flatMap {
                  //if by pure luck, the chunk length is exactly equal to the number of infotons in cm-well containing this same indexTime
                  case (_,hits) if hits <= results.size =>
                    expandSearchResultsForSortedIteration(results, sortedConsumeState.copy(from = idxT), total, formatter, contentType, xg, yg, ygChunkSize, nbg)
                  //if we were asked to expand chunk, but need to respond with a chunked response (workaround: try increasing length or search directly with adding `system.indexTime::${idxT}`)
                  case _ if xg.isDefined || yg.isDefined =>
                    Future.successful(UnprocessableEntity(s"encountered a large chunk which cannot be expanded using xg/yg. (indexTime=$idxT)"))
                  //chunked response
                  case (iterationResultsEnum,hits) => {
                    logger.info(s"sorted iteration encountered a large chunk [indexTime = $idxT]")

                    val id = ConsumeState.encode(sortedConsumeState.copy(from = idxT))
                    val src = streams.scrollSourceToByteString(iterationResultsEnum,formatter,withData,history,None,Set.empty,nbg) // TODO: get fieldsMask instead of Set.empty

                    val result = Ok.chunked(src).as(contentType).withHeaders("X-CM-WELL-POSITION" -> id, "X-CM-WELL-N-LEFT" -> (total - hits).toString)
                    Future.successful(result)
                  }
                }
              }
            }
          }
        }
      }.recover(asyncErrorHandler).get
    }
  }.recover(asyncErrorHandler).get



  def expandSearchResultsForSortedIteration(newResults: Seq[SearchThinResult],
                                            sortedIteratorState: SortedConsumeState,
                                            total: Long,
                                            formatter: Formatter,
                                            contentType: String,
                                            xg: Option[String],
                                            yg: Option[String],
                                            ygChunkSize: Int,
                                            nbg: Boolean): Future[Result] = {

    val id = ConsumeState.encode(sortedIteratorState)

    if (formatter.format.isThin) {
      require(xg.isEmpty && yg.isEmpty, "Thin formats does not carry data, and thus cannot be expanded! (xg/yg supplied together with a thin format)")
      val body = FormatterManager.formatFormattableSeq(newResults, formatter)
      Future.successful(Ok(body)
        .as(contentType)
        .withHeaders("X-CM-WELL-POSITION" -> id, "X-CM-WELL-N-LEFT" -> (total - newResults.length).toString))
    }
    else {

      Future.traverse(newResults)(str => crudServiceFS.getInfotonByUuidAsync(str.uuid).map(_ -> str.uuid)).flatMap { newInfotonsBoxes =>

        val newInfotons = newInfotonsBoxes.collect { case (FullBox(i), _) => i }

        if (newInfotons.length != newInfotonsBoxes.length) {
          val (fails,nones) = cmwell.util.collections.partitionWith(newInfotonsBoxes.filter(_._1.isEmpty)){
            case (BoxedFailure(e),u) => Left(e -> u)
            case (EmptyBox,u) => Right(u)
            case _ => !!!
          }
          if(nones.nonEmpty) logger.error("some uuids could not be retrieved: " + nones.mkString("[", ",", "]"))
          fails.foreach {
            case (e,u) => logger.error(s"uuid [$u] failed",e)
          }
        }

        //TODO: xg/yg handling should be factor out (DRY principle)
        val ygModified = yg match {
          case Some(ygp) if newInfotons.nonEmpty => {
            pathExpansionParser(ygp, newInfotons, ygChunkSize, cmwellRDFHelper, typesCache(nbg), nbg).map {
              case (ok, infotonsAfterYg) => ok -> infotonsAfterYg
            }
          }
          case _ => Future.successful(true -> newInfotons)
        }

        ygModified.flatMap {
          case (false, infotonsAfterYg) => {
            val body = FormatterManager.formatFormattableSeq(infotonsAfterYg, formatter)
            val result = InsufficientStorage(body)
              .as(contentType)
              .withHeaders("X-CM-WELL-POSITION" -> id, "X-CM-WELL-N-LEFT" -> (total - newInfotons.length).toString)
            Future.successful(result)
          }
          case (true, infotonsAfterYg) if infotonsAfterYg.isEmpty || xg.isEmpty => {
            val body = FormatterManager.formatFormattableSeq(infotonsAfterYg, formatter)
            val result = {
              if (newInfotonsBoxes.exists(_._1.isEmpty)) PartialContent(body)
              else Ok(body)
            }

            Future.successful(result
              .as(contentType)
              .withHeaders("X-CM-WELL-POSITION" -> id, "X-CM-WELL-N-LEFT" -> (total - newInfotonsBoxes.length).toString))
          }
          case (true, infotonsAfterYg) => {
            deepExpandGraph(xg.get, infotonsAfterYg,cmwellRDFHelper,typesCache(nbg),nbg).map {
              case (_, infotonsAfterXg) =>
                val body = FormatterManager.formatFormattableSeq(infotonsAfterXg, formatter)
                val result = {
                  if (newInfotonsBoxes.exists(_._1.isEmpty)) PartialContent(body)
                  else Ok(body)
                }

                result
                  .as(contentType)
                  .withHeaders("X-CM-WELL-POSITION" -> id, "X-CM-WELL-N-LEFT" -> (total - newInfotonsBoxes.length).toString)
            }
          }
        }
      }
    }
  }

  // This method is to enable to access handleScroll from routes.
  def handleScrollRoute = Action.async { implicit request =>
    handleScroll(request)
  }

  /**
   * WARNING: using xg with iterator, is at the user own risk!
   * results may be cut off if expansion limit is exceeded,
   * but no warning can be emmited, since we use a chunked response,
   * and it is impossible to change status code or headers.
   *
   * @param request
   * @return
   */
  private def handleScroll(request: Request[AnyContent]): Future[Result] = Try {

    import akka.pattern.ask

    request.getQueryString("iterator-id").fold(Future.successful(BadRequest("iterator-id query param is mandatory for this operation"))){ encodedActorAddress =>
      val xg = request.getQueryString("xg")
      val (yg,ygChunkSize) = request.getQueryString("yg").fold(Option.empty[String] -> 0)(Some(_) -> request.getQueryString("yg-chunk-size").flatMap(asInt).getOrElse(10))
      val scrollTtl = request.getQueryString("session-ttl").flatMap(asInt).getOrElse(15).min(60)
      val withDataFormat = request.getQueryString("with-data")
      val withData =
        (withDataFormat.isDefined && withDataFormat.get.toLowerCase != "false") ||
          (withDataFormat.isEmpty && (yg.isDefined || xg.isDefined)) //infer `with-data` implicitly, and don't fail the request
      val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)

      val fieldsMaskFut = extractFieldsMask(request,typesCache(nbg),cmwellRDFHelper, nbg)

      if (!withData && xg.isDefined) Future.successful(BadRequest("you can't use `xg` without also specifying `with-data`!"))
      else {
        val deadLine = Deadline(Duration(System.currentTimeMillis + 7000, MILLISECONDS))
        val itStateEitherFuture: Future[Either[String, IterationState]] = {
          val as = Grid.selectByPath(Key.decode(encodedActorAddress))
          Grid.getRefFromSelection(as, 10, 1.second).flatMap { ar =>
            (ar ? GetID)(akka.util.Timeout(10.seconds)).mapTo[IterationState].map {
              case IterationState(id, wh) if wh && (yg.isDefined || xg.isDefined) => {
                Left("iterator is defined to contain history. you can't use `xg` or `yg` operations on histories.")
              }
              case msg@IterationState(id, wh) => {
                as ! GotIt
                Right(msg)

              }
            }
          }
        }
        request.getQueryString("format").getOrElse("atom") match {
          case FormatExtractor(formatType) => itStateEitherFuture.flatMap {
            case Left(errMsg) => Future.successful(BadRequest(errMsg))
            case Right(IterationState(scrollId, withHistory)) => {
              val formatter = formatterManager.getFormatter(
                format = formatType,
                host = request.host,
                uri = request.uri,
                pretty = request.queryString.keySet("pretty"),
                callback = request.queryString.get("callback").flatMap(_.headOption),
                withData = withDataFormat,
                forceUniqueness = withHistory,
                nbg = nbg)

              val futureThatMayHang: Future[String] = crudServiceFS.scroll(scrollId, scrollTtl + 5, withData).flatMap { tmpIterationResults =>
                fieldsMaskFut.flatMap { fieldsMask =>
                  val rv = createScrollIdDispatcherActorFromIteratorId(tmpIterationResults.iteratorId, withHistory, scrollTtl.seconds)
                  val iterationResults = tmpIterationResults.copy(iteratorId = rv).masked(fieldsMask)

                  val ygModified = yg match {
                    case Some(ygp) if iterationResults.infotons.isDefined => {
                      pathExpansionParser(ygp, iterationResults.infotons.get, ygChunkSize,cmwellRDFHelper,typesCache(nbg),nbg).map {
                        case (ok, infotons) => ok -> iterationResults.copy(infotons = Some(infotons))
                      }
                    }
                    case _ => Future.successful(true -> iterationResults)
                  }

                  ygModified.flatMap {
                    case (ok, iterationResultsAfterYg) => {
                      (xg, iterationResultsAfterYg.infotons) match {
                        case t if t._1.isEmpty || t._2.isEmpty || !ok => Future(formatter.render(iterationResultsAfterYg))
                        case (Some(xgp), Some(infotons)) => {
                          val fIterationResults = deepExpandGraph(xgp, infotons, cmwellRDFHelper,typesCache(nbg),nbg).map { case (_, iseq) => iterationResultsAfterYg.copy(infotons = Some(iseq)) }
                          fIterationResults.map(formatter.render)
                        }
                      }
                    }
                  }
                }
              }

              val initialGraceTime = deadLine.timeLeft
              val injectInterval = 3.seconds
              val backOnTime: String => Result = Ok(_).as(overrideMimetype(formatter.mimetype, request)._2)
              val prependInjections: () => ByteString = formatter match {
                case a: AtomFormatter => {
                  val it = Iterator.single(ByteString(a.xsltRef)) ++ Iterator.continually(cmwell.ws.Streams.endln)
                  () => it.next()
                }
                case _ => () => cmwell.ws.Streams.endln
              }
              val injectOriginalFutureWith: String => ByteString = ByteString(_, StandardCharsets.UTF_8)
              val continueWithSource: Source[ByteString, NotUsed] => Result = Ok.chunked(_).as(overrideMimetype(formatter.mimetype, request)._2)

              guardHangingFutureByExpandingToSource[String,ByteString,Result](futureThatMayHang,initialGraceTime,injectInterval)(backOnTime,prependInjections,injectOriginalFutureWith,continueWithSource)
            }
          }.recover {
            case err: Throwable => {
              val actorAddress = Key.decode(encodedActorAddress)
              val id = actorAddress.split('/').last
              logger.error(s"[ID: $id] actor holding actual ES ID could not be found ($actorAddress)", err)
              ExpectationFailed("it seems like the iterator-id provided is invalid. " +
                "either it was already retrieved, or the specified session-ttl timeout exceeded, " +
                s"or it was malformed. error has been logged with ID = $id")
            }
          }
          case unrecognized: String => Future.successful(BadRequest(s"unrecognized format requested: $unrecognized"))
        }
      }
    }
  }.recover(asyncErrorHandler).get

  private def handleAggregate(request: Request[AnyContent]): Future[Result] = request
    .getQueryString("qp")
    .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
    .map { qpOpt =>

      val normalizedPath = normalizePath(request.path)
      val from = DateParser.parseDate(request.getQueryString("from").getOrElse(""), FromDate).toOption
      val to = DateParser.parseDate(request.getQueryString("to").getOrElse(""), ToDate).toOption
      val length = request.getQueryString("length").flatMap(asInt).getOrElse(10)
      val offset = request.getQueryString("offset").flatMap(asInt).getOrElse(0)
      val debugInfo = request.queryString.keySet("debug-info")
      val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
      val pathFilter = if (normalizedPath.length > 1) Some(PathFilter(normalizedPath, withDescendants)) else None
      val withHistory = request.queryString.keySet("with-history")
      val rawAggregationsFilters = AggregationsFiltersParser.parseAggregationParams(request.getQueryString("ap"))
      val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)

      rawAggregationsFilters match {
        case Success(raf) =>
          val apfut = Future.traverse(raf)(RawAggregationFilter.eval(_,typesCache(nbg),cmwellRDFHelper, nbg))
          val fieldsFiltersFut = qpOpt.fold(Future.successful(Option.empty[FieldFilter]))(rff => RawFieldFilter.eval(rff,typesCache(nbg),cmwellRDFHelper,nbg).map(Some.apply))
          fieldsFiltersFut.flatMap { fieldFilters =>
            apfut.flatMap { af =>
              crudServiceFS.aggregate(pathFilter, fieldFilters, Some(DatesFilter(from, to)), PaginationParams(offset, length), withHistory, af.flatten, debugInfo).map { aggResult =>
                request.getQueryString("format").getOrElse("json") match {
                  case FormatExtractor(formatType) => {
                    val formatter = formatterManager.getFormatter(
                      format = formatType,
                      host = request.host,
                      uri = request.uri,
                      pretty = request.queryString.keySet("pretty"),
                      callback = request.queryString.get("callback").flatMap(_.headOption),
                      nbg = nbg)
                    Ok(formatter.render(aggResult)).as(overrideMimetype(formatter.mimetype, request)._2)
                  }
                  case unrecognized: String => BadRequest(s"unrecognized format requested: $unrecognized")
                }
              }
            }
          }.recover(translateAggregateException andThen errorHandler)
        case Failure(e) => asyncErrorHandler(e)
      }
    }.recover(asyncErrorHandler).get

  private[this] def translateAggregateException: PartialFunction[Throwable, Throwable] = {
    case e: org.elasticsearch.transport.RemoteTransportException
      if e.getCause.isInstanceOf[org.elasticsearch.action.search.SearchPhaseExecutionException]
        && e.getCause.getMessage.contains("cannot be cast to org.elasticsearch.index.fielddata.IndexNumericFieldData") =>
      new BadFieldTypeException("Cannot cast field to numeric value. Did you try to use stats or histogram aggregations on non numeric field?", e)
    case e => e
  }

  private def handleSearch(r: Request[AnyContent]): Future[Result] =
    handleSearch(normalizePath(r.path),cmWellBase(r),r.host,r.uri)(r.queryString)

  private def handleSearch(normalizedPath: String,
                           cmWellBase: String,
                           requestHost: String,
                           requestUri: String)(implicit queryString: Map[String,Seq[String]]): Future[Result] =
    getQueryString("qp")
      .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
      .map { qpOpt =>

        val from = DateParser.parseDate(getQueryString("from").getOrElse(""), FromDate).toOption
        val to = DateParser.parseDate(getQueryString("to").getOrElse(""), ToDate).toOption
        val length = getQueryString("length").flatMap(asInt).getOrElse(10)
        val offset = getQueryString("offset").flatMap(asInt).getOrElse(0)
        val withDataFormat = getQueryString("with-data")
        val withData = withDataFormat.isDefined && withDataFormat.get.toLowerCase != "false"
        //FIXME: `getOrElse` swallows parsing errors that should come out as `BadRequest`
        val rawSortParams = getQueryString("sort-by").flatMap(SortByParser.parseFieldSortParams(_).toOption).getOrElse(RawSortParam.empty)
        val withDescendants = queryString.keySet("with-descendants") || queryString.keySet("recursive")
        val withDeleted = queryString.keySet("with-deleted")
        val pathFilter = Some(PathFilter(normalizedPath, withDescendants))
        val withHistory = queryString.keySet("with-history")
        val debugInfo = queryString.keySet("debug-info")
        val nbg = queryString.get("nbg").flatMap(_.headOption.flatMap(asBoolean)).getOrElse(tbg.get)
        val xg = queryString.keySet("xg")
        val yg = queryString.keySet("yg")

        if (offset > Settings.maxOffset) {
          Future.successful(BadRequest(s"Even Google doesn't handle offsets larger than ${Settings.maxOffset}!"))
        } else if (length > Settings.maxLength) {
          Future.successful(BadRequest(s"Length is larger than ${Settings.maxLength}!"))
        }
        else if (withHistory && (xg || yg))
          Future.successful(BadRequest(s"you can't mix `xg` nor `yg` with `with-history`: it makes no sense!"))
        else if (!withData && (xg || (yg && getQueryString("yg").get.split('|').exists(_.trim.startsWith(">")))))
          Future.successful(BadRequest(s"you can't mix `xg` nor '>' prefixed `yg` expressions without also specifying `with-data`: it makes no sense!"))
        else {
          val fieldSortParamsFut = RawSortParam.eval(rawSortParams,crudServiceFS,typesCache(nbg),cmwellRDFHelper,nbg)
          val fieldsFiltersFut = qpOpt.fold[Future[Option[FieldFilter]]](Future.successful(Option.empty[FieldFilter]))(rff => RawFieldFilter.eval(rff,typesCache(nbg),cmwellRDFHelper,nbg).map(Some.apply))
          fieldsFiltersFut.flatMap { fieldFilters =>
            fieldSortParamsFut.flatMap { fieldSortParams =>
              crudServiceFS.search(pathFilter, fieldFilters, Some(DatesFilter(from, to)),
                PaginationParams(offset, length), withHistory, withData, fieldSortParams, debugInfo, withDeleted).flatMap { unmodifiedSearchResult =>

                val ygModified = getQueryString("yg") match {
                  case Some(ygp) => {
                    pathExpansionParser(ygp, unmodifiedSearchResult.infotons, getQueryString("yg-chunk-size").flatMap(asInt).getOrElse(10),cmwellRDFHelper,typesCache(nbg),nbg).map { case (ok, infotons) =>
                      ok -> unmodifiedSearchResult.copy(
                        length = infotons.size,
                        infotons = infotons
                      )
                    }
                  }
                  case None => Future.successful(true -> unmodifiedSearchResult)
                }

                val fSearchResult = ygModified.flatMap {
                  case (true, sr) => getQueryString("xg") match {
                    case None => Future.successful(true -> sr)
                    case Some(xgp) => {
                      deepExpandGraph(xgp, sr.infotons,cmwellRDFHelper,typesCache(nbg),nbg).map { case (ok, infotons) =>
                        ok -> unmodifiedSearchResult.copy(
                          length = infotons.size,
                          infotons = infotons
                        )
                      }
                    }
                  }
                  case (b, sr) => Future.successful(b -> sr)
                }

                fSearchResult.flatMap { case (ok, searchResult) =>
                  extractFieldsMask(getQueryString("fields"),typesCache(nbg),cmwellRDFHelper, nbg).map { fieldsMask =>
                    // Prepare pagination info
                    val linkBase = cmWellBase + normalizedPath + getQueryString("format").map {
                      "?format=" + _
                    }.getOrElse("?") + getQueryString("with-descendants").map {
                      "&with-descendants=" + _
                    }.getOrElse("?") + getQueryString("recursive").map {
                      "&recursive=" + _
                    }.getOrElse("") + "&op=search" + searchResult.fromDate.map {
                      f => "&from=" + URLEncoder.encode(fullDateFormatter.print(f), "UTF-8")
                    }.getOrElse("") +
                      searchResult.toDate.map {
                        t => "&to=" + URLEncoder.encode(fullDateFormatter.print(t), "UTF-8")
                      }.getOrElse("") + getQueryString("qp").map {
                      "&qp=" + URLEncoder.encode(_, "UTF-8")
                    }.getOrElse("") + "&length=" + searchResult.length

                    val self = linkBase + "&offset=" + searchResult.offset
                    val first = linkBase + "&offset=0"
                    val last = searchResult.length match {
                      case l if l > 0 => linkBase + "&offset=" + ((searchResult.total / searchResult.length) * searchResult.length)
                      case _ => linkBase + "&offset=0"
                    }

                    val next = searchResult.offset + searchResult.length - searchResult.total match {
                      case x if x < 0 => Some(linkBase + "&offset=" + (searchResult.offset + searchResult.length))
                      case _ => None
                    }

                    val previous = (searchResult.offset - searchResult.length) match {
                      case dif if dif >= 0 => Some(linkBase + "&offset=" + dif)
                      case dif if dif < 0 && -dif < searchResult.length => Some(linkBase + "&offset=0")
                      case _ => None
                    }

                    val paginationInfo = PaginationInfo(first, previous, self, next, last)

                    //TODO: why not check for valid format before doing all the hard work for search?
                    getQueryString("format").getOrElse("atom") match {
                      case FormatExtractor(formatType) => {
                        val formatter = formatterManager.getFormatter(
                          format = formatType,
                          host = requestHost,
                          uri = requestUri,
                          pretty = queryString.keySet("pretty"),
                          callback = queryString.get("callback").flatMap(_.headOption),
                          fieldFilters = fieldFilters,
                          offset = Some(offset.toLong),
                          length = Some(length.toLong),
                          withData = withDataFormat,
                          forceUniqueness = withHistory,
                          nbg = nbg)
                        if (ok) Ok(formatter.render(SearchResponse(paginationInfo, searchResult.masked(fieldsMask)))).as(overrideMimetype(formatter.mimetype, getQueryString("override-mimetype"))._2)
                        else InsufficientStorage(formatter.render(SearchResponse(paginationInfo, searchResult))).as(overrideMimetype(formatter.mimetype, getQueryString("override-mimetype"))._2)
                      }
                      case unrecognized: String => BadRequest(s"unrecognized format requested: $unrecognized")
                    }
                  }
                }
              }
            }
          }.recover(errorHandler)
        }
      }.recover(asyncErrorHandler).get


  private def handleRead(request: Request[AnyContent], recursiveCalls: Int = 30): Future[Result] = Try {
    val length = request.getQueryString("length").flatMap(asInt).getOrElse(0)
    val offset = request.getQueryString("offset").flatMap(asInt).getOrElse(0)
    val format = request.getQueryString("format")
    val path = normalizePath(request.path)
    val xg = request.getQueryString("xg")
    val (yg,ygChunkSize) = request.getQueryString("yg").fold(Option.empty[String] -> 0)(Some(_) -> request.getQueryString("yg-chunk-size").flatMap(asInt).getOrElse(10))
    val withHistory = request.queryString.keySet("with-history")
    val limit = request.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)

    if (offset > Settings.maxOffset) {
      Future.successful(BadRequest(s"Even Google doesn't handle offsets larger than ${Settings.maxOffset}!"))
    } else if (length > Settings.maxLength) {
      Future.successful(BadRequest(s"Length is larger than ${Settings.maxOffset}!"))
    } else if (Set("/meta/ns/sys", "/meta/ns/nn")(path)) {
      handleGetForActiveInfoton(request, path)
    } else {
      lazy val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
      val reply = {
        if (withHistory && (xg.isDefined || yg.isDefined))
          Future.successful(BadRequest(s"you can't mix `xg` nor `yg` with `with-history`: it makes no sense!"))
        else if (request.queryString.keySet("with-history")) {
          if (isReactive(request)) {
            format.getOrElse("json") match {
              case f if !Set("text", "path", "tsv", "tab", "nt", "ntriples", "nq", "nquads")(f.toLowerCase) && !f.toLowerCase.startsWith("json") =>
                Future.successful(BadRequest(Json.obj("success" -> false, "message" -> "not a streamable type (use any json, or one of: 'text','tsv','ntriples', or 'nquads')")))
              case FormatExtractor(formatType) => {
                val formatter = formatterManager.getFormatter(format = formatType,
                  host = request.host,
                  uri = request.uri,
                  pretty = false,
                  callback = request.queryString.get("callback").flatMap(_.headOption),
                  withoutMeta = !request.queryString.keySet("with-meta"),
                  filterOutBlanks = true,
                  forceUniqueness = true,
                  nbg = nbg)

                val infotonsSource = crudServiceFS.getInfotonHistoryReactive(path)

                val f: Formattable => ByteString = formattableToByteString(formatter)
                val bytes = infotonsSource.map(f andThen {
                  _.toArray
                })

                Future.successful(Ok.chunked(bytes).as(overrideMimetype(formatter.mimetype, request)._2))
              }
            }
          }
          else {
            val formatter = format.getOrElse("atom") match {
              case FormatExtractor(formatType) => formatterManager.getFormatter(
                format = formatType,
                host = request.host,
                uri = request.uri,
                pretty = request.queryString.keySet("pretty"),
                callback = request.queryString.get("callback").flatMap(_.headOption),
                withData = request.getQueryString("with-data"),
                nbg = nbg)
            }

            crudServiceFS.getInfotonHistory(path, limit).map(ihv =>
              Ok(formatter.render(ihv)).as(overrideMimetype(formatter.mimetype, request)._2)
            )
          }
        }
        else {
          lazy val formatter = format.getOrElse("json") match {
            case FormatExtractor(formatType) =>
              formatterManager.getFormatter(
                formatType,
                request.host,
                request.uri,
                request.queryString.keySet("pretty"),
                request.queryString.get("callback").flatMap(_.headOption),
                nbg = nbg)
            case unknown => {
              logger.warn(s"got unknown format: $unknown")
              formatterManager.prettyJsonFormatter(nbg)
            }
          }

          crudServiceFS.getInfoton(path, Some(offset), Some(length),nbg).flatMap {
            case Some(UnknownNestedContent(i)) =>
              //TODO: should still allow xg expansion?
              Future.successful(PartialContent(formatter.render(i)).as(overrideMimetype(formatter.mimetype, request)._2))
            case infopt => {
              val i = infopt.fold(GhostInfoton.ghost(path)){
                case Everything(j) => j
                case _: UnknownNestedContent => !!!
              }
              extractFieldsMask(request,typesCache(nbg),cmwellRDFHelper, nbg).flatMap { fieldsMask =>
                val toRes = (f: Future[(Boolean, Seq[Infoton])]) => f.map {
                  case ((true, xs)) => Ok(formatter.render(BagOfInfotons(xs))).as(overrideMimetype(formatter.mimetype, request)._2)
                  case ((false, xs)) => InsufficientStorage(formatter.render(BagOfInfotons(xs))).as(overrideMimetype(formatter.mimetype, request)._2)
                }
                val ygFuncOpt = yg.map(ygPattern => (iSeq: Seq[Infoton]) => pathExpansionParser(ygPattern, iSeq, ygChunkSize,cmwellRDFHelper,typesCache(nbg),nbg))
                val xgFuncOpt = xg.map(xgPattern => (iSeq: Seq[Infoton]) => deepExpandGraph(xgPattern, iSeq,cmwellRDFHelper,typesCache(nbg),nbg))
                val xygFuncOpt = ygFuncOpt.flatMap(ygFunc => xgFuncOpt.map(xgFunc => (iSeq: Seq[Infoton]) => ygFunc(iSeq).flatMap {
                  case (true, jSeq) => xgFunc(jSeq)
                  case t => Future.successful(t)
                }))

                if(infopt.isEmpty && yg.isEmpty) infotonOptionToReply(request, None, recursiveCalls)
                else xygFuncOpt.map(_ andThen toRes).map(_ (Seq(i)))
                  .getOrElse(ygFuncOpt.map(_ andThen toRes).map(_ (Seq(i)))
                    .getOrElse(xgFuncOpt.map(_ andThen toRes).map(_ (Seq(i)))
                      .getOrElse(infotonOptionToReply(request, Some(i), recursiveCalls, fieldsMask))))
              }
            }
          }
        }
      }
      reply.recover(errorHandler)
    }
  }.recover(asyncErrorHandler).get

  def infotonOptionToReply(request: Request[AnyContent], infoton: Option[Infoton], recursiveCalls: Int = 30, fieldsMask: Set[String] = Set.empty): Future[Result] = Try {
    val offset = request.getQueryString("offset")
    val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
    (if(offset.isEmpty) actions.ActiveInfotonHandler.wrapInfotonReply(infoton) else infoton) match {
      case None => Future.successful(NotFound("Infoton not found"))
      case Some(DeletedInfoton(p, _, _, lm,_)) => Future.successful(NotFound(s"Infoton was deleted on ${fullDateFormatter.print(lm)}"))
      case Some(LinkInfoton(_, _, _, _, _, to, lType,_)) => lType match {
        case LinkType.Permanent => Future.successful(Redirect(to, request.queryString, MOVED_PERMANENTLY))
        case LinkType.Temporary => Future.successful(Redirect(to, request.queryString, TEMPORARY_REDIRECT))
        case LinkType.Forward if recursiveCalls > 0 => handleRead(Request(request.copy(path = to, uri = to + request.uri.drop(request.path.length)), request.body), recursiveCalls - 1)
        case LinkType.Forward => Future.successful(BadRequest("too deep forward link chain detected!"))
      }
      case Some(i) =>

        val maskedInfoton = i.masked(fieldsMask)

        def infotonIslandResult(prefix: String, suffix: String) = {
          val infotonStr = formatterManager.getFormatter(JsonlType,nbg = nbg).render(maskedInfoton)

          //TODO: find out why did we use "plumbing" API. we should let play determine content length etc'...
          val r = Ok(prefix + Utility.escape(infotonStr) + suffix)
            .as(overrideMimetype("text/html;charset=UTF-8", request)._2)
            .withHeaders(getNoCacheHeaders(): _*)
          //          val contentBytes = (prefix + Utility.escape(infotonStr) + suffix).getBytes("UTF-8")
          //          val r = new Result(header = ResponseHeader(200, Map(
          //            CONTENT_LENGTH -> String.valueOf(contentBytes.length), overrideMimetype("text/html;charset=UTF-8", request)) ++ getNoCacheHeaders().toMap), body = Enumerator(contentBytes))
          Future.successful(r)
        }

        //TODO: use formatter manager to get the suitable formatter
        request.getQueryString("format") match {
          case Some(FormatExtractor(formatType)) => {
            val formatter = formatterManager.getFormatter(
              format = formatType,
              host = request.host,
              uri = request.uri,
              pretty = request.queryString.keySet("pretty"),
              callback = request.getQueryString("callback"),
              offset = request.getQueryString("offset").map(_.toInt),
              length = request.getQueryString("length").map(_.toInt),
              nbg = nbg)

            Future.successful(Ok(formatter.render(maskedInfoton)).as(overrideMimetype(formatter.mimetype, request)._2))
          }
          // default format
          case _ => i match {
            case f: FileInfoton if request.headers.get("x-compile-to").contains("js") =>
              val scalaJsSource = new String(f.content.get.data.get, "UTF-8")
              val mime = "application/javascript"
              ScalaJsRuntimeCompiler.compile(scalaJsSource).flatMap(c =>
                treatContentAsAsset(request, c.getBytes("UTF-8"), mime, i.path, i.uuid, i.lastModified)
              )
            case f: FileInfoton => {
              val mt = f.content.get.mimeType
              val (content, mime) = {
                if (isMarkdown(mt) && ! request.queryString.keySet("raw")) {
                  (MarkdownFormatter.asHtmlString(f).getBytes, "text/html")
                } else (f.content.get.data.get, mt)
              }
              treatContentAsAsset(request, content, mime, i.path, i.uuid, i.lastModified)
            }
            case c: CompoundInfoton if c.children.exists(_.name.equalsIgnoreCase("index.html")) =>
              Future.successful(Redirect(routes.Application.handleGET(s"${c.path}/index.html".substring(1))))
            // ui
            case _ => {
              val isOldUi = request.queryString.keySet("old-ui")
              cachedSpa.getContent(isOldUi,nbg).flatMap { markup =>
                if (markup eq null)
                  Future.successful(ServiceUnavailable("System initialization was not yet completed. Please try again soon."))
                else
                  infotonIslandResult(markup + "<inject>", "</inject>")
              }
            }
          }
        }
    }
  }.recover(asyncErrorHandler).get

  def isMarkdown(mime: String): Boolean =
    mime.startsWith("text/x-markdown") || mime.startsWith("text/vnd.daringfireball.markdown")

  def boolFutureToRespones(fb: Future[Boolean]) = fb.map {
    case true => Ok(Json.obj("success" -> true))
    case false => BadRequest(Json.obj("success" -> false))
  }

  def handlePutInfoton(path:String): Request[RawBuffer] => Future[Result] = { implicit originalRequest =>

    requestSlashValidator(originalRequest).map {request =>
      val normalizedPath = normalizePath(request.path)

      if (!InfotonValidator.isInfotonNameValid(normalizedPath))
        Future.successful(BadRequest(Json.obj("success" -> false, "message" -> """you can't write to "meta" / "proc" / "ii" / or any path starting with "_" (service paths...)""")))
      //      else if (Settings.authSystemVersion == 1 && !security.RSAAuthorizationService.authorize(normalizedPath, request.headers.get("X-CM-WELL-TOKEN")))
      //        Future.successful(Forbidden(Json.obj("success" -> false, "message" -> """authorization failed!""")))
      else request match {
        case XCmWellType.Object() => {
          val bodyBytes = request.body.asBytes().fold(Array.emptyByteArray)(_.toArray[Byte])
          jsonToFields(bodyBytes) match {
            case Success(fields) =>
              InfotonValidator.validateValueSize(fields)
              boolFutureToRespones(crudServiceFS.putInfoton(ObjectInfoton(normalizedPath, Settings.dataCenter, None, fields)))
            // TODO handle validation
            case Failure(exception) => asyncErrorHandler(exception)
          }
        }
        case XCmWellType.File() => {
          val content = request.body.asBytes().fold(Array.emptyByteArray)(_.toArray[Byte])
          if (content.isEmpty) Future.successful(BadRequest(Json.obj("success" -> false, "cause" -> "empty content")))
          else {
            val contentType = request.headers.get("Content-Type").orElse(MimeTypeIdentifier.identify(content, normalizedPath.slice(normalizedPath.lastIndexOf("/"), normalizedPath.length))).getOrElse("text/plain")
            boolFutureToRespones(crudServiceFS.putInfoton(FileInfoton(path = normalizedPath, dc = Settings.dataCenter, content = Some(FileContent(content, contentType)))))
          }
        }
        case XCmWellType.FileMD() => {
          val bodyBytes = request.body.asBytes().fold(Array.emptyByteArray)(_.toArray[Byte])
          jsonToFields(bodyBytes) match {
            case Success(fields) =>
              InfotonValidator.validateValueSize(fields)
              boolFutureToRespones(crudServiceFS.putInfoton(FileInfoton(path = normalizedPath, dc = Settings.dataCenter, fields = Some(fields))))
            case Failure(exception) => Future.successful(BadRequest(Json.obj("success" -> false, "cause" -> exception.getMessage)))
          }
        }
        case XCmWellType.Link() => {
          val linkTo = request.body.asBytes().fold("")(_.utf8String)
          val linkTypeStr = request.headers.get("X-CM-WELL-LINK-TYPE").getOrElse("1")
          val linkType = linkTypeStr match {
            case "0" => LinkType.Permanent
            case "1" => LinkType.Temporary
            case "2" => LinkType.Forward
          }
          boolFutureToRespones(crudServiceFS.putInfoton(LinkInfoton(path = normalizedPath, dc = Settings.dataCenter, fields = Some(Map[String, Set[FieldValue]]()), linkTo = linkTo, linkType = linkType)))
        }
        case _ => Future.successful(BadRequest(Json.obj("success" -> false, "cause" -> "unrecognized type")))
      }
    }.recover(asyncErrorHandler).get
  }

  private def jsonToFields(jsonBytes: Array[Byte]): Try[Map[String, Set[FieldValue]]] = {

    Try(Json.parse(jsonBytes)).flatMap {
      case JsObject(fields) =>
        val data: MultiMap[String, FieldValue] = new HashMap[String, collection.mutable.Set[FieldValue]]() with MultiMap[String, FieldValue]
        fields.foreach {
          case (k, JsString(s)) => data.addBinding(k, FString(s))
          case (k, JsNumber(n)) => data.addBinding(k, Encoders.numToFieldValue(n))
          case (k, JsBoolean(b)) => data.addBinding(k, FString("" + b))
          case (k, JsArray(arr)) => arr.foreach {
            case (JsString(s)) => data.addBinding(k, FString(s))
            case (JsNumber(n)) => data.addBinding(k, Encoders.numToFieldValue(n))
            case (JsBoolean(b)) => data.addBinding(k, FString("" + b))
            case _ =>
          }
          case _ => Failure(new IllegalArgumentException("Json depth level allowed is 1"))
        }
        Success(data.map { case (x, y) => (x, y.toSet) }.toMap)
      case _ => Failure(new IllegalArgumentException("Json value must be an object"))
    }.recoverWith { case parseException =>
      Failure(new IllegalArgumentException("Request body is not a recognized json", parseException))
    }
  }

  def handleAuthGET(action: String) = Action.async { req =>
    action match {
      case "generatepassword" => {
        val pw = authUtils.generateRandomPassword()
        Future.successful(Ok(Json.obj(("password", JsString(pw._1)), ("encrypted", pw._2))))
      }
      case "changepassword" => {
        val currentPassword = req.getQueryString("current")
        val newPassword = req.getQueryString("new")
        val token = authUtils.extractTokenFrom(req)

        if (Seq(currentPassword, newPassword, token).forall(_.isDefined)) {
          authUtils.changePassword(token.get, currentPassword.get, newPassword.get).map {
            case true => Ok(Json.obj("success" -> true))
            case _ => Forbidden(Json.obj("error" -> "Current password does not match given token"))
          }
        } else {
          Future.successful(BadRequest(Json.obj("error" -> "insufficient arguments")))
        }
      }
      case _ => Future.successful(BadRequest(Json.obj("error" -> "No such action")))
    }
  }

  val endlnBytes: Array[Byte] = "\n".getBytes("UTF-8")
  val emptyBytes: Array[Byte] = Array.empty[Byte]

  def handleFix(req: Request[AnyContent]): Future[Result] = {
    val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
    if (!authUtils.isOperationAllowedForUser(security.Overwrite, authUtils.extractTokenFrom(req)))
      Future.successful(Forbidden("not authorized to overwrite"))
    else if(isReactive(req)){
      val formatter = getFormatter(req, formatterManager, "json", nbg)
      val parallelism = req.getQueryString("parallelism").flatMap(asInt).getOrElse(1)
      crudServiceFS.rFix(normalizePath(req.path),parallelism,nbg).map { source =>
        val s = source.map { bs =>
          val msg = if (bs._2.isEmpty) None else Some(bs._2)
          ByteString(formatter.render(SimpleResponse(bs._1, msg)), StandardCharsets.UTF_8)
        }.intersperse(ByteString.empty,Streams.endln,Streams.endln)
        Ok.chunked(s)
      }
    }
    else {
      val limit = req.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
      val f = crudServiceFS.fix(normalizePath(req.path),limit)
      val formatter = getFormatter(req, formatterManager, "json", nbg)
      val r = f.map {
        bs => Ok(formatter.render(SimpleResponse(bs._1, if (bs._2.isEmpty) None else Some(bs._2)))).as(overrideMimetype(formatter.mimetype, req)._2)
      }
      keepAliveByDrippingNewlines(r)
    }
  }

  def handleVerify(req: Request[AnyContent]): Future[Result] = {
    val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
    val limit = req.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
    val f = crudServiceFS.verify(normalizePath(req.path),limit)
    val formatter = getFormatter(req, formatterManager, "json", nbg)
    f.map {
      b => Ok(formatter.render(SimpleResponse(b, None))).as(overrideMimetype(formatter.mimetype, req)._2)
    }
  }

  def handleInfo(req: Request[AnyContent]): Future[Result] = {
    val path = normalizePath(req.path)
    val limit = req.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
    val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
    crudServiceFS.info(path,limit).map {
      case (cas, es, zs) => {
        val c = cas.map {
          case (u, io) => s"cas $u ${io.map(i => formatterManager.jsonFormatter(nbg).render(i)).getOrElse("none")}"
        }
        val e = es.groupBy(_._1).map { case (uuid, indices) => s"es  $uuid: ${indices.map(_._2).mkString("[", ",", "]")}" }
        val z = zs.map("zs  ".+)
        val body = (c ++ e ++ z).sortBy(_.drop(4)).mkString("\n")
        Ok(body).as(overrideMimetype("text/plain;charset=UTF8", req)._2)
      }
    }.recover {
      case e: Throwable => {
        logger.error("x-info future failed", e)
        InternalServerError(e.getMessage)
      }
    }
  }

  def handleFixDc(req: Request[AnyContent]): Future[Result] = {
    val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
    if (!authUtils.isOperationAllowedForUser(security.Overwrite, authUtils.extractTokenFrom(req)))
      Future.successful(Forbidden("not authorized to overwrite"))
    else {
      val f = crudServiceFS.fixDc(normalizePath(req.path), Settings.dataCenter)
      val formatter = getFormatter(req, formatterManager, "json", nbg)
      f.map {
        bs => Ok(formatter.render(SimpleResponse(bs, None))).as(overrideMimetype(formatter.mimetype, req)._2)
      }.recoverWith(asyncErrorHandler)
    }
  }

  def handlePurgeAll(req: Request[AnyContent]) = handlePurge(req, includeLast = true)

  def handlePurgeHistory(req: Request[AnyContent]) = handlePurge(req, includeLast = false)

  def handlePurgeLast(req: Request[AnyContent]) = handlePurge(req, includeLast = true, onlyLast = true)

  private def handlePurge(req: Request[AnyContent], includeLast: Boolean, onlyLast: Boolean = false): Future[Result] = {
    if (req.getQueryString("2").isDefined) handlePurge2(req) else {

      lazy val notImplemented = Future.failed(new NotImplementedError("This specific version of CM-Well does not support this operation."))

      val p = Promise[Result]()

      val path = normalizePath(req.path)
      val allowed = authUtils.filterNotAllowedPaths(Seq(path), PermissionLevel.Write, authUtils.extractTokenFrom(req)).isEmpty
      if (!allowed) {
        p.completeWith(Future.successful(Forbidden("Not authorized")))
      } else {
        val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
        val formatter = getFormatter(req, formatterManager, "json", nbg)
        val limit = req.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
        val res = if (onlyLast)
          notImplemented // CRUDServiceFS.rollback(path,limit)
        else if (includeLast) crudServiceFS.purgePath(path, includeLast, limit) else notImplemented // CRUDServiceFS.purgePath(path, includeLast, limit)

        res.onComplete {
          case Success(_) =>
            p.completeWith(Future.successful(Ok(formatter.render(SimpleResponse(true, None))).as(overrideMimetype(formatter.mimetype, req)._2)))
          case Failure(e) =>
            p.completeWith(Future.successful(Ok(formatter.render(SimpleResponse(false, Option(e.getCause.getMessage)))).as(overrideMimetype(formatter.mimetype, req)._2)))
        }
      }

      p.future
    }
  }

  private def handlePurge2(req: Request[AnyContent]): Future[Result] = {
    val path = normalizePath(req.path)
    val allowed = authUtils.filterNotAllowedPaths(Seq(path), PermissionLevel.Write, authUtils.extractTokenFrom(req)).isEmpty
    if(!allowed) {
      Future.successful(Forbidden("Not authorized"))
    } else {
      val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)
      val formatter = getFormatter(req, formatterManager,"json", nbg)
      val limit = req.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
      crudServiceFS.purgePath2(path,limit).map{
        b => Ok(formatter.render(SimpleResponse(true,None))).as(overrideMimetype(formatter.mimetype,req)._2)
      }
    }
  }

  def handleRawRow(uuid: String) = Action.async { req =>
    if(isReactive(req)) {
      val src = crudServiceFS.reactiveRawCassandra(uuid).intersperse("\n").map(ByteString(_,StandardCharsets.UTF_8))
      Future.successful(Ok.chunked(src).as(overrideMimetype("text/csv;charset=UTF-8", req)._2))
    }
    else crudServiceFS.getRawCassandra(uuid).map {
      case (payload,mimetype) => Ok(payload).as(overrideMimetype(mimetype, req)._2)
    }
  }

  def handlePoisonPill() = Action { implicit req =>

    if (!authUtils.isOperationAllowedForUser(security.Admin, authUtils.extractTokenFrom(req), evenForNonProdEnv = true)) {
      Forbidden("Not authorized")
    } else {
      val hostOpt = req.getQueryString("host")
      val jvmOpt = req.getQueryString("jvm")

      // retain the old behavior
      if (hostOpt.isEmpty || jvmOpt.isEmpty) {
        logger.info("Got poison pill. Will now exit.")
        cmwell.util.concurrent.SimpleScheduler.schedule(1 seconds) {
          sys.exit(1)
        }
        Ok("Goodbye")
      } else {
        val host = hostOpt.get
        val jvm = jvmOpt.get

        Grid.selectActor(ClientActor.name, GridJvm(host, jvm)) ! RestartJvm

        Ok(s"Restarted $jvm at $host")
      }
    }
  }

  def handleZzPost(uzid: String) = Action.async(parse.raw) { implicit req =>
    val allowed = authUtils.isOperationAllowedForUser(security.Admin, authUtils.extractTokenFrom(req), evenForNonProdEnv = true)
    req.body.asBytes() match {
      case Some(payload) if allowed =>
        val ttl = req.getQueryString("ttl").fold(0)(_.toInt)
        crudServiceFS.zStore.put(uzid, payload.toArray[Byte], ttl, req.queryString.keySet("batched")).map(_ => Ok("""{"success":true}""")).recover {
          case e => InternalServerError(s"""{"success":false,"message":"${e.getMessage}"}""")
        }
      case None if allowed => Future.successful(BadRequest("POST body may not be empty!"))
      case _ => Future.successful(Forbidden("Not allowed to use zz"))
    }
  }

  def requestDetailedView(path: String) = Action.async {
    implicit req => {
      implicit val timeout = Timeout(2.seconds)
      import akka.pattern.ask
      import cmwell.rts.Key
      val actorPath = Key.decode(path)
      val f = (Grid.selectByPath(actorPath) ? GetRequest).mapTo[CmwellRequest]
      f.map { cmwReq =>
        Ok(
          s"""
             | Type: ${cmwReq.requestType}
             | Path: ${cmwReq.path}
             | Query: ${cmwReq.queryString}
             | Body: ${cmwReq.requestBody}
          """.stripMargin)
      }.recover {
        case t: Throwable => Ok("This request is not monitored anymore")
      }
    }
  }

  def startRequestMonitoring = Action {
    RequestMonitor.startMonitoring
    Ok("Started monitor requests")
  }

  def stopRequestMonitoring = Action {
    RequestMonitor.stopMonitoring
    Ok("Stopped monitor requests")
  }

  def handleCssh = Action {
    Ok(generateCsshCommand("cssh --username"))
  }

  def handleCsshx = Action {
    Ok(generateCsshCommand("csshX --login"))
  }

  private def generateCsshCommand(command: String) = {
    val hosts = Grid.availableMachines.mkString(" ")
    val user = System.getProperty("user.name")
    s"$command $user $hosts"
  }

}

case class ParamExtractor(name: String, value: String => Boolean) {
  def unapply(request: RequestHeader): Boolean = request.getQueryString(name).exists(value)
}

object Operation {
  private[this] def func(s: String)(t: String) = t.equalsIgnoreCase(s)

  val search            = ParamExtractor("op", func("search")(_))
  val aggregate         = ParamExtractor("op", {s => s.equalsIgnoreCase("aggregate") || s.equalsIgnoreCase("stats")})
  val read              = ParamExtractor("op", func("read")(_))
  val startScroll       = ParamExtractor("op", func("create-iterator")(_))
  val scroll            = ParamExtractor("op", func("next-chunk")(_))
  val createConsumer    = ParamExtractor("op", func("create-consumer")(_))
  val consume           = ParamExtractor("op", func("consume")(_))
  val subscribe         = ParamExtractor("op", func("subscribe")(_))
  val unsubscribe       = ParamExtractor("op", func("unsubscribe")(_))
  val pull              = ParamExtractor("op", func("pull")(_))
  val stream            = ParamExtractor("op", func("stream")(_))
  val multiStream       = ParamExtractor("op", func("mstream")(_))
  val superStream       = ParamExtractor("op", func("sstream")(_))
  val queueStream       = ParamExtractor("op", func("qstream")(_))
  val bulkConsumer      = ParamExtractor("op", func("bulk-consume")(_))
  val fix               = ParamExtractor("op", func("x-fix")(_))
  val info              = ParamExtractor("op", func("x-info")(_))
  val verify            = ParamExtractor("op", func("x-verify")(_))
  val fixDc             = ParamExtractor("op", func("x-fix-dc")(_))
  val purgeAll          = ParamExtractor("op", func("purge-all")(_))
  val purgeLast         = ParamExtractor("op", func("purge-last")(_))
  val rollback          = ParamExtractor("op", func("rollback")(_)) // alias for purge-last
  val purgeHistory      = ParamExtractor("op", func("purge-history")(_))
}

object XCmWellType {
  val File = CMWellPostType("FILE")
  val FileMD = CMWellPostType("FILE_MD")
  val Object = CMWellPostType("OBJ")
  val Link = CMWellPostType("LN")
}

case class CMWellPostType(xCmWellType: String) {
  def unapply(request: RequestHeader): Boolean = request.headers.get("X-CM-WELL-TYPE").exists(_.equalsIgnoreCase(xCmWellType))
}

@Singleton
class CachedSpa @Inject()(crudServiceFS: CRUDServiceFS)(implicit ec: ExecutionContext) extends LazyLogging {


  val oldNbgCache = new SingleElementLazyAsyncCache[String](600000)(doFetchContent(true,true))(Combiner.replacer,ec)
  val newObgCache = new SingleElementLazyAsyncCache[String](600000)(doFetchContent(false,false))(Combiner.replacer,ec)
  val oldObgCache = new SingleElementLazyAsyncCache[String](600000)(doFetchContent(true,false))(Combiner.replacer,ec)
  val newNbgCache = new SingleElementLazyAsyncCache[String](600000)(doFetchContent(false,true))(Combiner.replacer,ec)

  private val contentPath = "/meta/app/index.html"
  private val newContentPath = "/meta/app/react/index.html"

  private def doFetchContent(isOldUi: Boolean, nbg: Boolean): Future[String] = {
    val path = if(isOldUi) contentPath else newContentPath
    crudServiceFS.getInfotonByPathAsync(path).collect {
      case FullBox(FileInfoton(_, _, _, _, _, Some(c),_)) => new String(c.data.get, "UTF-8")
      case somethingElse => {
        logger.error("got something else: " + somethingElse)
        ???
      }
    }
  }

  def getContent(isOldUi: Boolean, nbg: Boolean): Future[String] =
    if(isOldUi) {
      if(nbg) oldNbgCache.getAndUpdateIfNeeded
      else oldObgCache.getAndUpdateIfNeeded
    }
    else {
      if(nbg) newNbgCache.getAndUpdateIfNeeded
      else newObgCache.getAndUpdateIfNeeded
    }
}
