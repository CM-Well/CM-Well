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
package controllers

import actions.RequestMonitor
import akka.stream.Materializer
import cmwell.domain._
import cmwell.tracking._
import cmwell.util.concurrent._
import cmwell.util.collections.opfut
import cmwell.util.formats.JsonEncoder
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.exceptions.UnretrievableIdentifierException
import ld.exceptions.ServerComponentNotAvailableException
import cmwell.web.ld.util.LDFormatParser.ParsingResponse
import cmwell.web.ld.util._
import cmwell.ws.Settings
import cmwell.ws.util.{FieldKeyParser, TypeHelpers}
import com.typesafe.scalalogging.LazyLogging
import logic.{CRUDServiceFS, InfotonValidator}
import play.api.libs.json._
import play.api.mvc._
import security.{AuthUtils, PermissionLevel}
import wsutil._
import javax.inject._
import filters.Attrs
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json.JsValueWrapper

import scala.concurrent.duration._
import scala.concurrent._
import scala.language.postfixOps
import scala.util._

@Singleton
class InputHandler @Inject()(ingestPushback: IngestPushback,
                             crudService: CRUDServiceFS,
                             authUtils: AuthUtils,
                             cmwellRDFHelper: CMWellRDFHelper,
                             formatterManager: FormatterManager)
                            (implicit val mat: Materializer)
    extends InjectedController
    with LazyLogging
    with TypeHelpers { self =>

  val typesCaches = crudService.passiveFieldTypesCache
  val redlog = LoggerFactory.getLogger("bad_ingests")

  /**
    *
    * @param format
    * @return
    */
  def handlePost(format: String = "") = ingestPushback.async(parse.raw) { implicit req =>
    import scala.concurrent.ExecutionContext.Implicits.global
    RequestMonitor.add("in",
      req.path,
      req.rawQueryString,
      req.body.asBytes().fold("")(_.utf8String),
      req.attrs(Attrs.RequestReceivedTimestamp))

    // first checking "priority" query string. Only if it is present we will consult the UserInfoton which is more expensive (order of && below matters):
    if (req.getQueryString("priority").isDefined && !authUtils.isOperationAllowedForUser(security.PriorityWrite,
      authUtils
        .extractTokenFrom(req),
      evenForNonProdEnv = true)) {
      Future.successful(Forbidden(Json.obj("success" -> false, "message" -> "User not authorized for priority write")))
    } else {
      val resp =
        if ("jsonw" == format.toLowerCase) handlePostWrapped(req).zip(Future.successful(Iterable.empty -> Seq.empty[(String, String)]))
        else Future(handlePostRDF(req)).flatMap {
          case (normalResult, parsedPathsAndHeaders) => normalResult.zip(parsedPathsAndHeaders)
        }
      timeoutFuture(resp, Settings.clientRequestTimeout).andThen(printInLogs(req.body))
        .map(_._1)
        .recover(errorHandler)
    }
  }

  private def printInLogs(rawRequestBody: RawBuffer)
                         (implicit ec: ExecutionContext): PartialFunction[Try[(Result, (Iterable[String], Seq[(String, String)]))], Unit] = {
    case Failure(FutureTimeout(f)) =>
      val givingUpTimestamp = System.currentTimeMillis()
      val requestBody = rawRequestBody.asBytes().fold("")(_.utf8String)
      val id = cmwell.util.numeric.Radix64.encodeUnsigned(givingUpTimestamp) + "_" + cmwell.util.numeric.Radix64
        .encodeUnsigned(requestBody.hashCode())
      logger.error(s"_in ingest with id:[$id] got internal timeout. " +
        s"Additional information will should be in the log with the same id. The timestamp of the timeout is $givingUpTimestamp.")
      f.onComplete {
        case Success((Result(header, _, _, _, _), (paths: Iterable[String], _))) if header.status == OK =>
          val pathsStr = paths.mkString(",")
          logger.error(s"The _in internal processing for id:[$id] returned successfully ${System.currentTimeMillis() - givingUpTimestamp}ms after " +
            s"the timeout. Parsed request's paths are: [$pathsStr]")
        case Success((Result(header, body, _, _, _), (paths: Iterable[String], _))) =>
          val pathsStr = paths.mkString(",")
          logger.error(s"The _in internal processing for id:[$id] returned bad response ${System.currentTimeMillis() - givingUpTimestamp}ms " +
            s"after the timeout. Parsed request's paths are: [$pathsStr]. " +
            s"The header: [status: ${header.status}, headers: ${header.headers}]. The response body will be printed later with the same id.")
          body.consumeData.onComplete {
            case Success(value) => logger.error(s"The response body of id:[$id] is: ${value.utf8String}")
            case Failure(e) => logger.error(s"There was an error getting the response body of id:[$id]. The exception was: ", e)
          }
        case Failure(t) =>
          logger.error(s"The _in internal processing for id:[$id] returned with failure ${System.currentTimeMillis() - givingUpTimestamp}ms " +
            s"after the timeout. The request body was: $requestBody. The exception was: ", t)
      }
  }

  /**
    *
    * @param path
    * @return
    */
  private def escapePath(path: String): String = {
    path.replace(" ", "%20")
  }

  def handlePostForDCOverwrites = ingestPushback.async(parse.raw) { implicit req =>
    import scala.concurrent.ExecutionContext.Implicits.global

    val tokenOpt = authUtils.extractTokenFrom(req)
    if (!authUtils.isOperationAllowedForUser(security.Overwrite, tokenOpt, evenForNonProdEnv = true))
      Future.successful(Forbidden("not authorized"))
    else {
      Try {
        parseRDF(req, false, true)
          .flatMap {
            case ParsingResponse(infotonsMap,
                                 metaDataMap,
                                 cmwHostsSet,
                                 tmpDeleteMap,
                                 deleteValsMap,
                                 deletePaths,
                                 atomicUpdates,
                                 feedbacks) => {

              require(
                tmpDeleteMap.isEmpty && deleteValsMap.isEmpty && deletePaths.isEmpty && atomicUpdates.isEmpty,
                "can't use meta operations here! this API is used internaly, and only for overwrites!"
              )
              val (errs, _) = cmwell.util.collections.partitionWith(metaDataMap.iterator) {
                case (path, MetaData(mdType, _, data, text, mimeType, linkType, linkTo, dataCenter, indexTime, _ ,lastModifiedBy)) =>
                  var errors = List.empty[String]
                  if (indexTime.isEmpty) {
                    errors = "indexTime should be defined" :: errors
                  }
                  if (dataCenter.isEmpty) {
                    errors = "dataCenter should be defined" :: errors
                  }
                  if (lastModifiedBy.isEmpty) {
                    errors = "lastModifiedBy should be defined" :: errors
                  }
//                  else if (dataCenter.get == Settings.dataCenter) {
//                    errors = "dataCenter cannot be equal to current ID" :: errors
//                  }
                  if (mdType.isEmpty) {
                    errors = "infoton's kind (type) must be defined" :: errors
                  } else if (mdType.get == LinkMetaData) {
                    if (linkType.isEmpty) {
                      errors = "link kind (type) must be defined" :: errors
                    }
                    if (linkTo.isEmpty) {
                      errors = "link destination (to) must be defined" :: errors
                    }
                  } else if (mdType.get == FileMetaData) {
                    if (mimeType.isEmpty) {
                      errors = "file's media type (mimeType) must be defined" :: errors
                    }
                    if (data.isEmpty && text.isEmpty) {
                      errors = "file's content must be defined" :: errors
                    }
                  } else if (mdType.get != ObjectMetaData && mdType.get != DeletedMetaData) {
                    errors = s"infoton's kind (type) isn't recognized" :: errors
                  }

                  if (errors.isEmpty) Right(())
                  else Left(errors.mkString(s"path [$path] failed due to:\n\t", "\n\t", ""))
              }
              require(errs.isEmpty, errs.mkString("overwrites API failed to validate the request.\n\n", "\n", ""))

              val timeContext = req.attrs.get(Attrs.RequestReceivedTimestamp)
              val currentTime = timeContext.fold(DateTime.now(DateTimeZone.UTC))(tc => new DateTime(tc))
              val modifier = req.attrs(Attrs.UserName)

              enforceForceIfNeededAndReturnMetaFieldsInfotons(infotonsMap, modifier, currentTime, forceEnabled = true).flatMap { metaFields =>
                val infotonsWithoutFields = metaDataMap.keySet.filterNot(infotonsMap.keySet.apply) //meaning FileInfotons without extra data...

                val allInfotons = infotonsMap.toVector.map {
                  case (path, fields) => {
                    require(path.nonEmpty, "path cannot be empty!")
                    val escapedPath = escapePath(path)
                    InfotonValidator.validateValueSize(fields)
                    val fs = fields.map {
                      case (fk, vs) => fk.internalKey -> vs
                    }
                    infotonFromMaps(cmwHostsSet, escapedPath, Some(fs), metaDataMap.get(escapedPath), currentTime, modifier)
                  }
                }  ++ infotonsWithoutFields.map(
                p => infotonFromMaps(cmwHostsSet, p, None, metaDataMap.get(p), currentTime, modifier)
                ) ++ metaFields

                //logger.info(s"infotonsToPut: ${allInfotons.collect { case o: ObjectInfoton => o.toString }.mkString("[", ",", "]")}")

                val (metaInfotons, infotonsToPut) = allInfotons.partition(_.systemFields.path.startsWith("/meta/"))

                val f = crudService.putInfotons(metaInfotons)
                crudService
                  .putOverwrites(infotonsToPut)
                  .flatMap { b =>
                    f.map {
                      case true if b =>
                        feedbacks match {
                          case Nil       => Ok(Json.obj("success" -> true))
                          case List(msg) => Ok(Json.obj("success" -> true, "message" -> msg))
                          case multiples => Ok(Json.obj("success" -> true, "messages" -> multiples))
                        }
                      case _ => BadRequest(Json.obj("success" -> false))
                    }
                  }
                  .recover {
                    case err: Throwable => {
                      logger.error(
                        s"bad data received: ${err.getMessage}: ${req.body.asBytes().fold("NOTHING")(_.utf8String)}", err
                      )
                      wsutil.exceptionToResponse(err)
                    }
                  } //TODO: above recover might be unneeded
              }
            }
          }
          .recover {
            case err: Throwable => {
              logger.error(s"bad data received: ${err.getMessage}: ${req.body.asBytes().fold("NOTHING")(_.utf8String)}", err)
              wsutil.exceptionToResponse(err)
            }
          }
      }.recover {
        case err: Throwable => {
          logger.error(s"bad data received: ${err.getMessage}: ${req.body.asBytes().fold("NOTHING")(_.utf8String)}", err)
          Future.successful(wsutil.exceptionToResponse(err))
        }
      }.get
    }
  }

  private def parseRDF(req: Request[RawBuffer],
                       skipValidation: Boolean = false,
                       isOverwrite: Boolean = false): Future[ParsingResponse] = {

    import cmwell.web.ld.service.WriteService._

    val inputStream = req.body.asBytes().getOrElse(throw new RuntimeException("cant find valid content in body of request")).
      iterator.asInputStream

    val timeContext = req.attrs.get(Attrs.RequestReceivedTimestamp)

    req.getQueryString("format") match {
      case Some(f) =>
        handleFormatByFormatParameter(
          cmwellRDFHelper,
          crudService,
          authUtils,
          inputStream,
          Some(List[String](f)),
          req.contentType,
          authUtils.extractTokenFrom(req),
          skipValidation,
          isOverwrite,
          timeContext
        )
      case None =>
        handleFormatByContentType(cmwellRDFHelper,
                                  crudService,
                                  authUtils,
                                  inputStream,
                                  req.contentType,
                                  authUtils.extractTokenFrom(req),
                                  skipValidation,
                                  isOverwrite,
                                  timeContext)
    }
  }

  def enforceForceIfNeededAndReturnMetaFieldsInfotons(
    allInfotons: Map[String, Map[DirectFieldKey, Set[FieldValue]]],
    modifier: String,
    currentTime: DateTime,
    forceEnabled: Boolean = false,
    debugLog: Boolean = false
  )(implicit ec: ExecutionContext): Future[Vector[Infoton]] = {

    def getMetaFields(fields: Map[DirectFieldKey, Set[FieldValue]]) = collector(fields.iterator) {
      case (fk, fvs) => {
        val newTypes = fvs.map(FieldValue.prefixByType)

        val f: Set[Char] => Future[Option[Infoton]] = (types: Set[Char]) => {
          val chars = newTypes.diff(types)
          if (debugLog) {
            logger.info(s"getMetaFields.f: ${newTypes.mkString("[", ",", "]")} diff ${types
              .mkString("[", ",", "]")} = ${chars.mkString("[", ",", "]")}")
          }
          if (chars.isEmpty) Future.successful(None)
          else {
            require(
              forceEnabled || types.size != 1,
              "adding a new type to an existing field is probably wrong, and fails implicitly. " +
                "in case you are sure you know what you are doing, please provide `force` parameter in your query. " +
                "though you should be aware this may result in permanent system-wide performance downgrade " +
                "related to all the enhanced fields you supply when using `force`. " +
                s"(failed for field: ${fk.externalKey} and type/s: [${chars
                  .mkString(",")}] out of infotons: [${allInfotons.keySet.mkString(",")}])"
            )
            typesCaches.update(fk, chars).map { _ =>
              Some(
                infotonFromMaps(Set.empty,
                                fk.infoPath,
                                Some(Map("mang" -> chars.map(c => FString(c.toString, None, None): FieldValue))),
                                None,
                                currentTime,
                                modifier)
              )
            }
          }
        }

        typesCaches.get(fk, Some(newTypes)).transformWith {
          case Failure(_: NoSuchElementException) => f(Set.empty)
          case Success(types)                     => f(types)
          case Failure(error) =>
            Future.failed(ServerComponentNotAvailableException("ingest failed during types resolution", error))
        }
      }
    }

    val infotons = allInfotons //.filterKeys(!_.matches("/meta/(ns|nn).*"))

    if (infotons.isEmpty) Future.successful(Vector.empty)
    else {
      val aggFields = infotons.values.reduce[Map[DirectFieldKey, Set[FieldValue]]] {
        case (m1, m2) =>
          val mm = m2.withDefaultValue(Set.empty[FieldValue])
          m2 ++ m1.map {
            case (k, vs) => k -> (vs ++ mm(k))
          }
      }
      if (debugLog) {
        logger.info(s"enforcing type contraint on: $aggFields")
      }
      getMetaFields(aggFields)
    }
  }

  def printFailedIngests(message: String, allReqPaths: => Iterable[String], paths: => Iterable[String]): PartialFunction[Try[Boolean], Unit] = {
    case Failure(e) => redlog.info(s"$message. The whole paths of this request are: [${allReqPaths.mkString(",")}]. " +
      s"The issue was with at least one of those paths [${paths.mkString(",")}]. The exception was: ", e)
  }

  /**
    *
    * @return
    */
  def handlePostRDF(req: Request[RawBuffer],
                    skipValidation: Boolean = false): (Future[Result], Future[(Iterable[String], Seq[(String, String)])]) = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val timeContext = req.attrs.get(Attrs.RequestReceivedTimestamp)
    val modifier = req.attrs(Attrs.UserName)
    //(parsed infotons paths, headers)
    val p = Promise[(Iterable[String], Seq[(String, String)])]()
    lazy val id = cmwell.util.numeric.Radix64.encodeUnsigned(req.id)
    val debugLog = req.queryString.keySet("debug-log")
    val addDebugHeader: Result => Result = { res =>
      if (debugLog) res.withHeaders("X-CM-WELL-LOG-ID" -> id)
      else res
    }

    Try {
      parseRDF(req, skipValidation)
        .flatMap {
          case pRes @ ParsingResponse(infotonsMap,
                                      metaDataMap,
                                      cmwHostsSet,
                                      tmpDeleteMap,
                                      deleteValsMap,
                                      deletePaths,
                                      atomicUpdates,
                                      feedbacks) => {

            lazy val feedback: Option[(String, JsValueWrapper)] = feedbacks match {
              case Nil       => None
              case List(msg) => Some("message" -> msg)
              case multiples => Some("messages" -> multiples)
            }

            if (pRes.isEmpty) {
              logger.warn(s"[$id] bad user ingest resulted in parsed response: ${pRes.toString}")
              if (debugLog) p.success(infotonsMap.keys -> Seq("X-CM-WELL-LOG-ID" -> id))
              else p.success(infotonsMap.keys -> Nil)
              Future.successful(
                addDebugHeader(
                  UnprocessableEntity(
                    s"ingested data was well formed, but is meaningless and has no affect. error logged with id [$id]."
                  )
                )
              )
            } else {

              val currentTime = timeContext.fold(DateTime.now(DateTimeZone.UTC))(tc => new DateTime(tc))

              if (debugLog) logger.info(s"[$id] ParsingResponse: ${pRes.toString}")
              enforceForceIfNeededAndReturnMetaFieldsInfotons(infotonsMap,
                                                              modifier,
                                                              currentTime,
                                                              req.getQueryString("force").isDefined,
                                                              debugLog).flatMap { metaFields =>
                if (debugLog) {
                  logger.info(s"will add mangling data for the ingest: $metaFields")
                }

                //we divide the infotons to write into 2 lists: regular writes and updates
                val (deleteMap, (upserts, regular)) = {
                  var deleteMap: Map[String, Map[String, Option[Set[FieldValue]]]] = {
                    val t1 = tmpDeleteMap.map {
                      case (path, attSetQuadTuple) => {

                        val valueSets = attSetQuadTuple.groupBy(_._1).map {
                          case (field, set) => field -> Some(set.map(z => FNull(z._2).asInstanceOf[FieldValue]))
                        }

                        prependSlash(path) -> valueSets
                      }
                    }
                    val t2 = deleteValsMap.map {
                      case (path, fields) => prependSlash(path) -> fields.view.mapValues[Option[Set[FieldValue]]](Some.apply).toMap
                    }
                    t1 ++ t2
                  }
                  //path to needToReplace (boolean)
                  val s: String => Boolean = {
                    if (req.getQueryString("replace-mode").isEmpty) deleteMap.keySet ++ deleteValsMap.keySet
                    else {
                      val x = req.getQueryString("replace-mode").get

                      if (x.isEmpty) {
                        deleteMap = infotonsMap.map {
                          case (iPath, fMap) =>
                            prependSlash(iPath) -> fMap.map {
                              case (fk, vs) =>
                                val deleteValues: Option[Set[FieldValue]] = Some(vs.map(fv => FNull(fv.quad)))
                                fk.internalKey -> deleteValues
                            }
                        }
                      } else {
                        val quadForReplacement: FieldValue = {
                          x match {
                            case "default" => FNull(None)
                            case "*"       => FNull(Some("*"))
                            case alias if !FReference.isUriRef(alias) => {

                              def optionToFNull(o: Option[String]): FNull = o match {
                                //TODO: future optimization: check replace-mode's alias before invoking jena and parsing RDF document
                                case None => throw new UnretrievableIdentifierException("The alias '" + alias +
                                  "' provided for quad as replace-mode's argument does not exist. " +
                                  "Use explicit quad URL, or register a new alias using `graphAlias` meta operation.")
                                case someURI => FNull(someURI)
                              }

                              optionToFNull(cmwellRDFHelper.getQuadUrlForAlias(alias))
                            }
                            case uri => FNull(Some(uri))
                          }
                        }

                        deleteMap = infotonsMap.map {
                          case (iPath, fMap) =>
                            prependSlash(iPath) -> fMap.map {
                              case (fk, _) => fk.internalKey -> Some(Set(quadForReplacement))
                            }
                        }
                      }
                      _ =>
                        true //in case of "replace-mode", we want to update every field provided
                    }
                  }
                  (deleteMap, infotonsMap.partition { case (k, _) => s(prependSlash(k)) }) //always use the "/*" notations
                }

                val infotonsWithoutFields = metaDataMap.keySet.filterNot(infotonsMap.keySet(_)) //meaning FileInfotons without extra data...

                val infotonsToPut = (regular.toVector.map {
                  case (path, fields) => {
                    require(path.nonEmpty, "path cannot be empty!")
                    val escapedPath = escapePath(path)
                    InfotonValidator.validateValueSize(fields)
                    val fs = fields.map {
                      case (fk, vs) => fk.internalKey -> vs
                    }
                    infotonFromMaps(cmwHostsSet, escapedPath, Some(fs), metaDataMap.get(escapedPath), currentTime, modifier)
                  }
                }) ++ infotonsWithoutFields.map(
                  p => infotonFromMaps(cmwHostsSet, p, None, metaDataMap.get(p), currentTime, modifier)
                ) ++ metaFields

                val infotonsToUpsert = upserts.toList.map {
                  case (path, fields) => {
                    require(path.nonEmpty, "path cannot be empty!")
                    val escapedPath = escapePath(path)
                    InfotonValidator.validateValueSize(fields)
                    val fs = fields.map {
                      case (fk, vs) => fk.internalKey -> vs
                    }
                    infotonFromMaps(cmwHostsSet, escapedPath, Some(fs), metaDataMap.get(escapedPath), currentTime, modifier)
                  }
                }

                if (req.getQueryString("dry-run").isDefined) {
                  p.success(infotonsMap.keys -> Seq.empty)
                  val jres = feedback.fold(Json.obj("success" -> true, "dry-run" -> true))(
                    Json.obj("success" -> true, "dry-run" -> true, _)
                  )
                  Future(addDebugHeader(Ok(jres)))
                } else if (deletePaths.contains("/") || deleteMap.keySet("/")) {
                  p.success(infotonsMap.keys -> Seq.empty)
                  Future.successful(
                    addDebugHeader(
                      BadRequest(
                        Json.obj("success" -> false, "message" -> "Deleting Root Infoton does not make sense!")
                      )
                    )
                  )
                } else {

                  val isPriorityWrite = req.getQueryString("priority").isDefined

                  val tracking = req.getQueryString("tracking")
                  val blocking = req.getQueryString("blocking")

                  // Process Tracking / Blocking
                  val tidOptFut = opfut(
                    if (tracking.isDefined || blocking.isDefined) Some({
                      val allPaths = {
                        val b = Set.newBuilder[String]
                        b ++= deleteMap.keySet
                        b ++= upserts.keySet
                        b ++= regular.keySet
                        b.result()
                      }.filterNot(_.contains("/meta/ns"))

                      val actorId = cmwell.util.string.Hash.crc32(cmwell.util.numeric.toIntegerBytes(pRes.##))
                      TrackingUtil().spawn(actorId, allPaths, timeContext.get)
                    })
                    else None
                  )

                  tidOptFut.flatMap { arAndTidOpt =>
                    val (arOpt, tidOpt) = arAndTidOpt.map(_._1) -> arAndTidOpt.map(_._2)

                    val tidHeaderOpt = tidOpt.map("X-CM-WELL-TID" -> _.token)
                    p.success(infotonsMap.keys -> tidHeaderOpt.toSeq)

                    require(!infotonsToUpsert.exists(i => infotonsToPut.exists(_.systemFields.path == i.systemFields.path)),
                            s"write commands & upserts from same document cannot operate on the same path")
                    val secondStagePaths: Set[String] =
                      infotonsToUpsert.view.map(_.systemFields.path).to(Set).union(infotonsToPut.view.map(_.systemFields.path).to(Set))

                    val (dontTrack, track) = deletePaths.partition(secondStagePaths.apply)
                    require(dontTrack.forall(!atomicUpdates.contains(_)),
                            s"atomic updates cannot operate on multiple actions in a single ingest.")
                    require(
                      infotonsToUpsert.union(infotonsToPut).forall(_.kind != "DeletedInfoton"),
                      s"Writing a DeletedInfoton does not make sense. use proper delete API instead. malformed paths: ${infotonsToUpsert
                        .union(infotonsToPut)
                        .collect {
                          case DeletedInfoton(systemFields) => systemFields.path
                        }
                        .mkString("[", ",", "]")}"
                    )

                    val to = tidOpt.map(_.token)
                    val d1 = crudService.deleteInfotons(dontTrack.map((_, None)), modifier, isPriorityWrite = isPriorityWrite)
                      .andThen(printFailedIngests("delete (dontTrack) infotons failed", infotonsMap.keys, dontTrack))
                    val d2 = crudService.deleteInfotons(track.map((_, None)), modifier, to, atomicUpdates, isPriorityWrite)
                      .andThen(printFailedIngests("delete (track) failed", infotonsMap.keys, track))

                    d1.zip(d2).flatMap {
                      case (b01, b02) =>
                        val f1 =
                          crudService.upsertInfotons(infotonsToUpsert, deleteMap, modifier, to, atomicUpdates, isPriorityWrite)
                            .andThen(printFailedIngests("upsert infotons failed", infotonsMap.keys, infotonsToUpsert.map(_.systemFields.path)))
                        val f2 = crudService.putInfotons(infotonsToPut, to, atomicUpdates, isPriorityWrite)
                            .andThen(printFailedIngests("put infotons failed", infotonsMap.keys, infotonsToPut.map(_.systemFields.path)))
                        f1.zip(f2).flatMap {
                          case (b1, b2) =>
                            if (b01 && b02 && b1 && b2) {
                              val jres = feedback.fold(Json.obj("success" -> true))(Json.obj("success" -> true, _))
                              blocking
                                .fold(Future.successful(addDebugHeader(Ok(jres).withHeaders(tidHeaderOpt.toSeq: _*)))) {
                                  _ =>
                                    import akka.pattern.ask
                                    val blockingFut =
                                      arOpt.get.?(SubscribeToDone)(timeout = 5.minutes).mapTo[Seq[PathStatus]]
                                    blockingFut
                                      .map { data =>
                                        val payload = {
                                          //TODO: add `feedback` data to response
                                          val formatter = getFormatter(req,
                                                                       formatterManager,
                                                                       defaultFormat = "ntriples",
                                                                       withoutMeta = true)
                                          val payload = BagOfInfotons(data.map(pathStatusAsInfoton))
                                          formatter.render(payload)
                                        }
                                        addDebugHeader(Ok(payload).withHeaders(tidHeaderOpt.toSeq: _*))
                                      }
                                      .recover {
                                        case t: Throwable =>
                                          logger.error("Failed to use _in with Blocking, because", t)
                                          addDebugHeader(
                                            ServiceUnavailable(
                                              Json.obj("success" -> false,
                                                       "message" -> "Blocking is currently unavailable")
                                            )
                                          )
                                      }
                                }
                            } else Future.successful(addDebugHeader(InternalServerError(Json.obj("success" -> false))))
                        }
                    }
                  }
                }
              }
            }
          }
        }
        .recover {
          case err: Throwable => {
            logger.error(s"Bad data received or another error. The body is: ${req.body.asBytes().fold("NOTHING")(_.utf8String)}. The exception was: ", err)
            p.tryFailure(err)
            addDebugHeader(wsutil.exceptionToResponse(err))
          }
        }
    }.recover {
      case ex: Throwable => {
        logger.error("handlePostRDF failed", ex)
        p.tryFailure(ex)
        Future.successful(addDebugHeader(exceptionToResponse(ex)))
      }
    }.get -> p.future
  }

  def setZeroTimeForInfotons(v: Vector[Infoton]): Vector[Infoton] = {

    ???
  }

  /**
    *
    * @return
    */
  def handlePostWrapped(req: Request[RawBuffer],
                        skipValidation: Boolean = false,
                        setZeroTime: Boolean = false): Future[Result] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    if (req.getQueryString("dry-run").isDefined)
      Future.successful(
        BadRequest(Json.obj("success" -> false, "error" -> "dry-run is not implemented for wrapped requests."))
      )
    else {
      val charset = req.contentType match {
        case Some(contentType) =>
          contentType.lastIndexOf("charset=") match {
            case i if i != -1 => contentType.substring(i + 8).trim
            case _            => "utf-8"
          }
        case _ => "utf-8"
      }
      val timeContext = req.attrs.get(Attrs.RequestReceivedTimestamp)
      val modifier = req.attrs(Attrs.UserName)

      req.body.asBytes() match {
        case Some(bs) => {
          val body = bs.utf8String
          val vec: Option[Vector[Infoton]] = JsonEncoder.decodeBagOfInfotons(body).map(_.infotons.toVector) match {
            case s @ Some(_) => s
            case None        => JsonEncoder.decodeInfoton(body).map(Vector(_))
          }

          val isPriorityWrite = req.getQueryString("priority").isDefined

          vec match {
            case Some(infotonVec) => {

              val v = infotonVec.map(i => i.copyInfoton(i.systemFields.copy(lastModifiedBy = modifier)))

              if (skipValidation || v.forall(i => InfotonValidator.isInfotonNameValid(normalizePath(i.systemFields.path)))) {
                val unauthorizedPaths =
                  authUtils.filterNotAllowedPaths(v.map(_.systemFields.path), PermissionLevel.Write, authUtils.extractTokenFrom(req))
                if (unauthorizedPaths.isEmpty) {
                  Try(v.foreach { i =>
                    if (i.fields.isDefined) InfotonValidator.validateValueSize(i.fields.get)
                  }) match {
                    case Success(_) => {

                      //FIXME: following code is super ugly and hacky... do something about it. please!
                      val infotonsMap = v.collect {
                        case i if i.fields.isDefined =>
                          i.systemFields.path -> i.fields.get.map {
                            case (fieldName, valueSet) =>
                              (FieldKeyParser.fieldKey(fieldName) match {
                                case Success(Right(d)) => d
                                case Success(Left(fk)) => {
                                  Try[DirectFieldKey] {
                                    val (f, l) = Await.result(
                                      FieldKey.resolve(fk, cmwellRDFHelper, timeContext).map {
                                        case PrefixFieldKey(first, last, _) => first -> last
                                        case URIFieldKey(first, last, _)    => first -> last
                                        case unknown => {
                                          throw new IllegalStateException(s"unknown field key [$unknown]")
                                        }
                                      },
                                      10.seconds
                                    )
                                    HashedFieldKey(f, l)
                                  }.recover {
                                    case _ if fk.isInstanceOf[UnresolvedPrefixFieldKey] => NnFieldKey(fk.externalKey)
                                  }.get
                                }
                                case Failure(e) => throw e
                              }) -> valueSet
                          }
                      }.toMap

                      val currentTime = timeContext.fold(DateTime.now(DateTimeZone.UTC))(tc => new DateTime(tc))
                      enforceForceIfNeededAndReturnMetaFieldsInfotons(infotonsMap,
                                                                      modifier,
                                                                      currentTime,
                                                                      req.getQueryString("force").isDefined).flatMap {
                        metaFields =>
                          val infotonsToPut = (if (setZeroTime) v.map {
                                                 case i: ObjectInfoton => i.copy(i.systemFields.copy(lastModified = zeroTime))
                                                 case i: FileInfoton => {
                                                   logger.warn(s"FileInfoton ${i.systemFields.path} with ZERO time inserted")
                                                   i.copy(i.systemFields.copy(lastModified = zeroTime))
                                                 }
                                                 case i: LinkInfoton => {
                                                   logger.warn(s"LinkInfoton ${i.systemFields.path} with ZERO time inserted")
                                                   i.copy(i.systemFields.copy(lastModified = zeroTime))
                                                 }
                                                 case i: Infoton => i //to prevent compilation warnings...
                                               } else v) ++ metaFields
                          if (req.getQueryString("replace-mode").isEmpty)
                            crudService
                              .putInfotons(infotonsToPut, isPriorityWrite = isPriorityWrite)
                              .map(b => Ok(Json.obj("success" -> b)))
                          else {
                            val d: Map[String, Set[String]] = infotonsToPut.collect {
                              case i if i.fields.isDefined => prependSlash(i.systemFields.path) -> i.fields.get.keySet
                            } toMap;
                            crudService
                              .upsertInfotons(infotonsToPut.toList,
                                              d.view.mapValues(_.map(_ -> None).toMap).toMap,
                                              modifier,
                                              isPriorityWrite = isPriorityWrite)
                              .map(b => Ok(Json.obj("success" -> b)))
                          }
                      }
                    }
                    case Failure(e) => {
                      logger.error("handlePostWrapped failed", e)
                      Future.successful(exceptionToResponse(e))
                    }
                  }
                } else
                  Future(
                    Forbidden(
                      Json.obj("success" -> false, "message" -> unauthorizedPaths.mkString("\n\t", "\n\t", "\n\n"))
                    )
                  )
              } else
                Future(
                  BadRequest(Json.obj("success" -> false, "error" -> "one or more infotons in request are not valid"))
                )
            }
            case None => Future(BadRequest(Json.obj("success" -> false)))
          }
        }
        case None => Future(BadRequest(Json.obj("success" -> false, "error" -> "empty content")))
      }
    }
  }
}
