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

import actions.RequestMonitor
import akka.util.ByteString
import cmwell.domain._
import cmwell.tracking._
import cmwell.util.concurrent._
import cmwell.util.collections.opfut
import cmwell.util.formats.JsonEncoder
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.exceptions.UnretrievableIdentifierException
import cmwell.web.ld.util.LDFormatParser.ParsingResponse
import cmwell.web.ld.util._
import cmwell.ws.Settings
import cmwell.ws.util.FieldKeyParser
import com.typesafe.scalalogging.LazyLogging
import logic.{CRUDServiceFS, InfotonValidator}
import play.api.libs.json._
import play.api.mvc._
import security.{AuthUtils, PermissionLevel}
import wsutil._
import ld.cmw.PassiveFieldTypesCache
import org.joda.time.{DateTime, DateTimeZone}
import javax.inject._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.language.postfixOps
import scala.util._

@Singleton
class InputHandler  @Inject() (ingestPushback: IngestPushback) extends Controller with LazyLogging {

  val bo1 = collection.breakOut[List[Infoton],String,Set[String]]
  val bo2 = collection.breakOut[Vector[Infoton],String,Set[String]]

  /**
   *
   * @param format
   * @return
   */
  def handlePost(format: String = "") = ingestPushback.async(parse.raw) { implicit req =>
    RequestMonitor.add("in", req.path, req.rawQueryString, req.body.asBytes().fold("")(_.utf8String))
    val resp = if ("jsonw" == format.toLowerCase) handlePostWrapped(req) -> Future.successful(Seq.empty[(String,String)]) else handlePostRDF(req)
    resp._2.flatMap { headers =>
      keepAliveByDrippingNewlines(resp._1,headers)
    }.recover(errorHandler)
  }

  /**
   *
   * @param path
   * @return
   */
  private def escapePath(path : String) : String = {
    path.replace(" ", "%20")
  }

  def handlePostForDCOverwrites =  ingestPushback.async(parse.raw) { implicit req =>
    val tokenOpt = AuthUtils.extractTokenFrom(req)
    if (!AuthUtils.isOperationAllowedForUser(security.Overwrite, tokenOpt, evenForNonProdEnv = true))
      Future.successful(Forbidden("not authorized"))
    else {
      Try {
        parseRDF(req, false, true).flatMap {
          case ParsingResponse(infotonsMap, metaDataMap, cmwHostsSet, tmpDeleteMap, deleteValsMap, deletePaths, atomicUpdates) => {

            require(tmpDeleteMap.isEmpty && deleteValsMap.isEmpty && deletePaths.isEmpty && atomicUpdates.isEmpty, "can't use meta operations here! this API is used internaly, and only for overwrites!")
            require(metaDataMap.forall {
              case (path, MetaData(mdType, date, data, text, mimeType, linkType, linkTo, dataCenter, indexTime)) => {
                indexTime.isDefined &&
                  dataCenter.isDefined &&
                  dataCenter.get != Settings.dataCenter &&
                  mdType.isDefined &&
                  ((mdType.get == LinkMetaData && linkType.isDefined && linkTo.isDefined) ||
                    (mdType.get == FileMetaData && mimeType.isDefined && (data.isDefined || text.isDefined)) ||
                    (mdType.get == ObjectMetaData) ||
                    (mdType.get == DeletedMetaData))
              }
            }, "in overwrites API all meta data must be present! no implicit inference is allowed. (every infoton must have all relevant system fields added)")
            enforceForceIfNeededAndReturnMetaFieldsInfotons(infotonsMap, true).flatMap { metaFields =>
              val infotonsWithoutFields = metaDataMap.keySet.filterNot(infotonsMap.keySet.apply) //meaning FileInfotons without extra data...

              val allInfotons = (infotonsMap.toVector map {
                case (path, fields) => {
                  require(path.nonEmpty, "path cannot be empty!")
                  val escapedPath = escapePath(path)
                  InfotonValidator.validateValueSize(fields)
                  val fs = fields.map {
                    case (fk, vs) => fk.internal -> vs
                  }
                  infotonFromMaps(cmwHostsSet, escapedPath, Some(fs), metaDataMap.get(escapedPath))
                }
              }) ++ infotonsWithoutFields.map(p => infotonFromMaps(cmwHostsSet, p, None, metaDataMap.get(p))) ++ metaFields

              //logger.info(s"infotonsToPut: ${allInfotons.collect { case o: ObjectInfoton => o.toString }.mkString("[", ",", "]")}")

              val (metaInfotons, infotonsToPut) = allInfotons.partition(_.path.startsWith("/meta/"))

              val f = CRUDServiceFS.putInfotons(metaInfotons)
              CRUDServiceFS.putOverwrites(infotonsToPut).flatMap { b =>
                f.map {
                  case true if b => Ok(Json.obj("success" -> true))
                  case _ => BadRequest(Json.obj("success" -> false))
                }
              }.recover {
                case err: Throwable => {
                  logger.error(s"bad data received: ${err.getMessage}: ${req.body.asBytes().fold("NOTHING")(_.utf8String)}")
                  wsutil.exceptionToResponse(err)
                }
              } //TODO: above recover might be unneeded
            }
          }
        }.recover {
          case err: Throwable => {
            logger.error(s"bad data received: ${err.getMessage}: ${req.body.asBytes().fold("NOTHING")(_.utf8String)}")
            wsutil.exceptionToResponse(err)
          }
        }
      }.recover {
        case err: Throwable => {
          logger.error(s"bad data received: ${err.getMessage}: ${req.body.asBytes().fold("NOTHING")(_.utf8String)}")
          Future.successful(wsutil.exceptionToResponse(err))
        }
      }.get
    }
  }

  private def parseRDF(req: Request[RawBuffer], skipValidation: Boolean = false, isOverwrite: Boolean = false): Future[ParsingResponse] = {
    import java.io.ByteArrayInputStream

    import cmwell.web.ld.service.WriteService._

    val bais = req.body.asBytes() match {
      //FIXME: quick inefficient hack (there should be a better way to consume body as InputStream)
      case Some(bs) => new ByteArrayInputStream(bs.toArray[Byte])//.asByteBuffer.array())
      case _ => throw new RuntimeException("cant find valid content in body of request")
    }

    req.getQueryString("format") match {
      case Some(f) => handleFormatByFormatParameter(bais, Some(List[String](f)), req.contentType, AuthUtils.extractTokenFrom(req), skipValidation, isOverwrite)
      case None => handleFormatByContentType(bais, req.contentType, AuthUtils.extractTokenFrom(req), skipValidation, isOverwrite)
    }
  }


  def enforceForceIfNeededAndReturnMetaFieldsInfotons(allInfotons: Map[String, Map[DirectFieldKey, Set[FieldValue]]], forceEnabled: Boolean = false)
                                                     (implicit ec: ExecutionContext): Future[Vector[Infoton]] = {

    def getMetaFields(fields: Map[DirectFieldKey, Set[FieldValue]]) = collector(fields) {
      case (fk, fvs) => {
        val newTypes = fvs.map(FieldValue.prefixByType)
        PassiveFieldTypesCache.get(fk,Some(newTypes)).flatMap { types =>
          val chars = newTypes diff types
          if (chars.isEmpty) Future.successful(None)
          else {
            require(forceEnabled || types.size != 1,
              "adding a new type to an existing field is probably wrong, and fails implicitly. " +
                "in case you are sure you know what you are doing, please provide `force` parameter in your query. " +
                "though you should be aware this may result in permanent system-wide performance downgrade " +
                "related to all the enhanced fields you supply when using `force`. " +
                s"(failed for field: ${fk.externalKey} and type/s: [${chars.mkString(",")}] out of infotons: [${allInfotons.keySet.mkString(",")}])")
            PassiveFieldTypesCache.update(fk, chars).map { _ =>
              Some(infotonFromMaps(
                Set.empty,
                fk.infoPath,
                Some(Map("mang" -> chars.map(c => FString(c.toString, None, None): FieldValue))),
                None))
            }
          }
        }
      }
    }

    val infotons = allInfotons.filterKeys(!_.matches("/meta/(ns|nn).*"))

    if(infotons.isEmpty) Future.successful(Vector.empty)
    else {
      val aggFields = infotons.values.reduce[Map[DirectFieldKey,Set[FieldValue]]] { case (m1, m2) =>
        val mm = m2.withDefaultValue(Set.empty[FieldValue])
        m2 ++ m1.map {
          case (k, vs) => k -> (vs ++ mm(k))
        }
      }
      getMetaFields(aggFields)
    }
  }

  /**
   *
   * @return
   */
  def handlePostRDF(req: Request[RawBuffer], skipValidation: Boolean = false): (Future[Result],Future[Seq[(String,String)]]) = {

    val now = System.currentTimeMillis()
    val p = Promise[Seq[(String,String)]]()

    Try {
      parseRDF(req,skipValidation).flatMap {
        case pRes@ParsingResponse (infotonsMap, metaDataMap, cmwHostsSet, tmpDeleteMap, deleteValsMap, deletePaths, atomicUpdates) => {
          logger.trace("ParsingResponse: " + pRes.toString)

          enforceForceIfNeededAndReturnMetaFieldsInfotons(infotonsMap,req.getQueryString("force").isDefined).flatMap { metaFields =>

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
                val t2 = deleteValsMap.map { case (path, fields) => prependSlash(path) -> fields.mapValues[Option[Set[FieldValue]]](Some.apply) }
                t1 ++ t2
              }
              //path to needToReplace (boolean)
              val s: String => Boolean = {
                if (req.getQueryString("replace-mode").isEmpty) deleteMap.keySet ++ deleteValsMap.keySet
                else {
                  val x = req.getQueryString("replace-mode").get

                  if(x.isEmpty) {
                    deleteMap = infotonsMap.map {
                      case (iPath, fMap) => prependSlash(iPath) -> fMap.map {
                        case (fk, vs) =>
                          val deleteValues: Option[Set[FieldValue]] = Some(vs.map(fv => FNull(fv.quad)))
                          fk.internal -> deleteValues
                      }
                    }
                  }
                  else {
                    val quadForReplacement: FieldValue = {
                      x match {
                        case "default" => FNull(None)
                        case "*" => FNull(Some("*"))
                        case alias if !FReference.isUriRef(alias) => CMWellRDFHelper.getQuadUrlForAlias(alias) match {
                          //TODO: future optimization: check replace-mode's alias before invoking jena and parsing RDF document
                          case None => throw new UnretrievableIdentifierException(s"The alias '$alias' provided for quad as replace-mode's argument does not exist. Use explicit quad URL, or register a new alias using `graphAlias` meta operation.")
                          case someURI => FNull(someURI)
                        }
                        case uri => FNull(Some(uri))
                      }
                    }

                    deleteMap = infotonsMap map { case (iPath, fMap) => prependSlash(iPath) -> fMap.map { case (fk, _) => fk.internal -> Some(Set(quadForReplacement)) } }
                  }
                  _ => true //in case of "replace-mode", we want to update every field provided
                }
              }
              (deleteMap, infotonsMap.partition { case (k, _) => s(prependSlash(k)) }) //always use the "/*" notations
            }

            val infotonsWithoutFields = metaDataMap.keySet.filterNot(infotonsMap.keySet(_)) //meaning FileInfotons without extra data...

            val infotonsToPut = (regular.toVector map {
              case (path, fields) => {
                require(path.nonEmpty, "path cannot be empty!")
                val escapedPath = escapePath(path)
                InfotonValidator.validateValueSize(fields)
                val fs = fields.map {
                  case (fk, vs) => fk.internal -> vs
                }
                infotonFromMaps(cmwHostsSet, escapedPath, Some(fs), metaDataMap.get(escapedPath))
              }
            }) ++ infotonsWithoutFields.map(p => infotonFromMaps(cmwHostsSet, p, None, metaDataMap.get(p))) ++ metaFields

            val infotonsToUpsert = upserts.toList map {
              case (path, fields) => {
                require(path.nonEmpty, "path cannot be empty!")
                val escapedPath = escapePath(path)
                InfotonValidator.validateValueSize(fields)
                val fs = fields.map {
                  case (fk, vs) => fk.internal -> vs
                }
                infotonFromMaps(cmwHostsSet, escapedPath, Some(fs), metaDataMap.get(escapedPath))
              }
            }

            if (req.getQueryString("dry-run").isDefined)
              Future(Ok(Json.obj("success" -> true, "dry-run" -> true)))
            else {

              val tracking = req.getQueryString("tracking")
              val blocking = req.getQueryString("blocking")

              // Process Tracking / Blocking
              val tidOptFut = opfut(
                if(tracking.isDefined || blocking.isDefined) Some({
                  val allPaths = {
                    val b = Set.newBuilder[String]
                    b ++= deleteMap.keySet
                    b ++= upserts.keySet
                    b ++= regular.keySet
                    b.result()
                  }.filterNot(_.contains("/meta/ns"))

                  val actorId = cmwell.util.string.Hash.crc32(cmwell.util.numeric.toIntegerBytes(pRes.##))
                  TrackingUtil().spawn(actorId, allPaths, now)
                }) else None
              )

              tidOptFut.flatMap { arAndTidOpt =>
                val (arOpt, tidOpt) = arAndTidOpt.map(_._1) -> arAndTidOpt.map(_._2)

                val tidHeaderOpt = tidOpt.map("X-CM-WELL-TID" -> _.token)
                p.success(tidHeaderOpt.toSeq)

                require(!infotonsToUpsert.exists(i => infotonsToPut.exists(_.path == i.path)),s"write commands & upserts from same document cannot operate on the same path")
                val secondStagePaths: Set[String] = infotonsToUpsert.map(_.path)(bo1) union infotonsToPut.map(_.path)(bo2)

                val (dontTrack,track) = deletePaths.partition(secondStagePaths.apply)
                require(dontTrack.forall(!atomicUpdates.contains(_)),s"atomic updates cannot operate on multiple actions in a single ingest.")

                val to = tidOpt.map(_.token)
                val d1 = CRUDServiceFS.deleteInfotons(dontTrack.map(_ -> None))
                val d2 = CRUDServiceFS.deleteInfotons(track.map(_ -> None),to,atomicUpdates)

                d1.zip(d2).flatMap { case (b01,b02) =>
                  val f1 = CRUDServiceFS.upsertInfotons(infotonsToUpsert, deleteMap, to, atomicUpdates)
                  val f2 = CRUDServiceFS.putInfotons(infotonsToPut, to, atomicUpdates)
                  f1.zip(f2).flatMap { case (b1, b2) =>
                    if (b01 && b02 && b1 && b2)
                      blocking.fold(Future.successful(Ok(Json.obj("success" -> true)).withHeaders(tidHeaderOpt.toSeq: _*))) { _ =>
                        import akka.pattern.ask
                        val blockingFut = arOpt.get.?(SubscribeToDone)(timeout = 5.minutes).mapTo[Seq[PathStatus]]
                        blockingFut.map { data =>
                          val payload = {
                            val formatter = getFormatter(req, defaultFormat = "ntriples", withoutMeta = true)
                            val payload = BagOfInfotons(data map pathStatusAsInfoton)
                            formatter render payload
                          }
                          Ok(payload).withHeaders(tidHeaderOpt.toSeq: _*)
                        }.recover {
                          case t: Throwable =>
                            logger.error("Failed to use _in with Blocking, because", t)
                            ServiceUnavailable(Json.obj("success" -> false, "message" -> "Blocking is currently unavailable"))
                        }
                      }
                    else Future.successful(InternalServerError(Json.obj("success" -> false)))
                  }
                }
              }
            }
          }
        }
      }.recover {
        case err: Throwable => {
          logger.error(s"bad data received: ${req.body.asBytes().fold("NOTHING")(_.utf8String)}")
          p.tryFailure(err)
          wsutil.exceptionToResponse(err)
        }
      }
    }.recover {
      case ex: Throwable => {
        logger.error("handlePostRDF failed",ex)
        p.tryFailure(ex)
        Future.successful(exceptionToResponse(ex))
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
  def handlePostWrapped(req: Request[RawBuffer], skipValidation: Boolean = false, setZeroTime: Boolean = false): Future[Result] = {

    if (req.getQueryString("dry-run").isDefined)  Future.successful(BadRequest(Json.obj("success" -> false, "error" -> "dry-run is not implemented for wrapped requests.")))
    else {
      val charset = req.contentType match {
        case Some(contentType) => contentType.lastIndexOf("charset=") match {
          case i if i != -1 => contentType.substring(i + 8).trim
          case _ => "utf-8"
        }
        case _ => "utf-8"
      }

      req.body.asBytes() match {
        case Some(bs) => {
          val body = bs.utf8String
          val vec: Option[Vector[Infoton]] = JsonEncoder.decodeBagOfInfotons(body).map(_.infotons.toVector) match {
            case s@Some(_) => s
            case None => JsonEncoder.decodeInfoton(body).map(Vector(_))
          }

          vec match {
            case Some(v) => {
              if (skipValidation || v.forall(i => InfotonValidator.isInfotonNameValid(normalizePath(i.path)))) {
                val unauthorizedPaths = AuthUtils.filterNotAllowedPaths(v.map(_.path), PermissionLevel.Write, AuthUtils.extractTokenFrom(req))
                if(unauthorizedPaths.isEmpty) {
                  Try(v.foreach{ i => if(i.fields.isDefined) InfotonValidator.validateValueSize(i.fields.get)}) match {
                    case Success(_) => {

                      //FIXME: following code is super ugly and hacky... do something about it. please!
                      val infotonsMap = v.collect {
                        case i if i.fields.isDefined => i.path -> i.fields.get.map{
                          case (fieldName,valueSet) => (FieldKeyParser.fieldKey(fieldName) match {
                            case Success(d: DirectFieldKey) => d
                            case Success(r: ResolvedFieldKey) => {
                              Try[DirectFieldKey]{
                                val (f, l) = Await.result(r.firstLast, 10.seconds)
                                HashedFieldKey(f,l)
                              }.recover{
                                case _ if r.isInstanceOf[PrefixFieldKey] => NnFieldKey(r.externalKey)
                              }.get
                            }
                            case Failure(e) => throw e
                          }) -> valueSet
                        }
                      }.toMap

                      enforceForceIfNeededAndReturnMetaFieldsInfotons(infotonsMap, req.getQueryString("force").isDefined).flatMap { metaFields =>
                        val infotonsToPut = (if (setZeroTime) v.map {
                          case i: ObjectInfoton => i.copy(lastModified = zeroTime)
                          case i: FileInfoton => {
                            logger.warn(s"FileInfoton ${i.path} with ZERO time inserted"); i.copy(lastModified = zeroTime)
                          }
                          case i: LinkInfoton => {
                            logger.warn(s"LinkInfoton ${i.path} with ZERO time inserted"); i.copy(lastModified = zeroTime)
                          }
                          case i: Infoton => i //to prevent compilation warnings...
                        } else v) ++ metaFields
                        if (req.getQueryString("replace-mode").isEmpty)
                          CRUDServiceFS.putInfotons(infotonsToPut).map(b => Ok(Json.obj("success" -> b)))
                        else {
                          val d: Map[String, Set[String]] = infotonsToPut collect { case i if i.fields.isDefined => prependSlash(i.path) -> i.fields.get.keySet} toMap;
                          CRUDServiceFS.upsertInfotons(infotonsToPut.toList, d.mapValues(_.map(_ -> None).toMap)).map(b => Ok(Json.obj("success" -> b)))
                        }
                      }
                    }
                    case Failure(e) => {
                      logger.error("handlePostWrapped failed",e)
                      Future.successful(exceptionToResponse(e))
                    }
                  }
                }
                else
                  Future(Forbidden(Json.obj("success" -> false, "message" -> unauthorizedPaths.mkString("\n\t","\n\t","\n\n"))))
              }
              else Future(BadRequest(Json.obj("success" -> false, "error" -> "one or more infotons in request are not valid")))
            }
            case None => Future(BadRequest(Json.obj("success" -> false)))
          }
        }
        case None => Future(BadRequest(Json.obj("success" -> false, "error" -> "empty content")))
      }
    }
  }
}
