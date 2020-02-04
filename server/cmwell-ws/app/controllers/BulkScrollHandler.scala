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

import cmwell.domain._
import cmwell.formats._
import cmwell.fts._
import cmwell.ws.{Settings, Streams}
import cmwell.ws.adt.{BulkConsumeState, ConsumeState}
import cmwell.ws.util._
import logic.CRUDServiceFS
import play.api.mvc._
import wsutil._
import javax.inject._

import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.concurrent._
import scala.util.{Failure, Success}
import com.typesafe.scalalogging.LazyLogging
import cmwell.syntaxutils._
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Streams.Flows
import cmwell.ws.util.TypeHelpers.asInt
import filters.Attrs
import play.api.http.Writeable

import scala.concurrent.duration.FiniteDuration
import scala.math.{max, min}

@Singleton
class BulkScrollHandler @Inject()(crudServiceFS: CRUDServiceFS,
                                  streams: Streams,
                                  cmwellRDFHelper: CMWellRDFHelper,
                                  formatterManager: FormatterManager,
                                  action: DefaultActionBuilder,
                                  components: ControllerComponents)(implicit ec: ExecutionContext)
    extends AbstractController(components)
    with LazyLogging
    with TypeHelpers {

  //consts
  val paginationParamsForSingleResult = PaginationParams(0, 1)
  val paginationParamsForSingleResultWithOffset = PaginationParams(1000, 1) //TODO: take default offset from configuration

  def infotonWriteable(formatter: Formatter) =
    new Writeable[Infoton](formattableToByteString(formatter), Some(formatter.mimetype))

  sealed trait RangeForConsumption
  case class CurrRangeForConsumption(from: Long, to: Long, nextTo: Option[Long]) extends RangeForConsumption
  case class NextRangeForConsumption(from: Long, to: Option[Long]) extends RangeForConsumption
  case class ThinSearchParams(pathFilter: Option[PathFilter],
                              fieldFilters: Option[FieldFilter],
                              withHistory: Boolean,
                              withDeleted: Boolean)

  def fieldsFiltersFromTimeframeAndOptionalFilters(from: Long, to: Long, ffsOpt: Option[FieldFilter]): FieldFilter =
    ffsOpt.fold(onlyTimeframeFieldFilters(from, to)) {
      case ff @ SingleFieldFilter(Should, _, _, _) =>
        MultiFieldFilter(Must, MultiFieldFilter(Must, Seq(ff)) :: getFieldFilterSeq(from, to))
      case ff: SingleFieldFilter => MultiFieldFilter(Must, ff :: getFieldFilterSeq(from, to))
      case ff @ MultiFieldFilter(Should, _) =>
        MultiFieldFilter(Must, MultiFieldFilter(Must, Seq(ff)) :: getFieldFilterSeq(from, to))
      case ff: MultiFieldFilter => MultiFieldFilter(Must, ff :: getFieldFilterSeq(from, to))
    }

  def fieldsFiltersforSortedSearchFromTimeAndOptionalFilters(from: Long, ffsOpt: Option[FieldFilter]): FieldFilter = {
    val fromFilter: FieldFilter = SingleFieldFilter(Must, GreaterThanOrEquals, "system.indexTime", Some(from.toString))
    ffsOpt.fold(fromFilter) {
      case ff @ SingleFieldFilter(Should, _, _, _) =>
        MultiFieldFilter(Must, Seq[FieldFilter](MultiFieldFilter(Must, Seq(ff)), fromFilter))
      case ff: SingleFieldFilter            => MultiFieldFilter(Must, Seq(ff, fromFilter))
      case ff @ MultiFieldFilter(Should, _) => MultiFieldFilter(Must, Seq(MultiFieldFilter(Must, Seq(ff)), fromFilter))
      case ff: MultiFieldFilter             => MultiFieldFilter(Must, Seq(ff, fromFilter))
    }
  }

  def onlyTimeframeFieldFilters(from: Long, to: Long) = {
    MultiFieldFilter(Must, getFieldFilterSeq(from, to))
  }

  def getFieldFilterSeq(from: Long, to: Long) = {
    List(
      SingleFieldFilter(Must, GreaterThanOrEquals, "system.indexTime", Some(from.toString)),
      SingleFieldFilter(Must, LessThan, "system.indexTime", Some(to.toString))
    )
  }

  type ErrorMessage = String

  def findValidRange(thinSearchParams: ThinSearchParams, from: Long, threshold: Long, timeout: FiniteDuration, debugInfo: Boolean)(
    implicit ec: ExecutionContext
  ): Future[CurrRangeForConsumption] = {

    logger.debug(s"findValidRange: from[$from], threshold[$threshold], tsp[$thinSearchParams]")

    val ThinSearchParams(pf, ffsOpt, h, d) = thinSearchParams
    val now = org.joda.time.DateTime.now().minusSeconds(30).getMillis

    def toSeed: Future[Long] = {
      val ffs = fieldsFiltersFromTimeframeAndOptionalFilters(from, now, ffsOpt)
      crudServiceFS
        .thinSearch(
          pathFilter = pf,
          fieldFilters = Option(ffs),
          paginationParams = paginationParamsForSingleResultWithOffset,
          withHistory = h,
          fieldSortParams = SortParam("system.indexTime" -> Asc),
          withDeleted = d,
          debugInfo = debugInfo
        )
        .map {
          case SearchThinResults(_, _, _, results, _) =>
            //In case that there are more than the initial seed infotons (=1000) with the same index time the "from" will be equal to the "to"
            //This will fail the FieldFilter requirements (see getFieldFilterSeq) and thus approx. 1 second is added to the "from" as the new "to"
            results.headOption.fold(now)(i => math.max(i.indexTime, math.min(now, from + 1729L))) //https://en.wikipedia.org/wiki/1729_(number)
        }
    }

    toSeed.flatMap { to =>
      logger.debug(s"findValidRange: toSeed[$to], from[$from], tsp[$thinSearchParams]")
      val searchFunction = (to: Long) => {
        val ffs = fieldsFiltersFromTimeframeAndOptionalFilters(from, to, ffsOpt)
        crudServiceFS
          .thinSearch(
            pathFilter = pf,
            fieldFilters = Option(ffs),
            paginationParams = paginationParamsForSingleResult,
            withHistory = h,
            withDeleted = d,
            debugInfo = debugInfo
          )
          .map(_.total)
      }
      cmwell.util.algorithms.binRangeSearch(from, to, now, threshold, 0.5, timeout)(searchFunction).map {
        case (f, t, nextTo) => CurrRangeForConsumption(f, t, nextTo)
      }
    }
  }

  def createPathFilter(path: Option[String], recursive: Boolean) = path.flatMap { p =>
    if (p == "/" && recursive) None
    else Some(PathFilter(p, recursive))
  }

  def retrieveNextState(
    ff: Option[FieldFilter],
    from: Long,
    recursive: Boolean,
    withHistory: Boolean,
    withDeleted: Boolean,
    path: Option[String],
    chunkSizeHint: Long,
    debugInfo: Boolean
  )(implicit ec: ExecutionContext): Future[(BulkConsumeState, Option[Long])] = {

    val pf = createPathFilter(path, recursive)
    if (from == 0) {
      crudServiceFS
        .thinSearch(
          pathFilter = pf,
          fieldFilters = ff,
          paginationParams = paginationParamsForSingleResult,
          withHistory = withHistory,
          fieldSortParams = SortParam("system.indexTime" -> Asc),
          withDeleted = withDeleted,
          debugInfo = debugInfo
        )
        .flatMap {
          case SearchThinResults(_, _, _, results, _) => {
            lazy val consumeEverythingWithoutNarrowingSearch = {
              val now = org.joda.time.DateTime.now().minusSeconds(30).getMillis
              Future.successful(
                BulkConsumeState(0L, Some(now), path, withHistory, withDeleted, recursive, chunkSizeHint, ff) -> Option
                  .empty[Long]
              )
            }
            // if no results were found - just go ahead. scroll everything, which will return nothing -> 204
            results.headOption.fold(consumeEverythingWithoutNarrowingSearch) { i =>
              // first infoton found, gets to be the new from instead of 0, and we are going to find a real valid range
              val thinSearchParams = ThinSearchParams(pf, ff, withHistory, withDeleted)
              findValidRange(thinSearchParams,
                             i.indexTime,
                             threshold = chunkSizeHint,
                             timeout = cmwell.ws.Settings.consumeBulkBinarySearchTimeout,
                             debugInfo = debugInfo).map {
                case CurrRangeForConsumption(f, t, tOpt) =>
                  BulkConsumeState(f, Some(t), path, withHistory, withDeleted, recursive, chunkSizeHint, ff) -> tOpt
              }
            }
          }
        }
        .recoverWith {
          case e: Throwable => {
            logger.error(
              s"failed to retrieveNextState($ff,$from,$recursive,$withHistory,$withDeleted,$path,$chunkSizeHint)",
              e
            )
            Future.failed(e)
          }
        }
    } else {
      val thinSearchParams = ThinSearchParams(pf, ff, withHistory, withDeleted)
      findValidRange(thinSearchParams,
                     from,
                     threshold = chunkSizeHint,
                     timeout = cmwell.ws.Settings.consumeBulkBinarySearchTimeout,
                     debugInfo = debugInfo)
        .map {
          case CurrRangeForConsumption(f, t, tOpt) =>
            BulkConsumeState(f, Some(t), path, withHistory, withDeleted, recursive, chunkSizeHint, ff) -> tOpt
        }
        .recoverWith {
          case e: Throwable => {
            logger.error(
              s"failed to retrieveNextState($ff,$from,$recursive,$withHistory,$withDeleted,$path,$chunkSizeHint)",
              e
            )
            Future.failed(e)
          }
        }
    }
  }

  def getFormatter(request: Request[AnyContent], withHistory: Boolean) = {

    val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
    (extractInferredFormatWithData(request) match {
      case (fmt, b)
          if Set("text", "path", "tsv", "tab", "nt", "ntriples", "nq", "nquads")(fmt.toLowerCase) || fmt.toLowerCase
            .startsWith("json") =>
        Success(fmt -> b)
      case (badFormat, _) =>
        Failure(new IllegalArgumentException(s"requested format ($badFormat) is invalid for as streamable response."))
    }).map {
      case (format, forceData) =>
        val withData: Option[String] = request.getQueryString("with-data").orElse {
          if (forceData) Some("json")
          else None
        }

        val withMeta: Boolean = request.queryString.keySet("with-meta")
        format match {
          case FormatExtractor(formatType) => {
            /* RDF types allowed in mstream are: ntriples, nquads, jsonld & jsonldq
             * since, the jsons are not realy RDF, just flattened json of infoton per line,
             * there is no need to tnforce subject uniquness. but ntriples, and nquads
             * which split infoton into statements (subject-predicate-object triples) per line,
             * we don't want different versions to "mix" and we enforce uniquness only in this case
             */
            val forceUniqueness: Boolean = withHistory && (formatType match {
              case RdfType(NquadsFlavor)   => true
              case RdfType(NTriplesFlavor) => true
              case _                       => false
            })
            // cleanSystemBlanks set to true, so we won't output all the meta information we usually output.
            // it get's messy with streaming. we don't want each chunk to show the "document context"
            formatterManager.getFormatter(
              format = formatType,
              timeContext = timeContext,
              host = request.host,
              uri = request.uri,
              pretty = false,
              callback = request.queryString.get("callback").flatMap(_.headOption),
              fieldFilters = None,
              offset = None,
              length = None, //Some(500L),
              withData = withData,
              withoutMeta = !withMeta,
              filterOutBlanks = true,
              forceUniqueness = forceUniqueness
            ) -> withData.isDefined
          }
        }
    }
  }

  def handle(request: Request[AnyContent]): Future[Result] = {

    val debugInfo = request.queryString.keySet("debug-info")

    def wasSupplied(queryParamKey: String) = request.queryString.keySet(queryParamKey)

    val currStateEither = request
      .getQueryString("position")
      .fold[Either[ErrorMessage, Future[(BulkConsumeState, Option[Long])]]] {
        Left("position param is mandatory")
      } { pos: String =>
      // scalastyle:off
        if (wasSupplied("qp"))
          Left("you can't specify `qp` together with `position` (`qp` is meant to be used only in the first iteration request. after that, continue iterating using the received `position`)")
        else if (wasSupplied("index-time"))
          Left("`index-time` is determined in the beginning of the iteration. can't be specified together with `position`")
        else if (wasSupplied("with-descendants") || wasSupplied("recursive"))
          Left("`with-descendants`/`recursive` is determined in the beginning of the iteration. can't be specified together with `position`")
        else if (wasSupplied("with-history"))
          Left("`with-history` is determined in the beginning of the iteration. can't be specified together with `position`")
        else if (wasSupplied("with-deleted"))
          Left("`with-deleted` is determined in the beginning of the iteration. can't be specified together with `position`")
        else if (wasSupplied("length-hint"))
          Left("`length-hint` is determined in the beginning of the bulk consume iteration. can't be specified together with `position`")
        // scalastyle:on
        else {
          ConsumeState
            .decode[BulkConsumeState](pos)
            .map(bcs => bcs.copy(to = bcs.to.orElse(request.getQueryString("to-hint").flatMap(asLong)))) match {
            case Success(state @ BulkConsumeState(f, None, path, h, d, r, lengthHint, qpOpt)) =>
              Right(retrieveNextState(qpOpt, f, r, h, d, path, lengthHint, debugInfo))
            case Success(state @ BulkConsumeState(_, Some(t), _, _, _, _, _, _)) =>
              Right(Future.successful(state -> None))
            case Failure(err) =>
              Left(err.getMessage)
          }
        }
      }

    currStateEither match {
      case Left(err) => Future.successful(BadRequest(err))
      case Right(stateFuture) =>
        stateFuture
          .flatMap {
            case (state @ BulkConsumeState(from, Some(to), path, h, d, r, threshold, ffOpt), nextTo) => {
              if (debugInfo) {
                logger.info(s"""The search params:
                           |path             = $path,
                           |from             = $from,
                           |to               = $to,
                           |nextTo           = $nextTo,
                           |fieldFilters     = $ffOpt,
                           |withHistory      = $h,
                           |withDeleted      = $d,
                           |withRecursive    = $r """.stripMargin)
              }

              getFormatter(request, h) match {
                case Failure(exception) => Future.successful(BadRequest(exception.getMessage))
                case Success((formatter, withData)) => {

                  // Gets a scroll source according to received HTTP request parameters
                  def getScrollSource() = {
                    (if (wasSupplied("slow-bulk")) {
                       streams.scrollSource(pathFilter = createPathFilter(path, r),
                                            fieldFilters =
                                              Option(fieldsFiltersFromTimeframeAndOptionalFilters(from, to, ffOpt)),
                                            withHistory = h,
                                            withDeleted = d)
                     } else {
                       streams.superScrollSource(
                         pathFilter = createPathFilter(path, r),
                         fieldFilter = Option(fieldsFiltersFromTimeframeAndOptionalFilters(from, to, ffOpt)),
                         withHistory = h,
                         withDeleted = d,
                         parallelism =
                           request.getQueryString("parallelism").flatMap(asInt).getOrElse(Settings.sstreamParallelism)
                       )
                     }).map {
                      case (src, hits) =>
                        val s: Source[Infoton, NotUsed] = {
                          if (withData) src.via(Flows.iterationResultsToFatInfotons(crudServiceFS))
                          else src.via(Flows.iterationResultsToInfotons)
                        }
                        hits -> s
                    }
                  }

                  getScrollSource()
                    .map {
                      case (0L, _) =>
                        NoContent.withHeaders("X-CM-WELL-N" -> "0",
                                              "X-CM-WELL-POSITION" -> request.getQueryString("position").get)
                      case (hits, source) => {
                        val positionEncoded = ConsumeState.encode(state.copy(from = to, to = nextTo))

                        Ok.chunked(source)(infotonWriteable(formatter))
                          .withHeaders(
                            "X-CM-WELL-N" -> hits.toString,
                            "X-CM-WELL-POSITION" -> positionEncoded,
                            "X-CM-WELL-TO" -> to.toString
                          )
                      }
                    }
                    .recover(errorHandler)
                }
              }
            }
          }
          .recover(errorHandler)
    }
  }

//  def parseQpFromRequest(qp: String)(implicit ec: ExecutionContext): Future[Option[FieldFilter]] = {
//    FieldFilterParser.parseQueryParams(qp) match {
//      case Failure(err) => Future.failed(err)
//      case Success(rff) => RawFieldFilter.eval(rff,cache,cmwellRDFHelper).map(Option.apply)
//    }
//  }

  private def addIndexTime(fromCassandra: Seq[Infoton], uuidToindexTime: Map[String, Long]): Seq[Infoton] =
    fromCassandra.map {
      case i: ObjectInfoton if i.systemFields.indexTime.isEmpty  => i.copy(i.systemFields.copy(indexTime = uuidToindexTime.get(i.uuid)))
      case i: FileInfoton if i.systemFields.indexTime.isEmpty    => i.copy(i.systemFields.copy(indexTime = uuidToindexTime.get(i.uuid)))
      case i: LinkInfoton if i.systemFields.indexTime.isEmpty    => i.copy(i.systemFields.copy(indexTime = uuidToindexTime.get(i.uuid)))
      case i: DeletedInfoton if i.systemFields.indexTime.isEmpty => i.copy(i.systemFields.copy(indexTime = uuidToindexTime.get(i.uuid)))
      case i                                        => i
    }

}
