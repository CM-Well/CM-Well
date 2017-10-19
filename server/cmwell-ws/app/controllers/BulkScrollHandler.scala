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

import cmwell.domain._
import cmwell.formats._
import cmwell.fts._
import cmwell.ws.Streams
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
import play.api.http.Writeable

import scala.math.{max, min}

@Singleton
class BulkScrollHandler @Inject()(crudServiceFS: CRUDServiceFS,
                                  tbg: NbgToggler,
                                  streams: Streams,
                                  cmwellRDFHelper: CMWellRDFHelper,
                                  formatterManager: FormatterManager,
                                  action: DefaultActionBuilder,
                                  components: ControllerComponents)(implicit ec: ExecutionContext) extends AbstractController(components) with LazyLogging with TypeHelpers {

  def cache(nbg: Boolean) = if(nbg || tbg.get) crudServiceFS.nbgPassiveFieldTypesCache else crudServiceFS.obgPassiveFieldTypesCache

  //consts
  val paginationParamsForSingleResult = PaginationParams(0, 1)
  val paginationParamsForSingleResultWithOffset = PaginationParams(1000, 1) //TODO: take default offset from configuration

  def infotonWriteable(formatter: Formatter) = new Writeable[Infoton](formattableToByteString(formatter),Some(formatter.mimetype))

  sealed trait RangeForConsumption
  case class CurrRangeForConsumption(from: Long, to: Long, nextTo: Option[Long]) extends RangeForConsumption
  case class NextRangeForConsumption(from: Long, to: Option[Long]) extends RangeForConsumption
  case class ThinSearchParams(pathFilter: Option[PathFilter], fieldFilters: Option[FieldFilter], withHistory: Boolean, withDeleted: Boolean)

  def fieldsFiltersFromTimeframeAndOptionalFilters(from: Long, to: Long, ffsOpt: Option[FieldFilter]): FieldFilter = ffsOpt.fold(onlyTimeframeFieldFilters(from, to)) {
    case ff @ SingleFieldFilter(Should, _, _, _) => MultiFieldFilter(Must, MultiFieldFilter(Must, Seq(ff)) :: getFieldFilterSeq(from, to))
    case ff: SingleFieldFilter => MultiFieldFilter(Must, ff :: getFieldFilterSeq(from, to))
    case ff @ MultiFieldFilter(Should, _) => MultiFieldFilter(Must, MultiFieldFilter(Must, Seq(ff)) :: getFieldFilterSeq(from, to))
    case ff: MultiFieldFilter => MultiFieldFilter(Must, ff :: getFieldFilterSeq(from, to))
  }

  def fieldsFiltersforSortedSearchFromTimeAndOptionalFilters(from: Long, ffsOpt: Option[FieldFilter]): FieldFilter = {
    val fromFilter: FieldFilter = SingleFieldFilter(Must, GreaterThanOrEquals, "system.indexTime", Some(from.toString))
    ffsOpt.fold(fromFilter) {
      case ff@SingleFieldFilter(Should, _, _, _) => MultiFieldFilter(Must, Seq[FieldFilter](MultiFieldFilter(Must, Seq(ff)), fromFilter))
      case ff: SingleFieldFilter => MultiFieldFilter(Must, Seq(ff,fromFilter))
      case ff@MultiFieldFilter(Should, _) => MultiFieldFilter(Must, Seq(MultiFieldFilter(Must, Seq(ff)), fromFilter))
      case ff: MultiFieldFilter => MultiFieldFilter(Must, Seq(ff, fromFilter))
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

  def findValidRange(thinSearchParams: ThinSearchParams,
                     from: Long,
                     threshold: Long,
                     timeoutMarker: Future[Unit])(implicit ec: ExecutionContext): Future[CurrRangeForConsumption] = {

    logger.debug(s"findValidRange: from[$from], threshold[$threshold], tsp[$thinSearchParams]")

    val ThinSearchParams(pf, ffsOpt, h, d) = thinSearchParams
    val now = org.joda.time.DateTime.now().minusSeconds(30).getMillis

    lazy val toSeed: Future[Long] = {
      val ffs = fieldsFiltersforSortedSearchFromTimeAndOptionalFilters(from,ffsOpt)
      crudServiceFS.thinSearch(
          pathFilter = pf,
          fieldFilters = Option(ffs),
          paginationParams = paginationParamsForSingleResultWithOffset,
          withHistory = h,
          fieldSortParams = SortParam("system.indexTime" -> Asc),
          withDeleted = d
        ).map {
        case SearchThinResults(_, _, _, results, _) =>
          //In case that there are more than the initial seed infotons (=1000) with the same index time the "from" will be equal to the "to"
          //This will fail the FieldFilter requirements (see getFieldFilterSeq) and thus approx. 1 second is added to the "from" as the new "to"
          results.headOption.fold(now)(i => math.max(i.indexTime,from+1729L)) //https://en.wikipedia.org/wiki/1729_(number)
      }
    }

    def iterate(to: Long, step: Long, earlyCutOffResult: CurrRangeForConsumption, expanding: Boolean = true, nextTo: Option[Long] = None): Future[CurrRangeForConsumption] = {
      logger.debug(s"iterate: from[$from], to[$to], step[$step], expanding[$expanding], nextTo[${nextTo.getOrElse("N/A")}], ecor[$earlyCutOffResult]")

      //Take the maximum between the current "to" and the previous toSeed that is guaranteed to be at least 1000 infotons. The current "to" might be too early in time and thus will return 0 results.
      def safeUpperLimit(unsafeToLimit: Long) = {
        toSeed.value.flatMap(_.toOption).fold(unsafeToLimit)(max(_, unsafeToLimit))
      }

      def takeTheBetterEarlyCutOff: CurrRangeForConsumption = {
        val x = safeUpperLimit(to)
        if (earlyCutOffResult.to >= x) earlyCutOffResult
        else CurrRangeForConsumption(from, x, None) // nextTo should be none even if exist, because we are not done yet, and next chunk might be too big
      }

      if(!expanding && step < 10)
        //Stop condition: in case the expanding is false (the step is getting SMALLER each time) and the step small return the current values
        Future.successful(CurrRangeForConsumption(from, to, nextTo))
      else {
        // clipping date boundaries
        val ffs = fieldsFiltersFromTimeframeAndOptionalFilters(from, to, ffsOpt)

        // find number of infotons in given range
        crudServiceFS.thinSearch(
          pathFilter = pf,
          fieldFilters = Option(ffs),
          paginationParams = paginationParamsForSingleResult,
          withHistory = h,
          withDeleted = d
        ).flatMap {
          case SearchThinResults(total, _, _, _, _) => {

            logger.debug(s"iterate after search: total[$total]")

            if (total < threshold * 0.5) {
              // don't check above current time
              if (now <= to) Future.successful(CurrRangeForConsumption(from, now, None))
              else if(timeoutMarker.isCompleted) Future.successful(takeTheBetterEarlyCutOff) //CurrRangeForConsumption(from, safeUpperLimit(to), nextTo))
              else {
                val op: Long => Long = if(expanding) {l => l*2} else {l => l/2}
                val (nTo,nStep,nExpanding) = {
                  // if (to + step > now) we are expanding for sure because the next step passes now and wasn't stopped in if (now <= to) condition below
                  // the actual step in this case is until now and the next step should be half of it. Also from now on the step should be less thus expanding = false
                  if (to + step > now)
                    (now, (now - to)/2, false)
                  else (to + step, op(step), expanding)
                }
                iterate(nTo, nStep, takeTheBetterEarlyCutOff, nExpanding, nextTo)
              }
            }
            else if (total > threshold * 1.5) {
              val newNext = nextTo orElse {
                if (total > threshold * 3) None
                else Some(to)
              }
              if(timeoutMarker.isCompleted) Future.successful(earlyCutOffResult) // we might change it in the future to .copy(nextTo = newNext) for optimization but we must make sure the range is not too big by adding also the count until the nextTo
              else {
                val (newTo, newStep) = if (expanding) {
                  //1. If it was expanding before the step should be less now (it was too big)
                  //   Thus, the newTo should be half the step that got us to this trouble
                  //   The previous iteration called this one with double step that got to the current to.
                  //   i.e. the new to should go 1/4 of the provided step
                  //2. The new step should be half of the step to the next to
                  (to - step / 4, step / 8)
                }
                else {
                  //The previous iteration was not expanding (but it could be too big or too small)
                  //The new to should be the current one minus the step (the provided step is actually half of the step got us here).
                  //The new step should be half of the step to the next to
                  (to - step, step / 2)
                }
                iterate(newTo, newStep, earlyCutOffResult, expanding = false, newNext)
              }
            }
            else {
              Future.successful(CurrRangeForConsumption(from, to, nextTo))
            }
          }
        }
      }
    }

    toSeed.flatMap { to =>
      logger.debug(s"findValidRange: toSeed[$to], from[$from], tsp[$thinSearchParams]")
      iterate(to, to-from, CurrRangeForConsumption(from, to, None))
    }
  }

  def createPathFilter(path: Option[String], recursive: Boolean) = path.flatMap{ p =>
    if (p == "/" && recursive) None
    else Some(PathFilter(p, recursive))
  }

  def retrieveNextState(ff: Option[FieldFilter],
                        from: Long,
                        recursive: Boolean,
                        withHistory: Boolean,
                        withDeleted: Boolean,
                        path: Option[String],
                        chunkSizeHint: Long)(implicit ec: ExecutionContext): Future[(BulkConsumeState,Option[Long])] = {

    val futureMarksTheTimeOut = cmwell.util.concurrent.SimpleScheduler.schedule(cmwell.ws.Settings.consumeBulkBinarySearchTimeout)(())
    val pf = createPathFilter(path, recursive)
    if(from == 0) {
      crudServiceFS.thinSearch(
          pathFilter = pf,
          fieldFilters = ff,
          paginationParams = paginationParamsForSingleResult,
          withHistory = withHistory,
          fieldSortParams = SortParam("system.indexTime" -> Asc),
          withDeleted = withDeleted
        ).flatMap {
        case SearchThinResults(_, _, _, results, _) => {
          lazy val consumeEverythingWithoutNarrowingSearch = {
            val now = org.joda.time.DateTime.now().minusSeconds(30).getMillis
            Future.successful(BulkConsumeState(0L, Some(now), path, withHistory, withDeleted, recursive, chunkSizeHint, ff) -> Option.empty[Long])
          }
          // if no results were found - just go ahead. scroll everything, which will return nothing -> 204
          results.headOption.fold(consumeEverythingWithoutNarrowingSearch) { i =>
            // first infoton found, gets to be the new from instead of 0, and we are going to find a real valid range
            val thinSearchParams = ThinSearchParams(pf, ff, withHistory, withDeleted)
            findValidRange(thinSearchParams, i.indexTime, threshold = chunkSizeHint,timeoutMarker = futureMarksTheTimeOut).map {
              case CurrRangeForConsumption(f, t, tOpt) =>
                BulkConsumeState(f, Some(t), path, withHistory, withDeleted, recursive, chunkSizeHint, ff) -> tOpt
            }
          }
        }
      }.recoverWith {
        case e: Throwable => {
          logger.error(s"failed to retrieveNextState($ff,$from,$recursive,$withHistory,$withDeleted,$path,$chunkSizeHint)", e)
          Future.failed(e)
        }
      }
    } else {
      val thinSearchParams = ThinSearchParams(pf, ff, withHistory, withDeleted)
      findValidRange(thinSearchParams, from, threshold = chunkSizeHint,timeoutMarker = futureMarksTheTimeOut).map { case CurrRangeForConsumption(f, t, tOpt) =>
        BulkConsumeState(f, Some(t), path, withHistory, withDeleted, recursive, chunkSizeHint, ff) -> tOpt
      }.recoverWith {
        case e: Throwable =>{
          logger.error(s"failed to retrieveNextState($ff,$from,$recursive,$withHistory,$withDeleted,$path,$chunkSizeHint)", e)
          Future.failed(e)
        }
      }
    }
  }

  def getFormatter(request: Request[AnyContent], withHistory: Boolean, nbg: Boolean) = {

    (extractInferredFormatWithData(request) match {
      case (fmt,b) if Set("text", "path", "tsv", "tab", "nt", "ntriples", "nq", "nquads")(fmt.toLowerCase) || fmt.toLowerCase.startsWith("json") => Success(fmt -> b)
      case (badFormat,_) => Failure(new IllegalArgumentException(s"requested format ($badFormat) is invalid for as streamable response."))
    }).map { case (format,forceData) =>

      val withData: Option[String] = request.getQueryString("with-data") orElse{
        if(forceData) Some("json")
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
            case RdfType(NquadsFlavor) => true
            case RdfType(NTriplesFlavor) => true
            case _ => false
          })
          //cleanSystemBlanks set to true, so we won't output all the meta information we usually output. it get's messy with streaming. we don't want each chunk to show the "document context"
          formatterManager.getFormatter(
            format = formatType,
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
            forceUniqueness = forceUniqueness,
            nbg = nbg
          ) -> withData.isDefined
        }
      }
    }
  }

  def handle(request: Request[AnyContent]): Future[Result] = {

    def wasSupplied(queryParamKey: String) = request.queryString.keySet(queryParamKey)

    val currStateEither = request.getQueryString("position").fold[Either[ErrorMessage, Future[(BulkConsumeState,Option[Long])]]] {
      Left("position param is mandatory")
    } { pos: String =>
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
      else {
        ConsumeState.decode[BulkConsumeState](pos).map(bcs => bcs.copy(to = bcs.to.orElse(request.getQueryString("to-hint").flatMap(asLong)))) match {
          case Success(state @ BulkConsumeState(f, None, path, h, d, r, lengthHint, qpOpt)) =>
            Right(retrieveNextState(qpOpt, f, r, h, d, path, lengthHint))
          case Success(state @ BulkConsumeState(_, Some(t), _, _, _, _, _, _)) =>
            Right(Future.successful(state -> None))
          case Failure(err) =>
            Left(err.getMessage)
        }
      }
    }

    val nbg = request.getQueryString("nbg").flatMap(asBoolean).getOrElse(tbg.get)

    currStateEither match {
      case Left(err) => Future.successful(BadRequest(err))
      case Right(stateFuture) => stateFuture.flatMap {
        case (state@BulkConsumeState(from, Some(to), path, h, d, r, threshold, ffOpt), nextTo) => {
          if(request.queryString.keySet("debug-info")) {
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

          getFormatter(request, h, nbg) match {
            case Failure(exception) => Future.successful(BadRequest(exception.getMessage))
            case Success((formatter, withData)) => {

              // Gets a scroll source according to received HTTP request parameters
              def getScrollSource() = {
                (if (wasSupplied("slow-bulk")) {
                  streams.scrollSource(nbg,
                    pathFilter = createPathFilter(path, r),
                    fieldFilters = Option(fieldsFiltersFromTimeframeAndOptionalFilters(from, to, ffOpt)),
                    withHistory = h,
                    withDeleted = d)
                } else {
                  streams.superScrollSource(nbg,
                    pathFilter = createPathFilter(path, r),
                    fieldFilter = Option(fieldsFiltersFromTimeframeAndOptionalFilters(from, to, ffOpt)),
                    withHistory = h,
                    withDeleted = d)
                }).map { case (src, hits) =>
                  val s: Source[Infoton, NotUsed] = {
                    if (withData) src.via(Flows.iterationResultsToFatInfotons(nbg, crudServiceFS))
                    else src.via(Flows.iterationResultsToInfotons)
                  }
                  hits -> s
                }
              }

              getScrollSource().map {
                case (0L, _) => NoContent.withHeaders("X-CM-WELL-N" -> "0", "X-CM-WELL-POSITION" -> request.getQueryString("position").get)
                case (hits, source) => {
                  val positionEncoded = ConsumeState.encode(state.copy(from = to, to = nextTo))

                  Ok.chunked(source)(infotonWriteable(formatter))
                    .withHeaders(
                      "X-CM-WELL-N" -> hits.toString,
                      "X-CM-WELL-POSITION" -> positionEncoded,
                      "X-CM-WELL-TO" -> to.toString
                    )
                }
              }.recover(errorHandler)
            }
          }
        }
      }.recover(errorHandler)
    }
  }

  def parseQpFromRequest(qp: String, nbg: Boolean)(implicit ec: ExecutionContext): Future[Option[FieldFilter]] = {
    FieldFilterParser.parseQueryParams(qp) match {
      case Failure(err) => Future.failed(err)
      case Success(rff) => RawFieldFilter.eval(rff,cache(nbg),cmwellRDFHelper,nbg).map(Option.apply)
    }
  }

  private def addIndexTime(fromCassandra: Seq[Infoton], uuidToindexTime: Map[String, Long]): Seq[Infoton] = fromCassandra.map {
    case i: ObjectInfoton if i.indexTime.isEmpty => i.copy(indexTime = uuidToindexTime.get(i.uuid))
    case i: FileInfoton if i.indexTime.isEmpty => i.copy(indexTime = uuidToindexTime.get(i.uuid))
    case i: LinkInfoton if i.indexTime.isEmpty => i.copy(indexTime = uuidToindexTime.get(i.uuid))
    case i: DeletedInfoton if i.indexTime.isEmpty => i.copy(indexTime = uuidToindexTime.get(i.uuid))
    case i => i
  }

}
