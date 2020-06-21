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
package cmwell.ws

import javax.inject.Inject

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.scaladsl.{Concat, Flow, GraphDSL, Merge, Source}
import akka.util.ByteString
import cmwell.domain.{Infoton, _}
import cmwell.formats.Formatter
import cmwell.fts._
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import cmwell.util.concurrent.retry
import cmwell.util.string._
import cmwell.util.os.Props.os.getAvailableProcessors
import cmwell.util.stream.TakeWeighted
import cmwell.ws.util.DateParser
import com.typesafe.scalalogging.LazyLogging
import logic.CRUDServiceFS
import org.joda.time.DateTime

import scala.concurrent.{duration, ExecutionContext, Future}
import duration.DurationInt
import scala.util.{Failure, Success, Try}

object Streams extends LazyLogging {
  //CONSTS
  private val fsp = FieldSortParams(List("system.indexTime" -> Asc))
  val parallelism = math.min(cmwell.util.os.Props.os.getAvailableProcessors, Settings.cassandraBulkSize)
  val endln = ByteString(cmwell.util.os.Props.endln)

  object Flows {

    def infotonToByteString(formatter: Formatter): Flow[Infoton, ByteString, NotUsed] =
      Flow.fromFunction[Infoton, ByteString] { infoton =>
        val formatted = formatter.render(infoton)
        val trimmed = dropRightWhile(formatted)(_.isWhitespace)
        ByteString(trimmed) ++ endln
      }

    def searchThinResultToByteString(formatter: Formatter): Flow[SearchThinResult, ByteString, NotUsed] =
      Flow.fromFunction[SearchThinResult, ByteString] { str =>
        val formatted = formatter.render(str)
        val trimmed = dropRightWhile(formatted)(_.isWhitespace)
        ByteString(trimmed) ++ endln
      }

    def formattableToByteString(formatter: Formatter,
                                length: Option[Long] = None): Flow[Formattable, ByteString, NotUsed] = {
      if (formatter.mimetype.contains("json")) {
        val s = Flow[Formattable].mapConcat {
          case IterationResults(_, _, Some(iSeq), _, _) => iSeq.toList
          case InfotonHistoryVersions(iSeq)             => iSeq.toList
          case BagOfInfotons(iSeq)                      => iSeq.toList
          case i: Infoton                               => List(i)
          //          case str: SearchThinResult => List(str)
          //          case str: SearchThinResults => str.thinResults.toList
        }

        length
          .fold(s)(l => s.take(l))
          .map { i =>
            val rv = Try(formatter.render(i))
            rv.failed.foreach { err =>
              logger.error(s"error during formating of $i", err)
            }
            rv
          }
          .collect {
            case Success(renderedString) =>
              if (renderedString.endsWith(cmwell.util.os.Props.endln)) ByteString(renderedString)
              else ByteString(renderedString + cmwell.util.os.Props.endln)
          }
      } else {
        val f2bs: Formattable => ByteString = { formattable =>
          val formatted = formatter.render(formattable)
          val trimmed = dropRightWhile(formatted)(_.isWhitespace)
          ByteString(trimmed + cmwell.util.os.Props.endln)
        }

        length.fold(Flow.fromFunction(f2bs))(
          l =>
            Flow
              .fromGraph(
                TakeWeighted[Formattable](
                  l,
                  true, {
                    case IterationResults(_, _, iSeqOpt, _, _) => iSeqOpt.fold(0)(_.size).toLong
                    case InfotonHistoryVersions(iSeq)          => iSeq.size.toLong
                    case BagOfInfotons(iSeq)                   => iSeq.size.toLong
                    case i: Infoton                            => 1L
                  }
                )
              )
              .map(f2bs)
        )
      }
    }

    def searchThinResultToFatInfoton(crudServiceFS: CRUDServiceFS): Flow[SearchThinResult, Infoton, NotUsed] =
      Flow[SearchThinResult]
        .mapAsyncUnordered(parallelism)(str => crudServiceFS.getInfotonByUuidAsync(str.uuid))
        .collect { case FullBox(i) => i }

    val searchThinResultsFlattened: Flow[SearchThinResults, SearchThinResult, NotUsed] = Flow[SearchThinResults]
      .mapConcat { case SearchThinResults(_, _, _, str, _) => str.toList }

    def searchThinResultsToFatInfotons(crudServiceFS: CRUDServiceFS): Flow[SearchThinResults, Infoton, NotUsed] =
      searchThinResultsFlattened.via(searchThinResultToFatInfoton(crudServiceFS))

    def iterationResultsToFatInfotons(crudServiceFS: CRUDServiceFS)(implicit ec: ExecutionContext): Flow[IterationResults, Infoton, NotUsed] =
      Flow[IterationResults]
        .collect { case IterationResults(_, _, Some(iSeq), _, _) => iSeq }
        .mapConcat(_.view.map(_.uuid).to(List))
        .mapAsyncUnordered(parallelism){ uuid =>
          crudServiceFS.getInfotonByUuidAsync(uuid).map(_ -> uuid)
        }
      .map {
        case (FullBox(i), _) => i
        case (emptyBox@EmptyBox, uuid) =>
          logger.error(s"Got empty box for uuid $uuid")
          emptyBox.get
        case (failure@BoxedFailure(ex), uuid) =>
          logger.error(s"Got BoxedFailure when trying to get uuid $uuid. The exception was:", ex)
          failure.get
      }
//        .collect { case FullBox(i) => i } - removed! don't ignore errors!

    val iterationResultsToInfotons: Flow[IterationResults, Infoton, NotUsed] = Flow[IterationResults]
      .collect { case IterationResults(_, _, Some(iSeq), _, _) => iSeq }
      .mapConcat(_.toList)
  }
}

class Streams @Inject()(crudServiceFS: CRUDServiceFS) extends LazyLogging {

  import Streams._

  def scrollSourceToByteString(src: Source[IterationResults, NotUsed],
                               formatter: Formatter,
                               withData: Boolean = false,
                               withHistory: Boolean = false,
                               length: Option[Long] = None,
                               fieldsMask: Set[String] = Set.empty)(implicit ec: ExecutionContext): Source[ByteString, NotUsed] = {
    if (!withData) src.via(Flows.formattableToByteString(formatter, length))
    else {

      val s1: Source[Infoton, NotUsed] = length.fold(src.via(Flows.iterationResultsToFatInfotons(crudServiceFS))) { l =>
        src.via(Flows.iterationResultsToFatInfotons(crudServiceFS)).take(l)
      }

      val s2: Source[Infoton, NotUsed] = {
        if (fieldsMask.isEmpty) s1
        else s1.map(_.masked(fieldsMask))
      }

      if (withHistory)
        s2.grouped(Settings.cassandraBulkSize)
          .map(InfotonHistoryVersions.apply)
          .via(Flows.formattableToByteString(formatter))
      else s2.via(Flows.infotonToByteString(formatter))
    }
  }

  def lazySeqScrollSource(scrollStarter: ScrollStarter, maxParallelism: Int)(
    applyScrollStarter: ScrollStarter => Seq[() => Future[IterationResults]]
  )(implicit ec: ExecutionContext): Future[(Source[IterationResults, NotUsed], Long)] = {
    val readyToRunScrollFunctions = applyScrollStarter(scrollStarter)
    if (readyToRunScrollFunctions.length <= maxParallelism) seqScrollSourceHandler(readyToRunScrollFunctions.map { f =>
      f().map(ir => ir -> ir.totalHits)
    }, scrollStarter.withDeleted)
    else {
      val ScrollStarter(pf, ff, df, pagination, ttl, withHistory, withDeleted) = scrollStarter
      // if scroll is pulled lazily when no more than ${maxParallelism} shards are being pulled concurrently,
      // we need to make sure to have the total count available at start.
      // NOTE: this is not an exact number, and can't be,
      // since new infotons can be indexed in a shard in the time passed between performing the search,
      // and until the shard is actually being queried for the scroll results.
      val totalsF = crudServiceFS
        .thinSearch(pf, ff, df, pagination.copy(length = 1), withHistory, withDeleted = withDeleted)
        .map(_.total)

      val sources = readyToRunScrollFunctions.map { f =>
        val lazilyAsyncSrc =
          () => Source.fromFuture(f().map(singleScrollSourceHandler(withDeleted, ec))).flatMapConcat(identity)
        Source.lazily(lazilyAsyncSrc).mapMaterializedValue(_ => NotUsed)
      }

      val combinedSources = if (sources.isEmpty) {
        logger.warn("empty sources seq? shouldn't be possible...!")
        Source.empty
      } else if (sources.length == 1) sources.head
      else Source(sources.toList).flatMapMerge(maxParallelism, identity)

      totalsF.map(total => combinedSources -> total)
    }
  }

  private def singleScrollSourceHandler(withDeleted: Boolean, ec: ExecutionContext)(
    firstHit: IterationResults
  ): Source[IterationResults, NotUsed] = Source.unfoldAsync(firstHit) {
    //ES now give results even for the create scroll request there is no need for a special treatment for the first hit.
    //case ir @ `firstHit` => crudServiceFS.scroll(ir.iteratorId, 60, withData = false).map(iir => Some(iir -> ir))(ec)
    case ir @ IterationResults(iteratorId, _, infotonsOpt, _, _) => {
      infotonsOpt
        .collect { case xs if xs.nonEmpty => ir }
        .fold(Future.successful(Option.empty[(IterationResults, IterationResults)])) { ir =>
          crudServiceFS.scroll(iteratorId, 60, withData = false, debugInfo = false).map(iir => Some(iir -> ir))(ec)
        }
    }
  }

  private def seqScrollSourceHandler(
    firstHitsTuple: Seq[Future[(IterationResults, Long)]],
    withDeleted: Boolean
  )(implicit ec: ExecutionContext): Future[(Source[IterationResults, NotUsed], Long)] = {
    //converting the inner IterationResults into a Source which will fold on itself asynchronously
    val sfeir = cmwell.util.concurrent.travelist(firstHitsTuple)(_.map {
      case (ir, hits) =>
        Source.unfoldAsync(ir) {
          case ir@IterationResults(iteratorId, _, infotonsOpt, _, _) =>
            infotonsOpt
              .collect { case xs if xs.nonEmpty => ir }
              .fold(Future.successful(Option.empty[(IterationResults, IterationResults)])) { ir =>
                crudServiceFS.scroll(iteratorId, 60, withData = false, debugInfo = false).map(iir => Some(iir -> ir))
              }
        } -> hits
    })

    //converting the sequence of sources to a single source
    sfeir.map {
      case Nil           => throw new IllegalStateException("must have at least one index")
      case source :: Nil => source
      case (first, h1) :: (second, h2) :: rest =>
        val source = Source.combine(first, second, rest.map(_._1): _*)(size => Merge(size))
        val hits = h1 + h2 + rest.view.map(_._2).sum
        source -> hits
    }
  }

  def seqScrollSource(scrollStarter: ScrollStarter)(
    applyScrollStarter: ScrollStarter => Seq[Future[IterationResults]]
  )(implicit ec: ExecutionContext): Future[(Source[IterationResults, NotUsed], Long)] = {
    val firstHitsTuple = applyScrollStarter(scrollStarter).map(_.map(r => r -> r.totalHits))
    seqScrollSourceHandler(firstHitsTuple, scrollStarter.withDeleted)
  }

  def multiScrollSource(
    pathFilter: Option[PathFilter] = None,
    fieldFilter: Option[FieldFilter] = None,
    datesFilter: Option[DatesFilter] = None,
    paginationParams: PaginationParams = DefaultPaginationParams,
    withHistory: Boolean = false,
    withDeleted: Boolean = false
  )(implicit ec: ExecutionContext): Future[(Source[IterationResults, NotUsed], Long)] = {
    seqScrollSource(
      ScrollStarter(pathFilter, fieldFilter, datesFilter, paginationParams, 120, withHistory, withDeleted)
    ) {
      case ScrollStarter(pf, ff, df, pp, ttl, h, d) =>
        crudServiceFS.startMultiScroll(pf, ff, df, pp, ttl, h, d)
    }
  }

  def scrollSource(
    pathFilter: Option[PathFilter] = None,
    fieldFilters: Option[FieldFilter] = None,
    datesFilter: Option[DatesFilter] = None,
    paginationParams: PaginationParams = DefaultPaginationParams,
    scrollTTL: Long = 120L,
    withHistory: Boolean = false,
    withDeleted: Boolean = false,
    debugLogID: Option[String] = None
  )(implicit ec: ExecutionContext): Future[(Source[IterationResults, NotUsed], Long)] = {

    val firstHitsTuple = crudServiceFS
      .startScroll(
        pathFilter = pathFilter,
        fieldsFilters = fieldFilters,
        datesFilter = datesFilter,
        paginationParams = paginationParams,
        scrollTTL = scrollTTL,
        withHistory = withHistory,
        withDeleted = withDeleted,
        debugInfo = debugLogID.isDefined
      )
      .map { startScrollResult =>
        debugLogID.foreach(id => logger.info(s"[$id] startScrollResult: " +
          s"[totalHits=${startScrollResult.totalHits},iteratorId=${startScrollResult.iteratorId},debugInfo=${startScrollResult.debugInfo}]"))
        debugLogID.foreach(id => logger.info(s"[$id] scroll response: ${startScrollResult.infotons.fold("empty")(i => s"${i.size} results")}"))
        startScrollResult.totalHits -> startScrollResult
      }

    //converting the inner IterationResults into a Source which will unfold itself asynchronously
    firstHitsTuple.map {
      case (hits, first) =>
        val source = Source.unfoldAsync(first) {
          case ir@IterationResults(iteratorId, _, infotonsOpt, _, _) =>
            infotonsOpt
              .filter(_.nonEmpty)
              .fold(Future.successful(Option.empty[(IterationResults, IterationResults)])) { _ =>
                debugLogID.foreach(id => logger.info(s"[$id] scroll request: $iteratorId"))
                crudServiceFS
                  .scroll(iteratorId, scrollTTL, withData = false, debugLogID.isDefined)
                  .andThen {
                    case Success(res) =>
                      debugLogID.foreach(id => logger.info(s"[$id] scroll response: ${res.infotons.fold("empty")(i => s"${i.size} results")}"))
                    case Failure(err) => debugLogID.foreach(id => logger.error(s"[$id] scroll source failed", err))
                  }
                  .map(iir => Some(iir -> ir))
              }
        }
        source -> hits
    }
  }

  def superScrollSource(
    pathFilter: Option[PathFilter] = None,
    fieldFilter: Option[FieldFilter] = None,
    datesFilter: Option[DatesFilter] = None,
    paginationParams: PaginationParams = DefaultPaginationParams,
    withHistory: Boolean = false,
    withDeleted: Boolean = false,
    parallelism: Int = Settings.sstreamParallelism
  )(implicit ec: ExecutionContext): Future[(Source[IterationResults, NotUsed], Long)] = {
    lazySeqScrollSource(
      ScrollStarter(pathFilter, fieldFilter, datesFilter, paginationParams, 120, withHistory, withDeleted),
      parallelism
    ) {
      case ScrollStarter(pf, ff, df, pp, ttl, h, d) =>
        val functions = crudServiceFS.startSuperScroll(pf, ff, df, pp, ttl, h, d)
        functions
    }
  }

  private def enrichWithDataAndFlatten(src: Source[Seq[Infoton], NotUsed]): Source[Infoton, NotUsed] = {
    src
      .mapConcat {
        case infotons if infotons.isEmpty => List(Future.successful(Vector.empty[Infoton]))
        case infotons => {
          infotons
            .view
            .map(_.uuid).to(List)
            .grouped(Settings.cassandraBulkSize)
            .map(crudServiceFS.getInfotonsByUuidAsync)
            .toVector
        }
      }
      .mapAsync(1)(identity)
      .mapConcat(_.toVector)
  }

  def qStream(firstTimeStamp: Long,
              path: Option[String],
              history: Boolean,
              deleted: Boolean,
              descendants: Boolean,
              lengthHint: Int,
              fieldFilters: Option[FieldFilter])(implicit ec: ExecutionContext): Source[SearchThinResult, NotUsed] = {

    Source
      .unfoldAsync[Long, Source[SearchThinResult, NotUsed]](firstTimeStamp) { timeStamp =>
        val ffs = transformFieldFiltersForConsumption(fieldFilters, timeStamp)
        val pf = path.map(PathFilter(_, descendants))
        val pp = PaginationParams(0, lengthHint)

        val future = crudServiceFS.thinSearch(pathFilter = pf,
                                              fieldFilters = Some(ffs),
                                              paginationParams = pp,
                                              withHistory = history,
                                              withDeleted = deleted,
                                              fieldSortParams = fsp)

        future.flatMap {
          case SearchThinResults(total, offset, length, results, _) => {
            if (total == 0) Future.successful(None)
            else if (results.size == total) { //TODO: `if(results.size < lengthHint)` ???
              Future.successful(Some(results.maxBy(_.indexTime).indexTime -> Source(results.toVector)))
            } else {
              val indexTime = results.maxBy(_.indexTime).indexTime
              require(indexTime > 0, "all infotons in iteration must have a valid indexTime defined")

              //regular chunk with more than 1 indexTime
              if (results.exists(_.indexTime != indexTime)) {

                val filteredByIndexTimeInfotons = results.filter(_.indexTime < indexTime).toVector
                val e = Source(filteredByIndexTimeInfotons)
                val lastIndexTimeInChunk = filteredByIndexTimeInfotons.maxBy(_.indexTime).indexTime

                Future.successful(Some(lastIndexTimeInChunk -> e))
              }
              //a chunk consisting infotons with the same indexTime
              else {
                val ffs2 = fieldFilters.map(
                  ff =>
                    MultiFieldFilter(Must, List(ff, FieldFilter(Must, Equals, "system.indexTime", indexTime.toString)))
                )
                val fut = scrollSource(pathFilter = pf,
                                       fieldFilters = ffs2,
                                       paginationParams = PaginationParams(0, 500),
                                       withHistory = history,
                                       withDeleted = deleted)

                fut.map {
                  case (_, 0L)           => None
                  case (_, `lengthHint`) => Some(indexTime -> Source(results.toVector))
                  case (s, _) => {
                    val strs = s.mapConcat {
                      case IterationResults(_, _, infoptons, _, _) => {
                        infoptons.fold(List.empty[SearchThinResult])(_.view.map { i =>
                          SearchThinResult(i.systemFields.path,
                                           i.uuid,
                                           DateParser.fdf(i.systemFields.lastModified),
                                           lastModifiedBy = i.systemFields.lastModifiedBy,
                                           i.systemFields.indexTime.getOrElse(i.systemFields.lastModified.getMillis))
                        }.to(List))
                      }
                    }
                    Some(indexTime -> strs)
                  }
                }
              }
            }
          }
        }
      }
      .flatMapConcat(identity)
  }

  private def transformFieldFiltersForConsumption(fieldFilters: Option[FieldFilter], timeStamp: Long): FieldFilter =
    fieldFilters match {
      case None =>
        MultiFieldFilter(
          Must,
          List(
            FieldFilter(Must, GreaterThan, "system.indexTime", timeStamp.toString),
            FieldFilter(Must, LessThan, "system.indexTime", (System.currentTimeMillis() - 10000).toString)
          )
        )
      case Some(ff) =>
        MultiFieldFilter(
          Must,
          List(
            ff,
            FieldFilter(Must, GreaterThan, "system.indexTime", timeStamp.toString),
            FieldFilter(Must, LessThan, "system.indexTime", (System.currentTimeMillis() - 10000).toString)
          )
        )
    }
}

case class ScrollStarter(pathFilter: Option[PathFilter] = None,
                         fieldFilters: Option[FieldFilter] = None,
                         datesFilter: Option[DatesFilter] = None,
                         paginationParams: PaginationParams = DefaultPaginationParams,
                         scrollTTL: Long = 60,
                         withHistory: Boolean = false,
                         withDeleted: Boolean = false)
