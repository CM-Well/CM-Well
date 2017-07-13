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


package cmwell.ws

import javax.inject.Inject

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Source}
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

import scala.concurrent.{ExecutionContext, Future, duration}
import duration.DurationInt
import scala.util.{Success, Try}

object Streams extends LazyLogging {
  //CONSTS
  private val fsp = FieldSortParams(List("system.indexTime" -> Asc))
  val bo0 = scala.collection.breakOut[Seq[Infoton], ByteString, List[ByteString]]
  val bo1 = scala.collection.breakOut[Seq[Infoton], String, List[String]]
  val bo2 = scala.collection.breakOut[Seq[SearchThinResult], String, List[String]]
  val bo3 = scala.collection.breakOut[Seq[Infoton], SearchThinResult, List[SearchThinResult]]
  val parallelism = math.min(cmwell.util.os.Props.os.getAvailableProcessors,Settings.cassandraBulkSize)
  val endln = ByteString(cmwell.util.os.Props.endln)

  object Flows {

    def infotonToByteString(formatter: Formatter): Flow[Infoton,ByteString,NotUsed] = Flow.fromFunction[Infoton,ByteString] { infoton =>
      val formatted = formatter.render(infoton)
      val trimmed = dropRightWhile(formatted)(_.isWhitespace)
      ByteString(trimmed) ++ endln
    }

    def searchThinResultToByteString(formatter: Formatter): Flow[SearchThinResult,ByteString,NotUsed] = Flow.fromFunction[SearchThinResult,ByteString] { str =>
      val formatted = formatter.render(str)
      val trimmed = dropRightWhile(formatted)(_.isWhitespace)
      ByteString(trimmed) ++ endln
    }

    def formattableToByteString(formatter: Formatter, length: Option[Long] = None): Flow[Formattable,ByteString,NotUsed] = {
      if(formatter.mimetype.contains("json")) {
        val s = Flow[Formattable].mapConcat {
          case IterationResults(_, _, Some(iSeq), _, _) => iSeq.toList
          case InfotonHistoryVersions(iSeq) => iSeq.toList
          case BagOfInfotons(iSeq) => iSeq.toList
          case i: Infoton => List(i)
          //          case str: SearchThinResult => List(str)
          //          case str: SearchThinResults => str.thinResults.toList
        }

        length.fold(s)(l => s.take(l)).map { i =>
          val rv = Try(formatter.render(i))
          rv.failed.foreach { err =>
            logger.error(s"error during formating of $i", err)
          }
          rv
        }.collect {
          case Success(renderedString) =>
            if (renderedString.endsWith(cmwell.util.os.Props.endln)) ByteString(renderedString)
            else ByteString(renderedString + cmwell.util.os.Props.endln)
        }
      }
      else {
        val f2bs: Formattable => ByteString = { formattable =>
          val formatted = formatter.render(formattable)
          val trimmed = dropRightWhile(formatted)(_.isWhitespace)
          ByteString(trimmed + cmwell.util.os.Props.endln)
        }

        length.fold(Flow.fromFunction(f2bs))(l => Flow.fromGraph(TakeWeighted[Formattable](l,true,{
          case IterationResults(_, _, iSeqOpt, _, _) => iSeqOpt.fold(0)(_.size).toLong
          case InfotonHistoryVersions(iSeq) => iSeq.size.toLong
          case BagOfInfotons(iSeq) => iSeq.size.toLong
          case i: Infoton => 1L
        })).map(f2bs))
      }
    }

    def searchThinResultToFatInfoton(nbg: Boolean, crudServiceFS: CRUDServiceFS): Flow[SearchThinResult,Infoton,NotUsed] = Flow[SearchThinResult]
      .mapAsyncUnordered(parallelism)(str => crudServiceFS.getInfotonByUuidAsync(str.uuid, nbg))
      .collect{ case FullBox(i) => i}

    def searchThinResultsToFatInfotons(nbg: Boolean, crudServiceFS: CRUDServiceFS): Flow[SearchThinResults,Infoton,NotUsed] = Flow[SearchThinResults]
      .mapConcat { case SearchThinResults(_, _, _, str, _) => str.toList }
      .via(searchThinResultToFatInfoton(nbg,crudServiceFS))

    def iterationResultsToFatInfotons(nbg: Boolean, crudServiceFS: CRUDServiceFS): Flow[IterationResults,Infoton,NotUsed] = Flow[IterationResults]
      .collect { case IterationResults(_, _, Some(iSeq), _, _) => iSeq}
      .mapConcat(_.map(_.uuid)(bo1))
      .mapAsyncUnordered(parallelism)(crudServiceFS.getInfotonByUuidAsync(_,nbg))
      .collect{ case FullBox(i) => i}

    val iterationResultsToInfotons: Flow[IterationResults,Infoton,NotUsed] = Flow[IterationResults]
      .collect { case IterationResults(_, _, Some(iSeq), _, _) => iSeq}
      .mapConcat(_.toList)
  }
}

class Streams @Inject()(crudServiceFS: CRUDServiceFS) extends LazyLogging {

  import Streams._


  def scrollSourceToByteString(src: Source[IterationResults,NotUsed],
                               formatter: Formatter,
                               withData: Boolean = false,
                               withHistory: Boolean = false,
                               length: Option[Long] = None,
                               fieldsMask: Set[String] = Set.empty,
                               nbg: Boolean): Source[ByteString, NotUsed] = {
    if (!withData) src.via(Flows.formattableToByteString(formatter,length))
    else {

      val s1: Source[Infoton, NotUsed] = length.fold(src.via(Flows.iterationResultsToFatInfotons(nbg,crudServiceFS))){ l =>
        src.via(Flows.iterationResultsToFatInfotons(nbg,crudServiceFS)).take(l)
      }

      val s2: Source[Infoton, NotUsed] = {
        if (fieldsMask.isEmpty) s1
        else s1.map(_.masked(fieldsMask))
      }

      if (withHistory) s2.grouped(Settings.cassandraBulkSize).map(InfotonHistoryVersions.apply).via(Flows.formattableToByteString(formatter))
      else s2.via(Flows.infotonToByteString(formatter))
    }
  }

  def seqScrollSource(pathFilter: Option[PathFilter],
                      fieldFilter: Option[FieldFilter],
                      datesFilter: Option[DatesFilter],
                      paginationParams: PaginationParams,
                      scrollTTL: Long,
                      withHistory: Boolean,
                      withDeleted: Boolean,
                      nbg: Boolean)
                     (applyScrollStarter: ScrollStarter => Seq[Future[IterationResults]])
                     (implicit ec: ExecutionContext): Future[(Source[IterationResults,NotUsed],Long)] = {
    seqScrollSource(ScrollStarter(pathFilter,fieldFilter,datesFilter,paginationParams,scrollTTL,withHistory,withDeleted,nbg || withDeleted))(applyScrollStarter)(ec)
  }
  def seqScrollSource(scrollStarter: ScrollStarter)
                     (applyScrollStarter: ScrollStarter => Seq[Future[IterationResults]])
                     (implicit ec: ExecutionContext): Future[(Source[IterationResults,NotUsed],Long)] = {
    val firstHitsTuple = applyScrollStarter(scrollStarter)
      .map(_.map { startScrollResult => //map the `Future[IterationResults]` (empty initial results) to a tuple of another `Future[IterationResults]` (this time with data) and the number of total hits for that index
        crudServiceFS.scroll(startScrollResult.iteratorId, 360, false, scrollStarter.nbg || scrollStarter.withDeleted) -> startScrollResult.totalHits
      })

    //let's extract from the complicated firstHitsTuple the sequence(per index) of futures of the first scroll results
    val sfit = firstHitsTuple.map(_.flatMap(_._1))

    //converting the inner IterationResults into a Source which will fold on itself asynchronously
    val sfeir = cmwell.util.concurrent.travelist(firstHitsTuple) (_.flatMap {
      case (irf, hits) => irf.map { ir =>
        Source.unfoldAsync(ir) {
          case ir@IterationResults(iteratorId, _, infotonsOpt, _, _) =>
            infotonsOpt
              .collect { case xs if xs.nonEmpty => ir }
              .fold(Future.successful(Option.empty[(IterationResults,IterationResults)])){ ir =>
                crudServiceFS.scroll(iteratorId, 60, withData = false, scrollStarter.nbg || scrollStarter.withDeleted).map(iir => Some(iir -> ir))
              }
        } -> hits
      }
    })

    //converting the sequence of sources to a single source
    sfeir.map {
      case Nil => throw new IllegalStateException("must have at least one index")
      case source :: Nil => source
      case (first,h1) :: (second,h2) :: rest =>
        val source = Source.combine(first,second,rest.map(_._1):_*)(size => Merge(size))
        val hits = h1 + h2 + rest.view.map(_._2).sum
        source -> hits
    }
  }

  def multiScrollSource(pathFilter: Option[PathFilter] = None,
                        fieldFilter: Option[FieldFilter] = None,
                        datesFilter: Option[DatesFilter] = None,
                        paginationParams: PaginationParams = DefaultPaginationParams,
                        withHistory: Boolean = false,
                        withDeleted: Boolean = false,
                        nbg: Boolean = false)(implicit ec: ExecutionContext): Future[(Source[IterationResults,NotUsed],Long)] = {
    seqScrollSource(ScrollStarter(pathFilter,fieldFilter,datesFilter,paginationParams,120,withHistory,withDeleted,nbg)){
      case ScrollStarter(pf,ff,df,pp,ttl,h,d,nbg) =>
        crudServiceFS.startMultiScroll(pf,ff,df,pp,ttl,h,d,nbg || withDeleted)
    }
  }

  def scrollSource(nbg: Boolean,
                   pathFilter: Option[PathFilter] = None,
                   fieldFilters: Option[FieldFilter] = None,
                   datesFilter: Option[DatesFilter] = None,
                   paginationParams: PaginationParams = DefaultPaginationParams,
                   withHistory: Boolean = false,
                   withDeleted: Boolean = false
                  )(implicit ec: ExecutionContext): Future[(Source[IterationResults,NotUsed],Long)]  = {

    val firstHitsTuple = crudServiceFS.startScroll(
      pathFilter = pathFilter,
      fieldsFilters = fieldFilters,
      datesFilter = datesFilter,
      paginationParams = paginationParams,
      scrollTTL = 120,
      withHistory = withHistory,
      withDeleted = withDeleted,
      nbg = nbg || withDeleted
    ).flatMap { startScrollResult =>
      crudServiceFS.scroll(startScrollResult.iteratorId, 120, withData = false).map { firstScrollResult =>
        startScrollResult.totalHits -> firstScrollResult
      }
    }

    //converting the inner IterationResults into a Source which will fold on itself asynchronously
    firstHitsTuple.map {
      case (hits, first) => {
        val source = Source.unfoldAsync(first) {
          case ir@IterationResults(iteratorId, _, infotonsOpt, _, _) => {
            infotonsOpt
              .filter(_.nonEmpty)
              .fold(Future.successful(Option.empty[(IterationResults, IterationResults)])) { _ =>
                crudServiceFS.scroll(iteratorId, 120, withData = false, nbg = nbg || withDeleted).map(iir => Some(iir -> ir))
              }
          }
        }
        source -> hits
      }
    }
  }

  def superScrollSource(nbg: Boolean,
                        pathFilter: Option[PathFilter] = None,
                        fieldFilter: Option[FieldFilter] = None,
                        datesFilter: Option[DatesFilter] = None,
                        paginationParams: PaginationParams = DefaultPaginationParams,
                        withHistory: Boolean = false,
                        withDeleted: Boolean = false
                       )(implicit ec: ExecutionContext): Future[(Source[IterationResults, NotUsed],Long)] = {
    seqScrollSource(ScrollStarter(pathFilter,fieldFilter,datesFilter,paginationParams,120,withHistory,withDeleted,nbg)){
      case ScrollStarter(pf,ff,df,pp,ttl,h,d,nbg) =>
        crudServiceFS.startSuperScroll(pf,ff,df,pp,ttl,h,d,nbg || withDeleted)
    }
  }

  private def enrichWithDataAndFlatten(src: Source[Seq[Infoton],NotUsed], nbg: Boolean): Source[Infoton,NotUsed] = {
    src
      .mapConcat {
        case infotons if infotons.isEmpty => List(Future.successful(Vector.empty[Infoton]))
        case infotons => {
          infotons.map(_.uuid)(bo1)
            .grouped(Settings.cassandraBulkSize)
            .map(crudServiceFS.getInfotonsByUuidAsync(_,nbg))
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
              fieldFilters: Option[FieldFilter],
              nbg: Boolean)(implicit ec: ExecutionContext): Source[SearchThinResult,NotUsed] = {

    Source.unfoldAsync[Long, Source[SearchThinResult, NotUsed]](firstTimeStamp) { timeStamp =>

      val ffs = transformFieldFiltersForConsumption(fieldFilters, timeStamp)
      val pf = path.map(PathFilter(_, descendants))
      val pp = PaginationParams(0, lengthHint)

      val future = crudServiceFS.thinSearch(
                pathFilter       = pf,
                fieldFilters     = Some(ffs),
                paginationParams = pp,
                withHistory      = history,
                withDeleted      = deleted,
                fieldSortParams  = fsp,
                nbg = nbg || deleted)

      future.flatMap {
        case SearchThinResults(total, offset, length, results, _) => {
          if (total == 0) Future.successful(None)
          else if(results.size == total) { //TODO: `if(results.size < lengthHint)` ???
            Future.successful(Some(results.maxBy(_.indexTime).indexTime -> Source(results.toVector)))
          }
          else {
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
              val ffs2 = fieldFilters.map(ff => MultiFieldFilter(Must, List(ff, FieldFilter(Must, Equals, "system.indexTime", indexTime.toString))))
              val fut = scrollSource(
                pathFilter = pf,
                fieldFilters = ffs2,
                paginationParams = PaginationParams(0, 500),
                withHistory = history,
                withDeleted = deleted,
                nbg = nbg || deleted)

              fut.map {
                case (_, 0L) => None
                case (_, `lengthHint`) => Some(indexTime -> Source(results.toVector))
                case (s, _) => {
                  val strs = s.mapConcat {
                    case IterationResults(_,_,infoptons,_,_ ) => {
                      infoptons.fold(List.empty[SearchThinResult])(_.map { i =>
                        SearchThinResult(i.path, i.uuid, DateParser.fdf(i.lastModified), i.indexTime.getOrElse(i.lastModified.getMillis))
                      }(bo3))
                    }
                  }
                  Some(indexTime -> strs)
                }
              }
            }
          }
        }
      }
    }.flatMapConcat(identity)
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
}

case class ScrollStarter(pathFilter: Option[PathFilter] = None,
                         fieldFilters: Option[FieldFilter] = None,
                         datesFilter: Option[DatesFilter] = None,
                         paginationParams: PaginationParams = DefaultPaginationParams,
                         scrollTTL: Long = 60,
                         withHistory: Boolean = false,
                         withDeleted: Boolean = false,
                         nbg: Boolean = false)