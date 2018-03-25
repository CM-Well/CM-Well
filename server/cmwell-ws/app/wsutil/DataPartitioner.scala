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


/*
package wsutil

import akka.pattern.{pipe, ask}
import akka.actor.Actor.Receive
import akka.actor._
import akka.util.Timeout
import cmwell.domain._
import cmwell.fts._
import com.typesafe.scalalogging.LazyLogging
import controllers.Application._
import k.grid.Grid
import logic.CRUDServiceFS
import play.api.libs.iteratee.Enumerator
//import wsutil.DataPartitioner._


import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Try, Failure, Success}
import scala.concurrent.duration._

/**
 * Created by michael on 8/13/15.
 */


object RangeSlicer {

  def slice(tup : (Long, Long), slices : Int) : Seq[(Long, Long)] = slice(tup._1, tup._2, slices)

  def slice(from : Long, to : Long, slices : Int) : Seq[(Long, Long)] = {
    require(slices > 0, "Wrong number of slices")
    val step = (to - from)/slices
    val rangeMinus1 = for(i <- 1 to slices-1) yield {
      (from + (i-1)*step, from + i*step - 1)
    }

    rangeMinus1 ++ Seq((rangeMinus1.lastOption.getOrElse((from - 1 , from - 1))._2 + 1, to))
  }
}

object DataPartitioner extends LazyLogging {
  implicit val timeout = Timeout(10.seconds)
  val threshold = 10000L
  val defaultK = 100000L


  def crudSearchWrapper(pathFilter: Option[PathFilter] = None, fieldFilters: List[(FieldOperator, List[FieldFilter])] = Nil,
                  datesFilter: Option[DatesFilter] = None, paginationParams: PaginationParams = DefaultPaginationParams,
                  withHistory: Boolean = false, withData: Boolean = false, fieldSortParams: List[FieldSortParam] = Nil,
                  debugInfo: Boolean = false) : Future[SearchResults] = {
    val start = System.currentTimeMillis()
    val f = CRUDServiceFS.search(pathFilter, fieldFilters, datesFilter, paginationParams, withHistory, withData, fieldSortParams,debugInfo )
    f.onSuccess {
      case _ => logger.info(s"CRUDSearch took ${System.currentTimeMillis() - start} millis")
    }
    f
  }



  def findLastUuids(infotons : Seq[Infoton]) = {
    infotons.foldLeft[(Long,Set[String])](0L, Set.empty[String]) {
      case ((idxTime, uuids), infoton) =>
        val infotonIdxTime = infoton.indexTime.getOrElse(0L)
        if(infotonIdxTime > idxTime)
          (infotonIdxTime, Set(infoton.uuid))
        else if(infotonIdxTime < idxTime)
          (idxTime, uuids)
        else // infotonIdxTime == idxTime
          (idxTime, uuids + infoton.uuid)
    }
  }

  def getKElements(from : Long, K : Long, pathFilter: Option[PathFilter] = None, fieldFilters: List[(FieldOperator, List[FieldFilter])] = Nil,
                   datesFilter: Option[DatesFilter] = None, paginationParams: PaginationParams = DefaultPaginationParams,
                   withHistory: Boolean = false, withData: Boolean = false, fieldSortParams: List[FieldSortParam] = Nil,
                   debugInfo: Boolean = false, bottom : Long, upper : Long , isFirst : Boolean = true, total : Long = -1 ) : Future[DataPartitionResult] = {


    if (isFirst) {
      logger.info(s"isFirst $bottom - $upper")
      val ascF = crudSearchWrapper(pathFilter,
        fieldFilters ++ List((Must, List(FieldFilter(Must, GreaterThanOrEquals, "system.indexTime", from.toString))), (Must, List(FieldFilter(Must, LessThanOrEquals, "system.indexTime", upper.toString)))),
        datesFilter, PaginationParams(0, 1), withHistory, withData, List(FieldSortParam("system.indexTime", Asc)), debugInfo)

      val descF = crudSearchWrapper(pathFilter,
        fieldFilters ++ List((Must, List(FieldFilter(Must, GreaterThanOrEquals, "system.indexTime", from.toString))), (Must, List(FieldFilter(Must, LessThanOrEquals, "system.indexTime", upper.toString)))),
        datesFilter, PaginationParams(0, 1), withHistory, withData, List(FieldSortParam("system.indexTime", Desc)), debugInfo)

      val res = for {
        asc <- ascF
        desc <- descF
      } yield {
        val fromOpt = asc.infotons.head.indexTime
        val toOpt = desc.infotons.head.indexTime

        (fromOpt, toOpt) match {
          case (Some(fromRange), Some(toRange)) =>
            logger.info(s"BinarySearch: Performing search from $fromRange to $toRange.")
            if(asc.total <= K + threshold) {
              Future.successful(DataPartitionResult(asc.total, fromRange, toRange, asc.total))
            } else {
              getKElements(fromRange, K, pathFilter, fieldFilters, datesFilter, paginationParams, withHistory, withData, fieldSortParams, debugInfo, fromRange, toRange, false, asc.total)
            }

          case _ => throw new Exception("IndexTime is not present")
        }
      } recover {
          case err : Throwable => DataPartitionResult(0L, 0L, 0L, 0L)
        }
      res.flatMap{f => f}
    } else {

      logger.info(s"BinarySearch: (bottom, upper)  =====> ($bottom, $upper)")
      val mid = (upper + bottom) / 2
      crudSearchWrapper(pathFilter,
        fieldFilters ++ List((Must, List(FieldFilter(Must, GreaterThan, "system.indexTime", from.toString))), (Must, List(FieldFilter(Must, LessThan, "system.indexTime", mid.toString)))),
        datesFilter, PaginationParams(0, 1), withHistory, withData, fieldSortParams, debugInfo).flatMap {
        sres =>
          logger.info(s"BinarySearch: sres  =====> $sres")
          val length = sres.total

          if (length <= K + threshold && length >= K - threshold) {
            logger.info(s"BinarySearch: length <= K + threshold && length >= K - threshold  =====> $length <= $K + $threshold && $length >= $K - $threshold")
            Future.successful(DataPartitionResult(length, from, mid, total))
          }
          else if (length > K + threshold) {
            logger.info(s"BinarySearch: length > K + threshold  =====> $length > $K + $threshold")
            val start = System.currentTimeMillis()
            val f = getKElements(from, K, pathFilter, fieldFilters, datesFilter, paginationParams, withHistory, withData, fieldSortParams, debugInfo, bottom, mid - 1, false, total)
            f.onSuccess {
              case _ => logger.info(s"getKElements took ${System.currentTimeMillis() - start} millis")
            }
            f
          } else { // length < K - threshold
            logger.info("BinarySearch: =====> else")
            val start = System.currentTimeMillis()
            val f = getKElements(from, K, pathFilter, fieldFilters, datesFilter, paginationParams, withHistory, withData, fieldSortParams, debugInfo, mid + 1, upper, false, total)
            f.onSuccess {
              case _ => logger.info(s"getKElements took ${System.currentTimeMillis() - start} millis")
            }
            f
          }
      }
    }
  }
}


case class DataPartitionResult(partitionSize : Long, from : Long, to : Long, total : Long) {
  def isLast : Boolean = {
    partitionSize <= DataPartitioner.defaultK - DataPartitioner.threshold
  }
}



case class Partitions(aggs : Seq[Bucket])
class PartitionerHistogramActor(
  pathFilter: Option[PathFilter] = None,
  fieldFilters: List[(FieldOperator, List[FieldFilter])] = Nil,
  datesFilter: Option[DatesFilter] = None,
  paginationParams: PaginationParams = DefaultPaginationParams,
  withHistory: Boolean = false,
  withData: Boolean = false,
  fieldSortParams: List[FieldSortParam] = Nil,
  debugInfo: Boolean = false)
extends Actor with LazyLogging {

  var partitionsF : Future[Partitions] = _
  var currentIndex = 0

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
//    val current = System.currentTimeMillis()
//    val ascF = DataPartitioner.crudSearchWrapper(pathFilter,
//      fieldFilters ++ List((Must, List(FieldFilter(Must, GreaterThanOrEquals, "system.indexTime", "0"))), (Must, List(FieldFilter(Must, LessThanOrEquals, "system.indexTime", current.toString)))),
//      datesFilter, PaginationParams(0, 1), withHistory, withData, List(FieldSortParam("system.indexTime", Asc)), debugInfo)
//
//    val descF = DataPartitioner.crudSearchWrapper(pathFilter,
//      fieldFilters ++ List((Must, List(FieldFilter(Must, GreaterThanOrEquals, "system.indexTime", "0"))), (Must, List(FieldFilter(Must, LessThanOrEquals, "system.indexTime", current.toString)))),
//      datesFilter, PaginationParams(0, 1), withHistory, withData, List(FieldSortParam("system.indexTime", Desc)), debugInfo)
//
//    val res = for {
//      asc <- ascF
//      desc <- descF
//    } yield {
//        val fromOpt = asc.infotons.head.indexTime
//        val toOpt = desc.infotons.head.indexTime
//
//        (fromOpt, toOpt) match {
//          case (Some(fromRange), Some(toRange)) =>
//            logger.info(s"BinarySearch: Performing search from $fromRange to $toRange.")
//            if(asc.total <= K + threshold) {
//              Future.successful(DataPartitionResult(asc.total, fromRange, toRange, asc.total))
//            } else {
//              getKElements(fromRange, K, pathFilter, fieldFilters, datesFilter, paginationParams, withHistory, withData, fieldSortParams, debugInfo, fromRange, toRange, false, asc.total)
//            }
//
//          case _ => throw new Exception("IndexTime is not present")
//        }
//      } recover {
//        case err : Throwable => DataPartitionResult(0L, 0L, 0L, 0L)
//      }
//    res.flatMap{f => f}







    partitionsF = CRUDServiceFS.aggregate(pathFilter,
      fieldFilters,
      datesFilter,
      paginationParams,
      withHistory,
      List(HistogramAggregationFilter("HistogramAggregatio",Field(AnalyzedField,"system.indexTime"), 1000000, 1, None, None, List())),
      false).map {
      partitions =>
        val terms = partitions.responses.collect {case r : HistogramAggregationResponse => r}
        val buckets = terms.map(_.buckets).flatten.filterNot(_.docCount == 0L)
        logger.info(s"Hist: $buckets")
        Partitions(buckets)
    }

  }

  private def createPartition: Future[DataPartitionResult] = {

    partitionsF.map {
      partitions =>
        var size = 0L
        val from = partitions.aggs(currentIndex).key.value.toString.toLong
        while(size < DataPartitioner.defaultK && currentIndex < partitions.aggs.size) {
          size += partitions.aggs(currentIndex).docCount
          currentIndex += 1
        }
        logger.info(s"Hist: aggregated $size infotons")
        val to = partitions.aggs(Math.min(currentIndex,partitions.aggs.size - 1)).key.value.toString.toLong

        val dpr = DataPartitionResult(size, from, to , 0L)
        logger.info(s"Hist: $dpr")
        dpr
    }
  }

  override def receive: Actor.Receive = {
    case GetNextPartition => createPartition.pipeTo(sender)
    case GetFirstPartition => createPartition.pipeTo(sender)
  }
}


case object GetFirstPartition
case class PrepareSecondPartition(dpr : DataPartitionResult)
case object GetNextPartition
case class UpdateFrom(from : Long)
case class UpdateCurrentPartition(dps : DataPartitionResult)

class PartitionerBinarySearchActor(
  pathFilter: Option[PathFilter] = None,
  fieldFilters: List[(FieldOperator, List[FieldFilter])] = Nil,
  datesFilter: Option[DatesFilter] = None,
  paginationParams: PaginationParams = DefaultPaginationParams,
  withHistory: Boolean = false,
  withData: Boolean = false,
  fieldSortParams: List[FieldSortParam] = Nil,
  debugInfo: Boolean = false)
extends Actor with LazyLogging {
  implicit val timeout = Timeout(10.seconds)
  private[this] var from = 0L
  private[this] val to = System.currentTimeMillis()

  private[this] var currentPartitionF : Future[DataPartitionResult] = _


  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    logger.debug("Started PartitionerActor")
  }
  override def receive: Receive = {
    case GetFirstPartition =>
      logger.debug("GetFirstPartition")
      val start = System.currentTimeMillis()
      val res = DataPartitioner.getKElements(0, DataPartitioner.defaultK, pathFilter,
                                                             fieldFilters,
                                                             datesFilter,
                                                             paginationParams,
                                                             withHistory,
                                                             withData,
                                                             fieldSortParams,
                                                             debugInfo,
                                                             0L,
                                                             to)
      val s = sender()
      res.onSuccess {
        case _ => logger.info(s"PartitionSearch (First) took ${System.currentTimeMillis() - start} millis")
      }
      res.map {
        r =>
          logger.debug(s"Getting partition (first): $r" )
          self ! UpdateCurrentPartition(r)
          s ! r
      }
      //currentPartitionF = res


    case GetNextPartition =>
      logger.debug("GetNextPartition")
      val s = sender()
      val start = System.currentTimeMillis()

      currentPartitionF.onSuccess {
        case _ => logger.info(s"PartitionSearch took ${System.currentTimeMillis() - start} millis")
      }

      currentPartitionF.map {
        currentPartition =>
          logger.debug(s"Getting partition: $currentPartition")
          s ! currentPartition
          self ! UpdateCurrentPartition(currentPartition)
      }
    case UpdateCurrentPartition(dps) =>
      logger.debug(s"UpdateCurrentPartition($dps)")
      currentPartitionF =  DataPartitioner.getKElements(dps.to, DataPartitioner.defaultK, pathFilter,
        fieldFilters,
        datesFilter,
        paginationParams,
        withHistory,
        withData,
        fieldSortParams,
        debugInfo,
        dps.to,
        to)
      currentPartitionF.onComplete {
        case Success(cp) => logger.debug(s"Current partition is $cp")
        case Failure(err) => logger.debug(s"failed to create new partition")
      }
  }
}



case class DoSearch(from : Long, to : Long)
class PartitionerWorkerActor(
                              pathFilter: Option[PathFilter] = None,
                              fieldFilters: List[(FieldOperator, List[FieldFilter])] = Nil,
                              datesFilter: Option[DatesFilter] = None,
                              paginationParams: PaginationParams = DefaultPaginationParams,
                              withHistory: Boolean = false,
                              withData: Boolean = false,
                              fieldSortParams: List[FieldSortParam] = Nil,
                              debugInfo: Boolean = false) extends Actor with LazyLogging {
  private[this] val searchIteration = 5000

  private def combineSearchResults(searchresult1 : SearchResults, searchResult2 : SearchResults) : SearchResults = {
    //fromDate:Option[DateTime], toDate:Option[DateTime], total:Long, offset:Long, length:Long,
    //infotons:Seq[Infoton], debugInfo:Option[String] = None

    val fromDate = (searchresult1.fromDate, searchResult2.fromDate) match {
      case (Some(fd1), Some(fd2)) => fd1.compareTo(fd2) match {
        case 0 => Some(fd1)
        case -1 => Some(fd2)
        case 1 => Some(fd1)
      }
      case (None, Some(fd2)) => Some(fd2)
      case (Some(fd1), None) => Some(fd1)
      case (None, None) => None
    }

    val toDate = (searchresult1.toDate, searchResult2.toDate) match {
      case (Some(td1), Some(td2)) => td1.compareTo(td2) match {
        case 0 => Some(td1)
        case -1 => Some(td1)
        case 1 => Some(td2)
      }
      case (None, Some(td2)) => Some(td2)
      case (Some(td1), None) => Some(td1)
      case (None, None) => None
    }
    val total = searchresult1.total + searchResult2.total
    val offset = 0
    val length = searchresult1.length + searchResult2.length
    val infotons = searchresult1.infotons ++ searchResult2.infotons

    SearchResults(fromDate, toDate, total, offset, length, infotons, None)
  }

  private def doSearch(from : Long, to : Long, excludeUuids : Set[String] = Set.empty[String]) : Future[SearchResults] = {
    DataPartitioner.crudSearchWrapper(pathFilter,
      fieldFilters ++ List((Must, List(FieldFilter(Must, GreaterThanOrEquals, "system.indexTime", from.toString))), (Must, List(FieldFilter(Must, LessThanOrEquals, "system.indexTime", to.toString)))),
      None,
      PaginationParams(0,searchIteration),
      withHistory,
      withData,
      List(FieldSortParam("system.indexTime", Asc)),
      false ).flatMap {
      sr =>
        val noDups = sr.copy(infotons = sr.infotons.filterNot(inf => excludeUuids.contains(inf.uuid)))
        if(sr.length == searchIteration) {
          val last = DataPartitioner.findLastUuids(noDups.infotons)
          doSearch(last._1, to, last._2).map {
            sr2 =>
              combineSearchResults(noDups, sr2)
          }
        } else {
          Future.successful(noDups)
        }
    }
  }

  override def receive: Actor.Receive = {
    case msg@DoSearch(from, to) =>
      val s = sender()
      logger.debug(s"Worker doing search $msg")
      val start = System.currentTimeMillis()
      val f = doSearch(from, to)
      f.onSuccess {
        case _ => logger.info(s"WorkerJob took ${System.currentTimeMillis() - start} millis")
      }
      f.pipeTo(sender)
  }
}



*/
