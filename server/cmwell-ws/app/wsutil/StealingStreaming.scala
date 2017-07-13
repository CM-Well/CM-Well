///**
//  * Copyright 2015 Thomson Reuters
//  *
//  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *   http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
//  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  *
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
//
//
//package wsutil
//
//import akka.actor.{ActorRef, Actor}
//import akka.actor.Actor.Receive
//import akka.pattern.{pipe, ask}
//import akka.util.Timeout
//import cmwell.domain.{SearchResults, Infoton}
//import cmwell.fts._
//import com.typesafe.scalalogging.LazyLogging
//import logic.CRUDServiceFS
//import scala.collection.immutable.Queue
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.Future
//import scala.concurrent.duration._
//import scala.concurrent.blocking
//
///**
// * Created by michael on 9/7/15.
// */
//object StealingStreaming {
//  def slice(tup : (Long, Long), slices : Int) : Seq[(Long, Long)] = slice(tup._1, tup._2, slices)
//
//  def slice(from : Long, to : Long, slices : Int) : Seq[(Long, Long)] = {
//    require(slices > 0, "Wrong number of slices")
//    val step = (to - from)/slices
//    val rangeMinus1 = for(i <- 1 to slices-1) yield {
//      (from + (i-1)*step, from + i*step - 1)
//    }
//
//    rangeMinus1 ++ Seq((rangeMinus1.lastOption.getOrElse((from - 1 , from - 1))._2 + 1, to))
//  }
//
//  def findLastUuids(infotons : Seq[Infoton]) = {
//    val idx = infotons.head.indexTime.getOrElse(0L)
//    if(infotons.forall(_.indexTime.getOrElse(0L) == idx))
//      (idx+1L, Set.empty[String])
//    else {
//      infotons.foldLeft[(Long,Set[String])](0L, Set.empty[String]) {
//        case ((idxTime, uuids), infoton) =>
//          val infotonIdxTime = infoton.indexTime.getOrElse(0L)
//          if(infotonIdxTime > idxTime)
//            (infotonIdxTime, Set(infoton.uuid))
//          else if(infotonIdxTime < idxTime)
//            (idxTime, uuids)
//          else // infotonIdxTime == idxTime
//            (idxTime, uuids + infoton.uuid)
//      }
//    }
//  }
//}
//
//case class IdleWorker(workerName : String)
//case class GetRanges(numberOfRanges : Int)
//case class Ranges(ranges : Seq[(Long, Long)], total : Long)
//
//case class AdjustRange(from : Long, to : Long)
//
//case object IsFinished
//case object FlushFinished
//
//
//class WorkManager(numberOfWorkers : Int, pathFilter: Option[PathFilter] = None,
//                  fieldFilters: Option[FieldFilter] = None,
//                  datesFilter: Option[DatesFilter] = None,
//                  withHistory: Boolean = false,
//                  withData: Boolean = false) extends Actor with LazyLogging {
//  val idles = scala.collection.mutable.Queue.empty[ActorRef]
//
//
//  @throws[Exception](classOf[Exception])
//  override def preStart(): Unit = {
//    context.system.scheduler.schedule(0.seconds, 2.seconds,self, FlushFinished)
//  }
//
//  override def receive: Receive = {
//    case FlushFinished => {
//      if(idles.size == numberOfWorkers) {
//        idles.foreach(_ ! Range(0L, 0L))
//      }
//    }
//    case IsFinished => {
//      val finished = idles.size == numberOfWorkers
//      logger.info(s"idles=${idles.size} workers=$numberOfWorkers")
//      sender() ! finished
//    }
//    case GetRanges(numberOfRanges) => {
//      val from = 0L
//      val to = System.currentTimeMillis()
//      val ff = fieldFilters.map[FieldFilter]{ fldf =>
//        MultiFieldFilter(Must,List(fldf,FieldFilter(Must, GreaterThanOrEquals, "system.indexTime", from.toString),FieldFilter(Must, LessThanOrEquals, "system.indexTime", to.toString)))
//      }
//
//      logger.debug(s"NSTREAM: path filter: $pathFilter")
//      logger.debug(s"NSTREAM: field filter: $ff")
//      logger.debug(s"NSTREAM: dates filter: $datesFilter")
//
//      val ascF = CRUDServiceFS.search(pathFilter,
//        ff,
//        datesFilter, PaginationParams(0, 1), withHistory, withData, FieldSortParams(List("system.indexTime" -> Asc)), false)
//
//      val descF = CRUDServiceFS.search(pathFilter,
//        ff,
//        datesFilter, PaginationParams(0, 1), withHistory, withData, FieldSortParams(List("system.indexTime" -> Desc)), false)
//
//      val res = for {
//        asc <- ascF
//        desc <- descF
//      } yield {
//          val f = asc.infotons.head.indexTime.getOrElse(0L)
//          val t = desc.infotons.head.indexTime.getOrElse(0L)
//          logger.debug(s"NSTREAM: from: $f to: $t")
//          Ranges(StealingStreaming.slice(f, t, numberOfRanges), asc.total)
//      }
//
//      res pipeTo sender
//    }
//    case IdleWorker(name) => {
//      if(!idles.contains(sender()))
//        idles += sender()
//      logger.info(s"NSTREAM: worker($name) is idle. idles=${idles.size}, workers=$numberOfWorkers")
//    }
//    case AdjustRange(from, to) => {
//      if(idles.isEmpty) {
//        sender ! Range(from, to)
//      } else {
//        sender ! Range(from, (from + to) / 2)
//        val idleWorker = idles.dequeue()
//        idleWorker ! Range((from + to) / 2 + 1, to)
//      }
//    }
//  }
//}
//
//case class DoSearch(from : Long, to : Long)
//case class Range(from : Long, to : Long)
//
//case class WorkerSearchResult(newRange : Range, searchResults : SearchResults)
//
//class StealingWorker(name : String , manager : ActorRef ,iterationSize : Int, pathFilter: Option[PathFilter] = None,
//             fieldFilters: Option[FieldFilter] = None,
//             datesFilter: Option[DatesFilter] = None,
//             withHistory: Boolean = false,
//             withData: Boolean = false) extends Actor with LazyLogging {
//  implicit val timeout = Timeout(20.seconds)
//  var searchCounter = -1
//
//  var s : ActorRef = _
//
//  var sr : SearchResults = _
//
//  override def receive: Actor.Receive = {
//    case DoSearch(from, to) => {
//      s = sender()
//      val start = System.currentTimeMillis()
//      searchCounter += 1
//      logger.info(s"NSTREAM: worker($name) starting search $searchCounter")
//
//      CRUDServiceFS.search(pathFilter,
//        fieldFilters.map(fldf =>
//          MultiFieldFilter(Must,
//            List(
//              fldf,
//              FieldFilter(Must, GreaterThanOrEquals, "system.indexTime", from.toString),
//              FieldFilter(Must, LessThanOrEquals, "system.indexTime", to.toString)))),
//        datesFilter,
//        PaginationParams(0, iterationSize),
//        withHistory,
//        withData,
//        FieldSortParams(List("system.indexTime" -> Asc)),
//        false)
//      .map {
//        searchRes =>
//          logger.info(s"NSTREAM: worker($name) search $searchCounter took ${System.currentTimeMillis() - start} millis.")
//          if(searchRes.length == 0){
//            logger.info(s"NSTREAM: worker($name) no results")
//            sr = searchRes
//            manager ! IdleWorker(name)
//          } else {
//            val (idxTime,uuids) = StealingStreaming.findLastUuids(searchRes.infotons)
//            logger.info(s"NSTREAM: worker($name) findLastUuids=($idxTime, $uuids)")
//            val newLength = searchRes.length - uuids.size
//            val infotons = searchRes.infotons.filterNot(inf => uuids.contains(inf.uuid))
//            logger.info(s"NSTREAM: worker($name) length=$newLength total=${searchRes.total} from=($from, $idxTime), to=$to")
//            sr = searchRes.copy(infotons = infotons, length = newLength)
//            manager ! AdjustRange(idxTime, to)
//          }
//        }
//    }
//    case r@Range(from, to) => {
//      val res = WorkerSearchResult(r, sr)
//      logger.debug(s"NSTREAM: worker($name) is sending back $res")
//      s ! res
//      sr = null
//    }
//  }
//}