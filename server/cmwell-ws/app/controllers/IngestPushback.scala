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

import akka.actor.Actor
import cmwell.util.concurrent.{Combiner, SimpleScheduler, SingleElementLazyAsyncCache}
import cmwell.ws._
import cmwell.ws.Settings._
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid
import akka.pattern.ask
import k.grid.dmap.impl.persistent.PersistentDMap
import play.api.mvc._
import javax.inject._

import actions.DashBoard
import filters.Attrs

import scala.concurrent.duration.DurationLong
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class IngestPushback @Inject() (backPressureToggler: BackPressureToggler, dashBoard: DashBoard, pbp: PlayBodyParsers)
                               (implicit override val executionContext: ExecutionContext) extends ActionBuilder[Request,AnyContent] with LazyLogging {

  override val parser = pbp.defaultBodyParser

  lazy val bGMonitorProxy = new SingleElementLazyAsyncCache[OffsetsInfo](10000L,null)({
    Grid.serviceRef(BGMonitorActor.serviceName).ask(GetOffsetInfo)(akka.util.Timeout(bgMonitorAskTimeout), Actor.noSender).mapTo[OffsetsInfo]
  })(Combiner.replacer[OffsetsInfo],implicitly,implicitly)

  // we use our own custom filter instead of mixing in ActionFilter,
  // to enable pushback by hanging the request,
  // with wrapping user code in invokeBlock.
  // (note that ActionFilter finalizes invokeBlock...)
  private def filterByKLog(): Future[Option[Result]] = {
    bGMonitorProxy.getAndUpdateIfNeeded.map {
      case OffsetsInfo(partitionOffsetsInfos, _) => {
        val (persist, index) = partitionOffsetsInfos.values.partition(_.topic == "persist_topic")
        val persistLoad = persist.foldLeft(Map.empty[Int, Long]) {
          case (sumsByPartition, PartitionOffsetsInfo(_, partition, readOffset, writeOffset, _)) =>
            sumsByPartition.updated(partition, sumsByPartition.getOrElse(partition, 0L) + writeOffset - readOffset)
        }
        val indexLoad = index.foldLeft(Map.empty[Int, Long]) {
          case (sumsByPartition, PartitionOffsetsInfo(_, partition, readOffset, writeOffset, _)) =>
            sumsByPartition.updated(partition, sumsByPartition.getOrElse(partition, 0L) + writeOffset - readOffset)
        }
        if (persistLoad.exists(_._2 > maximumQueueBuildupAllowed))
          Some(Results.ServiceUnavailable("Persistence queue is full. You may try again later"))
        else if (indexLoad.exists(_._2 > maximumQueueBuildupAllowed))
          Some(Results.ServiceUnavailable("Index queue is full. You may try again later"))
        else None
      }
    }.recover {
      case ex: akka.pattern.AskTimeoutException =>
        logger.error("Kafka queue monitor can't accept monitoring requests at the moment. You may try again later", ex)
        Some(Results.ServiceUnavailable("Kafka queue monitor can't accept monitoring requests at the moment. You may try again later"))
      case e: Throwable => {
        logger.error("unexpected error occurred in IngestPushback.filterByKLog()",e)
        Some(Results.InternalServerError("Unexpected error occurred in IngestPushback.filterByKLog()"))
      }
    }
  }

   private def filterByTLog(): Option[Result] = {//Future[Option[Result]] = {

     val (uwh,urh,iwh,irh) = dashBoard.BatchStatus.get._2

     if(uwh - urh > maximumQueueBuildupAllowedUTLog) Some(Results.ServiceUnavailable("Updates tlog queue on this node is full. You may try again later or against a different node."))
     else if(iwh - irh > maximumQueueBuildupAllowedITLog) Some(Results.ServiceUnavailable("Indexer tlog queue on this node is full. You may try again later or against a different node."))
     else None

     //following commented out code backpressures against old TLog, but cluster-wise, and not per node.
//     CtrlClient.getBatchStatus.map{
//       case (m,_) => mapFirst(m.values) {
//         case _: BatchNotIndexing => Some(Results.ServiceUnavailable("Batch worker cannot index more commands at the moment"))
//         case _: BatchDown => Some(Results.ServiceUnavailable("Batch worker is currently down"))
//         case BatchOk(impSize, impLocation, indexerSize, indexerLocation, impRate, indexerRate, genTime) => {
//           if(impSize-impLocation > maximumQueueBuildupAllowed) Some(Results.ServiceUnavailable("Updates tlog queue is full. You may try again later"))
//           else if(indexerSize-indexerLocation > maximumQueueBuildupAllowed) Some(Results.ServiceUnavailable("Indexer tlog queue is full. You may try again later"))
//           else None
//         }
//       }
//     }.recover {
//      //FIXME: hack to disable backpressure in the first 5 minutes (because Michael's HealthActor can't be initialized - catch 22)
//      case _: akka.pattern.AskTimeoutException if cmwell.util.os.Props.getProcessUptime < 300000 => None
//      case _: akka.pattern.AskTimeoutException =>
//        Some(Results.ServiceUnavailable("TLog queue monitor can't accept monitoring requests at the moment. You may try again later"))
//      case e: Throwable => {
//        logger.error("unexpected error occurred in IngestPushback.filterByTLog()",e)
//        Some(Results.InternalServerError("Unexpected error occurred in IngestPushback.filterByTLog()"))
//      }
//    }
   }

  override def invokeBlock[A](request: Request[A], block: Request[A] => Future[Result]): Future[Result] = {
    val startTime = request.attrs(Attrs.RequestReceivedTimestamp)

    def resOptToFilterBy(resOpt: Option[Result]) = resOpt.fold(block(request)) { result =>
      val requestTime = {
        val endTime = System.currentTimeMillis()
        (endTime - startTime).millis
      }
      if (requestTime >= ingestPushbackByServer) Future.successful(result)
      else SimpleScheduler.schedule(ingestPushbackByServer - requestTime)(result)
    }

    if(request.getQueryString("priority").isDefined) block(request) // Authorization of Priority usage is handled in InputHandler. Existence of the query parameter is sufficient to do nothing here.
    else PersistentDMap.get(backPressureToggler.BACKPRESSURE_TRIGGER).flatMap(_.as[String]).getOrElse(Settings.pushbackpressure) match {
      case "new" => filterByKLog().flatMap(resOptToFilterBy)
      case "old" => resOptToFilterBy(filterByTLog())
      case "off" => block(request)
      case "all" => filterByTLog().fold(filterByKLog().flatMap(resOptToFilterBy))(Future.successful)
      case "bar" => Future.successful(Results.ServiceUnavailable(s"Ingests has been barred by an admin. Please try again later."))
      case unknown => Future.successful(Results.InternalServerError(s"unknown state for 'BACKPRESSURE_TRIGGER' [$unknown]"))
    }
  }
}

// TODO: implement SinglePathIngestPushback that should take the path from the input request,
// TODO: and only check the partition it belongs to instead of all the partitions.