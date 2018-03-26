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
package cmwell.dc.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpEntity, HttpResponse}
import akka.stream.Supervision._
import akka.stream._
import akka.stream.contrib.Retry
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Merge, Partition, Sink}
import akka.util.ByteString
import cmwell.dc.{LazyLogging, Settings}
import cmwell.dc.stream.MessagesTypesAndExceptions._
import cmwell.dc.stream.SingleMachineInfotonIngester.{IngestInput, IngestOutput, IngestState, IngestStateStatus}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by eli on 27/06/16.
  *
  * Distributes each infoton to its matching machine for ingestion
  */
object InfotonAllMachinesDistributerAndIngester extends LazyLogging {

  val breakOut =
    scala.collection.breakOut[IngestInput, (Future[IngestInput], IngestState), List[(Future[IngestInput], IngestState)]]
  val initialBulkStatus = IngestStateStatus(Settings.initialBulkIngestRetryCount, 0, None)

  def apply(dataCenterId: String, hosts: Vector[(String, Option[Int])], decider: Decider)(implicit sys: ActorSystem,
                                                                                          mat: Materializer) = {
    val size = hosts.length
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val part = b.add(Partition[InfotonData](size, {
        case InfotonData(im, _) =>
          (cmwell.util.string.Hash.adler32long(im.path) % size).toInt
      }))
      val mergeIngests = b.add(Merge[(Try[IngestOutput], IngestState)](size))
      part.outlets.zipWithIndex.foreach {
        case (o: Outlet[InfotonData @unchecked], i) => {
          val (host, portOpt) = hosts(i)
          val location = s"$host:${portOpt.getOrElse(80)}"
          val ingestFlow = SingleMachineInfotonIngester(dataCenterId, location, decider)
          val infotonAggregator = b.add(
            InfotonAggregator(Settings.maxIngestInfotonCount,
                              Settings.maxIngestByteSize,
                              Settings.maxTotalInfotonCountAggregatedForIngest)
          )
          val initialStateAdder = b.add(
            Flow[scala.collection.immutable.Seq[InfotonData]]
              .map(ingestData => Future.successful(ingestData) -> (ingestData -> initialBulkStatus))
          )
          val ingestRetrier =
            Retry.concat(Settings.ingestRetryQueueSize, ingestFlow)(retryDecider(dataCenterId, location))
          o ~> infotonAggregator ~> initialStateAdder ~> ingestRetrier ~> mergeIngests.in(i)
        }
      }
      FlowShape(part.in, mergeIngests.out)
    })
  }

  def retryDecider(dataCenterId: String, location: String)(implicit sys: ActorSystem, mat: Materializer) =
    (state: IngestState) =>
      state match {
        case (ingestSeq,
              IngestStateStatus(retriesLeft, singleRetryCount, Some(ex: IngestServiceUnavailableException))) => {
          logger.warn(
            s"Data Center ID $dataCenterId: Ingest to machine $location failed (Service Unavailable). Will keep trying again until the service will be available. The exception is: ${ex.getMessage} ${ex.getCause.getMessage}"
          )
          Util.warnPrintFuturedBodyException(ex)
          val ingestState =
            (ingestSeq,
             IngestStateStatus(retriesLeft, singleRetryCount + (if (ingestSeq.size == 1) 1 else 0), Some(ex)))
          Some(
            List(
              akka.pattern
                .after(Settings.ingestServiceUnavailableDelay, sys.scheduler)(Future.successful(ingestSeq)) -> ingestState
            )
          )
        }
        case (ingestSeq, IngestStateStatus(retriesLeft, singleRetryCount, ex)) =>
          if (ingestSeq.size == 1 && retriesLeft == 0) {
            val originalRequest = ingestSeq.foldLeft(empty)(_ ++ _.data).utf8String
            if (!originalRequest.contains("meta/sys#indexTime")) {
              val originalInfotonData = state._1.head
              val idxTime = originalInfotonData.meta.indexTime
              val subject = originalInfotonData.data.takeWhile(_ != space).utf8String
              val idxTimeQuad =
                s"""$subject <cmwell://meta/sys#indexTime> "$idxTime"^^<http://www.w3.org/2001/XMLSchema#long> ."""
              logger.warn(
                s"""Data Center ID $dataCenterId: Ingest of uuid ${originalInfotonData.meta.uuid.utf8String} to machine $location didn't have index time. Adding "$idxTimeQuad" from metadata manually"""
              )
              val ingestData = Seq(
                InfotonData(originalInfotonData.meta, originalInfotonData.data ++ ByteString(idxTimeQuad) ++ endln)
              )
              val ingestState = ingestData -> IngestStateStatus(Settings.initialSingleIngestRetryCount,
                                                                singleRetryCount + 1,
                                                                ex)
              Some(
                List(
                  akka.pattern
                    .after(Settings.ingestRetryDelay, sys.scheduler)(Future.successful(ingestData)) -> ingestState
                )
              )
            } else {
              ex.get match {
                case e: FuturedBodyException =>
                  logger.error(
                    s"${e.getMessage} ${e.getCause.getMessage} No more retries will be done. Please use the red log to see the list of all the failed ingests."
                  )
                  Util.errorPrintFuturedBodyException(e)
                case e =>
                  logger.error(
                    s"Data Center ID $dataCenterId: Ingest of uuid ${ingestSeq.head.meta.uuid.utf8String} to machine $location failed. No more reties will be done. Please use the red log to see the list of all the failed ingests. The exception is: ",
                    e
                  )
              }
              logger.trace(
                s"Original Ingest request for uuid ${ingestSeq.head.meta.uuid.utf8String} was: $originalRequest"
              )
              redlog.info(
                s"Data Center ID $dataCenterId: Ingest of uuid ${ingestSeq.head.meta.uuid.utf8String} to machine $location failed"
              )
              Some(Nil)
            }
          } else if (ingestSeq.size == 1) {
            logger.trace(
              s"Data Center ID $dataCenterId: Ingest of uuid ${ingestSeq.head.meta.uuid.utf8String} to machine $location failed. Retries left $retriesLeft. Will try again. The exception is: ",
              ex.get
            )
            Util.tracePrintFuturedBodyException(ex.get)
            val ingestState = (ingestSeq, IngestStateStatus(retriesLeft - 1, singleRetryCount + 1, ex))
            Some(
              List(
                akka.pattern
                  .after(Settings.ingestRetryDelay, sys.scheduler)(Future.successful(ingestSeq)) -> ingestState
              )
            )
          } else if (retriesLeft == 0) {
            logger.trace(
              s"Data Center ID $dataCenterId: Ingest of bulk uuids to machine $location failed. No more bulk retries left. Will split to request for each uuid and try again. The exception is: ",
              ex.get
            )
            Util.tracePrintFuturedBodyException(ex.get)
            Some(ingestSeq.map { infotonMetaAndData =>
              val ingestData = Seq(infotonMetaAndData)
              val ingestState = ingestData -> IngestStateStatus(Settings.initialSingleIngestRetryCount,
                                                                singleRetryCount,
                                                                ex)
              akka.pattern.after(Settings.ingestRetryDelay, sys.scheduler)(Future.successful(ingestData)) -> ingestState
            }(breakOut))
          } else {
            logger.trace(
              s"Data Center ID $dataCenterId: Ingest of bulk uuids to machine $location failed. Retries left $retriesLeft. Will try again. The exception is: ",
              ex.get
            )
            Util.tracePrintFuturedBodyException(ex.get)
            val ingestState = (ingestSeq, IngestStateStatus(retriesLeft - 1, singleRetryCount, ex))
            Some(
              List(
                akka.pattern
                  .after(Settings.ingestRetryDelay, sys.scheduler)(Future.successful(ingestSeq)) -> ingestState
              )
            )
          }
    }
}
