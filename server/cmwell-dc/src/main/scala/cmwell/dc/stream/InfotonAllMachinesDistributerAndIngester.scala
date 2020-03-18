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
import cmwell.dc.stream.SingleMachineInfotonIngester.{
  IngestInput,
  IngestOutput,
  IngestState,
  IngestStateStatus
}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by eli on 27/06/16.
  *
  * Distributes each infoton to its matching machine for ingestion
  */
object InfotonAllMachinesDistributerAndIngester extends LazyLogging {

  val initialBulkStatus =
    IngestStateStatus(Settings.initialBulkIngestRetryCount, 0, None)

  def apply(dckey: DcInfoKey,
            hosts: Vector[(String, Option[Int])],
            decider: Decider)(implicit sys: ActorSystem, mat: Materializer) = {
    val size = hosts.length
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val part = b.add(Partition[BaseInfotonData](size, {
        case BaseInfotonData(path, _) =>
          (cmwell.util.string.Hash.adler32long(path) % size).toInt
      }))
      val mergeIngests = b.add(Merge[(Try[IngestOutput], IngestState)](size))
      part.outlets.zipWithIndex.foreach {
        case (o: Outlet[BaseInfotonData @unchecked], i) => {
          val (host, portOpt) = hosts(i)
          val location = s"$host:${portOpt.getOrElse(80)}"
          val ingestFlow =
            SingleMachineInfotonIngester(dckey, location, decider)
          val infotonAggregator = b.add(
            InfotonAggregator[BaseInfotonData](
              Settings.maxIngestInfotonCount,
              Settings.maxIngestByteSize,
              Settings.maxTotalInfotonCountAggregatedForIngest,
              identity
            )
          )
          val initialStateAdder = b.add(
            Flow[scala.collection.immutable.Seq[BaseInfotonData]].map(
              ingestData =>
                Future
                  .successful(ingestData) -> (ingestData -> initialBulkStatus)
            )
          )
          val ingestRetrier =
            //todo: discuss with Moria the implication of this
            Retry.concat(1, Settings.ingestRetryQueueSize, ingestFlow)(
              retryDecider(dckey, location)
            )
          o ~> infotonAggregator ~> initialStateAdder ~> ingestRetrier ~> mergeIngests
            .in(i)
        }
      }
      FlowShape(part.in, mergeIngests.out)
    })
  }

  def retryDecider(dcKey: DcInfoKey, location: String)(implicit sys: ActorSystem, mat: Materializer) =
    (state: IngestState) =>
      state match {
        case (
            ingestSeq,
            IngestStateStatus(
              retriesLeft,
              singleRetryCount,
              Some(ex: IngestServiceUnavailableException)
            )
            ) => {
          logger.warn(s"Sync $dcKey: Ingest to machine $location failed (Service Unavailable). " +
                      s"Will keep trying again until the service will be available. The exception is: ${ex.getMessage} ${ex.getCause.getMessage}")
          Util.warnPrintFuturedBodyException(ex)
          val ingestState =
            (
              ingestSeq,
              IngestStateStatus(
                retriesLeft,
                singleRetryCount + (if (ingestSeq.size == 1) 1 else 0),
                Some(ex)
              )
            )
          Some(
            List(
              akka.pattern.after(
                Settings.ingestServiceUnavailableDelay,
                sys.scheduler
              )(Future.successful(ingestSeq)) -> ingestState
            )
          )
        }
        case (
            ingestSeq,
            IngestStateStatus(retriesLeft, singleRetryCount, ex)
            ) =>
          if (ingestSeq.size == 1 && retriesLeft == 0) {
            val originalRequest =
              ingestSeq.foldLeft(empty)(_ ++ _.data).utf8String
              ex.get match {
                case e: FuturedBodyException =>
                  logger.error(
                    s"${e.getMessage} ${e.getCause.getMessage} No more retries will be done. Please use the red log to see the list of all the failed ingests."
                  )
                  Util.errorPrintFuturedBodyException(e)
                case e =>
                  val u = Util.extractUuid(ingestSeq.head)
                  logger.error(s"Sync $dcKey: Ingest of uuid $u to machine $location failed. No more reties will be done. " +
                               "Please use the red log to see the list of all the failed ingests. The exception is: ", e)
              }
              logger.trace(s"Original Ingest request for uuid ${Util.extractUuid(ingestSeq.head)} was: $originalRequest")
              redlog.info(s"Sync $dcKey: Ingest of uuid ${Util.extractUuid(ingestSeq.head)} to machine $location failed")
              Some(Nil)

          } else if (ingestSeq.size == 1) {
            logger.trace(s"Sync $dcKey: Ingest of uuid ${Util.extractUuid(ingestSeq.head)} to " +
                         s"machine $location failed. Retries left $retriesLeft. Will try again. The exception is: ", ex.get)
            Util.tracePrintFuturedBodyException(ex.get)
            val ingestState =
              (
                ingestSeq,
                IngestStateStatus(retriesLeft - 1, singleRetryCount + 1, ex)
              )
            Some(
              List(
                akka.pattern.after(Settings.ingestRetryDelay, sys.scheduler)(
                  Future.successful(ingestSeq)
                ) -> ingestState
              )
            )
          } else if (retriesLeft == 0) {
            logger.trace(s"Sync $dcKey: Ingest of bulk uuids to machine $location failed. No more bulk retries left. " +
                         s"Will split to request for each uuid and try again. The exception is: ", ex.get)
            Util.tracePrintFuturedBodyException(ex.get)
            Some(ingestSeq.view.map { infotonMetaAndData =>
              val ingestData = Seq(infotonMetaAndData)
              val ingestState = ingestData -> IngestStateStatus(
                Settings.initialSingleIngestRetryCount,
                singleRetryCount,
                ex
              )
              akka.pattern.after(Settings.ingestRetryDelay, sys.scheduler)(
                Future.successful(ingestData)
              ) -> ingestState
            }.to(List))
          } else {
            logger.trace(
              s"Sync $dcKey: Ingest of bulk uuids to machine $location failed. Retries left $retriesLeft. Will try again. The exception is: ",
              ex.get
            )
            Util.tracePrintFuturedBodyException(ex.get)
            val ingestState =
              (
                ingestSeq,
                IngestStateStatus(retriesLeft - 1, singleRetryCount, ex)
              )
            Some(
              List(
                akka.pattern.after(Settings.ingestRetryDelay, sys.scheduler)(
                  Future.successful(ingestSeq)
                ) -> ingestState
              )
            )
          }
    }
}
