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

import akka.actor.{ActorSystem, Scheduler}
import akka.event.Logging
import akka.stream._
import akka.stream.scaladsl.{RunnableGraph, Sink}
import cmwell.dc.stream.MessagesTypesAndExceptions._
import cmwell.dc.stream.akkautils.ConcurrentFlow
import cmwell.dc.stream.algo.AlgoFlow
import cmwell.dc.{LazyLogging, Settings}

import scala.concurrent.ExecutionContext


class FingerPrintEngine(dstServersVec: Vector[(String, Option[Int])])
    extends LazyLogging {

  import DataCenterSyncManager._
  def createSyncingEngine(
    dcInfo: DcInfo
  )(implicit sys:ActorSystem, mat:ActorMaterializer, ec:ExecutionContext): RunnableGraph[SyncerMaterialization] = {
    val localDecider: Supervision.Decider = { e: Throwable =>
      // The decider is not used anymore the restart is done by watching the Future[Done] of the stream - no need for
      // the log print (It's left for completeness until the decider is totally removed.
      logger.debug(s"The stream of sync ${dcInfo.key} got an exception caught" +
        s" in local decider. It inner stream will be stopped (the whole one may continue). The exception is:", e)
      Supervision.Stop
    }
    val tsvSource = dcInfo.tsvFile.fold(
      TsvRetriever(dcInfo, localDecider).mapConcat(identity)
    )(_ => TsvRetrieverFromFile(dcInfo))
    val infotonDataTransformer: BaseInfotonData => BaseInfotonData = Util.createInfotonDataTransformer(dcInfo)
    val syncingEngine: RunnableGraph[SyncerMaterialization] =
      tsvSource
        //        .buffer(Settings.tsvBufferSize, OverflowStrategy.backpressure)
        .async
        .via(RatePrinter(dcInfo.key, _ => 1, "elements", "infoton TSVs from TSV source", 500))
        .via(InfotonAggregator.apply[InfotonData](Settings.maxRetrieveInfotonCount,
          Settings.maxRetrieveByteSize, Settings.maxTotalInfotonCountAggregatedForRetrieve))
        //        .async
        .via(RatePrinter(dcInfo.key, bucket => bucket.size, "elements", "infoton TSVs from InfotonAggregator", 500))
        .via(ConcurrentFlow(Settings.retrieveParallelism)(InfotonRetriever(dcInfo.key, localDecider)))
        .mapConcat(identity)
        .via(AlgoFlow.algoFlow(dcInfo))
        .async
        .via(RatePrinter(dcInfo.key, _.data.size / 1000D, "KB", "KB infoton Data from InfotonRetriever", 5000))
        .map(infotonDataTransformer)
        .via(ConcurrentFlow(Settings.ingestParallelism)(InfotonAllMachinesDistributerAndIngester(dcInfo.key, dstServersVec, localDecider)))
        .toMat(Sink.ignore) {
          case (left, right) =>
            SyncerMaterialization(
              left._1,
              right
                .flatMap { _ =>
                  logger.info(s"The Future of the sink of the stream ${dcInfo.key} " +
                    s"finished with success. Still waiting for the position key.")
                  left._2
                }
                .map { posKeys =>
                  logger.info(s"The Future of the TSV retriever of the stream of sync ${dcInfo.key} " +
                    s"finished with success. The position keys got are: $posKeys")
                  posKeys.last.getOrElse(posKeys.head.get)
                }
            )
        }
        .withAttributes(ActorAttributes.supervisionStrategy(localDecider))
    syncingEngine
  }

}


