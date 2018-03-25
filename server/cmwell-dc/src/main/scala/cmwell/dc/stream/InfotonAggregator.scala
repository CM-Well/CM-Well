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

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import cmwell.dc.stream.InfotonAggregator.InfotonBucket
import cmwell.dc.stream.MessagesTypesAndExceptions.{InfotonData, InfotonMeta}

import scala.collection.immutable.Queue
import scala.collection.mutable

/**
  * Created by eli on 29/06/16.
  */
object InfotonAggregator {

  case class InfotonBucket(paths: mutable.Set[String], infotons: mutable.Builder[InfotonData, Vector[InfotonData]], var weight: Long, var infotonCount: Long = 0)

  def apply(maxInfotonsPerBucket: Int, maxBucketByteSize: Long, maxTotalCount: Long) = new InfotonAggregator(maxInfotonsPerBucket, maxBucketByteSize, maxTotalCount)

}

class InfotonAggregator(maxInfotonsPerBucket: Int, maxBucketByteSize: Long, maxTotalInfotons: Long) extends GraphStage[FlowShape[InfotonData, scala.collection.immutable.Seq[InfotonData]]] {
  val in = Inlet[InfotonData]("InfotonAggregator.in")
  val out = Outlet[scala.collection.immutable.Seq[InfotonData]]("InfotonAggregator.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var bucketQueue: Queue[InfotonBucket] = Queue.empty[InfotonBucket]
    private var pending: InfotonData = _
    private var totalInfotonsInBuckets: Long = 0
    private var firstBucketIndexToSearchFrom: Int = 0

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        //at this point pending was already used and can be freely overwritten
        pending = grab(in)
        putPendingElementIntoBucketQueueIfPossible()
        if (isAvailable(out)) pushBucketIfAvailable()
        RequestAnotherInfotonIfNeededAndCompleteStageIfNeeded()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        val y: Iterator[Vector[InfotonData]] = bucketQueue.map(_.infotons.result()).iterator
        emitMultiple(out,y,() => Option(pending).foreach(id => emit(out, Vector(id))))
        super.onUpstreamFailure(ex)
      }

      override def onUpstreamFinish(): Unit = {
        if (bucketQueue.isEmpty && (pending eq null)) completeStage()
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        putPendingElementIntoBucketQueueIfPossible()
        pushBucketIfAvailable()
        RequestAnotherInfotonIfNeededAndCompleteStageIfNeeded()
      }
    })

    private def pushBucketIfAvailable(): Unit = {
      if (bucketQueue.nonEmpty) {
        val (bucket, newQueue) = bucketQueue.dequeue
        bucketQueue = newQueue
        totalInfotonsInBuckets -= bucket.infotonCount
        firstBucketIndexToSearchFrom -= 1
        if (firstBucketIndexToSearchFrom == -1) firstBucketIndexToSearchFrom = 0
        push(out, bucket.infotons.result)
      }
    }

    private def putPendingElementIntoBucketQueueIfPossible(): Unit = {
      if (pending ne null) {
        val correctBucketIdx = bucketQueue.indexWhere({
          bucket => !bucket.paths.contains(pending.meta.path) &&
            bucket.weight + pending.data.size.toLong <= maxBucketByteSize &&
            bucket.infotonCount < maxInfotonsPerBucket
        }, firstBucketIndexToSearchFrom)
        if (correctBucketIdx == -1) {
          //no bucket fits the new infoton. Create a new bucket (only if not exceeded the max bucket count)
          //Note: this solves the case of a very big infoton the exceeds the max byte size for a bucket - at the initial insert do not check the size
          if (totalInfotonsInBuckets < maxTotalInfotons) {
            val infotonBuilder = Vector.newBuilder[InfotonData]
            infotonBuilder.sizeHint(maxInfotonsPerBucket)
            infotonBuilder += pending
            bucketQueue = bucketQueue.enqueue(InfotonBucket(mutable.Set(pending.meta.path), infotonBuilder, pending.data.size.toLong, 1))
            totalInfotonsInBuckets += 1
            if (maxInfotonsPerBucket == 1) firstBucketIndexToSearchFrom += 1
            pending = null
          }
        }
        else {
          val bucket = bucketQueue(correctBucketIdx)
          bucket.paths += pending.meta.path
          bucket.infotons += pending
          bucket.weight = bucket.weight + pending.data.size.toLong
          bucket.infotonCount += 1
          totalInfotonsInBuckets += 1
          if (bucket.infotonCount == maxInfotonsPerBucket) firstBucketIndexToSearchFrom += 1
          pending = null
        }
      }
    }

    def RequestAnotherInfotonIfNeededAndCompleteStageIfNeeded(): Unit = {
      if (pending == null && !hasBeenPulled(in)) {
        if (!isClosed(in)) pull(in) else if (bucketQueue.isEmpty) completeStage()
      }
    }

/*
    def updateFirstBucketIndexToSearchFrom(): Unit = {
      val tmpFirstBucketIndexToSearchFrom = bucketQueue.indexWhere(bucket => bucket.infotonCount < maxInfotonsPerBucket, firstBucketIndexToSearchFrom)
      if (tmpFirstBucketIndexToSearchFrom != -1) firstBucketIndexToSearchFrom = tmpFirstBucketIndexToSearchFrom
    }
*/
  }
}