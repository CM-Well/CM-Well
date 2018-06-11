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
package cmwell.bg

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import cmwell.common.OffsetsService
import cmwell.common.formats.{CompleteOffset, Offset}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future
import scala.collection.JavaConverters._

object OffsetUtils extends LazyLogging {

  def commitOffsetSink(streamId: String, topicName: String, startingOffset: Long, startingOffsetPriority: Long,
                       offsetsService: OffsetsService): Sink[Seq[Offset], Future[Done]] = {
    var lastOffsetPersisted = startingOffset - 1
    var lastOffsetPersistedPriority = startingOffsetPriority - 1
    val doneOffsets = new java.util.TreeSet[Offset]()
    val doneOffsetsPriority = new java.util.TreeSet[Offset]()
    Flow[Seq[Offset]]
      .groupedWithin(6000, 3.seconds)
      .toMat {
        Sink.foreach { offsetGroups =>
          val (offsets, offsetsPriority) =
            offsetGroups.flatten.partition(_.topic == topicName)
          logger.debug(s"commit offset sink of $streamId: offsets: $offsets")
          logger.debug(s"commit offset sink of $streamId: priority offsets: $offsetsPriority")
          doneOffsets
            .synchronized { // until a suitable concurrent collection is found
              if (offsets.nonEmpty) {
                var prev = lastOffsetPersisted
                mergeOffsets(streamId, doneOffsets, offsets)
                val it = doneOffsets.iterator()
                while ( {
                  val next = if (it.hasNext) Some(it.next) else None
                  val continue =
                    next.fold(false)(
                      o => o.isInstanceOf[CompleteOffset] && o.offset - prev == 1
                    )
                  if (continue)
                    prev = next.get.offset
                  continue
                }) {
                  it.remove()
                }

                if (prev > lastOffsetPersisted) {
                  logger.debug(
                    s"commit offset sink of $streamId: prev: $prev is greater than lastOffsetPersisted: $lastOffsetPersisted"
                  )
                  offsetsService.write(s"${streamId}_offset", prev + 1L)
                  lastOffsetPersisted = prev
                }
              }

              if (offsetsPriority.nonEmpty) {
                var prevPriority = lastOffsetPersistedPriority
                mergeOffsets(streamId, doneOffsetsPriority, offsetsPriority)
                val itPriority = doneOffsetsPriority.iterator()
                while ( {
                  val next =
                    if (itPriority.hasNext) Some(itPriority.next) else None
                  val continue = next.fold(false)(
                    o =>
                      o.isInstanceOf[CompleteOffset] && o.offset - prevPriority == 1
                  )
                  if (continue)
                    prevPriority = next.get.offset
                  continue
                }) {
                  itPriority.remove()
                }

                if (prevPriority > lastOffsetPersistedPriority) {
                  logger.debug(
                    s"commit offset sink of $streamId: prevPriority: $prevPriority is greater than lastOffsetPersistedPriority: $lastOffsetPersistedPriority"
                  )
                  offsetsService.write(s"$streamId.p_offset", prevPriority + 1L)
                  lastOffsetPersistedPriority = prevPriority
                }
              }
            }
        }
      }(Keep.right)
  }

  def mergeOffsets(streamId: String, doneOffsets: java.util.TreeSet[Offset],
                   newOffsets: Seq[Offset]) = {
    logger.debug(
      s"commit offset sink of $streamId: merging doneOffsets:\n $doneOffsets \n with newOffsets:\n $newOffsets"
    )
    val (completedOffsets, partialOffsets) =
      newOffsets.partition(_.isInstanceOf[CompleteOffset])
    logger.debug(
      s"commit offset sink of $streamId: completedOffsests:\n $completedOffsets \n partialOffsets:\n$partialOffsets"
    )
    doneOffsets.addAll(completedOffsets.asJava)
    logger.debug(
      s"commit offset sink of $streamId: doneOffsets after adding all completed new offsets:\n$doneOffsets"
    )
    partialOffsets.groupBy(_.offset).foreach {
      case (_, o) =>
        logger.debug(s"commit offset sink of $streamId: handling new partial offset: $o")
        if (o.size == 2) {
          logger.debug(
            s"commit offset sink of $streamId: two new partial offsets become one completed, adding to doneOffsets"
          )
          doneOffsets.add(CompleteOffset(o.head.topic, o.head.offset))
        } else if (doneOffsets.contains(o.head)) {
          logger.debug(
            s"commit offset sink of $streamId: doneOffsets already contained 1 partial offset for ${o.head} removing it and adding completed instead"
          )
          doneOffsets.remove(o.head)
          doneOffsets.add(CompleteOffset(o.head.topic, o.head.offset))
        } else {
          logger.debug(s"commit offset sink of $streamId: adding new partial ${o.head} to doneOffsets")
          doneOffsets.add(o.head)
        }
    }
    logger.debug(
      s"commit offset sink of $streamId: doneOffsets after adding partial new offsets:\n $doneOffsets"
    )
  }
}
