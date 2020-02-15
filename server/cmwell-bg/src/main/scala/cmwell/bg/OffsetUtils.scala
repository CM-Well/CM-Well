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
package cmwell.bg

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import cmwell.common.OffsetsService
import cmwell.common.formats.{CompleteOffset, Offset}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

object OffsetUtils extends LazyLogging {

  def commitOffsetSink(streamId: String, topicName: String, startingOffset: Long, startingOffsetPriority: Long,
                       offsetsService: OffsetsService)(implicit ec: ExecutionContext): Sink[Seq[Offset], Future[Done]] = {
    var lastOffsetPersisted = startingOffset - 1
    var lastOffsetPersistedPriority = startingOffsetPriority - 1
    val doneOffsets = new java.util.TreeSet[Offset]()
    val doneOffsetsPriority = new java.util.TreeSet[Offset]()
    Flow[Seq[Offset]]
      .groupedWithin(6000, 3.seconds)
      .mapAsync(1) { offsetGroups =>
        val (allOffsets, allOffsetsPriority) =
          offsetGroups.flatten.partition(_.topic == topicName)
        val offsets = allOffsets.filter(o => o.offset >= startingOffset)
        val offsetsPriority = allOffsetsPriority.filter(o => o.offset >= startingOffsetPriority)

        logger.debug(s"commit offset sink of $streamId: offsets: $offsets")
        logger.debug(s"commit offset sink of $streamId: priority offsets: $offsetsPriority")
        val (offsetToPersist, offsetPriorityToPersist) = doneOffsets
          .synchronized { // until a suitable concurrent collection is found
            val offsetToPersist = if (offsets.nonEmpty) {
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
                logger.debug(s"commit offset sink of $streamId: prev: $prev is greater than lastOffsetPersisted: $lastOffsetPersisted")
                lastOffsetPersisted = prev
                Some(prev)
              } else None
            } else None

            val offsetPriorityToPersist = if (offsetsPriority.nonEmpty) {
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
                logger.debug(s"commit offset sink of $streamId: prevPriority: $prevPriority is greater than " +
                  s"lastOffsetPersistedPriority: $lastOffsetPersistedPriority")
                lastOffsetPersistedPriority = prevPriority
                Some(prevPriority)
              } else None
            } else None
            (offsetToPersist, offsetPriorityToPersist)
          }
        offsetToPersist.fold(Future.successful(())) { prev =>
          offsetsService.writeAsync(s"${streamId}_offset", prev + 1L)
        }
          .flatMap(_ => offsetPriorityToPersist.fold(Future.successful(())) { prevPriority =>
            offsetsService.writeAsync(s"$streamId.p_offset", prevPriority + 1L)
          })
      }
      .toMat(Sink.ignore)(Keep.right)
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
    //the equals method of the Offset class is based only on the offset member. In order to change PartialOffset to CompleteOffset it should be deleted before.
    doneOffsets.removeIf(completedOffsets.contains)
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
          val toAdd = CompleteOffset(o.head.topic, o.head.offset)
          //See below for the complete offsets case. In order to change PartialOffset to CompleteOffset it should be deleted before.
          doneOffsets.remove(toAdd)
          doneOffsets.add(toAdd)
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

  def getOffsetBoundries(bootStrapServers: String, topicPartition: TopicPartition): (Long, Long) = {
    import java.util
    import java.util.Properties

    import org.apache.kafka.clients.consumer.KafkaConsumer
    import org.apache.kafka.common.TopicPartition

    val kafkaConsumerProps = new Properties()
    kafkaConsumerProps.put("bootstrap.servers", bootStrapServers)
    kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaConsumerProps)
    val topicPartitionList = new util.ArrayList[TopicPartition]()
    topicPartitionList.add(topicPartition)
    consumer.assign(topicPartitionList)
    consumer.seekToBeginning(topicPartitionList)
    val earliest = consumer.position(topicPartition)
    consumer.seekToEnd(topicPartitionList)
    val latest = consumer.position(topicPartition)
    (earliest, latest)
  }
}
