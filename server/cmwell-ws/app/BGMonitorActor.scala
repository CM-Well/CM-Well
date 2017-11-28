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

import java.util.Properties

import akka.actor.Actor
import cmwell.common.ExitWithError
import cmwell.common.OffsetsService
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.joda.time.DateTime
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


/**
  * Created by israel on 01/12/2016.
  */
class BGMonitorActor(zkServers:String, offsetService:OffsetsService, implicit val ec:ExecutionContext = concurrent.ExecutionContext.Implicits.global) extends Actor with LazyLogging {

  val zkClient = new ZkClient(zkServers, 10000, 10000, ZKLikeStringSerializer)
  val zkUtils = ZkUtils(zkClient,false)
  val allBrokers = zkUtils.getAllBrokersInCluster().map{ b =>
    val endPoint = b.endPoints.head
    s"${endPoint.host}:${endPoint.port}"
  }.mkString(",")
  val topics = Seq("persist_topic", "persist_topic.priority", "index_topic", "index_topic.priority")
  val partitionsForTopics = zkUtils.getPartitionsForTopics(topics)
  val topicsPartitionsAndGroups = partitionsForTopics.flatMap{
    case ("persist_topic", partitions) => partitions.map{partition => (new TopicPartition("persist_topic", partition), s"imp.$partition")}
    case ("persist_topic.priority", partitions) => partitions.map{partition => (new TopicPartition("persist_topic.priority", partition), s"imp.p.$partition")}
    case ("index_topic", partitions) => partitions.map{partition => (new TopicPartition("index_topic", partition), s"indexer.$partition")}
    case ("index_topic.priority", partitions) => partitions.map{partition => (new TopicPartition("index_topic.priority", partition), s"indexer.p.$partition")}
  }

  val topicsPartitionsAndConsumers = topicsPartitionsAndGroups.map { case (topicPartition, groupId) =>
    val kafkaConsumerProps = new Properties()
    kafkaConsumerProps.put("bootstrap.servers", allBrokers)
    kafkaConsumerProps.put("group.id", groupId)
    kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaConsumerProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaConsumerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    (topicPartition,new KafkaConsumer[Array[Byte], Array[Byte]](kafkaConsumerProps))
  }

  val topicsPartitions = topicsPartitionsAndConsumers.keys

  var previousOffsetInfo:OffsetsInfo = OffsetsInfo(Map.empty[String, PartitionOffsetsInfo], DateTime.now())
  @volatile var currentOffsetInfo:OffsetsInfo = OffsetsInfo(Map.empty[String, PartitionOffsetsInfo], DateTime.now())

  var lastFetchDuration:Long = 0


  import java.util.concurrent.ConcurrentHashMap

  val redSince:collection.concurrent.Map[Int, Long] = new ConcurrentHashMap[Int, Long]().asScala

  self ! CalculateOffsetInfo


  override def receive: Receive = {
    case GetOffsetInfo =>
      logger debug s"got GetOffsetInfo message returning $currentOffsetInfo"
      sender() ! currentOffsetInfo
    case CalculateOffsetInfo =>
      logger debug s"got inner request to generate new offsets info"
      generateOffsetsInfo
  }

  @volatile var statusesCheckedTime:Long = System.currentTimeMillis()

  private def generateOffsetsInfo = {

    logger debug s"generating offsets info"

    def calculateOffsetInfo():Future[(OffsetsInfo, Long)] = {
      import concurrent._
      Future {
        blocking {
          val start = System.currentTimeMillis()
          val topicPartitionsWriteOffsets = topicsPartitionsAndConsumers.head._2.endOffsets(topicsPartitions.asJavaCollection)
          val partitionsOffsetsInfo:Map[String, PartitionOffsetsInfo] = topicPartitionsWriteOffsets.asScala.map{ case (topicPartition, writeOffset) =>
            val streamId = topicPartition.topic() match {
              case "persist_topic" => s"imp.${topicPartition.partition()}_offset"
              case "persist_topic.priority" => s"imp.${topicPartition.partition()}.p_offset"
              case "index_topic" => s"indexer.${topicPartition.partition()}_offset"
              case "index_topic.priority" => s"indexer.${topicPartition.partition()}.p_offset"
            }
            val readOffset = offsetService.read(streamId).getOrElse(0L)
            ((topicPartition.topic() + topicPartition.partition()), PartitionOffsetsInfo(topicPartition.topic(), topicPartition.partition(), readOffset, writeOffset))
          }.toMap
          val end = System.currentTimeMillis()
          (OffsetsInfo(partitionsOffsetsInfo, new DateTime()), end - start)
        }
      }
    }

    calculateOffsetInfo().onComplete {
      case Success((info, duration)) =>
        logger debug s"calculate offset info successful: \nInfo:$info\nDuration:$duration"
        val now = System.currentTimeMillis()
        if(now - statusesCheckedTime > 1 * 60 * 1000){
            logger debug s"more than 1 minute has past since last checked statuses, let's check"
          statusesCheckedTime = now
          previousOffsetInfo = currentOffsetInfo
          try {
            val partitionsOffsetInfoUpdated = info.partitionsOffsetInfo.map { case (key, partitionInfo) =>
              val readDiff = partitionInfo.readOffset - previousOffsetInfo.partitionsOffsetInfo.get(key).map {
                _.readOffset
              }.getOrElse(0L)
                val partitionStatus = {
                  if(readDiff > 0)
                    Green
                  else if (partitionInfo.readOffset - partitionInfo.writeOffset == 0) {
                    Green
                  } else if ((previousOffsetInfo).partitionsOffsetInfo.get(key).map{_.partitionStatus}.getOrElse(Green) == Green) {
                    Yellow
                  } else {
                    Red
                  }
                }
              if(partitionStatus == Red) {
                val currentTime = System.currentTimeMillis()
                redSince.get(partitionInfo.partition) match {
                  case None =>
                      logger warn s"BG status for partition ${partitionInfo.partition} turned RED"
                    redSince.putIfAbsent(partitionInfo.partition, currentTime)
                  case Some(since) if ((currentTime - since) > 15 * 60 * 1000) =>
                    logger error s"BG status for partition ${partitionInfo.partition} is RED for more than 15 minutes. sending it an exit message"
                    Grid.serviceRef(s"BGActor${partitionInfo.partition}") ! ExitWithError
                    redSince.replace(partitionInfo.partition, currentTime)
                  case Some(since) =>
                    logger warn s"BG for partition ${partitionInfo.partition} is RED since ${(currentTime - since)/1000} seconds ago"
                }
              }
              key -> partitionInfo.copy(partitionStatus = partitionStatus)
            }

            currentOffsetInfo = info.copy(partitionsOffsetInfo = partitionsOffsetInfoUpdated)
          } catch {
            case t:Throwable => logger error ("exception ingesting offset info", t)
          }
        } else if(currentOffsetInfo.partitionsOffsetInfo.nonEmpty){
          currentOffsetInfo = info.copy(partitionsOffsetInfo = info.partitionsOffsetInfo.map{
            case (topic, info) => (topic, info.copy(partitionStatus = currentOffsetInfo.partitionsOffsetInfo(topic).partitionStatus))
          })
        } else {
          currentOffsetInfo = info
        }
        lastFetchDuration = duration
          logger debug s"updated currentOffsetInfo: $currentOffsetInfo"
        context.system.scheduler.scheduleOnce(math.max(10000, lastFetchDuration).milliseconds, self, CalculateOffsetInfo)
      case Failure(exception) =>
        logger error("failed to calculate offset info", exception)
        context.system.scheduler.scheduleOnce(math.max(10000, lastFetchDuration).milliseconds, self, CalculateOffsetInfo)
    }
  }

}

object BGMonitorActor {
  def serviceName = classOf[BGMonitorActor].getName
}

object ZKLikeStringSerializer extends ZkSerializer {

  def serialize(data : Object) : Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  def deserialize(bytes : Array[Byte]) : Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

case object GetOffsetInfo
case object CalculateOffsetInfo
case class OffsetsInfo(partitionsOffsetInfo:Map[String, PartitionOffsetsInfo], timeStamp:DateTime)
trait PartitionStatus
case object Green extends PartitionStatus
case object Yellow extends PartitionStatus
case object Red extends PartitionStatus
case class PartitionOffsetsInfo(topic:String, partition:Int, readOffset:Long, writeOffset:Long, partitionStatus:PartitionStatus = Green)
