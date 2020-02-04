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

//NOTE: ZkUtils was removed from newer versions of Kafka. Also, this class never actually worked so it's not a regression to remove it.
/*
import akka.actor.Actor
import com.typesafe.scalalogging.LazyLogging
import kafka.admin.{ReassignmentCompleted, ReassignmentFailed, ReassignmentInProgress, ReassignmentStatus}
import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}
//Eli: should not shadow Seq (or map???)
import scala.collection.{Map, Seq}
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

/**
  * Created by israel on 27/12/2016.
  */
class KafkaMonitorActor(zkServers: String,
                        outagePeriod: Long = 15 * 60 * 1000, // defaults to 15 minutes
                        implicit val ec: ExecutionContext = global)
    extends Actor
    with LazyLogging {

  val zkClient = new ZkClient(zkServers, 10000, 10000, ZKLikeStringSerializer)
  implicit val zkUtils = ZkUtils(zkClient, false)

  val topics = Seq("persist_topic", "index_topic")
  val partitionsForTopics = zkUtils.getPartitionsForTopics(topics)

  val missingBrokers: MMap[Int, Long] = MMap.empty // Map[BrokerID -> missing from date]

  context.system.scheduler.scheduleOnce(60.seconds, self, PeriodicCheck)

  override def receive: Receive = normal

  def normal: Receive = {
    case PeriodicCheck =>
      logger.debug("starting periodic check")
      val partitionAssignment = zkUtils.getPartitionAssignmentForTopics(topics)
      logger.debug(s"partitionAssignment: ${partitionAssignment}")
      val assignedBrokers = partitionAssignment.values.flatten.flatMap { case (_ /*partition*/, brokers) => brokers }.toSet
      logger.debug(s"assignedBrokers: ${assignedBrokers.mkString(",")}")
      val availableBrokers = zkUtils.getAllBrokersInCluster().map { _.id }.toSet
      logger.debug(s"availableBrokers: ${availableBrokers.mkString(",")}")
      val unavailableBrokers = assignedBrokers.diff(availableBrokers)
      logger.debug(s"unavailableBrokers: ${unavailableBrokers.mkString(",")}")
      val unassignedBrokers = availableBrokers.diff(assignedBrokers)
      logger.debug(s"unassignedBrokers: ${unassignedBrokers.mkString(",")}")

      val returnedBrokers = missingBrokers.keys.filter { !unavailableBrokers.contains(_) }
      // handle the wonder where broker gone missing and miraculously returned before we decommissioned it
      if (returnedBrokers.nonEmpty) {
        logger.info(s"the following brokers have miraculously returned without us doing a thing. Yehh!!")
        missingBrokers --= returnedBrokers
      }

      val decommissionBrokerCandidates = unavailableBrokers.filter { unavailableBrokerId =>
        missingBrokers
          .get(unavailableBrokerId)
          .fold {
            // this is a new missing broker, add it
            logger.info(
              s"broker id: $unavailableBrokerId is first to be noticed as missing, adding it with current time"
            )
            missingBrokers.put(unavailableBrokerId, System.currentTimeMillis())
            false
          } {
            // this broker is already missing
            since =>
              // if decommission period has passed
              if (System.currentTimeMillis() - since > outagePeriod) {
                logger.info(s"""broker id: $unavailableBrokerId has been missing since
                   |${ISODateTimeFormat.basicDateTime().print(since)}. adding it to decommission
                   |candidates""".stripMargin)
                true // add to candidates
              } else
                false // let's wait a bit longer
          }
      }
      if (decommissionBrokerCandidates.nonEmpty || unassignedBrokers.nonEmpty) {
        logger.info(s"Going to decommission brokers IDs: ${decommissionBrokerCandidates.mkString(", ")}")
        logger.info(s"Going to add unassigned brokers ids: ${unassignedBrokers.mkString(",")}")

        val assignToBrokers = assignedBrokers.diff(decommissionBrokerCandidates).union(unassignedBrokers)

        // building new assignment map
        val newAssignment = KafkaAssigner.generateAssignment(topics, assignToBrokers)

        logger.info(s"switching to mitigating state and sending ReassignPartitions message for:\n$newAssignment")
        context.become(mitigating)
        self ! ReassignPartitions(newAssignment)

      } else {
        logger.debug("nothing to decommission or add, scheduling next check in 60 seconds")
        context.system.scheduler.scheduleOnce(60.seconds, self, PeriodicCheck)
      }

  }

  def mitigating: Receive = {

    case ReassignPartitions(newAssignment) =>
      val jsonReassignmentData = ZkUtils.formatAsReassignmentJson(newAssignment)
      logger.info(s"requested to reassign partitions as follows:\n${jsonReassignmentData}")
      zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath, jsonReassignmentData)
      logger.info(s"created request for partitions reassignment in zookeeper and scheduling validation in 30 seconds")
      context.system.scheduler.scheduleOnce(30.seconds, self, ValidateAssignment(newAssignment))

    case ValidateAssignment(partitionsAssignment) =>
      logger.info(s"requested to validate partitions reassignment")
      val reassignedPartitionsStatus = KafkaAssigner.checkIfReassignmentSucceeded(zkUtils, partitionsAssignment)
      if (reassignedPartitionsStatus.forall {
            _._2 match { case ReassignmentCompleted => true; case _ => false }
          }) {
        logger.info("all partitions were reassigned successfully. switching back to normal mode")
        context.become(normal)
        context.system.scheduler.scheduleOnce(60.seconds, self, PeriodicCheck)
      } else if (reassignedPartitionsStatus.exists {
                   _._2 match { case ReassignmentInProgress => true; case _ => false }
                 }) {
        logger.info("reassigning partitions still in progress scheduling another check in 30 seconds")
        context.system.scheduler.scheduleOnce(30.seconds, self, ValidateAssignment(partitionsAssignment))
      } else {
        logger.error(
          s"some/all partitions failed to reassign. Maybe will succeeded in the next round: $reassignedPartitionsStatus"
        )
        context.become(normal)
        context.system.scheduler.scheduleOnce(60.seconds, self, PeriodicCheck)
      }
  }

  case object PeriodicCheck
  case class ReassignPartitions(newAssignment: Map[TopicAndPartition, Seq[Int]])
  case class ValidateAssignment(partitionsAssignment: Map[TopicAndPartition, Seq[Int]])
}

object KafkaAssigner extends LazyLogging {

  def generateAssignment(topics: Seq[String],
                         assignToBrokers: Set[Int])(implicit zkUtils: ZkUtils): Map[TopicAndPartition, Seq[Int]] = {
//    val kafkaAssigner = new KafkaTopicAssigner
    val currentAssignment = zkUtils
      .getPartitionAssignmentForTopics(topics)
      .map {
        case (k, v) =>
          (k, v.map {
            case (key, value) =>
              (key: java.lang.Integer, value.map { i =>
                i: java.lang.Integer
              }.asJava)
          }.asJava)
      }
      .asJava
    logger.debug(s"current assignment: $currentAssignment")

    val assignToBrokersJ = assignToBrokers.map { i =>
      i: java.lang.Integer
    }.asJava

    // building new assignment map
//    val newAssignment:MMap[TopicAndPartition, Seq[Int]] = MMap.empty
    KafkaAssigner.generateAssignment(topics, assignToBrokers)
//    topics.foreach{ topic =>
//      KafkaAssigner.generateAssignment(topic, currentAssignment.get(topic),
//        assignToBrokersJ, Map.empty[java.lang.Integer, String].asJava, -1).asScala.map{
//        case (partition, brokers) =>
//          new TopicAndPartition(topic, partition) -> brokers.asScala.map{i => i:Int}
//      }.foreach{ case (topicAndPartition, brokers) =>
//        newAssignment.put(topicAndPartition, brokers)
//      }
//    }
//    newAssignment.toMap
  }

  def reassignPartitions(assignmentMap: Map[TopicAndPartition, Seq[Int]])(implicit zkUtils: ZkUtils) = {
    val jsonReassignmentData = ZkUtils.formatAsReassignmentJson(assignmentMap)
    logger.debug(s"requested to reassign partitions as follows:\n${jsonReassignmentData}")
    zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath, jsonReassignmentData)
  }

  def checkIfReassignmentSucceeded(
    zkUtils: ZkUtils,
    partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]
  ): Map[TopicAndPartition, ReassignmentStatus] = {

    def checkIfPartitionReassignmentSucceeded(
      zkUtils: ZkUtils,
      topicAndPartition: TopicAndPartition,
      partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]],
      partitionsBeingReassigned: Map[TopicAndPartition, Seq[Int]]
    ): ReassignmentStatus = {
      val newReplicas = partitionsToBeReassigned(topicAndPartition)
      partitionsBeingReassigned.get(topicAndPartition) match {
        case Some(partition) => ReassignmentInProgress
        case None            =>
          // check if the current replica assignment matches the expected one after reassignment
          val assignedReplicas = zkUtils.getReplicasForPartition(topicAndPartition.topic, topicAndPartition.partition)
          if (assignedReplicas == newReplicas)
            ReassignmentCompleted
          else {
            ReassignmentFailed
          }
      }
    }

    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned().view.mapValues(_.newReplicas).toMap
    partitionsToBeReassigned.keys.map { topicAndPartition =>
      (topicAndPartition,
       checkIfPartitionReassignmentSucceeded(zkUtils,
                                             topicAndPartition,
                                             partitionsToBeReassigned,
                                             partitionsBeingReassigned))
    }.toMap

  }

}

object ZKLikeStringSerializer extends ZkSerializer {

  def serialize(data: Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  def deserialize(bytes: Array[Byte]): Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}
*/
