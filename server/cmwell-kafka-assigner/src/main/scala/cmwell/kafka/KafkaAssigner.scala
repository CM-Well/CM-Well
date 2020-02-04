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
package cmwell.kafka

/**
  * KafkaAssigner code is influenced & inspired by
  * https://github.com/SiftScience/kafka-assigner
  */
class KafkaAssigner {
  val assignmentContext = Map.empty[Int, Map[Int, Int]]

  def generateAssignment(topic: String,
                         currentAssignment: Assignment,
                         brokers: Set[Int],
                         rackAssignment: Map[Int, String],
                         desiredReplicationFactor: Int): Assignment = {

    val replicationFactor = {
      if (desiredReplicationFactor >= 0) desiredReplicationFactor
      else currentAssignment.head._2.size
    }

    val partitions: Set[Int] = currentAssignment.view.map {
      case entry @ (partition, replicas) => {
        if (desiredReplicationFactor < 0) {
          require(replicationFactor == replicas.size,
                  s"Topic $topic has partition $partition with unexpected replication factor ${replicas.size}")
        }
        partition
      }
    }.to(Set)

    // Make sure that we actually managed to process something and get the replication factor
    require(replicationFactor > 0, s"Topic $topic does not have a positive replication factor!")
    require(replicationFactor <= brokers.size,
            s"Topic $topic has a higher replication factor ($replicationFactor) than available brokers!")

    KafkaAssignmentStrategy.getRackAwareAssignment(topic,
                                                   currentAssignment,
                                                   rackAssignment,
                                                   brokers,
                                                   partitions,
                                                   replicationFactor)
  }
}
