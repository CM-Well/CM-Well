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

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{EitherValues, FunSpec, Inspectors, Matchers}

class KafkaTopicAssignerTest extends FunSpec with Matchers with Inspectors with EitherValues with LazyLogging {

  def verifyPartitionsAndBuildReplicaCounts(currentAssignment: Assignment,
                                            newAssignment: Assignment,
                                            minimalMovementThreshold: Int): Map[Int,Int] = {
    newAssignment.foldLeft(Map.empty[Int,Int]) {
      case (brokerReplicaCounts,(partition,replicas)) => {
        val replicaSet = replicas.toSet

        // Ensure that no broker is assigned twice to a replica
        replicas should have size replicaSet.size

        // Keep track of how often a broker pops up
        val brokerReplicaCountsUpdated = replicas.foldLeft(brokerReplicaCounts) {
          case(brc,broker) =>
            brc.updated(broker,brc.getOrElse(broker,0) + 1)
        }

        // Ensure that movement was minimal
        // Each partition should have brokers from the original assignment that "stuck"
        val prevReplicaSet = currentAssignment(partition).toSet
        val intersection = replicaSet intersect prevReplicaSet
        intersection.size should be >= minimalMovementThreshold

        brokerReplicaCountsUpdated
      }
    }
  }

  describe("KafkaAssigner should") {
    it("expand rack aware") {
      val topic = "test"
      val currentAssignment: Assignment = Map(
        0 -> List(10,11),
        1 -> List(11,12),
        2 -> List(12,10),
        3 -> List(10,12))
      val newBrokers = Set(10,11,12,13,14)
      val rackAssignments = Map(
        10 -> "a",
        11 -> "b",
        12 -> "c",
        13 -> "a",
        14 -> "b")
      val assigner = new KafkaAssigner
      val newAssignment = assigner.generateAssignment(topic, currentAssignment, newBrokers, rackAssignments, -1)
      val brokerReplicaCounts = verifyPartitionsAndBuildReplicaCounts(currentAssignment, newAssignment, 1)

      // Ensure that no broker is doing too much
      // In this case, there are 5 brokers, 4 partitions, and 2 replicas,
      // so there should be 2 brokers serving 1 partition each, and 3 brokers serving 2 each
      val partitionCounts = brokerReplicaCounts.values
      partitionCounts.count(1.==) should be(2)
      partitionCounts.count(2.==) should be(3)
    }

    it("expand cluster") {
      val topic = "test"
      val currentAssignment: Assignment = Map(
        0 -> List(10,11),
        1 -> List(11,12),
        2 -> List(12,10),
        3 -> List(10,12))
      val newBrokers = Set(10,11,12,13)
      val assigner = new KafkaAssigner
      val newAssignment = assigner.generateAssignment(topic, currentAssignment, newBrokers, Map.empty, -1)
      val brokerReplicaCounts = verifyPartitionsAndBuildReplicaCounts(currentAssignment, newAssignment, 1)

      // Ensure that no broker is doing too much
      // In this case, there are 4 brokers, 4 partitions, and 2 replicas,
      // so each broker should be serving exactly 2 total partition replicas.
      forAll(brokerReplicaCounts) {
        case (broker,count) => count should be(2)
      }
    }

    it("decommission"){
      val topic = "test"
      val currentAssignment: Assignment = Map(
        0 -> List(10,11),
        1 -> List(11,12),
        2 -> List(12,13),
        3 -> List(13,10))
      val newBrokers = Set(10,11,13)
      val assigner = new KafkaAssigner
      val newAssignment = assigner.generateAssignment(topic, currentAssignment, newBrokers, Map.empty, -1)
      val brokerReplicaCounts = verifyPartitionsAndBuildReplicaCounts(currentAssignment, newAssignment, 1)

      brokerReplicaCounts should not contain key(12)

      // Ensure that no broker is doing too much
      // In this case, there are 3 brokers, 4 partitions, and 2 replicas,
      // so one broker should be serving 2 replicas, and the others 3.
      brokerReplicaCounts.values.count(2.==) should be(1)
      brokerReplicaCounts.values.count(3.==) should be(2)
    }

    it("replace node") {
      val topic = "test"
      val currentAssignment: Assignment = Map(
        0 -> List(10,11),
        1 -> List(11,12),
        2 -> List(12,10),
        3 -> List(10,12))
      val newBrokers = Set(10,11,13)
      val assigner = new KafkaAssigner
      val newAssignment = assigner.generateAssignment(topic, currentAssignment, newBrokers, Map.empty, -1)
      val brokerReplicaCounts = verifyPartitionsAndBuildReplicaCounts(currentAssignment, newAssignment, 1)

      brokerReplicaCounts should not contain key(12)

      // there should have been no change here since broker 12 never served partition 0
      currentAssignment(0) should be(newAssignment(0))

      // 11 should still be present in 1, and it can be joined by either 10 or 13
      newAssignment(1) should contain(11)
      newAssignment(1) should (contain(10) or contain(13))

      // 10 should still be present in 2, and it can be joined by either 11 or 13
      newAssignment(2) should contain(10)
      newAssignment(2) should (contain(11) or contain(13))

      // 10 should still be present in 3, and it can be joined by either 11 or 13
      newAssignment(3) should contain(10)
      newAssignment(3) should (contain(11) or contain(13))
    }
  }
}
