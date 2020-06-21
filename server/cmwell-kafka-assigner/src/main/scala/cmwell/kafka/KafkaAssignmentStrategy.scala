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

import scala.collection.mutable.{Set => MSet, Map => MMap}
import scala.math.{abs, ceil, round}

object KafkaAssignmentStrategy {

  def getRackAwareAssignment(topicName: String,
                             currentAssignment: Assignment,
                             nodeRackAssignment: Map[Int, String],
                             nodes: Set[Int],
                             partitions: Set[Int],
                             replicationFactor: Int): Assignment = {
    // Initialize nodes with capacities and nothing assigned
    val maxReplicas: Int = getMaxReplicasPerNode(nodes, partitions, replicationFactor)
    val nodeMap: Map[Int, Node] = createNodeMap(nodeRackAssignment, nodes, maxReplicas)

    // Using the current assignment, reassign as many partitions as each node can accept
    fillNodesFromAssignment(currentAssignment, nodeMap)

    // Figure out the replicas that have not been assigned yet
    val orphanedReplicas: Map[Int, Int] = getOrphanedReplicas(nodeMap, partitions, replicationFactor)

    // Assign those replicas to nodes that can accept them
    assignOrphans(topicName, nodeMap, orphanedReplicas)

    computePreferenceLists(topicName, nodeMap)
  }

  def getMaxReplicasPerNode(nodes: Set[Int], partitions: Set[Int], replicationFactor: Int): Int = {
    val totalReplicas: Double = partitions.size * replicationFactor
    round(ceil(totalReplicas / nodes.size)).toInt
  }

  def createNodeMap(nodeRackAssignment: Map[Int, String], nodes: Set[Int], maxReplicas: Int): Map[Int, Node] = {
    val rackMap = MMap.empty[String, Rack]
    nodes.foldLeft(Map.empty[Int, Node]) {
      case (nodeMap, nodeId) => {
        val rackId = nodeRackAssignment.getOrElse(nodeId, nodeId.toString)
        val rack = rackMap.getOrElseUpdate(rackId, Rack(rackId))
        val node = Node(nodeId, maxReplicas, rack)
        nodeMap.updated(nodeId, node)
      }
    }
  }

  def fillNodesFromAssignment(assignment: Assignment, nodeMap: Map[Int, Node]): Unit = {
    var assignmentIterators = {
      // originally: assignment.mapValues(_.iterator)
      // but the function `_.iterator` is applied lazily
      // which means a new iterator upon every invocation
      val b = Map.newBuilder[Int, Iterator[Int]]
      assignment.foreach {
        case (partition, nodes) =>
          b += partition -> nodes.iterator
      }
      b.result()
    }
    while (assignmentIterators.nonEmpty) {
      assignmentIterators.foreach {
        case (partition, nodeIt) => {
          if (nodeIt.hasNext) {
            val nodeId = nodeIt.next()
            nodeMap.get(nodeId).foreach { node =>
              if (node.canAccept(partition)) {
                // The node from the current assignment must still exist and be able to
                // accept the partition.
                node.accept(partition)
              }
            }
          } else {
            assignmentIterators = assignmentIterators - partition
          }
        }
      }
    }
  }

  def getOrphanedReplicas(nodeMap: Map[Int, Node], partitions: Set[Int], replicationFactor: Int): Map[Int, Int] = {
    // Get the number of assigned replicas per partition
    val partitionCounter = MMap.empty[Int, Int]
    nodeMap.values.foreach { node =>
      node.assignedPartitions.foreach { remainingReplicas =>
        val count = partitionCounter.getOrElse(remainingReplicas, 0)
        partitionCounter.update(remainingReplicas, count + 1)
      }
    }
    // Using the above information, and the replication factor, get the number of unassigned
    // replicas per partition
    partitions.view
      .map { partition =>
        val remainingReplicas = replicationFactor - partitionCounter.getOrElse(partition, 0)
        partition -> remainingReplicas
      }
      .filter(_._2 > 0)
      .toMap
  }

  def assignOrphans(topicName: String, nodeMap: Map[Int, Node], orphanedReplicas: Map[Int, Int]): Unit = {
    // Don't process nodes in the same order for all topics to ensure that topics with fewer
    // replicas than nodes are equally likely to be assigned anywhere (and not overload the
    // brokers with earlier IDs).

    val keys = nodeMap.keys.toList.sorted

    val nodeProcessingOrderList = getNodeProcessingOrder(topicName, keys)

    // Assign unassigned replicas to nodes that can accept them
    orphanedReplicas.foreach { p2r =>
      val partition = p2r._1
      var remainingReplicas = p2r._2
      val nodeIt = nodeProcessingOrderList.iterator
      while (nodeIt.hasNext && remainingReplicas > 0) {
        nodeMap.get(nodeIt.next()).foreach { node =>
          if (node.canAccept(partition)) {
            node.accept(partition)
            remainingReplicas -= 1
          }
        }
      }
    }
  }

  def getNodeProcessingOrder(topicName: String, nodeIds: List[Int]): List[Int] = {
    val index = abs(topicName.##) % nodeIds.size
    val (heads, tails) = nodeIds.splitAt(nodeIds.size - index)
    tails ::: heads
  }

  def computePreferenceLists(topicName: String, nodeMap: Map[Int, Node]): Assignment = {
    // First, get unordered assignment lists from the nodes
    val unorderedPreferences = nodeMap
      .foldLeft(Map.empty[Int, List[Int]]) {
        case (m, (_, node)) =>
          node.assignedPartitions.foldLeft(m) {
            case (mm, partition) => {
              val brokers = mm.getOrElse(partition, List.empty)
              mm.updated(partition, node.id :: brokers)
            }
          }
      }
      .view.mapValues(_.reverse).toMap

    val balanceLeadersTracker = new PreferenceListOrderTracker(topicName)
    unorderedPreferences.map {
      case (partitionId, preferenceList) => {
        val replicationFactor = preferenceList.size
        val orderedPreferenceList = List.newBuilder[Int]
        orderedPreferenceList.sizeHint(replicationFactor)
        var nodeSet = MSet(preferenceList: _*)
        for (replica <- 0 until replicationFactor) {
          val nodeToSelect = balanceLeadersTracker.getLeastSeenNodeForReplicaId(replica, nodeSet.toList)
          nodeSet -= nodeToSelect
          orderedPreferenceList += nodeToSelect
        }
        val orderedPreferenceListRes = orderedPreferenceList.result()
        balanceLeadersTracker.updateCountersFromList(orderedPreferenceListRes)
        partitionId -> orderedPreferenceListRes
      }
    }
  }

  class PreferenceListOrderTracker(topicName: String) {
    private[this] val nodeAssignmentCounters = MMap.empty[Int, MMap[Int, Int]]

    def getLeastSeenNodeForReplicaId(replicaId: Int, nodes: List[Int]): Int = {
      val nodeProcessingOrder = getNodeProcessingOrder(topicName, nodes)
      nodeProcessingOrder.minBy(ensureCount(_, replicaId))
    }

    def updateCountersFromList(preferences: List[Int]): Unit = {
      // Also give fallback leaders a higher weight than the ends of the lists since we want
      // to evenly distribute fallbacks.
      preferences.view.zipWithIndex.foreach(incrementCountSafeTupled)
    }

    private[this] val incrementCountSafeTupled = {
      import scala.language.postfixOps
      incrementCountSafe _ tupled
    }

    private[this] def incrementCountSafe(nodeId: Int, replicaId: Int): Unit = {
      val currentCount = ensureCount(nodeId, replicaId)
      nodeAssignmentCounters(nodeId).update(replicaId, currentCount + 1)
    }

    private[this] def ensureCount(nodeId: Int, replicaId: Int): Int = {
      val replicaCount = nodeAssignmentCounters.getOrElseUpdate(nodeId, MMap.empty[Int, Int])
      replicaCount.getOrElseUpdate(replicaId, 0)
    }
  }

  case class Rack(id: String) {
    private[this] val assignedPartitions = MSet.empty[Int]

    def canAccept(partition: Int): Boolean = !assignedPartitions.contains(partition)
    def accept(partition: Int): Unit = {
      require(canAccept(partition), s"Attempted to accept unacceptable partition[$partition] for rack[$id]")
      assignedPartitions += partition
    }

    override def toString: String = "[Rack(" + id + "):" + assignedPartitions + "]"
  }

  case class Node(id: Int, capacity: Int, rack: Rack) {
    val assignedPartitions: MSet[Int] = MSet.empty[Int]

    def canAccept(partition: Int): Boolean = {
      !assignedPartitions.contains(partition) &&
      assignedPartitions.size < capacity &&
      rack.canAccept(partition)
    }
    def accept(partition: Int): Unit = {
      require(canAccept(partition), s"Attempted to accept unacceptable partition[$partition] for node[$id]")
      assignedPartitions += partition
      rack.accept(partition)
    }

    override def toString: String =
      "[Node(" + id + "," + capacity + "," + rack.toString() + "):" + assignedPartitions + "]"
  }
}
