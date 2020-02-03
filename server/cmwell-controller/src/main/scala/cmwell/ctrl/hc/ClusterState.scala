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
package cmwell.ctrl.hc

import akka.actor.Cancellable
import cmwell.ctrl.controllers.{CassandraController, ElasticsearchController}
import cmwell.ctrl.config.Config
import cmwell.ctrl.config.Config._
import cmwell.ctrl.ddata.DData
import cmwell.ctrl.utils.{AlertReporter}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import k.grid.Grid
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.language.postfixOps

object ClusterState extends LazyLogging with AlertReporter {
  val clusterLogger = LoggerFactory.getLogger("cluster_state")
  var esResumeRebalancingTask: Cancellable = _
  private var currentState: ClusterState = Stable
  clusterLogger.info(s"initial state: $currentState")
  def newState(cs: ClusterEvent) = {
    alert(cs)
    val newState = cs.act(currentState)
    clusterLogger.info(s"current state: $currentState, event: $cs, new state: $newState")
    currentState = newState
  }

  def getCurrentState = currentState

}

sealed trait ClusterState

case object Stable extends ClusterState
case class DownNodes(nodes: Set[String]) extends ClusterState

sealed trait ClusterEvent extends LazyLogging {

  def act(currentState: ClusterState): ClusterState
}

case class DownNodesDetected(nodes: Set[String]) extends ClusterEvent with LazyLogging {
  def act(currentState: ClusterState): ClusterState = {
    logger.info(s"[ClusterState] DownNodesDetected $nodes")
    currentState match {
      // aggregate the down nodes.
      case DownNodes(n) => DownNodes(nodes ++ n)
      // start grace time.
      case _ =>
//        todo: Remove this comment completely below when the Elasticsearch's delayed unassigned shards will prove itself.
//        ElasticsearchController.stopElasticsearchRebalancing
        logger.info(s"Starting grace time scheduler of $downNodesGraceTimeMinutes minutes")
        ClusterState.esResumeRebalancingTask = Grid.system.scheduler.scheduleOnce(downNodesGraceTimeMinutes minutes) {
          HealthActor.ref ! EndOfGraceTime
        }
        DownNodes(nodes)
    }
  }
}

case class NodesJoinedDetected(nodes: Set[String]) extends ClusterEvent with LazyLogging {
  override def act(currentState: ClusterState): ClusterState = {
    logger.info(s"[ClusterState] NodesJoinedDetected $nodes")
    currentState match {
      case DownNodes(n) =>
        if ((n -- nodes).size > 0)
          DownNodes(n -- nodes)
        else {
          if (ClusterState.esResumeRebalancingTask != null) {
            ClusterState.esResumeRebalancingTask.cancel()
          }
//          todo: Remove this comment completely below when the Elasticsearch's delayed unassigned shards will prove itself.
//          ElasticsearchController.startElasticsearchRebalancing
          Stable
        }
      case cs: ClusterState => cs
    }
  }
}

case object EndOfGraceTime extends ClusterEvent with LazyLogging {
  override def act(currentState: ClusterState): ClusterState = {
    currentState match {
      // act only if current state is DownNodes. todo: actually remove the nodes.
      case DownNodes(n) =>
        logger.info("[ClusterState] EndOfGraceTime reached.")
        ElasticsearchController.startElasticsearchRebalancing

        try {
          ClusterState.esResumeRebalancingTask.cancel()
        } catch {
          case _: Throwable => // do nothing.
        }

        ClusterState.esResumeRebalancingTask = null

        Grid.system.scheduler.scheduleOnce(20.seconds) {
          logger.info("Will remove Cassandra down nodes.")
          CassandraController.removeCassandraDownNodes
        }

        n.foreach { dn =>
          HealthActor.ref ! RemoveNode(dn)
        }
        DData.setDownNodes(n)
        Stable
      // keep the same state
      case cs: ClusterState => cs
    }
  }
}
