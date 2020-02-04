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
package cmwell.ctrl.tasks

import akka.actor.ActorSelection
import akka.util.Timeout
import cmwell.ctrl.checkers.{CassandraDown, CassandraOk, ElasticsearchDown, GreenStatus}
import cmwell.ctrl.commands._
import cmwell.ctrl.controllers.CassandraController
import cmwell.ctrl.hc._
import cmwell.ctrl.server.CommandActor
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid

import scala.concurrent.{Future, Promise}

import scala.concurrent.duration._
import akka.pattern.ask
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 3/10/16.
  */
case class ClearNode(node: String) extends Task with LazyLogging {
  implicit val timeout = Timeout(15.seconds)
  private def stopElasticsearch(cmd: ActorSelection, prom: Promise[Unit]): Unit = {
    logger.info(s"Stopping Elasticsearch on node $node")
    cmd ! StopElasticsearch
    cmd ! StopElasticsearchMaster

    Grid.system.scheduler.scheduleOnce(60.seconds) {
      (HealthActor.ref ? GetClusterDetailedStatus).mapTo[ClusterStatusDetailed].map { f =>
        val ocs = f.esStat.get(node)
        //todo: Remove this log info.
        logger.info(s"ES OCS: $ocs")

        ocs match {
          case Some(s) =>
            s match {
              case ElasticsearchDown(hm, gt) => prom.success(())
              case _                         => stopElasticsearch(cmd, prom)
            }
          case None => prom.success(())
        }
      }
    }
  }

  private def stopCassandra(cmd: ActorSelection, prom: Promise[Unit]): Unit = {
    logger.info(s"Stopping Cassandra on node $node")
    cmd ! StopCassandra
    CassandraController.removeCassandraDownNodes

    Grid.system.scheduler.scheduleOnce(60.seconds) {
      (HealthActor.ref ? GetClusterDetailedStatus).mapTo[ClusterStatusDetailed].map { f =>
        val ocs = f.casStat.get(node)
        //todo: Remove this log info.
        logger.info(s"Cass OCS: $ocs")

        ocs match {
          case Some(s) =>
            s match {
              case co @ CassandraOk(m, rm, gt) if (co.m.isEmpty) => prom.success(())
              case CassandraDown(gt)                             => prom.success(())
              case _                                             => stopCassandra(cmd, prom)
            }
          case None => prom.success(())
        }
      }
    }
  }

  override def exec: Future[TaskResult] = {
    val cmd = CommandActor.select(node)
    val esPromise = Promise[Unit]
    val casPromise = Promise[Unit]

    stopCassandra(cmd, casPromise)
    stopElasticsearch(cmd, esPromise)

    // todo: kill CM-WELL processes before CAS & ES
    val esCancelable = cancel(esPromise, 1.hours)
    val casCancelable = cancel(casPromise, 1.hours)
    val esFuture = esPromise.future
    val casFuture = casPromise.future

    val fut = for {
      esStopped <- esFuture
      casStopped <- casFuture
    } yield {
      logger.info("Stopping CM-WELL components")
      cmd ! StopKafka
      cmd ! StopWebserver
      cmd ! StopBg
      cmd ! StopCw
      cmd ! StopDc
    }
    fut
      .map { r =>
        logger.info("Task status: TaskSuccessful")
        TaskSuccessful
      }
      .recover {
        case err: Throwable =>
          logger.info("Task status: TaskFailed")
          TaskFailed
      }
  }
}
