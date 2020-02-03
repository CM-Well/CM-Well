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
import akka.actor.FSM.Failure
import akka.util.Timeout
import cmwell.ctrl.checkers.{ComponentState, GreenStatus, YellowStatus}
import cmwell.ctrl.commands._
import cmwell.ctrl.hc._
import cmwell.ctrl.server.CommandActor
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import akka.pattern.ask

import scala.util.Success
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 3/9/16.
  */
case class AddNode(node: String) extends Task with LazyLogging {
  implicit val timeout = Timeout(15.seconds)

  private def startElasticsearch(cmd: ActorSelection, prom: Promise[Unit]): Unit = {
    logger.info(s"Starting Elasticsearch on node $node")
    cmd ! StartElasticsearch
    Grid.system.scheduler.scheduleOnce(60.seconds) {
      (HealthActor.ref ? GetElasticsearchDetailedStatus).mapTo[ElasticsearchGridStatus].map { f =>
        f.getStatesMap.get(node) match {
          case Some(s) =>
            if (s.getColor == GreenStatus || s.getColor == YellowStatus) prom.success(())
            else startElasticsearch(cmd, prom)
          case None => startElasticsearch(cmd, prom)
        }
      }
    }
  }

  private def startCassandra(cmd: ActorSelection, prom: Promise[Unit]): Unit = {
    logger.info(s"Starting Cassandra on node $node")
    cmd ! StartCassandra
    Grid.system.scheduler.scheduleOnce(60.seconds) {
      (HealthActor.ref ? GetCassandraDetailedStatus).mapTo[CassandraGridStatus].map { f =>
        f.getStatesMap.get(node) match {
          case Some(s) =>
            if (s.getColor == GreenStatus) prom.success(())
            else startCassandra(cmd, prom)
          case None => startCassandra(cmd, prom)
        }
      }
    }
  }

  override def exec: Future[TaskResult] = {
    val cmd = CommandActor.select(node)
    val esPromise = Promise[Unit]
    val casPromise = Promise[Unit]

    startElasticsearch(cmd, esPromise)
    startCassandra(cmd, casPromise)

    val esCancelable = cancel(esPromise, 24.hours)
    val casCancelable = cancel(casPromise, 24.hours)
    val esFuture = esPromise.future
    val casFuture = casPromise.future

    // cancel the cancelables when the future succeeded
    esFuture.foreach(x => esCancelable.cancel())
    casFuture.foreach(x => casCancelable.cancel())

    val fut = for {
      esStarted <- esFuture
      casStarted <- casFuture
    } yield {
      logger.info("Starting CM-WELL components")
      cmd ! StartKafka
      cmd ! StartBg
      cmd ! StartWebserver
      cmd ! StartCw
      cmd ! StartDc
    }

    fut.map(r => TaskSuccessful).recover { case err: Throwable => TaskFailed }
  }
}
