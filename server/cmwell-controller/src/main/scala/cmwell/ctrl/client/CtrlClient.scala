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
package cmwell.ctrl.client

import akka.actor.{ActorPath, ActorRef, ActorSelection, ActorSystem}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}

import akka.pattern.ask
import akka.util.Timeout
import cmwell.ctrl.checkers._
import cmwell.ctrl.config.{Config, Jvms}
import cmwell.ctrl.config.Config._
import cmwell.ctrl.exceptions.BadHostException
import cmwell.ctrl.server.{BashCommand, CtrlServer}
import cmwell.ctrl.tasks._
import com.typesafe.scalalogging.LazyLogging
import k.grid._
import cmwell.ctrl.hc._

import scala.None
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 11/26/14.
  */
object CtrlClient extends LazyLogging {
  var currentHost: String = ""
  var healthActor: ActorRef = _

  var ctrlServers: Map[String, ActorRef] = _

  var nodes: Map[String, Node] = _

  val seedPort = Config.seedPort
  val commandActorName = Config.commandActorName

  implicit val timeout = Timeout(60 seconds)

  def controllers: Map[String, Node] = {
    Grid
      .actors("command-actor", Some("ControllerServer"))
      .map { s =>
        val ip = s.anchorPath.address.host.getOrElse("0.0.0.0")
        val ref = Await.result(s.resolveOne(), timeout.duration)
        ip -> Node(ip, ref)
      }
      .toMap
  }

  def init {
    healthActor = HealthActor.ref
  }

  def getHealthControl(): Unit = {
    healthActor = Grid.serviceRef("HealthActor")
  }

  /**
    * IMPORTANT: Please use this init method in processes outside cm-well (e.g. cons)
    * since we use here Await to get actor ref from selection.
    * @param joinToHost
    */
  def init(joinToHost: String): Unit = {
    currentHost = joinToHost
    getHealthControl()
    Thread.sleep(10000)
  }

  def getClusterStatus: Future[ClusterStatus] = {
    (healthActor ? GetClusterStatus).mapTo[ClusterStatus]
  }

  def getCassandraStatus: Future[CassandraState] = {
    (healthActor ? GetCassandraStatus).mapTo[CassandraState]
  }

  def getElasticsearchStatus: Future[ElasticsearchState] = {
    (healthActor ? GetElasticsearchStatus).mapTo[ElasticsearchState]
  }

  def getBgStatus: Future[(Map[String, BgState], StatusColor)] = {
    (healthActor ? GetBgStatus).mapTo[(Map[String, BgState], StatusColor)]
  }

  def getWebStatus: Future[(Map[String, WebState], StatusColor)] = {
    (healthActor ? GetWebStatus).mapTo[(Map[String, WebState], StatusColor)]
  }

  def getCassandraDetailedStatus: Future[Map[String, CassandraState]] = {
    (healthActor ? GetCassandraDetailedStatus).mapTo[Map[String, CassandraState]]
  }

  def getElasticsearchDetailedStatus: Future[Map[String, ElasticsearchState]] = {
    (healthActor ? GetElasticsearchDetailedStatus).mapTo[Map[String, ElasticsearchState]]
  }

  def getClusterDetailedStatus: Future[ClusterStatusDetailed] = {
    (healthActor ? GetClusterDetailedStatus).mapTo[ClusterStatusDetailed]
  }

  def getDataCenterStatus: Future[Map[String, ComponentState]] = {
    (healthActor ? GetDcStatus).mapTo[Map[String, ComponentState]]
  }

  def getActiveNodes: Future[ActiveNodes] = {
    (healthActor ? GetActiveNodes).mapTo[ActiveNodes]
  }

  def removeNode(ip: String, retries: Int = 10): Unit = {

    if (retries > 0) {
      val r = Try(Await.result((healthActor ? RemoveNode(ip)), 10.seconds))

      r match {
        case Success(a) => // do nothing
        case Failure(e) => removeNode(ip, retries - 1)
      }
    }

  }

  def addNode(ip: String) = {
//    logger.info(s"inside addNode with ip $ip")
    healthActor ! RemoveFromDownNodes(ip)
//    healthActor ! AddNode(ip)
  }

  def waitForHealth: Unit = {
    var counter = 20
    var healthIsActive = false
    while (counter > 0) {
      val f = (healthActor ? WhoAreYou).mapTo[WhoIAm]
      Try(Await.result(f, 10.seconds)) match {
        case Success(_) =>
          counter = 0
          healthIsActive = true
        case Failure(_) =>
          counter -= 1
      }
    }
  }

}
