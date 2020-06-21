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
package cmwell.ctrl.server

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection}
import akka.pattern.pipe
import cmwell.ctrl.checkers._
import cmwell.ctrl.commands.{ControlCommand, StartElasticsearchMaster}
import cmwell.ctrl.config.{Config, Jvms}
import cmwell.ctrl.utils.ProcUtil
import k.grid.{Grid, GridJvm, GridJvm$}
import scala.concurrent.Future
import scala.sys.process._
import Config._

import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 11/25/14.
  */
case class BashCommand(com: String)

object CommandActor {
  def select(host: String): ActorSelection = {
    Grid.selectActor(commandActorName, GridJvm(host, Some(Jvms.node)))
  }

  def all: Set[ActorSelection] = {
    Grid.availableMachines.map(host => Grid.selectActor(commandActorName, GridJvm(host, Some(Jvms.node))))
  }
}

class CommandActor extends Actor with ActorLogging {

  override def receive: Receive = {
    case BashCommand(com)   => sender ! ProcUtil.executeCommand(com)
    case CheckWeb           => WebChecker.check.pipeTo(sender())
    case CheckElasticsearch => ElasticsearchChecker.check.pipeTo(sender())
    case CheckCassandra     => CassandraChecker.check.pipeTo(sender())
    case cc: ControlCommand => cc.execute
  }
}
