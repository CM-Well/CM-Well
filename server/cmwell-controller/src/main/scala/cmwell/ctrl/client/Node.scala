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

import akka.pattern.ask
import akka.actor.ActorRef
import akka.util.Timeout
import cmwell.ctrl.checkers._
import cmwell.ctrl.server.BashCommand

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by michael on 12/9/14.
  */
case class Node(ip: String, ctrlActor: ActorRef) {
  implicit val timeout = Timeout(5 seconds)
  def command(com: String): Future[String] = {
    (ctrlActor ? BashCommand(com)).mapTo[String]
  }

  def getWebStatus: Future[WebState] = {
    (ctrlActor ? CheckWeb).mapTo[WebState]

  }

  def getBgStatus: Future[BgState] = {
    (ctrlActor ? CheckBg).mapTo[BgState]
  }

  def getElasticsearchStatus: Future[ElasticsearchState] = {
    (ctrlActor ? CheckElasticsearch).mapTo[ElasticsearchState]
  }

  def getCassandraStatus: Future[CassandraState] = {
    (ctrlActor ? CheckCassandra).mapTo[CassandraState]
  }
}
