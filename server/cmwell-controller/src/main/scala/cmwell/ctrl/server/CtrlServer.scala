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

import akka.util.Timeout
import cmwell.ctrl.config.Config._
import com.typesafe.scalalogging.LazyLogging
import k.grid._
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by michael on 11/25/14.
  */
object CtrlServer extends App with LazyLogging {
  logger.info("Starting CtrlServer process")
  //SLF4J initialization is not thread safe, so it's "initialized" by writing some log and only then using sendSystemOutAndErrToSLF4J.
  //Without it there will be en error in stderr and some log line at the beginning will be lost
  SysOutOverSLF4J.sendSystemOutAndErrToSLF4J()

  implicit val timeout = Timeout(3 seconds)
  Grid.setGridConnection(GridConnection(memberName = "ctrl"))
  Grid.join
  Grid.create(classOf[CommandActor], commandActorName)
}
