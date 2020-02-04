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
package k.grid.monitoring

import akka.util.Timeout
import k.grid.{Grid, WhoAreYou, WhoIAm}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import akka.pattern.{ask, pipe}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import cmwell.util.concurrent.successes

/**
  * Created by michael on 7/6/15.
  */
case object PingChildren
case object GetSingletons
case class SingletonData(name: String, role: String, location: String)

object MonitorUtil {
  implicit val timeout = Timeout(10.seconds)

  def pingChildren = (Grid.createAnon(classOf[ActorsCrawler]) ? PingActors).mapTo[ActiveActors]
}
