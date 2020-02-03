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
package controllers

import play.api.mvc.{Action, InjectedController}
import javax.inject._

/**
  * Created with IntelliJ IDEA.
  * User: israel
  * Date: 10/23/13
  * Time: 11:05 AM
  * To change this template use File | Settings | File Templates.
  */
@Singleton
class Help @Inject() extends InjectedController {

  def handleHelp(page: String) = Action { request =>
    page match {
      case "in"     => Ok(views.txt._in(request))
      case "out"    => Ok(views.txt._out(request))
      case "cmd"    => Ok(views.txt._cmd(request))
      case "sp"     => Ok(views.txt._sp(request))
      case "sparql" => Ok(views.html._sparql(request))
      case "zz"     => Ok(views.txt.zz(request))
      case "kafka"  => Ok(views.txt._kafka(request))
      case _        => NotFound
    }
  }

  def iiBlockedRequests = Action {
    //TODO: this is not enough since one can submit a bulk of infotons, or RDF document which contains an infoton named "ii" or "ii/*"
    req =>
      BadRequest("\"ii\" is reserved for infoton id's retrieval.")
  }
}
