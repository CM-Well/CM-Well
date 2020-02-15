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

import play.api.mvc._, BodyParsers.parse
import javax.inject._
import scala.language.postfixOps

//TODO: this must be refactored...!
@Singleton
class CommandHandler @Inject()(inputHandler: InputHandler) extends InjectedController {
  def handlePost(op: String) = Action.async(parse.raw) { implicit req =>
    {
      op.toLowerCase match {
        case "init" => {
          val format = req.getQueryString("format").getOrElse("jsonw")
          if ("jsonw" == format.toLowerCase) inputHandler.handlePostWrapped(req, true, true)
          else inputHandler.handlePostRDF(req, true)._1
        }
      }
    }
  }
}
