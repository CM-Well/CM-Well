/**
  * Copyright 2015 Thomson Reuters
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
package actions

import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import logic.CRUDServiceFS
import cmwell.domain.{Everything, Infoton}

/**
  * Created with IntelliJ IDEA.
  * User: gilad
  * Date: 12/8/13
  * Time: 10:45 AM
  * To change this template use File | Settings | File Templates.
  */
object StreamingInfotons {

//  val streamLinesBodyParser: BodyParser[List[Future[Option[Infoton]]]] = BodyParser {
//    request =>
//      val upToNewLine = Traversable.splitOnceAt[String, Char](_ != '\n').transform(Iteratee.consume()).map {
//        url =>
//          println("parsing: \'" + url + "\'")
//          if (url.isEmpty) Nil
//          else List(CRUDServiceFS.getInfoton(url, None, None).map(_.collect{ case Everything(i) => i }))
//      }
//      val decode = Enumeratee.map[Array[Byte]](arr => new String(arr, request.charset.getOrElse("UTF-8")))
//      val e = Enumeratee.grouped(upToNewLine)
//      decode.transform(e.transform(Iteratee.consume())).map(Right(_))
//  }
}
