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


package cmwell.ctrl.utils

import akka.io.IO
import akka.util.Timeout
import cmwell.ctrl.config.Config
import com.ning.http.client.Response
import com.typesafe.scalalogging.LazyLogging

import k.grid.Grid
//import spray.can.Http
//import spray.http._
import scala.concurrent.duration._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
/**
 * Created by michael on 12/4/14.
 */


case class UtilHttpResponse(code : Int, content : String)

object HttpUtil extends LazyLogging {
//  implicit val timeout = Timeout(15.seconds)
//
//  private def sendRequest(rq : HttpRequest) : Future[UtilHttpResponse] = {
//    (io ? rq).mapTo[HttpResponse].map {
//      res =>
//        UtilHttpResponse(res.status.intValue, res.entity.asString(HttpCharsets.`UTF-8`))
//    }
//  }
//
//  val io = IO(Http)(Grid.system)
//
//  def httpGet(dest : String) : Future[UtilHttpResponse] = {
//    val rq = HttpRequest(HttpMethods.GET, uri = dest )
//    sendRequest(rq)
//  }
//
//  def httpPost(dest : String, body : String ) : Future[UtilHttpResponse] = {
//    val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)` ,body)
//    val rq = HttpRequest(HttpMethods.POST, uri = dest, entity = entity)
//    sendRequest(rq)
//  }
//
//  def httpPut(dest : String, body : String ) : Future[UtilHttpResponse] = {
//    val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)` ,body)
//    val rq = HttpRequest(HttpMethods.PUT, uri = dest, entity = entity)
//    sendRequest(rq)
//  }
}
