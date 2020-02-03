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
//package cmwell.util
//
//import cmwell.util.exceptions._
//import com.typesafe.scalalogging.LazyLogging
//import dispatch.{url => req ,_} ,Defaults._
//import scala.concurrent.{Promise, Await}
//import scala.concurrent.duration._
//import scala.util.{Success, Failure}
//import scala.language.postfixOps
//
///**
// * Created with IntelliJ IDEA.
// * User: gilad
// * Date: 7/30/13
// * Time: 5:41 PM
// * To change this template use File | Settings | File Templates.
// */
//package object connection extends LazyLogging {
//
//  def put(url: String, headers: List[(String, String)] = Nil, data: String = null): Future[String] = {
//    val msg = req(url).PUT <:< headers << data
//    Http(msg OK as.String)
//  }
//
//  def post(url: String, headers: List[(String, String)] = Nil, data: String = null): Future[String] = {
//    val p:Promise[String] = Promise()
//    val msg = req(url).POST <:< headers << data
//    val res = Http(msg)
//    res.onComplete{
//      case Failure(t) => t.printStackTrace(); p.failure(t)
//      case Success(response) => response.getStatusCode match {
//        case 200 => p.success(response.getResponseBody)
//        case x =>
//          logger debug (s"post failed with error code $x and content:\n ${response.getResponseBody}")
//          p.failure(new Exception(s"post failed with response: $x"))
//      }
//    }
//
//    p.future
//
//  }
//
//  def get(url: String, headers: List[(String, String)] = Nil): Future[String] = {
//    val msg = req(url).GET <:< headers
//    Http(msg OK as.String)
//  }
//
//  def blockPut(url: String, headers: List[(String, String)] = Nil, data: String = null)(implicit timeToBlock: Duration = 60 seconds): String = {
//    Await.result(put(url, headers, data), timeToBlock)
//  }
//
//  def blockPost(url: String, headers: List[(String, String)] = Nil, data: String = null)(implicit timeToBlock: Duration = 60 seconds): String = {
//    Await.result(post(url, headers, data), timeToBlock)
//  }
//
//  def blockGet(url: String, headers: List[(String, String)] = Nil)(implicit timeToBlock: Duration = 60 seconds): String = {
//    Await.result(get(url, headers), timeToBlock)
//  }
//}
