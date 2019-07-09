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
package cmwell.dc.stream.fingerprint

import java.util.concurrent.Executors

import akka.actor.{ActorSystem, Scheduler}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Framing
import akka.util.ByteString
import cmwell.util.http.{SimpleHttpClient, SimpleResponse, SimpleResponseHandler}
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.json.Json

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object FingerPrintWebService extends LazyLogging{

  val endl = ByteString("\n", "UTF-8")
  case class FingerPrintData(uuid:String, data:String)
  val lineSeparatorFrame = Framing.delimiter(delimiter = endl,
    200000,
    allowTruncation = false)



  def generateFingerPrint(url:String)(implicit ec:ExecutionContext,
                                      mat:ActorMaterializer, system:ActorSystem) = {
    logger.info("lala ws url="  + url)
    implicit val scheduler = system.scheduler
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    val res = retry(10.seconds, 5){ SimpleHttpClient.get(url) }
    res.flatMap(res => toRDF(res.body._2))
  }

  private def retry[T](delay: FiniteDuration, retries: Int = -1)(task: => Future[SimpleResponse[String]])(implicit ec: ExecutionContext, scheduler: Scheduler):
  Future[SimpleResponse[String]] = {
    task.recoverWith {
      case e: Throwable if retries > 0 =>
        logger.error("Failed to request fp web service, going to retry, retry count=" + retries, e)
        akka.pattern.after(delay, scheduler)(retry(delay, retries - 1)(task))
    }
  }


  def toRDF(data:String)(implicit ec:ExecutionContext) = {
    Future {
      //TODO:remove the filter of "Nan after Yaron's fix
      val data2 = data.split("\n")(0).replace("\\", "\\\\")
      val uuid = (Json.parse(data2) \ "account_info" \ "UUID").as[String]
      val subject = s"<http://graph.link/ees/FP-$uuid>"
      val rdf2 =
        s"""$subject <cmwell://meta/sys#type> \"FileInfoton\" .
           |$subject  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://graph.link/ees/type/FingerPrint> .
           |$subject  <cmwell://meta/sys#data> \"${data2.replace("\"", "\\\"")}\" .
       """.stripMargin
      ByteString(rdf2)
    }

  }

}

//object FingerPrintWebServiceMain extends App{
//
//  implicit val system = ActorSystem("MySystem")
//  implicit val scheduler = system.scheduler
//  val executor = Executors.newFixedThreadPool(5)
// implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutor(executor)
//  implicit val mat = ActorMaterializer()
//
//  val f = FingerPrintWebService.generateFingerPrint("http://xch0:9090/user/SL1-78ZZM2D")
//}
