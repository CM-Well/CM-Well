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

import akka.actor.{ActorSystem, Scheduler}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Framing, Source}
import akka.util.ByteString
import cmwell.dc.LazyLogging
import play.api.libs.json.Json

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class WebServiceConnectException(message: String) extends  Exception(message)
object FingerPrintWebService extends LazyLogging{

  val endl = ByteString("\n", "UTF-8")
  case class FingerPrintData(uuid:String, data:String)
  val lineSeparatorFrame = Framing.delimiter(delimiter = endl,
    200000,
    allowTruncation = true)



  def generateFingerPrint(url:String, cluster:String)(implicit ec:ExecutionContext, scheduler:Scheduler,
                                      mat:ActorMaterializer, system:ActorSystem) = {
    sendHttpRequest(url)
    .map(res=> toRDF(res.entity.withSizeLimit(1000000).dataBytes, cluster)).flatten
  }

  def sendHttpRequest(url:String)(implicit ec:ExecutionContext, scheduler:Scheduler,
                                           mat:ActorMaterializer, system:ActorSystem) :Future[HttpResponse] = {
    logger.info("lala url=" + url)
    retry(10.seconds, 5){ sendRequest(url) }
//    cmwell.util.concurrent.retry(5, 10.seconds)(sendRequest(url))(ec)
  }


  private def sendRequest(url: String)(implicit ec:ExecutionContext, mat:ActorMaterializer, system:ActorSystem) = {
    val webServiceRes = Http(system).singleRequest(HttpRequest(uri = url))
    val randomId = java.util.UUID.randomUUID.toString
    val futureResponse = webServiceRes.flatMap(res =>
      if (res.status.isSuccess()) Future.successful(res)
      else if(res.status.intValue() == 503) Future.failed(WebServiceConnectException(s"Got 503 error status code, headers=${res.headers}"))
      else {
        val blank = ByteString("", "UTF-8")
        res.entity.dataBytes.runFold(blank)(_ ++ _).map(_.utf8String)
          .onComplete {
            case Success(msg) =>
              logger.error("Message body id=" + randomId + " message=" + msg)
            case Failure(e) =>
              logger.error("Got a failure during print entity body, Message body id=" + randomId, e)
          }
        Future.failed(new Throwable(s"Got post error code," + res.status + ", headers=" + res.headers +
          ", message body will be displayed later, message id=" + randomId))
      }
    )
    futureResponse
  }

  private def retry[T](delay: FiniteDuration, retries: Int = -1)(task: => Future[HttpResponse])(implicit ec: ExecutionContext, scheduler: Scheduler):
  Future[HttpResponse] = {
    task.recoverWith {
      case e: WebServiceConnectException =>
        logger.error("Failed to request fingerprint web service,", e.printStackTrace())
        logger.error("Going to retry till web service will be available")
        akka.pattern.after(delay, scheduler)(retry(delay)(task))
      case e: Throwable if retries > 0 =>
        logger.error("Failed to request web service, need to write to RED log the bulk", e)
        logger.error("Going to retry, retry count=" + retries)
        akka.pattern.after(delay, scheduler)(retry(delay, retries - 1)(task))
    }
  }

  def toRDF(data:Source[ByteString, _], cluster:String)(implicit ec: ExecutionContext,mat:ActorMaterializer) = {
    //TODO:remove the filter of "Nan after Yaron's fix
    data
      .via(lineSeparatorFrame)
      .map(fp => fp.utf8String)
      .map(fp => fp.replace("\\", "\\\\"))
      .filterNot(x => x.contains("NaN"))
      .map(fp => FingerPrintData((Json.parse(fp) \ "account_info" \ "UUID").as[String], fp.replace("\"", "\\\"")))
      .map(fpData => {
        val subject = s"<http://$cluster/graph.link/ees/FP-${fpData.uuid}>"
        val fpInfotonMetaData = subject + " <http://" + cluster + "/meta/sys#type> \"FileInfoton\" .\n" +
          subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://graph.link/ees/type/FingerPrint> .\n" +
          subject + " <http://" + cluster + "/meta/sys#data> \""
       fpInfotonMetaData + fpData.data + "\" ."
      })
      .map(rdfStr => ByteString(rdfStr))
      .runFold(ByteString.empty){case (acc, fp) => acc ++ fp}

  }

}
