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
import akka.stream.ActorMaterializer
import akka.util.ByteString
import cmwell.util.http.{SimpleHttpClient, SimpleResponse}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object FingerPrintWebService extends LazyLogging{

  val endl = ByteString("\n", "UTF-8")
  case class FingerPrintData(uuid:String, data:String)

case class ConnectException(msg:String) extends Exception
  def generateFingerPrint(url:String, uuid:String)(implicit ec:ExecutionContext,
                                      mat:ActorMaterializer, system:ActorSystem) = {
    implicit val scheduler = system.scheduler
    val res = retry(5.seconds, 3){handleGet(url)}
    res.map(res => toRDF(uuid, res.body._2))
      .recoverWith{
      case e:Exception =>
          logger.error(s"Failed create fingerprint for url=$url", e)
          Future.successful(ByteString(""))
    }
  }

  private def handleGet(url:String)(implicit ec:ExecutionContext, system:ActorSystem, mat:ActorMaterializer) = {
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    SimpleHttpClient.get(url)(UTF8StringHandler, implicitly[ExecutionContext], system, mat)
      .map(x=> x.status match{
        case 200 => x
        case 503 => throw ConnectException(s"Got 503 error code during calling fingerprint web service,url=$url headers=${x.headers}, erroMsg=${x.body}")
        case _ => throw new Exception(s"Failure during call fingerprint web service,, url=$url statuscode=${x.status}, headers=${x.headers}, erroMsg=${x.body}")
      })
  }
  private def retry[T](delay: FiniteDuration, retries: Int = -1)(task: => Future[SimpleResponse[String]])(implicit ec: ExecutionContext, scheduler: Scheduler):
  Future[SimpleResponse[String]] = {
    task.recoverWith {
      case e:ConnectException =>
        logger.warn("Failed to request fp web service, going to retry", e)
        akka.pattern.after(delay, scheduler)(retry(delay)(task))
      case e: Exception if retries > 0 =>
        logger.warn(s"Failed to request fp web service, going to retry, retry count=$retries", e)
        akka.pattern.after(delay, scheduler)(retry(delay, retries - 1)(task))
    }
  }


  def toRDF(uuid:String, data:String)(implicit ec:ExecutionContext) = {
    val fpJsonData = data.split("\n")(0).replace("\\", "\\\\")
    logger.info(s"lala uuid=$uuid")
      val subject = s"<http://graph.link/ees/FP-$uuid>"
      val rdf =
        s"""$subject <cmwell://meta/sys#type> "FileInfoton" .
           |$subject  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://graph.link/ees/type/FingerPrint> .
           |$subject  <cmwell://meta/sys#data> "${fpJsonData.replace("\"", "\\\"")}" .
       """.stripMargin
      ByteString(rdf)

  }

}


