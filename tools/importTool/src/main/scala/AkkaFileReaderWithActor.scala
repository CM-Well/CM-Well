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
import java.nio.file.{Files, Paths}

import akka.actor.{Actor, ActorSystem, Props, Scheduler}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.HttpCharsets._
import akka.http.scaladsl.model.MediaTypes.`text/plain`
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Framing, Sink}
import akka.util.ByteString

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class ActorInput(inputUrl: String, outputFormat:String, cluster:String)
case class TripleWithKey(subject: String, triple: String)

class AkkaFileReaderWithActor(inputUrl:String, format:String, cluster:String) extends Actor {

  import system.dispatcher
  implicit val system = context.system
  implicit val scheduler = system.scheduler

  implicit val mat = ActorMaterializer()

  override def receive: Receive = {
    case ActorInput => {
      println("Starting flow.....")
      readAndImportFile(inputUrl, format, cluster)
    }
  }

  def readFileFromServer(inputUrl:String) = {
    println("Going to send get request to inputUrl=" + inputUrl)
    val lastOffset = getLastOffset
    val ranges: String = "bytes=" + lastOffset + "-"
    println("Resending get request for ranges=" + ranges)
    val request = Get(inputUrl).addHeader(RawHeader.apply("Range", ranges))
    Http().singleRequest(request)

  }

  def readAndImportFile(inputUrl:String, format:String, cluster:String)= {
    val httpResponse = readFileFromServer(inputUrl)
    httpResponse.onComplete(
      {
        case Success(r) if  r.status.isSuccess() => println("Get file from server successfully")
        case Success(r) =>
        {
          println("Failed to read file, got status code:" + r.status)
          system.scheduler.scheduleOnce(7000.milliseconds, self, ActorInput)
        }
        case Failure(e) => {
          println("Got a failure while reading the file")
          e.printStackTrace()
          system.scheduler.scheduleOnce(7000.milliseconds, self, ActorInput)
        }})
    httpResponse.foreach({ response =>
      println("Get response status=" + response.status)
      val contentLengthHeader = response.headers.find(_.name == "Content-Range").map(_.value).getOrElse("")
      val fileSize = contentLengthHeader.split("/")(1).toLong + 1
      response.entity.withSizeLimit(fileSize).dataBytes
        .via(Framing.delimiter(ByteString(System.lineSeparator()), 100000, true))
        .map(_.utf8String)
        .map(getTripleKey)
        .sliding(2, 1)
        .splitAfter(pair => pair.head.subject != pair.last.subject)
        .map(_.head)
        .fold(List.empty[String]) { case (allTriples, TripleWithKey(_, triples)) => triples :: allTriples }
        .mergeSubstreams
        .grouped(25)
        //need to consider the default num of akka connection pool in order to increase the paralism of map async
        .mapAsync(1)(batch=> postWithRetry(batch, format, cluster))
        .statefulMapConcat {
          val list = List.empty
          var lastOffset = getLastOffset
          var count = 0
          () => {
            httpResponse => {
              lastOffset += httpResponse._2
              OffsetFileHandler.persistOffset(lastOffset)
              count+=1
              if(count % 1000000 == 0) {
                val progressPercentage = (lastOffset * 100.0f) / fileSize
                print("Progress of ingest infotons: " + f"$progressPercentage%1.2f" + "%\r")
                count=0
              }
            }
              list
          }
        }
        .runWith(Sink.ignore)
        .onComplete(
          {
            case Success(r) => {
              println("Process completed successfully")
              system.terminate()
            }
            case Failure(e) => {

              println("Got a failure during the process, ")
              e.printStackTrace()
              system.scheduler.scheduleOnce(7000.milliseconds, self, ActorInput)
            }})

    })
  }

  private def getLastOffset = {
    if (Files.exists(Paths.get("./lastOffset"))) OffsetFileHandler.readOffset else 0L
  }

  def postWithRetry(batch: Seq[List[String]], format:String, cluster:String): Future[(HttpResponse, Long)] = {
    retry(10.seconds, 3){ ingest(batch, format, cluster) }
  }

  def retry[T](delay: FiniteDuration, retries: Int)(task: => Future[(T, Long)])(implicit ec: ExecutionContext, scheduler: Scheduler): Future[(T, Long)] = {
    task.recoverWith {
      case e: Throwable if retries > 0 =>
        println("Failed to ingest", e.getMessage)
        println("Going to retry, retry count=" + retries)
        akka.pattern.after(delay, scheduler)(retry(delay, retries - 1)(task))
    }
  }

  def getTripleKey(triple:String): TripleWithKey = {
    TripleWithKey(triple.split(" ")(0), triple)
  }


  def ingest(batch: Seq[List[String]], format:String, cluster:String): Future[(HttpResponse, Long)] = {
    val infotonsList = batch.flatten
    val batchContentBytes = infotonsList.mkString.getBytes
    val batchTotalSizeInBytes = batchContentBytes.length + infotonsList.size
    val postRequest = HttpRequest(
      HttpMethods.POST,
      "http://"+ cluster + "/_in?format=" + format,
      entity = HttpEntity(`text/plain` withCharset `UTF-8`, batchContentBytes)
    )
    val postHttpResponse = Http(system).singleRequest(postRequest)
    val randomId = java.util.UUID.randomUUID.toString
    val futureResponse = postHttpResponse.flatMap(res=>
      if (res.status.isSuccess()) Future.successful(res, batchTotalSizeInBytes.longValue())
      else {
        val blank = ByteString("", "UTF-8")
        res.entity.dataBytes.runFold(blank)(_ ++ _).map(_.utf8String)
          .onComplete
          {
            case Success(msg) =>
              println("Message body id=" + randomId + " message=" + msg)
            case Failure(e) =>
              println("Got a failure during print entity body, Message body id=" + randomId)
              e.printStackTrace()
          }
        Future.failed(new Throwable("Got post error code," + res.status + ", headers=" + res.headers + ", message body will be displayed later, message id=" + randomId))
      }
    )
    futureResponse
  }

}