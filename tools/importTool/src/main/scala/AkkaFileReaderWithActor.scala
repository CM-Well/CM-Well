
import java.io.File

import akka.http.scaladsl.model.HttpProtocols.`HTTP/1.0`
import akka.http.scaladsl.model.MediaTypes.`text/plain`
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, HttpResponse, StatusCodes, _}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Flow, Framing, RestartSource, Sink, Source}
import akka.{Done, NotUsed, http}
import akka.util.{ByteString, Timeout}

import akka.pattern.ask
import akka.actor.{Actor, ActorSystem, Props, Scheduler, Status}
import akka.http.scaladsl.{Http, model}
import akka.stream.ActorMaterializer
import HttpCharsets._
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.headers.{ByteRange, RawHeader}

import scala.concurrent.duration._

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

case class ActorInput(inputUrl: String, outputFormat:String, cluster:String)

class AkkaFileReaderWithActor extends Actor {

  import system.dispatcher
  implicit val system = ActorSystem("app")
  implicit val scheduler = system.scheduler
  implicit val timeout = Timeout(5 seconds)

  implicit val mat = ActorMaterializer()
  val bytesAccumulatorActor = system.actorOf(Props(new BytesAccumulatorActor()), name = "offsetActor")

  case class TripleWithKey(subject: String, triple: String)

  def receive: Receive = {
    case ActorInput(inputUrl, format, cluster) => {
      println("Starting flow.....")
      readAndImportFile(inputUrl, format, cluster)
    }

  }

  def readFileFromServer(inputUrl:String) = {
    println("Going to send get request to inputUrl=" + inputUrl)
      val future = bytesAccumulatorActor ? LastOffset
      future.flatMap(x => {
        val ranges: String = "bytes=" + x + "-"
        println("Resending get request for ranges=" + ranges)
        val request = Get(inputUrl).addHeader(RawHeader.apply("Range", ranges))
        Http().singleRequest(request)
      })
  }

  def readAndImportFile(inputUrl:String, format:String, cluster:String)= {
    val httpResponse = readFileFromServer(inputUrl)
    httpResponse.onComplete(
      {
        case Success(r) => println("Get file from server successfully")
        case Failure(e) => {
          println("Got a failure while reading the file, ", e.getMessage)
          system.scheduler.scheduleOnce(7000 milliseconds, self, ActorInput(inputUrl, format, cluster))
        }})
    val graphResult = httpResponse.map ({ response =>
      println("Get response status=" + response.status)
      val contentLengthHeader = response.headers.find(_.name == "Content-Range").map(_.value).getOrElse("")
      val fileSize = contentLengthHeader.split("/")(1).toLong + 1
      bytesAccumulatorActor ! FileSize(fileSize)
      response.entity.withSizeLimit(fileSize).dataBytes
        .via(Framing.delimiter(ByteString(System.lineSeparator()), 100000, true))
        .map(_.utf8String)
        .map(getTripleKey)
        .sliding(2, 1)
        .splitAfter(pair => (pair.head, pair.last) match {
        case (TripleWithKey(sub1, triple1), TripleWithKey(sub2, triple2)) => sub1 != sub2
      })
        .mapConcat(_.headOption.toList)
        .fold("" -> List.empty[String]) {
        case ((_, values), TripleWithKey(subject, triple)) => subject -> (triple :: values)
      }
        .map(x => x._2)
        .mergeSubstreams
        .grouped(25)
        .mapAsync(1)(batch=> postWithRetry(batch, format, cluster))
        .statefulMapConcat {
          () => {
            x => {
              bytesAccumulatorActor ! Message(x._2)
              List.empty
            }
          }
        }
        .runWith(Sink.head)
        .onComplete(
          {
            case Success(r) => {
              println("Process completed successfully")
              system.terminate()
            }
            case Failure(e) => {
              println("Got a failure during the process, ", e.getMessage)
              system.scheduler.scheduleOnce(7000 milliseconds, self, ActorInput(inputUrl, format, cluster))
            }})

    })
    graphResult
  }

  def postWithRetry(batch: Seq[List[String]], format:String, cluster:String): Future[(HttpResponse, Int)] = {
    retry(10.seconds, 3){ ingest(batch, format, cluster) }
  }

  def retry[T](delay: FiniteDuration, retries: Int)(task: => Future[(T, Int)])(implicit ec: ExecutionContext, scheduler: Scheduler): Future[(T, Int)] = {
    task.recoverWith {
      case e: Throwable if retries > 0 =>
        println("Failed to ingest,", e.getMessage)
        println("Going to retry, retry count=" + retries)
        akka.pattern.after(delay, scheduler)(retry(delay, retries - 1)(task))
    }

  }

  def getTripleKey(triple:String): TripleWithKey = {
    TripleWithKey(triple.split(" ")(0), triple)
  }


  def ingest(batch: Seq[List[String]], format:String, cluster:String): Future[(HttpResponse, Int)] = {
    val infotonsList = batch.flatten
    val batchSizeInBytes = infotonsList.mkString.getBytes.length + infotonsList.size
    val postRequest = HttpRequest(
      HttpMethods.POST,
      "http://"+ cluster + ":9000/_in?format=" + format,
      entity = HttpEntity(`text/plain` withCharset `UTF-8`, infotonsList.mkString.getBytes),
      protocol = `HTTP/1.0`
    )
    val postHttpResponse = Http(system).singleRequest(postRequest)
    val futureResponse = postHttpResponse.flatMap(res=> if (res.status.isSuccess()) Future.successful(res, batchSizeInBytes) else {
      Future.failed(new Throwable("Got post error code"))})
    futureResponse
  }

}

//object AkkaActorMain extends App {
//  val system = ActorSystem("MySystem")
//  implicit val timeout = Timeout(5 seconds)
//  println("Hi")
//  val myActor = system.actorOf(Props(new AkkaFileReaderWithActor()), name = "myactor")
//  myActor ! ActorInput("http://localhost:8080/oa-ok.ntriples", "nquads", "localhost")
//
//
//}