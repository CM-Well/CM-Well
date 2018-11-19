
import java.io.File

import akka.http.scaladsl.model.HttpProtocols.`HTTP/1.0`
import akka.http.scaladsl.model.MediaTypes.`text/plain`
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, HttpResponse, StatusCodes, _}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Flow, Framing, RestartSource, Sink, Source}
import akka.{Done, NotUsed, http}
import akka.util.{ByteString, Timeout}
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentLinkedDeque

import akka.pattern.ask
import akka.actor.{Actor, ActorSystem, Props, Scheduler, Status}
import akka.http.scaladsl.{Http, model}
import akka.stream.ActorMaterializer
import HttpCharsets._
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.headers.{ByteRange, RawHeader}
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}

import scala.concurrent.duration._
import akka.stream.StreamRefMessages.ActorRef

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}
import org.rogach.scallop._

case class ActorInput(inputUrl: String, outputFormat:String)

class AkkaFileReaderWithActor extends Actor {

  import system.dispatcher
  implicit val system = ActorSystem("app")
  implicit val s = system.scheduler
  implicit val timeout = Timeout(5 seconds)

  implicit val mat = ActorMaterializer()
  val helloActor = system.actorOf(Props(new BytesAccumulatorActor()), name = "helloactor")
  var outputFormat = ""


  def receive: Receive = {
    case ActorInput(inputUrl, outputFormat) => {
      println("Starting flow")
      parseAndIngestLargeFile(inputUrl)
      this.outputFormat = outputFormat

    }
  }

  def sendGetRequest(inputUrl:String) = {
    println("Going to send get request to inputUrl=" + inputUrl)
      val future = helloActor ? "test"
      future.flatMap(x => {
//        println("YOOOOOOOOOOo, x=" + x)
        var ranges: String = "bytes=" + x + "-"
        println("Resending get request for ranges=" + ranges)
//        val request = Get("http://localhost:8080/oa-ok.ntriples").addHeader(RawHeader.apply("Range", ranges))
        val request = Get(inputUrl).addHeader(RawHeader.apply("Range", ranges))
        val responseFuture = Http().singleRequest(request)
        responseFuture
      })
  }

  def parseAndIngestLargeFile(inputUrl:String)= {
    val res = sendGetRequest(inputUrl)
//    val sourceHttp = Source.fromFuture(res)
    res .onComplete(
      {
        case Success(r) => {println("Success")}
        case Failure(e) => {
          println("oopss...Got a failure in get request")
          system.scheduler.scheduleOnce(7000 milliseconds, self, ActorInput(inputUrl, outputFormat))
        }})
    val materializedFuture = res.map ({ response =>
      println("Get response status=" + response.status)
      val hashedBody = response.headers.find(_.name == "Content-Range").map(_.value).getOrElse("")
      response.entity.withSizeLimit(hashedBody.split("/")(1).toLong + 1).dataBytes
        .via(Framing.delimiter(ByteString(System.lineSeparator()), 100000, true))
        .map(_.utf8String)
        .map(x => (getTripleKey(x), x))
        .sliding(2, 1)
        .splitAfter(pair => (pair.headOption, pair.lastOption) match {
        case (Some((key1, _)), Some((key2, _))) => key1 != key2
      })
        .mapConcat(_.headOption.toList)
        .fold("" -> List.empty[String]) {
        case ((_, values), (key, value)) => key -> (value :: values)
      }
        .map(x => x._2)
        .mergeSubstreams
        .grouped(25)
        .mapAsync(1)(postWithRetry)
        .statefulMapConcat {
          () => {
            x => {
              helloActor ! Message(x._2)
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
              println("oopss...Got a failure in get request")
              system.scheduler.scheduleOnce(7000 milliseconds, self, ActorInput(inputUrl, outputFormat))
            }})

    })
    materializedFuture
  }

  def postWithRetry(batch: Seq[List[String]]): Future[(HttpResponse, Int)] = {
    retry(10.seconds, 3){ post(batch) }
  }


  def retry[T](/*f: Seq[String] => Future[T], batch: => Seq[String],*/ delay: FiniteDuration, retries: Int)(task: => Future[(T, Int)])(implicit ec: ExecutionContext, s: Scheduler): Future[(T, Int)] = {
    task.recoverWith {
      case e: Throwable if retries > 0 =>
        println("Going to retry, retry count=" + retries)
        //        post(batch)
        println("Failed to retry post request,", e.getMessage)
        akka.pattern.after(delay, s)(retry(delay, retries - 1)(task))
    }

  }

  def getTripleKey(triple:String): String = {
    triple.split(" ")(0)
  }

  def post(batch: Seq[List[String]]): Future[(HttpResponse, Int)] = {
    var flatList = batch.flatten
    println("Infotons " + flatList)
    println("great,infotons batch size= " + batch.size + ", batch count lines=" + flatList.size)
    val batchSizeInBytes = flatList.mkString.getBytes.length + flatList.size
    var postRequest = HttpRequest(
      HttpMethods.POST,
      "http://localhost:9000/_in?format=" + outputFormat,
      entity = HttpEntity(`text/plain` withCharset `UTF-8`, flatList.mkString.getBytes),
      protocol = `HTTP/1.0`
    )
    var postResponse = Http(system).singleRequest(postRequest)
    println(postResponse.foreach(res=> println("post status code=" + res.status)))
    var filteredResponse = postResponse.flatMap(res=> if (res.status.isSuccess()) Future.successful(res, batchSizeInBytes) else {
      Future.failed(new Throwable("Got post error code"))})
    filteredResponse
  }

}

//
//object AkkaActorMain extends App {
//  val system = ActorSystem("MySystem")
//  implicit val timeout = Timeout(5 seconds)
//  println("Hi")
//  val myActor = system.actorOf(Props(new AkkaFileReaderWithActor()), name = "myactor")
//  myActor ! ActorInput("http://localhost:8080/oa-ok.ntriples", "nquads")
//
//
//}