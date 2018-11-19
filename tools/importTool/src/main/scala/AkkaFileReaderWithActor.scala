
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

case class ActorInput(inputUrl: String, outputFormat:String)

class AkkaFileReaderWithActor extends Actor {

  import system.dispatcher
  implicit val system = ActorSystem("app")
  implicit val scheduler = system.scheduler
  implicit val timeout = Timeout(5 seconds)

  implicit val mat = ActorMaterializer()
  val bytesAccumulatorActor = system.actorOf(Props(new BytesAccumulatorActor()), name = "offsetActor")
  var format = ""


  def receive: Receive = {
    case ActorInput(inputUrl, format) => {
      println("Starting flow.....")
      readAndImportFile(inputUrl)
      this.format = format

    }

  }

  def readFileFromServer(inputUrl:String) = {
    println("Going to send get request to inputUrl=" + inputUrl)
      val future = bytesAccumulatorActor ? "lastOffsetByte"
      future.flatMap(x => {
        var ranges: String = "bytes=" + x + "-"
        println("Resending get request for ranges=" + ranges)
        val request = Get(inputUrl).addHeader(RawHeader.apply("Range", ranges))
        Http().singleRequest(request)
      })
  }

  def readAndImportFile(inputUrl:String)= {
    val httpResponse = readFileFromServer(inputUrl)
    httpResponse .onComplete(
      {
        case Success(r) => println("Read the whole file from server successfully")
        case Failure(e) => {
          println("oopss...Got a failure in get request")
          system.scheduler.scheduleOnce(7000 milliseconds, self, ActorInput(inputUrl, format))
        }})
    val graphResult = httpResponse.map ({ response =>
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
              println("oopss...Got a failure in get request")
              system.scheduler.scheduleOnce(7000 milliseconds, self, ActorInput(inputUrl, format))
            }})

    })
    graphResult
  }

  def postWithRetry(batch: Seq[List[String]]): Future[(HttpResponse, Int)] = {
    retry(10.seconds, 3){ ingest(batch) }
  }


  def retry[T](/*f: Seq[String] => Future[T], batch: => Seq[String],*/ delay: FiniteDuration, retries: Int)(task: => Future[(T, Int)])(implicit ec: ExecutionContext, scheduler: Scheduler): Future[(T, Int)] = {
    task.recoverWith {
      case e: Throwable if retries > 0 =>
        println("Going to retry, retry count=" + retries)
        println("Failed to retry post request,", e.getMessage)
        akka.pattern.after(delay, scheduler)(retry(delay, retries - 1)(task))
    }

  }

  def getTripleKey(triple:String): String = {
    triple.split(" ")(0)
  }

  def ingest(batch: Seq[List[String]]): Future[(HttpResponse, Int)] = {
    var infotonsList = batch.flatten
    println("Infotons " + infotonsList)
    println("great,infotons batch size= " + batch.size + ", batch count lines=" + infotonsList.size)
    val batchSizeInBytes = infotonsList.mkString.getBytes.length + infotonsList.size
    var postRequest = HttpRequest(
      HttpMethods.POST,
      "http://localhost:9000/_in?format=" + format,
      entity = HttpEntity(`text/plain` withCharset `UTF-8`, infotonsList.mkString.getBytes),
      protocol = `HTTP/1.0`
    )
    var postHttpResponse = Http(system).singleRequest(postRequest)
    println(postHttpResponse.foreach(res=> println("post status code=" + res.status)))
    var futureResponse = postHttpResponse.flatMap(res=> if (res.status.isSuccess()) Future.successful(res, batchSizeInBytes) else {
      Future.failed(new Throwable("Got post error code"))})
    futureResponse
  }

}


//object AkkaActorMain extends App {
//  val system = ActorSystem("MySystem")
//  implicit val timeout = Timeout(5 seconds)
//  println("Hi")
//  val myActor = system.actorOf(Props(new AkkaFileReaderWithActor()), name = "myactor")
//  myActor ! ActorInput("http://localhost:8080/oa-ok.ntriples", "nquads")
//
//
//}