
import akka.actor.{Actor, ActorSystem, Props, Scheduler, Terminated}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.HttpCharsets._
import akka.http.scaladsl.model.HttpProtocols.`HTTP/1.0`
import akka.http.scaladsl.model.MediaTypes.`text/plain`
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, HttpResponse, _}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Framing, Sink}
import akka.util.{ByteString, Timeout}

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
  val bytesAccumulatorActor = system.actorOf(Props(new BytesAccumulatorActor()), name = "offsetActor")
  context.watch(bytesAccumulatorActor)

  override def receive: Receive = {
    case ActorInput => {
      println("Starting flow.....")
      readAndImportFile(inputUrl, format, cluster)
    }
    case Terminated(bytesAccumulatorActor) => {
      println("BytesAccumulatorActor has been crashed, going to restart the flow")
      readAndImportFile(inputUrl, format, cluster)

    }
  }

  def readFileFromServer(inputUrl:String) = {
    println("Going to send get request to inputUrl=" + inputUrl)
    val lastOffset = (bytesAccumulatorActor ? LastOffset)(Timeout(5.seconds))
      lastOffset.flatMap(x => {
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
        case Success(r) if  r.status.isSuccess() => println("Get file from server successfully")
        case Success(r) =>
        {
          println("Failed to read file, got status code:" + r.status)
          system.scheduler.scheduleOnce(7000.milliseconds, self, ActorInput)
        }
        case Failure(e) => {
          println("Got a failure while reading the file, ", e.getMessage)
          system.scheduler.scheduleOnce(7000.milliseconds, self, ActorInput)
        }})
    httpResponse.foreach({ response =>
      println("Get response status=" + response.status)
      val contentLengthHeader = response.headers.find(_.name == "Content-Range").map(_.value).getOrElse("")
      val fileSize = contentLengthHeader.split("/")(1).toLong + 1
      bytesAccumulatorActor ! FileSize(fileSize)
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
        .mapAsync(1)(batch=> postWithRetry(batch, format, cluster))
        .map(x=> bytesAccumulatorActor ! Message(x._2))
        .runWith(Sink.ignore)
        .onComplete(
          {
            case Success(r) => {
              println("Process completed successfully")
              system.terminate()
            }
            case Failure(e) => {
              println("Got a failure during the process, ", e.getMessage)
              system.scheduler.scheduleOnce(7000.milliseconds, self, ActorInput)
            }})

    })
  }

  def postWithRetry(batch: Seq[List[String]], format:String, cluster:String): Future[(HttpResponse, Long)] = {
    retry(10.seconds, 3){ ingest(batch, format, cluster) }
  }

  def retry[T](delay: FiniteDuration, retries: Int)(task: => Future[(T, Long)])(implicit ec: ExecutionContext, scheduler: Scheduler): Future[(T, Long)] = {
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


  def ingest(batch: Seq[List[String]], format:String, cluster:String): Future[(HttpResponse, Long)] = {
    val infotonsList = batch.flatten
    val batchSizeBytes = infotonsList.mkString.getBytes
    val batchTotalSizeInBytes = batchSizeBytes.length + infotonsList.size
    val postRequest = HttpRequest(
      HttpMethods.POST,
      "http://"+ cluster + "/_in?format=" + format,
      entity = HttpEntity(`text/plain` withCharset `UTF-8`, batchSizeBytes)
    )
    val postHttpResponse = Http(system).singleRequest(postRequest)
    val futureResponse = postHttpResponse.flatMap(res=> if (res.status.isSuccess()) Future.successful(res, batchTotalSizeInBytes.longValue()) else {
      Future.failed(new Throwable("Got post error code," + res.status))})
    futureResponse
  }

}