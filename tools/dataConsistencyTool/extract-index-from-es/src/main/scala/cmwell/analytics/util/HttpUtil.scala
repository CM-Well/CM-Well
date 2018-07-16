package cmwell.analytics.util

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.RequestEntityAcceptance.Tolerated
import akka.http.scaladsl.model.{HttpMethod, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.{MILLISECONDS, _}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object HttpUtil {

  private val mapper = new ObjectMapper()

  private val config = ConfigFactory.load
  private val ReadTimeout = FiniteDuration(config.getDuration("extract-index-from-es.read-timeout").toMillis, MILLISECONDS)

  // Elasticsearch uses the POST verb in some places where the request is actually idempotent.
  // Requests that use POST, but are known to be idempotent can use this method.
  // The presence of any non-idempotent request in-flight causes Akka to not retry, and that will tend result in
  // entire downloads failing more often.
  val SAFE_POST = HttpMethod(
    value = "POST",
    isSafe = true,
    isIdempotent = true,
    requestEntityAcceptance = Tolerated)

  def resultAsync(request: HttpRequest,
                  action: String)
                 (implicit system: ActorSystem,
                  executionContext: ExecutionContextExecutor,
                  actorMaterializer: ActorMaterializer): Future[ByteString] =
    Http().singleRequest(request).map {

      case HttpResponse(status, _, entity, _) if status.isSuccess =>
        entity.dataBytes
          .fold(ByteString.empty)(_ ++ _)
          .runWith(Sink.head)

      case HttpResponse(status, _, entity, _) =>
        val message = Await.result(entity.toStrict(10.seconds).map(_.data), 10.seconds).utf8String
        throw new RuntimeException(s"HTTP request for $action failed. Status code: $status, message:$message")
    }
      .flatMap(identity)

  def result(request: HttpRequest,
             action: String,
             timeout: FiniteDuration = ReadTimeout)
            (implicit system: ActorSystem,
             executionContext: ExecutionContextExecutor,
             actorMaterializer: ActorMaterializer): ByteString =
    Await.result(resultAsync(request, action), timeout)

  def jsonResult(request: HttpRequest,
                 action: String,
                 timeout: FiniteDuration = ReadTimeout)
                (implicit system: ActorSystem,
                 executionContext: ExecutionContextExecutor,
                 actorMaterializer: ActorMaterializer): JsonNode =
    mapper.readTree(result(request, action, timeout).utf8String)

  def jsonResultAsync(request: HttpRequest,
                      action: String)
                     (implicit system: ActorSystem,
                      executionContext: ExecutionContextExecutor,
                      actorMaterializer: ActorMaterializer): Future[JsonNode] =
    resultAsync(request, action).map((bytes: ByteString) => mapper.readTree(bytes.utf8String))
}
