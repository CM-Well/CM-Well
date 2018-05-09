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
package cmwell.tools.data.utils.akka

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpEntity.{Chunked, LastChunk}
import akka.http.scaladsl.model.StatusCodes.{ClientError, ServerError}
import akka.http.scaladsl.model._
import akka.pattern._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.ArgsManipulations.HttpAddress
import cmwell.tools.data.utils.akka.HeaderOps._
import cmwell.tools.data.utils.logging.{DataToolsLogging, LabelId}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Retry extends DataToolsLogging with DataToolsConfig {

  /**
    * Sends HTTP requests containing `Seq[ByteString]` payload data and context of type `T`.
    *
    * When receiving a response with status=[[akka.http.scaladsl.model.StatusCodes.ServerError ServerError]],
    * the request is sent again after the given amount of time.
    *
    * When receiving a response with status=[[akka.http.scaladsl.model.StatusCodes.ClientError ClientError]],
    * the request is split to single ByteString elements and these requests are sent again.
    * If given payload contains only 1 element, the request if not being retried.
    *
    * @param delay delay time between retries of same request
    * @param parallelism number of concurrent requests to be sent
    * @param createRequest function for creating HTTP request from `Seq[ByteString]`
    * @param system actor system
    * @param mat materializer
    * @param ec execution context
    * @tparam T context type, element paired with each request
    * @return flow which sends (and retries) HTTP requests and returns Try of results paired with request data and context
    */
  def retryHttp[T](delay: FiniteDuration,
                   parallelism: Int,
                   baseUrl: String,
                   limit: Option[Int] = None)(
                   createRequest: (Seq[ByteString]) => HttpRequest,
                   responseValidator: (ByteString, Seq[HttpHeader]) => Try[Unit] = (_, _) => Success(Unit))(
                   implicit system: ActorSystem,
                   mat: Materializer,
                   ec: ExecutionContext,
                   label: Option[LabelId] = None) = {

    val labelValue = label.map { case LabelId(id) => s"[$id]" }.getOrElse("")
    val toStrictTimeout = 30.seconds

    case class State(data: Seq[ByteString],
                     context: Option[T] = None,
                     response: Option[HttpResponse] = None,
                     count: Option[Int] = limit)

    def stringifyData(data: Seq[ByteString]) =
      concatByteStrings(data, ByteString(",")).utf8String

    def retryWith(
      state: State
    ): Option[immutable.Iterable[(Future[Seq[ByteString]], State)]] =
      state match {
        case State(data, _, Some(HttpResponse(s, h, e, _)), _)
            if s == StatusCodes.TooManyRequests =>
          // api garden quota error
          e.toStrict(toStrictTimeout)
            .map { strictEntity =>
              logger.error(
                s"$labelValue retry data: api garden mis-configured, call Yaniv to increase! host=${getHostname(
                  h
                )} status=$s entity=${strictEntity.data.utf8String} data=${concatByteStrings(data, endl).utf8String}"
              )
            }
            .onFailure {
              case err =>
                logger.error(
                  s"$labelValue api garden mis-configured, call Yaniv to increase! host=${getHostname(h)} status=$s cannot read entity",
                  err
                )
            }

          // schedule a retry to http stream
//        e.discardBytes()
          val future = after(delay, system.scheduler)(Future.successful(data))
          Some(immutable.Seq(future -> state))

        case State(data, _, Some(HttpResponse(s: ServerError, h, e, _)), _) =>
          // server error
          // special case: sparql-processor //todo: remove this in future
          if (e.toString contains "Fetching") {
            logger.warn(
              s"$labelValue will not schedule a retry on data ${concatByteStrings(data, endl).utf8String}"
            )
            e.discardBytes()
            None
          } else {
            e.discardBytes()

            // schedule a retry to http stream
            logger.warn(
              s"$labelValue server error: will retry again in $delay host=${getHostname(h)} status=$s entity=$e"
            )
            val future = after(delay, system.scheduler)(Future.successful(data))
            Some(immutable.Seq(future -> state))
          }

        case State(
            data,
            context,
            Some(HttpResponse(s: ClientError, h, e, _)),
            _
            ) =>
          // client error
          if (data.size > 1) {
            // before retry a request we should consume previous entity bytes
            e.withoutSizeLimit()
              .dataBytes
              .runFold(blank)(_ ++ _)
              .map { entityBytes =>
                logger.warn(
                  s"$labelValue client error: will retry again in $delay to send a single request, host=${getHostnameValue(
                    h
                  )} status=$s, entity=${entityBytes.utf8String}, request data=${stringifyData(data)}"
                )
                redLogger.error(
                  s"$labelValue client error: host=${getHostnameValue(h)} status=$s data=${stringifyData(data)} entity=${entityBytes.utf8String}"
                )
                badDataLogger.info(
                  s"$labelValue data=${concatByteStrings(data, endl).utf8String}"
                )
              }

            // failed to send a chunk of data, split to singles and retry
            Some(
              data
                .map(
                  dataElement =>
                    Future.successful(Seq(dataElement)) -> State(
                      Seq(dataElement),
                      context
                  )
                )
                .to[immutable.Iterable]
            )
          } else {
            e.toStrict(toStrictTimeout)
              .map { strictEntity =>
                logger.warn(
                  s"$labelValue client error: will not retry sending request, host=${getHostnameValue(
                    h
                  )} status=$s, entity=${strictEntity.data.utf8String}"
                )
                badDataLogger.info(
                  s"$labelValue data=${concatByteStrings(data, endl).utf8String}"
                )
              }
              .onFailure {
                case err =>
                  logger.warn(s"$labelValue client error: will retry again in $delay to send a single request, " +
                              s"host=${getHostnameValue(h)} status=$s, cannot read entity, request data=${stringifyData(data)}", err)
              }

            None // failed to send a single data element
          }

        case State(data, _, Some(HttpResponse(s, h, e, _)), count) =>

          redLogger.error(
            s"$labelValue error: host=${getHostnameValue(h)} status=$s entity=$e data=${stringifyData(data)}"
          )

          s.isSuccess match {
            case true =>
              // 200 OK, but errors response validator returned false
              logger.warn(s"$labelValue received $s but response body is not valid. " +
                s"host=${getHostnameValue(h)} data=${stringifyData(data)}")
            case _ => logger.warn(s"$labelValue error: host=${getHostnameValue(h)}" +
                s" status=$s data=${stringifyData(data)}")
          }

          count match {
            case Some(c) if c > 0 =>
              e.discardBytes()
              logger.debug(
                s"$labelValue received $s, count=$count will retry again in $delay host=${getHostnameValue(h)} data=${stringifyData(data)}"
              )
              val future =
                after(delay, system.scheduler)(Future.successful(data))
              Some(immutable.Seq(future -> state.copy(count = Some(c - 1))))
            case Some(0) =>
              logger.warn(
                s"$labelValue received $s, count=$count will not not retry sending request, host=${getHostnameValue(h)} data=${stringifyData(data)}"
              )
              badDataLogger.info(
                s"$labelValue data=${concatByteStrings(data, endl).utf8String}"
              )
              None
            case None =>
              e.discardBytes()
              logger.warn(
                s"$labelValue received $s, will retry again in $delay host=${getHostnameValue(h)} data=${stringifyData(data)}"
              )
              val future =
                after(delay, system.scheduler)(Future.successful(data))
              Some(immutable.Seq(future -> state))
          }

        case State(data, _, None, count) =>
          count match {
            case Some(c) if c > 0 =>
              logger.warn(
                s"$labelValue error: could not send http request, counter=$c will retry again in $delay data=${stringifyData(data)}"
              )
              val future =
                after(delay, system.scheduler)(Future.successful(data))
              Some(immutable.Seq(future -> state.copy(count = Some(c - 1))))
            case Some(0) =>
              logger.warn(
                s"$labelValue error: could not send http request, counter=0, will not retry sending request, data=${stringifyData(data)}"
              )
              badDataLogger.info(
                s"$labelValue data=${concatByteStrings(data, endl).utf8String}"
              )
              None
            case None =>
              logger.warn(
                s"$labelValue error: could not send http request, will retry again in $delay data=${stringifyData(data)}"
              )
              val future =
                after(delay, system.scheduler)(Future.successful(data))
              Some(immutable.Seq(future -> state))
          }
        case x =>
          logger.error(s"$labelValue unexpected message: $x")
          None
      }

//    val conn = Http().superPool[State]() // http connection flow
//    val conn = Http().newHostConnectionPool[State](host = baseUrl, port = port) // http connection flow
    val HttpAddress(protocol, host, port, uriPrefix) =
      ArgsManipulations.extractBaseUrl(baseUrl)
    val conn = HttpConnections.newHostConnectionPool[State](
      host,
      port,
      protocol
    ) // http connection flow
    val maxConnections =
      config.getInt("akka.http.host-connection-pool.max-connections")
    val httpPipelineSize =
      config.getInt("akka.http.host-connection-pool.pipelining-limit")
    val httpParallelism = maxConnections * httpPipelineSize

    val job = Flow[(Future[Seq[ByteString]], State)]
      .mapAsyncUnordered(httpParallelism) {
        case (data, state) => data.map(_ -> state)
      } // used for delay between executions
      .map { case (data, state) => createRequest(data) -> state }
      .via(conn)
      .mapAsyncUnordered(httpParallelism) {
        case (response @ Success(HttpResponse(s, _, e, _)), state)
            if !s.isSuccess() =>
          // consume HTTP response bytes
          e.discardBytes()
          logger.error(s"$labelValue status is not success ($s) $e")
          Future.successful(Failure(new Exception(s"status is not success ($s) $e")) -> state.copy(response = response.toOption))
        case (response @ Success(res @ HttpResponse(s, headers, e, _)), state) =>
          // consume HTTP response bytes and later pack them in fake response
          val responseAndState = (e match {
            case chunkedEntity: Chunked => {
              chunkedEntity.chunks.runFold[(ByteString, Option[Seq[HttpHeader]])]((ByteString.empty, None)) {
                case (accumulatedEntity, chunk: LastChunk) =>
                  (accumulatedEntity._1 ++ chunk.data, Option(chunk.trailer))
                case (accumulatedEntity, chunk) => (accumulatedEntity._1 ++ chunk.data, None)
              }
            }
            case entity: HttpEntity => {
              entity.withoutSizeLimit.dataBytes.runFold(blank)(_ ++ _).map(_ -> None)
            }
          }).map {
            case (entityBytes, trailerHeaders) =>
              val combinedHeaders = trailerHeaders.fold(headers)(headers ++ _)

              responseValidator(entityBytes, combinedHeaders) match {
                case Success(_) =>
                  Success(res.copy(entity = entityBytes, headers = combinedHeaders)) -> state.copy(
                    response = response.toOption
                  )
                case Failure(err) =>
                  logger.error(s"responseValidator function failed: ${err.getMessage}")
                  Failure(err) -> state.copy(response = response.toOption)
              }
          }

          responseAndState.recover {
            case err =>
              logger.error(
                s"$labelValue error in consuming data bytes from response, paths=${stringifyData(state.data)}, err=$err",
                err
              )
              Failure(err) -> state.copy(response = response.toOption)
          }

        case (res @ Failure(err), state) =>
          logger.error(s"$labelValue received failure", err);
          Future.successful(res -> state)

        case (x, state) =>
          logger.error(s"$labelValue unexpected message: $x")
          Future.successful(
            Failure(new Exception("unexpected message")) -> state
          )
      }

    Flow[(Seq[ByteString], Option[T])]
      .map {
        case (data, context) =>
          Future.successful(data) -> State(data = data, context = context)
      }
      .via(GoodRetry.concat(Long.MaxValue, job)(retryWith))
      .map { case (result, state) => (result, state.data, state.context) }
  }
}
