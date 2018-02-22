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


package cmwell.tools.data.sparql

import java.io.InputStream

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.stream.Materializer
import akka.stream.scaladsl._
import akka.util._
import cmwell.tools.data.downloader.DataPostProcessor
import cmwell.tools.data.downloader.consumer._
import cmwell.tools.data.utils.ArgsManipulations._
import cmwell.tools.data.utils.akka.{Retry, _}
import cmwell.tools.data.utils.logging.{DataToolsLogging, LabelId}
import cmwell.tools.data.utils.ops.VersionChecker

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object SparqlProcessor extends DataToolsLogging with DataToolsConfig{
  val format = "ntriples"

  def createSourceFromQuery(baseUrl: String,
                            path: String,
                            qp: String,
                            spQueryParamsBuilder: Seq[String] => String = _ => "",
                            parallelism: Int = 4,
                            isNeedWrapping: Boolean = true,
                            indexTime: Long = 0L,
                            isBulk: Boolean = false,
                            sparqlQuery: String,
                            label: Option[String] = None)
                           (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    val source = Downloader.createTsvSource(
      baseUrl = baseUrl,
      path   = path,
      qp     = qp,
      indexTime = indexTime,
      isBulk = isBulk,
      label = label
    ).map{ case (token, tsv) => tsv.path -> Some(token) }

    createSparqlSourceFromPaths(
      baseUrl = baseUrl,
      spQueryParamsBuilder = spQueryParamsBuilder,
      parallelism = parallelism,
      sparqlQuery = sparqlQuery,
      isNeedWrapping = isNeedWrapping,
      source = source,
      label = label
    )
  }

  def createSourceFromToken(baseUrl: String,
                            spQueryParamsBuilder: Seq[String] => String = _ => "",
                            parallelism: Int = 4,
                            isNeedWrapping: Boolean = true,
                            token: String,
                            isBulk: Boolean = false,
                            sparqlQuery: String,
                            label: Option[String] = None)
                           (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {
    val source = Downloader.createTsvSource(
      baseUrl = baseUrl,
      isBulk  = isBulk,
      token = Some(token),
      label = label
    ).map { case (token, tsv) => tsv.path -> Some(token)}

    createSparqlSourceFromPaths(
      baseUrl = baseUrl,
      spQueryParamsBuilder = spQueryParamsBuilder,
      parallelism = parallelism,
      isNeedWrapping = isNeedWrapping,
      sparqlQuery = sparqlQuery,
      source = source,
      label = label
    )
  }

  def createSourceFromPathsInputStream(baseUrl: String,
                                       spQueryParamsBuilder: Seq[String] => String = _ => "",
                                       parallelism: Int = 4,
                                       isNeedWrapping: Boolean = true,
                                       sparqlQuery: String,
                                       in: InputStream,
                                       label: Option[String] = None)
                                      (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {
    val source = StreamConverters.fromInputStream(() => in)
      .via(lineSeparatorFrame)
      .map(line => line -> None)

    createSparqlSourceFromPaths(
      baseUrl = baseUrl,
      spQueryParamsBuilder = spQueryParamsBuilder,
      parallelism = parallelism,
      sparqlQuery = sparqlQuery,
      isNeedWrapping = isNeedWrapping,
      source = source,
      label = label)
  }

  def createSparqlSourceFromPaths[T](baseUrl: String,
                                  spQueryParamsBuilder: Seq[String] => String = _ => "",
                                  format: Option[String] = None,
                                   parallelism: Int = 4,
                                  isNeedWrapping: Boolean = true,
                                  sparqlQuery: String,
                                  source: Source[(ByteString, Option[T]), _],
                                  label: Option[String] = None)
                                 (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    new SparqlProcessor(
      baseUrl = baseUrl,
      spQueryParamsBuilder = spQueryParamsBuilder,
      parallelism = parallelism,
      sparqlQuery = sparqlQuery,
      isNeedWrapping = isNeedWrapping,
      format = format,
      source = source,
      label = label
    ).createSource()
  }
}

class SparqlProcessor[T](baseUrl: String,
                      spQueryParamsBuilder: Seq[String] => String = _ => "",
                      parallelism: Int = 4,
                      isNeedWrapping: Boolean = true,
                      sparqlQuery: String,
                      format: Option[String] = None,
                      source: Source[(ByteString, Option[T]), _],
                      override val label: Option[String] = None
                     ) extends DataToolsLogging {

  import SparqlProcessor._

  val maxConnections = config.getInt("akka.http.host-connection-pool.max-connections")
  val httpPipelineSize = config.getInt("akka.http.host-connection-pool.pipelining-limit")
  val httpParallelism = maxConnections * httpPipelineSize

  private [sparql] var retryTimeout = {
    val timeoutDuration = Duration(config.getString("cmwell.sparql.http-retry-timeout")).toCoarsest
    FiniteDuration( timeoutDuration.length, timeoutDuration.unit )
  }

  def createSource()(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {
    type Paths = Seq[ByteString]
    type StartTime = Long

    def validateResponseBody(body: ByteString) = {
      !(body containsSlice "Could not process request")
    }

    def sparqlFlow() = {
      def createRequest(paths: Paths) = {
        val body = if (isNeedWrapping) {
          s"""
             |PATHS
             |${paths.map(_.utf8String).mkString("\n")}
             |
             |SPARQL
             |$sparqlQuery
           """.stripMargin
        } else {
          sparqlQuery
        }

        val formatValue = format.map(f => s"&format=$f") getOrElse ""

        val uri = s"${formatHost(baseUrl)}/_sp?${spQueryParamsBuilder(paths.map(_.utf8String))}$formatValue"

        logger.debug("send HTTP sparql request: {}", uri)

        HttpRequest(
          uri = uri,
          method = HttpMethods.POST,
          entity = body
        )
      }

      implicit val labelToRetry = label.map(LabelId.apply)
      import cmwell.tools.data.utils.akka.HeaderOps._

      Flow[(Seq[ByteString], Option[T])]
        .map { case (data, context) =>
          val startTime = System.currentTimeMillis
          data -> Some(context -> startTime)
        }
        .via(Retry.retryHttp(retryTimeout, parallelism, baseUrl)(createRequest, validateResponseBody))
        .map {
          case (Success(HttpResponse(s,h,e,p)), paths, contextAndStartTime) if s.isSuccess() =>
            logger.debug(s"host=${getHostnameValue(h)} received sparql response for paths: {}", concatByteStrings(paths, ByteString(",")).utf8String)

            val f = DataPostProcessor.postProcessByFormat(SparqlProcessor.format, e.withoutSizeLimit().dataBytes)
              .via(lineSeparatorFrame)
              .filter(_.nonEmpty)

            contextAndStartTime -> f
          case (Success(HttpResponse(s,h,e,_)), paths, contextAndStartTime) =>
            e.discardBytes()
            redLogger.error(s"host=${getHostnameValue(h)} error: status=$s entity=$e paths=${paths.map(_.utf8String).mkString(",")}")

            contextAndStartTime -> Source.empty
          case (Failure(err), paths, contextAndStartTime) =>
            redLogger.error(s"error: $err paths=${paths.map(_.utf8String).mkString(",")}", err)
            contextAndStartTime -> Source.empty

          case x =>
            logger.error(s"unexpected message: $x")
            None -> Source.empty
        }
        .mapAsyncUnordered(httpParallelism) {
          case (Some((context, startTime)), dataLines) =>
//            val delayTime = FiniteDuration(System.currentTimeMillis - startTime, TimeUnit.MILLISECONDS)
//            logger.info(s"duty cycle will emit the response in $delayTime")
//            akka.pattern.after(delayTime, system.scheduler)(Future.successful(dataLines -> context))
            logger.debug("received response from sparql and startTime={}", startTime)
            Future.successful(dataLines -> context)

          case x =>
            logger.info(s"unexpected message: $x")
            Future.successful(Source.empty -> None)
        }
        .mapAsyncUnordered(httpParallelism) {case (dataLines, context) =>
          dataLines.runFold(blank)(_ ++ _ ++ endl).map(_.trim() -> context)
        }
        .filter { case (data, context) => data.nonEmpty}
//        .flatMapConcat {case (dataLines, context) => dataLines.map(_ -> context)}
    }

    source//.async
      .map {case (path, context) => Seq(path) -> context}
//      .via(sparqlFlow())
      .via(balancer(sparqlFlow(), parallelism))
  }
}
