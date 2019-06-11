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

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpHeader, HttpMethods, HttpRequest, HttpResponse}
import akka.stream.Materializer
import akka.stream.scaladsl._
import akka.util._
import cmwell.tools.data.downloader.DataPostProcessor
import cmwell.tools.data.downloader.consumer._
import cmwell.tools.data.utils.ArgsManipulations._
import cmwell.tools.data.utils.akka.HeaderOps.getHostnameValue
import cmwell.tools.data.utils.akka.Retry.State
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import cmwell.tools.data.utils.logging.DataToolsLogging

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

case class StpMetadata (agentConfig: Config, infotonTokenReporter: ActorRef)

object SparqlProcessor extends DataToolsLogging with DataToolsConfig {

  val format = "ntriples"

  case class SparqlRuntimeConfig(hostUpdatesSource: String,
                                 useQuadsInSp: Boolean,
                                 label: Option[String],
                                 sparqlMaterializer: String)

  def materializedStatsFlow(tokenReporterOpt: Option[ActorRef], format: String, sensorName: String,  initialDownloadStatsOpt: Option[DownloadStats] = None) = {

    import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats._

    type DownloadElement = (ByteString, Option[StpMetadata])
    val emptyElement: DownloadElement = (ByteString.empty , None)
    val start = System.currentTimeMillis

    def markStats(elem: DownloadElement, downloadStats: DownloadStats) = {
      val now = System.currentTimeMillis()
      val executionTime = now - start

      DownloadStats(receivedInfotons =
          downloadStats.receivedInfotons + countInfotonsInChunk(elem, format),
        statsTime = System.currentTimeMillis(),
        runningTime = executionTime,
        totalRunningTime = downloadStats.totalRunningTime + (executionTime-downloadStats.runningTime),
        receivedBytes = downloadStats.receivedBytes + bytesRead(elem),
        label = downloadStats.label)
    }

    Flow[DownloadElement]
      .scan(initialStats(initialDownloadStatsOpt, sensorName, emptyElement))((stats, elem) =>
        (markStats(elem, stats._1), elem))
      .groupedWithin(30,5.seconds)
      .map(collatedPaths => {
       collatedPaths.last match {
          case (downloadStats, _) =>
            tokenReporterOpt.foreach { _ ! downloadStats }
        }
        collatedPaths
      })
      .mapConcat(_.map(_._2))
      .filter { case(bytes,_) => bytes.nonEmpty }
  }

  def createSourceFromQuery(
                             baseUrl: String,
                             path: String,
                             qp: String,
                             spQueryParamsBuilder: (Seq[String] , Map[String,String], Boolean) => String = (_,_,_) => "",
                             parallelism: Int = 4,
                             isNeedWrapping: Boolean = true,
                             indexTime: Long = 0L,
                             isBulk: Boolean = false,
                             sparqlQuery: String,
                             label: Option[String] = None
                           )(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    val source = Downloader
      .createTsvSource(
        baseUrl = baseUrl,
        path = path,
        qp = qp,
        indexTime = indexTime,
        isBulk = isBulk,
        label = label
      )
      .map { case ((token, tsv), _, _) => tsv.path -> Some(token) }

    createSparqlSourceFromPaths(
      baseUrl = baseUrl,
      spQueryParamsBuilder = spQueryParamsBuilder,
      parallelism = parallelism,
      sparqlQuery = sparqlQuery,
      isNeedWrapping = isNeedWrapping,
      source = source.map {
        s =>  (s._1, Map.empty[String,String]) -> s._2
      },
      label = label
    )
  }

  def createSourceFromToken(
                             baseUrl: String,
                             spQueryParamsBuilder: (Seq[String] , Map[String,String], Boolean) => String = (_,_,_) => "",
                             parallelism: Int = 4,
                             isNeedWrapping: Boolean = true,
                             token: String,
                             isBulk: Boolean = false,
                             sparqlQuery: String,
                             label: Option[String] = None
                           )(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {
    val source = Downloader
      .createTsvSource(
        baseUrl = baseUrl,
        isBulk = isBulk,
        token = Some(token),
        label = label
      )
      .map { case ((token, tsv), _, _) => tsv.path -> Some(token) }

    createSparqlSourceFromPaths(
      baseUrl = baseUrl,
      spQueryParamsBuilder = spQueryParamsBuilder,
      parallelism = parallelism,
      isNeedWrapping = isNeedWrapping,
      sparqlQuery = sparqlQuery,
      source = source.map {
        s =>  (s._1, Map.empty[String,String]) -> s._2
      },
      label = label
    )
  }

  def createSourceFromPathsInputStream(
                                        baseUrl: String,
                                        spQueryParamsBuilder: (Seq[String] , Map[String,String], Boolean) => String = (_,_,_) => "",
                                        parallelism: Int = 4,
                                        isNeedWrapping: Boolean = true,
                                        sparqlQuery: String,
                                        in: InputStream,
                                        label: Option[String] = None
                                      )(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {
    val source = StreamConverters
      .fromInputStream(() => in)
      .via(lineSeparatorFrame)
      .map(line => line -> None)

    createSparqlSourceFromPaths(
      baseUrl = baseUrl,
      spQueryParamsBuilder = spQueryParamsBuilder,
      parallelism = parallelism,
      sparqlQuery = sparqlQuery,
      isNeedWrapping = isNeedWrapping,
      source = source.map {
        s =>  (s._1, Map.empty[String,String]) -> s._2
      },
      label = label
    )
  }



  def sparqlSourceFromPathsFlow[T](baseUrl: String,
                                   spQueryParamsBuilder: (Seq[String] , Map[String,String], Boolean) => String = (_,_,_) => "",
                                   format: Option[String] = None,
                                   parallelism: Int = 4,
                                   isNeedWrapping: Boolean = true,
                                   sparqlQuery: String,
                                   //source: Source[((ByteString, Map[String,String]), Option[T]), _],
                                   label: Option[String] = None
                                  )(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    def extractSparqlContext[T](context: Option[(T, Long)]) = {
      SparqlRuntimeConfig(baseUrl, false, label, sparqlQuery)
    }

    Flow[((ByteString, Map[String,String]), Option[T])].map { case ((path,vars), context) => ( Seq(path), vars) -> (context,1L) }
      .via(balancer(SparqlProcessor.sparqlFlow(
        extractContext =  extractSparqlContext,
        parallelism = parallelism,
        format=format,
        isNeedWrapping = isNeedWrapping,
        spQueryParamsBuilder = spQueryParamsBuilder), parallelism))
      .map{
        case (data,(context,_)) => (data -> context)
      }

  }



  def createSparqlSourceFromPaths[T](
                                      baseUrl: String,
                                      spQueryParamsBuilder: (Seq[String] , Map[String,String], Boolean) => String = (_,_,_) => "",
                                      format: Option[String] = None,
                                      parallelism: Int = 4,
                                      isNeedWrapping: Boolean = true,
                                      sparqlQuery: String,
                                      source: Source[((ByteString, Map[String,String]), Option[T]), _],
                                      label: Option[String] = None
                                    )(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    def extractSparqlContext[T](context: Option[(T, Long)]) = {
      SparqlRuntimeConfig(baseUrl, false, label, sparqlQuery)
    }

    source.map { case ((path,vars), context) => ( Seq(path), vars) -> (context,1L) }
        .via(balancer(SparqlProcessor.sparqlFlow(
          extractContext =  extractSparqlContext,
          parallelism = parallelism,
          format=format,
          isNeedWrapping = isNeedWrapping,
          spQueryParamsBuilder = spQueryParamsBuilder), parallelism))
      .map{
        case (data,(context,_)) => (data -> context)
      }
  }

  type Paths = Seq[ByteString]
  type StartTime = Long

  val maxConnections = config.getInt("akka.http.host-connection-pool.max-connections")
  val httpPipelineSize = config.getInt("akka.http.host-connection-pool.pipelining-limit")
  val httpParallelism =  config.getInt("cmwell.sparql.http-parallelisation")

  val retryCountLimit = config.hasPath("cmwell.sparql.http-retry-count") match {
    case true => Some(config.getInt("cmwell.sparql.http-retry-count"))
    case false => None
  }

  private[sparql] var retryTimeout = {
    val timeoutDuration = Duration(config.getString("cmwell.sparql.http-retry-timeout")).toCoarsest
    FiniteDuration(timeoutDuration.length, timeoutDuration.unit)
  }

  def validateResponse(body: ByteString, headers: Seq[HttpHeader]): Try[Unit] = {
    def validateBody(body: ByteString) = {
      if (!(body containsSlice "Could not process request")) None
      else Some("[Response body was not valid]")
    }

    def validateHeaders(headers: Seq[HttpHeader], errorHeader: String) = {
      headers.find(_.name == errorHeader).map(header=>Some(s"[${header.name} returned errors: ${header.value}]"))
    }

    val errors = List(validateBody(body), validateHeaders(headers, "X-CM-WELL-SG-RS")).flatten

    if (errors.isEmpty)
      Success(Unit)
    else
      Failure(new Exception(errors.mkString("Failures in Http Response:", " ", "")))
  }

  def sparqlFlow[T](extractContext: Option[(T,Long)] => SparqlRuntimeConfig,
                    connectionPool: Option[Flow[(HttpRequest,State[(T,Long)]), (Try[HttpResponse], State[(T,Long)]), Http.HostConnectionPool]] = None,
                    parallelism : Int = 4,
                    format: Option[String] = None,
                    isNeedWrapping: Boolean = false,
                    spQueryParamsBuilder: (Seq[String] , Map[String,String], Boolean) => String = (_,_,_) => "")
                (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext)  = {

    def createRequest(paths: Paths, vars: Map[String,String], context: Option[(T,Long)]) = {

      val SparqlRuntimeConfig(baseUrl, useQuadsInSp, label, sparqlQuery) = extractContext(context)

      val formatValue = format.map(f => s"&format=$f").getOrElse("")
      val uri = s"${formatHost(baseUrl)}/_sp?${spQueryParamsBuilder(paths.map(_.utf8String), vars, useQuadsInSp)}$formatValue"

      logger.debug(s"$label send HTTP sparql request: {}", uri)

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

      HttpRequest(
        uri = uri,
        method = HttpMethods.POST,
        entity = body
      )
    }

    Flow[((Seq[ByteString], Map[String, String]), T)].map {
      case (pathAndVars, context) => pathAndVars -> Some(context -> System.currentTimeMillis)
    }
    .via(Retry.retryHttp(retryTimeout, parallelism, retryCountLimit, managedConnection = connectionPool)(createRequest, validateResponse))
    .map {
      case (Success(HttpResponse(s, h, e, p)), paths, contextAndStartTime) if s.isSuccess() =>
        logger.debug(s"host=${getHostnameValue(h)} received sparql response for paths: {}",
          concatByteStrings(paths, ByteString(",")).utf8String)

        val f = DataPostProcessor
          .postProcessByFormat(SparqlProcessor.format, e.withoutSizeLimit().dataBytes)
          .via(lineSeparatorFrame)
          .filter(_.nonEmpty)

        contextAndStartTime -> f
      case (Success(HttpResponse(s, h, e, _)), paths, contextAndStartTime) =>
        e.discardBytes()
        redLogger.error(
          s"host=${getHostnameValue(h)} error: status=$s entity=$e paths=${paths.map(_.utf8String).mkString(",")}"
        )
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
        logger.debug("received response from sparql and startTime={}", startTime)
        Future.successful(dataLines -> context)
    }
    .mapAsyncUnordered(httpParallelism) {
      case (dataLines, context) =>
        dataLines.runFold(blank)(_ ++ _ ++ endl).map(_.trim() -> context)
    }
    .filter { case (data, _) => data.nonEmpty }
  }

}



