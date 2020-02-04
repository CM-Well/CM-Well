/**
  * © 2019 Refinitiv. All Rights Reserved.
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
package cmwell.tools.data.downloader.streams

import java.io.InputStream

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import cmwell.tools.data.downloader.DataPostProcessor
import cmwell.tools.data.utils.ArgsManipulations._
import cmwell.tools.data.utils.akka.{DataToolsConfig, HttpConnections, Retry, lineSeparatorFrame}
import cmwell.tools.data.utils.logging.DataToolsLogging
import org.slf4j.LoggerFactory
import cmwell.tools.data.utils.akka.{concatByteStrings, _}

import scala.concurrent.duration._
import scala.util._

/**
  * Downloads infotons from CM-Well
  */
object Downloader extends DataToolsLogging with DataToolsConfig {
  // separator of response uuids
  private val defaultSortAsc = false
  private val numInfotonsPerRequest = 25

  /**
    * Creates a [[akka.stream.scaladsl.Source]] which downloads data from target CM-Well
    *
    * @param baseUrl address of target cm-well
    * @param path path in cm-well
    * @param params cm-well url params
    * @param qp cm-well query params
    * @param format desired cm-well data format
    * @param op operation type (default = stream)
    * @param length max number of records to receive (e.g., 1, 10, all if set to `None`)
    * @param recursive true if need to get records in a recursive way
    * @param numInfotonsPerRequest how many infotons will be requested in each HTTP request (batching factor)
    * @param system actor system
    * @param mat akka stream materializer
    * @return [[akka.stream.scaladsl.Source Source]] which returns data chunks from cm-well
    * @see [[Downloader#createSourceFromQuery()]]
    */
  def createSourceFromQuery(
    baseUrl: String,
    path: String,
    params: String = "",
    qp: String = "",
    format: String = "trig",
    op: String = "stream",
    length: Option[Int] = Some(50),
    recursive: Boolean = false,
    numInfotonsPerRequest: Int = numInfotonsPerRequest
  )(implicit system: ActorSystem, mat: Materializer) = {

    val downloader = new Downloader(baseUrl = baseUrl,
                                    path = path,
                                    params = params,
                                    qp = qp,
                                    format = format,
                                    op = op,
                                    length = length,
                                    recursive = recursive,
                                    numInfotonsPerRequest)

    downloader.createSourceFromQuery()
  }

  /**
    * Downloads data from target CM-Well and apply the given outputHandler on each data chunk
    *
    * @param baseUrl address of target cm-well
    * @param path path in cm-well
    * @param params cm-well url params
    * @param qp cm-well query params
    * @param format desired cm-well data format
    * @param op operation type (default = stream)
    * @param length max number of records to receive (e.g., 1, 10, all if set to `None`)
    * @param recursive true if need to get records in a recursive way
    * @param numInfotonsPerRequest how many infotons will be requested in each HTTP request (batching factor)
    * @param outputHandler function which handles given data (e.g., write data to file)
    * @param system actor system
    * @param mat akka stream materializer
    * @return [[scala.concurrent.Future Future]] of download process
    * @see [[cmwell.tools.data.downloader.streams.Downloader#createSourceFromQuery]]
    */
  def downloadFromQuery(
    baseUrl: String,
    path: String,
    params: String = "",
    qp: String = "",
    format: String = "trig",
    op: String = "stream",
    length: Option[Int] = Some(50),
    recursive: Boolean = false,
    numInfotonsPerRequest: Int = numInfotonsPerRequest,
    outputHandler: (String) => Unit = (s: String) => ()
  )(implicit system: ActorSystem, mat: Materializer) = {

    createSourceFromQuery(baseUrl = baseUrl,
                          path = path,
                          params = params,
                          qp = qp,
                          format = format,
                          op = op,
                          length = length,
                          recursive = recursive,
                          numInfotonsPerRequest = numInfotonsPerRequest)
      .runForeach(data => outputHandler(data.utf8String))
  }

  /**
    * Downloads data from input stream containing uuids
    *
    * @param baseUrl address of target cm-well
    * @param format desired cm-well data format
    * @param numInfotonsPerRequest how many infotons will be requested in each HTTP request (batching factor)
    * @param outputHandler function which handles given data (e.g., write data to file)
    * @param in input stream containing uuids
    * @param system actor system
    * @param mat akka stream materializer
    * @return [[scala.concurrent.Future Future]] of download process
    * @see [[cmwell.tools.data.downloader.streams.Downloader#createSourceFromUuidInputStream]]
    */
  def downloadFromUuidInputStream(baseUrl: String,
                                  format: String = "trig",
                                  numInfotonsPerRequest: Int = numInfotonsPerRequest,
                                  outputHandler: (String) => Unit = (s: String) => (),
                                  in: InputStream)(implicit system: ActorSystem, mat: Materializer) = {

    createSourceFromUuidInputStream(baseUrl = baseUrl,
                                    format = format,
                                    numInfotonsPerRequest = numInfotonsPerRequest,
                                    in = in)
      .runForeach(data => outputHandler(data.utf8String))
  }

  /**
    * Creates a [[akka.stream.scaladsl.Source]] from uuid [[java.io.InputStream InputStream]] which downloads data from target CM-Well.
    *
    * @param baseUrl address of target cm-well
    * @param format desired cm-well data format
    * @param numInfotonsPerRequest how many infotons will be requested in each HTTP request (batching factor)
    * @param in input stream containing uuids
    * @param system actor system
    * @param mat akka stream materializer
    * @return [[akka.stream.scaladsl.Source Source]] which returns data chunks from cm-well
    */
  def createSourceFromUuidInputStream(baseUrl: String,
                                      format: String = "trig",
                                      numInfotonsPerRequest: Int = numInfotonsPerRequest,
                                      in: InputStream)(implicit system: ActorSystem, mat: Materializer) = {

    val source = StreamConverters
      .fromInputStream(() => in)
      .via(lineSeparatorFrame)

    createSourceFromUuids(
      baseUrl = baseUrl,
      format = format,
      numInfotonsPerRequest = numInfotonsPerRequest,
      source = source
    )
  }

  /**
    * Creates a [[akka.stream.scaladsl.Source Source]] from uuid [[akka.stream.scaladsl.Source Source]]
    * which downloads data from target CM-Well.
    *
    * @param baseUrl address of target cm-well
    * @param format desired cm-well data format
    * @param numInfotonsPerRequest how many infotons will be requested in each HTTP request (batching factor)
    * @param source [[akka.stream.scaladsl.Source]] which emits uuids elements
    * @param system actor system
    * @param mat akka stream materializer
    * @return [[akka.stream.scaladsl.Source Source]] which returns data chunks from cm-well
    */
  def createSourceFromUuids(baseUrl: String,
                            format: String = "trig",
                            numInfotonsPerRequest: Int = numInfotonsPerRequest,
                            source: Source[ByteString, _])(implicit system: ActorSystem, mat: Materializer) = {
    val downloader =
      new Downloader(baseUrl = baseUrl, path = "/", format = format, numInfotonsPerRequest = numInfotonsPerRequest)

    source
      .via(downloader.downloadDataFromUuids)
      .recover { case t => System.err.println(t); ByteString(t.toString) }
  }

  /**
    * Downloads data from input stream containing infoton paths
    *
    * @param baseUrl address of target cm-well
    * @param format desired cm-well data format
    * @param numInfotonsPerRequest how many infotons will be requested in each HTTP request (batching factor)
    * @param outputHandler function which handles given data (e.g., write data to file)
    * @param in input stream containing infoton paths
    * @param system actor system
    * @param mat akka stream materializer
    * @return [[scala.concurrent.Future Future]] of download process
    */
  def downloadFromPathsInputStream(baseUrl: String,
                                   format: String = "trig",
                                   numInfotonsPerRequest: Int = numInfotonsPerRequest,
                                   outputHandler: (String) => Unit = (s: String) => (),
                                   in: InputStream)(implicit system: ActorSystem, mat: Materializer) = {
    createSourceFromPathsInputStream(baseUrl = baseUrl,
                                     format = format,
                                     numInfotonsPerRequest = numInfotonsPerRequest,
                                     in = in)
      .runForeach(data => outputHandler(data.utf8String))
  }

  /**
    * Creates a [[akka.stream.scaladsl.Source Source]] from paths [[java.io.InputStream]]
    * which downloads data from target CM-Well.
    *
    * @param baseUrl address of target cm-well
    * @param format desired cm-well data format
    * @param numInfotonsPerRequest how many infotons will be requested in each HTTP request (batching factor)
    * @param in input stream containing infoton paths
    * @param system actor system
    * @param mat akka stream materializer
    * @return [[akka.stream.scaladsl.Source Source]] which returns data chunks from cm-well
    */
  def createSourceFromPathsInputStream(baseUrl: String,
                                       format: String = "trig",
                                       numInfotonsPerRequest: Int = numInfotonsPerRequest,
                                       in: InputStream)(implicit system: ActorSystem, mat: Materializer) = {
    val source = StreamConverters
      .fromInputStream(() => in)
      .via(lineSeparatorFrame)

    createSourceFromPaths(
      baseUrl = baseUrl,
      format = format,
      numInfotonsPerRequest = numInfotonsPerRequest,
      source = source
    )
  }

  /**
    * Creates a [[akka.stream.scaladsl.Source Source]] from uuid [[akka.stream.scaladsl.Source Source]]
    * which downloads data from target CM-Well.
    *
    * @param baseUrl address of target cm-well
    * @param format desired cm-well data format
    * @param params params in cm-well URI
    * @param numInfotonsPerRequest how many infotons will be requested in each HTTP request (batching factor)
    * @param source [[akka.stream.scaladsl.Source]] which emits infoton paths elements
    * @param system actor system
    * @param mat akka stream materializer
    * @return [[akka.stream.scaladsl.Source Source]] which returns data chunks from cm-well
    */
  def createSourceFromPaths(baseUrl: String,
                            format: String = "trig",
                            params: String = "",
                            numInfotonsPerRequest: Int = numInfotonsPerRequest,
                            source: Source[ByteString, _])(implicit system: ActorSystem, mat: Materializer) = {

    val downloader = new Downloader(baseUrl = baseUrl,
                                    path = "/",
                                    format = format,
                                    params = params,
                                    numInfotonsPerRequest = numInfotonsPerRequest)

    source
      .via(downloader.downloadDataFromPaths)
      .recover { case t => System.err.println(t); ByteString(t.toString) }
  }
}

class Downloader(baseUrl: String,
                 path: String,
                 params: String = "",
                 qp: String = "",
                 format: String = "trig",
                 op: String = "stream",
                 length: Option[Int] = Some(50),
                 recursive: Boolean = false,
                 numInfotonsPerRequest: Int = Downloader.numInfotonsPerRequest,
                 outputHandler: (String) => Unit = (s: String) => ())(implicit system: ActorSystem, mat: Materializer)
    extends DataToolsLogging {

  type Data = Seq[ByteString]
  import Downloader._

  private[streams] var retryTimeout = {
    val timeoutDuration = Duration(config.getString("cmwell.downloader.streams.http-retry-timeout")).toCoarsest
    FiniteDuration(timeoutDuration.length, timeoutDuration.unit)
  }
  private val badUuidsLogger = LoggerFactory.getLogger("bad-uuids")

  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global //system.dispatcher

  private val bufferSize = config.getInt("akka.http.host-connection-pool.max-connections")

  private val HttpAddress(protocol, host, port, uriPrefix) = extractBaseUrl(baseUrl)

  /**
    * Creates a flow for downloading infoton data from uuid strings
    *
    * @return flow that gets uuids and download their data
    */
  def downloadDataFromUuids() = {
    def createDataRequest(uuids: Seq[ByteString], vars: Map[String,String], context: Option[String]) = {
      HttpRequest(
        uri = s"${formatHost(baseUrl)}/_out?format=$format",
        method = HttpMethods.POST,
        entity = uuids.map(_.utf8String).mkString("/ii/", "\n/ii/", "")
      )
    }

    Flow[ByteString]
      .grouped(numInfotonsPerRequest)
      .buffer(bufferSize, OverflowStrategy.backpressure)
      .map(uuids => uuids -> (None : Option[String]))
      .via(Retry.retryHttp(retryTimeout, bufferSize)(createDataRequest))
      .flatMapConcat {
        case (Success(HttpResponse(s, h, e, p)), _, _) if s.isSuccess() =>
          DataPostProcessor.postProcessByFormat(format, e.withoutSizeLimit().dataBytes)
        case (Success(res @ HttpResponse(s, h, e, p)), uuids, _) =>
          // entity databytes were discarded in job flow
          logger.error(s"error: status=$s")
          badUuidsLogger.error(s"uuids: ${uuids.map(_.utf8String).mkString("\n")}")
          Source.empty
        case (Failure(err), uuids, _) =>
          logger.error(s"cannot download data from uuids", err)
          Source.empty
      }
  }

  /**
    * Creates [[akka.stream.scaladsl.Source Source]] which downloads data from CM-Well:
    *  - query cm-well and receive uuids
    *  - download infotons' data of previous uuids in requested format
    *  - apply given output handler on received infoton data
    *
    * @return [[akka.stream.scaladsl.Source Source]] of download process which emits data chunks
    */
  def createSourceFromQuery() = {

    /**
      * Creates an http query request for getting uuids according to given parameters
      *
      * @return HttpRequest for cm-well querying for infotons
      */
    def createQueryRequest(): HttpRequest = {
      val qpValue = if (qp.isEmpty) "" else s"&qp=$qp"
      val paramsValue = if (params.isEmpty) "" else s"&$params"

      val recursiveValue = if (recursive) "&recursive" else ""
      val lengthValue = if (length.isDefined) s"&length=${length.get}" else ""

      val uri = s"${formatHost(baseUrl)}$path?op=$op&format=tsv$qpValue$paramsValue$recursiveValue$lengthValue"

      logger.debug(s"query request: GET $uri")

      HttpRequest(uri = uri)
    }

    def tsvToUuid(tsv: ByteString) = {
      tsv
        .dropWhile(_ != '\t')
        .drop(1) // path
        .dropWhile(_ != '\t')
        .drop(1) // lastModified
        .takeWhile(_ != '\t') // uuid
    }

    val conn = HttpConnections.outgoingConnection(host, port, protocol)

    Source
      .single(createQueryRequest())
      .via(conn)
      .flatMapConcat {
        case res @ HttpResponse(s, h, e, p) if s.isSuccess() =>
          e.withoutSizeLimit()
            .dataBytes
            .via(lineSeparatorFrame)
            .buffer(1000, OverflowStrategy.backpressure)
            .map(tsvToUuid)
            .filter(_.nonEmpty)

        case res @ HttpResponse(s, _, e, _) =>
          res.discardEntityBytes()
          logger.error(s"error in getting uuids: status=$s")
          Source.empty[ByteString]
      }
      .buffer(1000, OverflowStrategy.backpressure)
//      .recover{case t => System.err.println(t); t.toString }
      .via(downloadDataFromUuids)
//      .recover{case t => System.err.println(t); ByteString(t.toString) }
  }

  /**
    * Creates a flow for downloading infoton data from infoton path strings
    *
    * @return flow that gets paths and download their data
    */
  def downloadDataFromPaths() = {
    def createDataRequest(paths: Seq[ByteString], vars: Map[String,String], ctx: Option[String] = None) = {
      val paramsValue = if (params.isEmpty) "" else s"&$params"

      HttpRequest(
        uri = s"${formatHost(baseUrl)}/_out?format=$format$paramsValue",
        method = HttpMethods.POST,
        entity = concatByteStrings(paths, endl).utf8String
      )
    }

    Flow[ByteString]
      .groupedWithin(numInfotonsPerRequest, 3.seconds)
      .map(paths => paths -> (None : Option[String]))
      .via(Retry.retryHttp(retryTimeout, bufferSize)(createDataRequest))
      .flatMapConcat {
        case (Success(HttpResponse(s, h, e, p)), _, _) if s.isSuccess() =>
          DataPostProcessor.postProcessByFormat(format, e.withoutSizeLimit().dataBytes)

        case (Success(res @ HttpResponse(s, h, e, p)), paths, _) =>
          res.discardEntityBytes()

          logger.error(s"error: status=$s")
          badUuidsLogger.error(s"paths: ${paths.map(_.utf8String).mkString("\n")}")
          Source.empty

        case (Failure(err), paths, _) =>
          logger.error("error: cannot process data response", err)
          badUuidsLogger.error(s"paths: ${paths.map(_.utf8String).mkString("\n")}")
          Source.empty
      }
  }
}
