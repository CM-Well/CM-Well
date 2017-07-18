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


package cmwell.tools.data.downloader.consumer


import akka.NotUsed
import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.model._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import cmwell.tools.data.downloader.DataPostProcessor
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.ArgsManipulations.{HttpAddress, _}
import cmwell.tools.data.utils.akka.HeaderOps._
import cmwell.tools.data.utils.akka.{concatByteStrings, _}
import cmwell.tools.data.utils.logging._
import cmwell.tools.data.utils.ops.VersionChecker
import cmwell.tools.data.utils.text.Tokens
import play.api.libs.json.{JsArray, Json}

import scala.collection.immutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Consumer-based CM-Well downloader
  */
object Downloader extends DataToolsLogging with DataToolsConfig{
  private val PATH_INDEX = 0
  private val UUID_INDEX = 2
  private val numUuidsPerRequest = 25
  private val bufferSize = config.getInt("akka.http.host-connection-pool.max-connections")

  type Token = String
  type Uuid  = ByteString
  type Path  = ByteString

  sealed class Tsv
  case class TsvData(path: Path, uuid: Uuid) extends Tsv
  case object TsvEmpty extends Tsv

  type TokenAndTsv = (Token, TsvData)

  /**
    * Creates token (position) from given query
    *
    * @param baseUrl address of target cm-well
    * @param path path in cm-well
    * @param params params in cm-well URI
    * @param qp query params in cm-well
    * @param recursive is query recursive
    * @param length cm-well length-hint paramter
    * @param system actor system
    * @param mat materializer
    * @param ec execution context
    * @return cm-well token (position) from given query parameters
    */
  def getToken(baseUrl: String,
               path: String = "/",
               params: String = "",
               qp: String = "",
               recursive: Boolean = false,
               length: Option[Int] = None,
               indexTime: Long = 0L,
               isBulk: Boolean)
              (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext): Future[Token] = {
    val qpValue = if (qp.isEmpty) "" else s"&qp=$qp"
    val paramsValue = if (params.isEmpty) "" else s"&$params"
    val recursiveValue = if (recursive) "&recursive" else ""

    val lengthValue = if (length.isEmpty || isBulk) "" else s"&length=${length.get}"
    if (length.nonEmpty && isBulk) {
      logger.warn("length-hint and bulk mode cannot be both enabled: length was set to default value")
    }

    val uri = s"${formatHost(baseUrl)}$path?op=create-consumer$qpValue$paramsValue$recursiveValue$lengthValue&index-time=$indexTime"
    logger.debug("send HTTP request: {}", uri)

    val tokenFuture = Source.single(Seq(blank) -> None)
      .via(Retry.retryHttp(5.seconds, 1, formatHost(baseUrl))(_ => HttpRequest(uri =uri)))
      .map {
        case (Success(HttpResponse(s, h, e, _)), _, _) if s.isSuccess() =>
          e.discardBytes()
          HeaderOps.getPosition(h)
        case (Success(res), _, _) =>
          res.discardEntityBytes()
          None
        case _ =>
          None
      }
      .map {
        case Some(positionHeader) => positionHeader.value
        case None =>
          logger.error("cannot get initial token: {}", uri)
          redLogger.error("cannot receive initial token from query: {}", uri)
          throw new Exception("cannot continue without initial token")
      }.runWith(Sink.head)

    tokenFuture
  }

  /**
    * Extracts TSV data from input [[akka.util.ByteString ByteString]]
    *
    * @param bytes input ByteString
    * @return extracted tsv data
    */
  def extractTsv(bytes: ByteString) = {
    val arr = bytes.split('\t')

    TsvData(path = arr(PATH_INDEX), uuid = arr(UUID_INDEX))
  }

  /**
    * Gets data from given token (position)
    *
    * @param baseUrl address of target cm-well
    * @param path path in cm-well
    * @param params params in cm-well URI
    * @param qp query params in cm-well
    * @param format output data format
    * @param recursive is query recursive
    * @param isBulk indicates whether use bulk consumer API
    * @param token input token containing position in consumer API (otherwise, None)
    * @param system actor system
    * @param mat materializer
    * @param ec execution context
    * @return future of next token (position) and received data
    */
  def getChunkData(baseUrl: String,
                   path: String = "/",
                   params: String = "",
                   qp: String = "",
                   format: String = "trig",
                   recursive: Boolean = false,
                   isBulk: Boolean = false,
                   token: Option[String] = None)
                  (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    // check if need to create initial token
    val tokenFuture = token match {
      case Some(t) => Future.successful(t)
      case None => getToken(
        baseUrl = baseUrl,
        path = path,
        params = params,
        qp = qp,
        recursive = recursive,
        isBulk = isBulk)
    }

    val downloader = new Downloader(
      baseUrl = baseUrl,
      path = path,
      params = params,
      qp = qp,
      format = format,
      recursive = recursive,
      isBulk = isBulk
    )

    tokenFuture.flatMap{ tokenValue => downloader.getChunkData(tokenValue) }
  }

  /**
    * Gets paths from given token (position)
    *
    * @param baseUrl address of target cm-well
    * @param path path in cm-well
    * @param params params in cm-well URI
    * @param qp query params in cm-well
    * @param recursive is query recursive
    * @param isBulk indicates whether use bulk consumer API
    * @param token input token containing position in consumer API (otherwise, None)
    * @param system actor system
    * @param mat materializer
    * @param ec execution context
    * @return future of next token (position) and received paths
    */
  def getChunkPaths(baseUrl: String,
                    path: String = "/",
                    params: String = "",
                    qp: String = "",
                    recursive: Boolean = false,
                    isBulk: Boolean = false,
                    token: Option[String] = None)
                   (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    // check if need to create initial token
    val tokenFuture = token match {
      case Some(t) => Future.successful(t)
      case None => getToken(
        baseUrl = baseUrl,
        path = path,
        params = params,
        qp = qp,
        recursive = recursive,
        isBulk = isBulk)
    }

    val downloader = new Downloader(
      baseUrl = baseUrl,
      path = path,
      params = params,
      qp = qp,
      isBulk = isBulk,
      recursive = recursive)

    tokenFuture.flatMap { tokenValue =>
      downloader.getTsvFromToken(tokenValue).flatMap { case (nextToken, src) =>
        if (nextToken == tokenValue) {
          Future.successful(nextToken -> Seq.empty[String])
        } else {
          src
            .map { tsv => nextToken -> tsv.path }
            .fold("" -> Seq.empty[ByteString]) { case ((oldToken, paths), (newToken, path)) =>
              newToken -> (paths :+ path)
            }
            .mapConcat { case (token, paths) => paths.grouped(numUuidsPerRequest)
              .map(path => (token, path))
              .toStream
            }
            .map { case (token, paths) => token -> paths.map(_.utf8String) }
            .runWith(Sink.head)
        }
      }
    }
  }

  /**
    * Creates [[akka.stream.scaladsl.Source Source]] of TSVs, starting from given token
    * @param baseUrl address of target cm-well
    * @param path path in cm-well
    * @param params params in cm-well URI
    * @param qp query params in cm-well
    * @param recursive is query recursive
    * @param isBulk indicates whether use bulk consumer API
    * @param token input token containing position in consumer API (otherwise, None)
    * @param updateFreq indicates when to check for updates when no data is available to consume.
    *               [[None]] will not check for updates and complete execution.
    * @param system actor system
    * @param mat materializer
    * @param ec execution context
    * @return [[akka.stream.scaladsl.Source Source]] of token (position) and its corresponding TSVs
    */
  def createTsvSource(baseUrl: String,
                      path: String = "/",
                      params: String = "",
                      qp: String = "",
                      recursive: Boolean = false,
                      isBulk: Boolean = false,
                      token: Option[String] = None,
                      updateFreq: Option[FiniteDuration] = None,
                      indexTime: Long = 0L,
                      label: Option[String] = None)
                     (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext): Source[(Token, TsvData), NotUsed] = {
    val downloader = new Downloader(
      baseUrl = baseUrl,
      path = path,
      params = params,
      qp = qp,
      isBulk = isBulk,
      indexTime = indexTime,
      recursive = recursive,
      label = label)

    downloader.createTsvSource(token, updateFreq)
  }

  /**
    * Creates a [[akka.stream.scaladsl.Source Source]] from uuid [[akka.stream.scaladsl.Source Source]]
    * which downloads data from target CM-Well.
    * @param baseUrl address of target cm-well
    * @param format desired cm-well data format
    * @param path path in cm-well
    * @param params params in cm-well URI
    * @param qp query params in cm-well
    * @param recursive is query recursive
    * @param isBulk indicates whether use bulk consumer API
    * @param token input token which contains start position
    * @param usePaths internally use infoton paths instead of uuids when downloading data
    * @param system actor system
    * @param mat akka stream materializer
    * @param ec execution context
    * @return [[akka.stream.scaladsl.Source Source]] which returns pairs of token and data chunks from cm-well
    */
  def createDataSource(baseUrl: String,
                       path: String = "/",
                       params: String = "",
                       qp: String = "",
                       format: String = "trig",
                       recursive: Boolean = false,
                       isBulk: Boolean = false,
                       token: Option[String] = None,
                       usePaths: Boolean = false)
                      (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    val downloader = new Downloader(
      baseUrl = baseUrl,
      path = path,
      params = params,
      format = format,
      qp = qp,
      isBulk = isBulk,
      recursive = recursive)


    val tsvSource = downloader.createTsvSource(token).async

    if (usePaths) {
      tsvSource
        .map {case (token, tsv) => token -> tsv.path }
        .via(downloader.downloadDataFromPaths).async
    } else {
      tsvSource
        .map {case (token, tsv) => token -> tsv.uuid }
        .via(downloader.downloadDataFromUuids).async
    }
  }
}

sealed trait DownloadError
case class DownloadHttpError(token: String, httpStatus: StatusCode) extends DownloadError
case class DownloadException(token: String, ex: Throwable) extends DownloadError

class Downloader(baseUrl: String,
                 path: String = "/",
                 params: String = "",
                 qp: String = "",
                 format: String = "trig",
                 length: Option[Int] = None,
                 recursive: Boolean = false,
                 isBulk: Boolean = false,
                 indexTime: Long = 0L,
                 override val label: Option[String] = None)
                (implicit system: ActorSystem, mat: Materializer) extends DataToolsLogging {
  import Downloader._

  implicit val labelId = label.map(LabelId.apply)

  private [Downloader] val retryTimeout = {
    val timeoutDuration = Duration(config.getString("cmwell.downloader.consumer.http-retry-timeout")).toCoarsest
    FiniteDuration( timeoutDuration.length, timeoutDuration.unit )
  }

  private [Downloader] val numDataFetchRetries = config.getInt("cmwell.downloader.consumer.data-fetch-retries")

  private val HttpAddress(protocol, host, port, uriPrefix) = ArgsManipulations.extractBaseUrl(baseUrl)

  /**
    * Creates a flow for downloading infoton data from path ByteString
    *
    * @return flow that gets uuids and download their data
    */
  private[data] def downloadDataFromPaths()(implicit ec: ExecutionContext) = {
    def createDataRequest(paths: Seq[ByteString]) = {
      val paramsValue = if (params.isEmpty) "" else s"&$params"

      HttpRequest (
        uri = s"${formatHost(baseUrl)}/_out?format=$format$paramsValue",
        method = HttpMethods.POST,
        entity = HttpEntity(concatByteStrings(paths, ByteString(",\n")).utf8String)
          .withContentType(ContentTypes.`text/plain(UTF-8)`)
      )
    }

    def getMissingPaths(receivedData: ByteString, paths: Seq[String]) = {
      def extractPathNtriples(data: ByteString) = data.utf8String
        .split("\n")
        .filter(_ contains "/meta/sys#path>")
        .map{line =>
          val index = line.lastIndexOf('>') + 3
          line.substring(index).dropRight(3)
        }
        .toSeq
        .distinct

      def extractPathJson(data: ByteString) = {
        val jsonValue = Json.parse(data.toArray)
        val jsonPaths = (jsonValue \ "infotons" \\ "system").map(_ \ "path")

        jsonPaths.map(_.get.toString.init.tail)
      }

      def extractMissingPathsJson(data: ByteString) = {
        val jsonValue = Json.parse(data.toArray)

        val missing = jsonValue \\ "irretrievablePaths"

        missing.head match {
          case JsArray(arr) if arr.isEmpty => Seq.empty[String]
          case JsArray(arr) => arr.map(_.toString)
          case x => logger.error(s"unexpected message: $x"); ???
        }
      }

      def extractPathTsv(data: ByteString) = data.utf8String.lines.map(_.takeWhile(_ != '\t')).toSeq

      format match {
        case "json"                => paths diff extractPathJson(receivedData) //extractMissingPathsJson(receivedData)
        case "ntriples" | "nquads" => paths diff extractPathNtriples(receivedData)
        case "tsv"                 => paths diff extractPathTsv(receivedData)
        case _                     => paths diff extractPathNtriples(receivedData)
      }
    }

    def sendPathRequest(timeout: FiniteDuration, parallelism: Int, limit: Int = 0)
                       (createRequest: (Seq[Path]) => HttpRequest)
                       (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

      case class State(pathsToRequest: Seq[Path],
                       token: Token,
                       retrievedData: Seq[ByteString] = Seq() ,
                       retriesLeft: Int = limit)


      def retryWith(state: State): Option[immutable.Iterable[(Seq[Path], State)]] = state match {
        case State(uuidsToRequest, _, _, _) => Some(immutable.Seq(uuidsToRequest -> state))
      }

      val job = Flow[(Seq[Path], State)]
        .map { case (paths, state) => paths -> Some(state) }
        .via(Retry.retryHttp(timeout, parallelism, baseUrl)(createRequest)) // fetch data from paths
        .mapAsyncUnordered(parallelism){
        case (Success(res@HttpResponse(s,h,e,p)), sentPaths, Some(state)) if s.isSuccess() =>
          DataPostProcessor.postProcessByFormat(format, e.withoutSizeLimit().dataBytes)
            .runFold(blank)(_ ++ _)
            .map{ receivedData => // accumulate received data
              val totalReceivedData = state.retrievedData :+ receivedData


              getMissingPaths(receivedData, sentPaths.map(_.utf8String)) match {
                case Seq() =>
                  // no missing data, success!
                  Success(totalReceivedData) -> state.copy(pathsToRequest = Seq(), retrievedData = totalReceivedData)
                case missingPaths if state.retriesLeft > 0 =>
                  logger.debug(s"received data length=${receivedData.size} $h")
                  logger.debug(s"sent ${sentPaths.size} paths but received ${sentPaths.size - missingPaths.size}")
                  logger.debug(s"sent ${sentPaths.map(_.utf8String).mkString("\n")}")

                  // retry retrieving data from missing uuids
                  val newState = state.copy(
                    pathsToRequest = missingPaths.map(ByteString.apply),
                    retrievedData = totalReceivedData,
                    retriesLeft = state.retriesLeft - 1)

                  logger.error(s"detected missing paths size=${missingPaths.size} retries left=${state.retriesLeft}")
                  Failure(new Exception("missing data from paths")) -> newState
                case missingPaths =>
                  // do not retrieve data from missing paths
                  val newState = state.copy(
                    pathsToRequest = missingPaths.map(ByteString.apply),
                    retrievedData = totalReceivedData)

                  logger.error(s"got ${missingPaths.size} missing paths")
                  badDataLogger.error(missingPaths.mkString("\n"))
                  Success(totalReceivedData) -> newState
              }
            }
        case (Success(HttpResponse(s,h,e,p)), sentPaths, Some(state)) =>
          e.discardBytes()
          Future.successful(Failure(new Exception("cannot send request to send paths")) -> state)
        case (Failure(err), sendPaths, Some(state)) =>
          logger.error(s"error: token=${state.token} $err")
          Future.successful(Failure(new Exception("cannot send request to send paths")) -> state)
      }

      Flow[(Seq[Path], Token)]
        .map{ case (paths, token) => paths -> State(paths, token, Seq.empty[ByteString], limit) }
        .via(GoodRetry.concat(Long.MaxValue, job)(retryWith))
        .collect { case (Success(receivedData), state) => receivedData -> state.token }
    }

    Flow[(Token, Path)]
      .groupedWithin(numUuidsPerRequest, 3.seconds)
      .mapConcat { _.groupBy { case (token, _) => token}
        .map { case (token, tokenAndPath) => tokenAndPath.map { case (token, path) => path } -> token }
      }
      .via(sendPathRequest(retryTimeout, bufferSize, numDataFetchRetries)(createDataRequest))
      .flatMapConcat {
        case (responseBytes, token) => Source.fromIterator(() => responseBytes.map(token -> _).iterator)
      }
  }


  /**
    * Creates a flow for downloading infoton data from uuid ByteString
    *
    * @return flow that gets uuids and download their data
    */
  private[data] def downloadDataFromUuids()(implicit ec: ExecutionContext) = {
    def createDataRequest(uuids: Seq[ByteString]) = {
      val paramsValue = if (params.isEmpty) "" else s"&$params"

      HttpRequest (
        uri = s"${formatHost(baseUrl)}/_out?format=$format$paramsValue",
        method = HttpMethods.POST,
        entity = HttpEntity(concatByteStrings(uuids, "/ii/", "\n/ii/", "").utf8String)
          .withContentType(ContentTypes.`text/plain(UTF-8)`)
      )
    }

    def getMissingUuids(receivedData: ByteString, uuids: Seq[String]) = {
      def extractUuidNtriples(data: ByteString) = data.utf8String
        .split("\n")
        .filter(_ contains "/meta/sys#uuid>")
        .map{line =>
          val index = line.lastIndexOf('>') + 3
          line.substring(index, index + 32)
        }
        .toSeq
        .distinct

      def extractMissingUuidsJson(data: ByteString) = {
        val jsonValue = Json.parse(data.toArray)

        val missing = jsonValue \\ "irretrievablePaths"

        missing.head match {
          case JsArray(arr) if arr.isEmpty => Seq.empty[String]
          case JsArray(arr) => arr.map(_.toString)
          case x => logger.error(s"unexpected message: $x"); ???
        }
      }

      def extractUuidsTsv(data: ByteString) = data.utf8String.lines.map(
          _.dropWhile(_ != '\t').drop(1)
           .dropWhile(_ != '\t').drop(1)
           .takeWhile(_ != '\t'))
          .toSeq

      format match {
        case "json"                => extractMissingUuidsJson(receivedData)
        case "ntriples" | "nquads" => uuids diff extractUuidNtriples(receivedData)
        case "tsv"                 => uuids diff extractUuidsTsv(receivedData)
        case "text"                => Seq.empty[String] // format text does not contain uuid
        case _                     => uuids diff extractUuidNtriples(receivedData)
      }
    }

    def sendUuidRequest(timeout: FiniteDuration, parallelism: Int, limit: Int = 0)
                     (createRequest: (Seq[Uuid]) => HttpRequest)
                    (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

      case class State(uuidsToRequest: Seq[Uuid],
                       token: Token,
                       retrievedData: Seq[ByteString] = Seq() ,
                       retriesLeft: Int = limit)


      def retryWith(state: State): Option[immutable.Iterable[(Seq[Uuid], State)]] = state match {
        case State(uuidsToRequest, _, _, _) => Some(immutable.Seq(uuidsToRequest -> state))
      }

      val job = Flow[(Seq[Uuid], State)]
        .map { case (uuids, state) => uuids -> Some(state) }
        .via(Retry.retryHttp(timeout, parallelism, baseUrl)(createRequest)) // fetch data from uuids
        .mapAsyncUnordered(parallelism){
          case (Success(res@HttpResponse(s,h,e,p)), sentUuids, Some(state)) if s.isSuccess() =>
            DataPostProcessor.postProcessByFormat(format, e.withoutSizeLimit().dataBytes)
              .runFold(blank)(_ ++ _)
              .map{ receivedData => // accumulate received data
                val totalReceivedData = state.retrievedData :+ receivedData

                getMissingUuids(receivedData, sentUuids.map(_.utf8String)) match {
                  case Seq() =>
                    // no missing data, success!
                    Success(totalReceivedData) -> state.copy(uuidsToRequest = Seq(), retrievedData = totalReceivedData)
                  case missingUuids if state.retriesLeft > 0 =>
                    logger.debug(s"received data length=${receivedData.size} $h")
                    logger.debug(s"sent ${sentUuids.size} uuids but receieved ${sentUuids.size - missingUuids.size}")
                    logger.debug(s"sent ${sentUuids.map(_.utf8String).mkString("/ii/", "\n/ii/", "\n")}")

                    // retry retrieving data from missing uuids
                    val newState = state.copy(
                      uuidsToRequest = missingUuids.map(ByteString.apply),
                      retrievedData = totalReceivedData,
                      retriesLeft = state.retriesLeft - 1)

                    logger.error(s"detected missing uuids size=${missingUuids.size} retries left=${state.retriesLeft}")
                    Failure(new Exception("missing data from uuids")) -> newState
                  case missingUuids =>
                    // do not retrieve data from missing uuids
                    val newState = state.copy(
                      uuidsToRequest = missingUuids.map(ByteString.apply),
                      retrievedData = totalReceivedData)

                    logger.error(s"got ${missingUuids.size} missing uuids")
                    badDataLogger.error(missingUuids.mkString("\n"))
                    Success(totalReceivedData) -> newState
                }
              }
          case (Success(HttpResponse(s,h,e,p)), sentUuids, Some(state)) =>
            e.discardBytes()
            Future.successful(Failure(new Exception("cannot send request to send uuids")) -> state)
          case (Failure(err), sendUuids, Some(state)) =>
            logger.error(s"error: token=${state.token} $err")
            Future.successful(Failure(new Exception("cannot send request to send uuids")) -> state)
        }

      Flow[(Seq[Uuid], Token)]
        .map{ case (uuids, token) => uuids -> State(uuids, token, Seq.empty[ByteString], limit) }
        .via(GoodRetry.concat(Long.MaxValue, job)(retryWith))
        .collect { case (Success(receivedData), state) => receivedData -> state.token }
    }

    Flow[(Token, Uuid)]
      .groupedWithin(numUuidsPerRequest, 3.seconds)
      .mapConcat { _.groupBy { case (token, _) => token}
        .map { case (token, tokenAndUuid) => tokenAndUuid.map { case (token, uuid) => uuid } -> token }
      }
      .via(sendUuidRequest(retryTimeout, bufferSize, numDataFetchRetries)(createDataRequest))
      .flatMapConcat {
        case (responseBytes, token) => Source.fromIterator(() => responseBytes.map(token -> _).iterator)
      }
  }



  /**
    * Gets cm-well uuids and next token from given token (position)
    *
    * @param token input token (position) in cm-well
    * @return future of [[akka.stream.scaladsl.Source Source]] containing next token and sequence of uuids
    */
  private[consumer] def getTsvFromToken(token: String)(implicit ec: ExecutionContext) = {
    // create HTTP request from token
    val consumeHandler = if (isBulk) "_bulk-consume" else "_consume"
    val req = HttpRequest(uri = s"${formatHost(baseUrl)}/$consumeHandler?position=$token&format=tsv")

    // send request and extract uuids
    Source.single(req)
      .via(HttpConnections.outgoingConnection(host, port, protocol))
      .map {
        case res@HttpResponse(status, headers, entity, _) if status.isFailure() =>
          // failure
          redLogger.error(s"http request failed: status=$status entity=$entity")
          res.discardEntityBytes()
          "" -> Source.failed[TsvData](new Exception("failure in http response"))

        case res@HttpResponse(status, headers, entity, _) if status == StatusCodes.NoContent =>
          res.discardEntityBytes()
          val nextToken = getPositionValue(headers)
          nextToken -> Source.empty[TsvData]

        case res@HttpResponse(status, headers, entity, _) =>
          val nextToken = getPositionValue(headers)
          nextToken -> entity.withoutSizeLimit().dataBytes
            .via(lineSeparatorFrame)
            .map(extractTsv)

    }.runWith(Sink.head)
  }

  /**
    * Gets data from given token (position)
    *
    * @param token input token containing position in consumer API
    * @return next token (position) and retrieved data
    */
  def getChunkData(token: String)(implicit ec: ExecutionContext) = {
    getTsvFromToken(token).flatMap { case (nextToken, src) =>
      if (nextToken == token) {
        Future.successful(nextToken -> "")
      } else {
        src
          .map ( tsv => tsv.uuid )
          .map (nextToken -> _)
          .via (downloadDataFromUuids )
          .fold("" -> Seq.empty[ByteString]) {
            case ((_, buffer), (token, line)) => token -> (buffer :+ line :+ endl)
          }.map { case (token, data) => token -> concatByteStrings(data, endl).utf8String }
          .runWith(Sink.head)
      }
    }
  }

  /**
    * Creates [[akka.stream.scaladsl.Source Source]] of TSVs, starting from given token
    *
    * @param token input token which contains start position
    * @param updateFreq indicates whether this source should wait for data updates indefinitely
    *                   ([[Duration.Undefined]] completes this source upon fully consumption)
    * @return [[akka.stream.scaladsl.Source Source]] of token (position) and its corresponding TSVs
    */
  def createTsvSource(token: Option[Token] = None, updateFreq: Option[FiniteDuration] = None)(implicit ec: ExecutionContext) = {
    import akka.pattern._
    val prefetchBufferSize = 3000000

    // contains http response data of current chunk
    val buffer = new java.util.concurrent.ArrayBlockingQueue[Option[TokenAndTsv]](prefetchBufferSize)

    val initTokenFuture = token match {
      case Some(t) => Future.successful(t)
      case None => Downloader.getToken(
        baseUrl = baseUrl,
        path = path,
        params = params,
        qp = qp,
        recursive = recursive,
        indexTime = indexTime,
        length = length,
        isBulk = isBulk)
    }

    val bufferFillerActor = system.actorOf(Props(new BufferFillerActor(
      threshold = (prefetchBufferSize * 0.3).toInt,
      initToken = initTokenFuture,
      baseUrl = baseUrl,
      params = params,
      isBulk = isBulk,
      updateFreq = updateFreq,
      label = label
    )))

    class FakeState(initToken: Token) {

      var currConsumeState: ConsumeState = SuccessState(0)

      // indicates if no data is left to be consumed
      var noDataLeft = false

      // init of filling buffer with consumed data from token
//      Future { blocking { fillBuffer(initToken) } }

      var consumerStatsActor = system.actorOf(Props(new ConsumerStatsActor(baseUrl, initToken, params)))


      /**
        * Gets next data element from buffer
        * @return Option of position token -> Tsv data element
        */
      def next(): Future[Option[(Token, TsvData)]] = {
        implicit val timeout = akka.util.Timeout(5.seconds)

        val elementFuture = (bufferFillerActor ? BufferFillerActor.GetData).mapTo[Option[(Token, TsvData)]]

        elementFuture.map {
          case Some((token,tsv)) =>
            Some(token -> tsv)
          case None =>
            noDataLeft = true // received the signal of last element in buffer
            None
          case null if (noDataLeft) =>
            logger.debug("buffer is empty and noDataLeft=true")
            None // tried to get new data but no data available
          case x =>
            logger.error(s"unexpected message: $x")
            None
        }.recoverWith{ case _ => next() }
      }
    }


    Source.fromFuture(initTokenFuture)
      .flatMapConcat { initToken =>
        Source.unfoldAsync(new FakeState(initToken)) { fs =>
          fs.next().map {
            case Some(tokenAndData) => Some(fs -> tokenAndData)
            case None => None
          }
        }.via(BufferFillerKiller(bufferFillerActor))
      }
  }
}
