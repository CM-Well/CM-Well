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
package cmwell.tools.data.ingester

import java.io._

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import cmwell.tools.data.downloader.consumer.Downloader.config
import cmwell.tools.data.utils.chunkers.{GroupChunker, SizeChunker}
import cmwell.tools.data.utils.logging.{DataToolsLogging, LabelId}
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.ArgsManipulations._
import cmwell.tools.data.utils.akka.HeaderOps._
import cmwell.tools.data.utils.akka.stats.IngesterStats
import cmwell.tools.data.utils.ops.VersionChecker
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Contains logic of pushing data to CM-Well
  */
object Ingester extends DataToolsLogging with DataToolsConfig {
  val chunkSize = 25 * 1024
  private val bufferSize = config.getInt("akka.http.host-connection-pool.max-connections")

  private[ingester] var retryTimeout = {
    val timeoutDuration = Duration(config.getString("cmwell.ingester.http-retry-timeout")).toCoarsest
    FiniteDuration(timeoutDuration.length, timeoutDuration.unit)
  }

  /**
    * Checks if given line contains relevant data
    *
    * @param line input string to be checked
    * @return true if input contains relevant data, otherwise false
    */
  private def isInteresting(line: ByteString): Boolean = {
    if (line.isEmpty)
      false
    else
      line(0) match {
        case '#' | ' ' | '\t' | '\n' | '\r' => false
        case _                              => true
      }
  }

  /**
    * Performs data ingestion from [[java.io.PipedOutputStream PipedOutputStream]] to the target CM-Well
    *
    * @param baseUrl address of destination CM-Well
    * @param format format of data to be pushed
    * @param writeToken CM-Well write token permission (if needed)
    * @param replaceMode replace-mode parameter in cm-well
    * @param force force parameter in cm-well
    * @param isPriority use priority mode in cm-well
    * @param pipe pipe of data
    * @param within group infotons to bulks within given duration
    * @param system actor system
    * @param mat materializer
    * @param ec execution context
    * @return future of [[akka.Done Done]] which signals when data ingestion is completed
    */
  def fromPipe(
    baseUrl: String,
    format: String,
    writeToken: Option[String] = None,
    parallelism: Int = 100,
    replaceMode: Boolean = false,
    force: Boolean = false,
    isPriority: Boolean = false,
    pipe: PipedOutputStream,
    within: FiniteDuration = 10.seconds
  )(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext): Source[IngestEvent, _] = {

    fromInputStream(
      baseUrl = baseUrl,
      format = format,
      writeToken = writeToken,
      replaceMode = replaceMode,
      force = force,
      within = within,
      in = new PipedInputStream(pipe)
    )
  }

  /**
    * Performs data ingestion from [[java.io.InputStream InputStream]] to the target CM-Well
    *
    * @param baseUrl address of destination CM-Well
    * @param format format of data to be pushed
    * @param writeToken CM-Well write token permission (if needed)
    * @param replaceMode replace-mode parameter in cm-well
    * @param force force parameter in cm-well
    * @param isPriority use priority mode in cm-well
    * @param in source of data
    * @param within group infotons to bulks within given duration
    * @param system actor system
    * @param mat materializer
    * @param ec execution context
    * @return future of [[akka.Done Done]] which signals when data ingestion is completed
    */
  def fromInputStream(
    baseUrl: String,
    format: String,
    writeToken: Option[String] = None,
    replaceMode: Boolean = false,
    force: Boolean = false,
    isPriority: Boolean = false,
    in: InputStream,
    within: FiniteDuration = 10.seconds
  )(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext): Source[IngestEvent, _] = {
    // create akka source from input stream
    val source = StreamConverters
      .fromInputStream(() => in)
      .via(lineSeparatorFrame)
      .filter(isInteresting)
      .via(GroupChunker(GroupChunker.formatToGroupExtractor(format)))
      .map(concatByteStrings(_, endl))

    // perform data ingestion
    ingest(baseUrl = baseUrl,
           format = format,
           writeToken = writeToken,
           replaceMode = replaceMode,
           isPriority = isPriority,
           force = force,
           source = source,
           within = within)
  }

  /**
    * Performs data ingestion from Akka Source to the target CM-Well
    *
    * @param baseUrl address of destination CM-Well
    * @param format format of data to be pushed
    * @param writeToken CM-Well write token permission (if needed)
    * @param replaceMode replace-mode parameter in cm-well
    * @param force force parameter in cm-well
    * @param isPriority use priority mode in cm-well
    * @param source [[akka.stream.scaladsl.Source Source]] of data which
    * @param within group infotons to bulks within given duration
    * @param system actor system
    * @param mat materializer
    * @return future of [[akka.Done Done]] which signals when data ingestion is completed
    * @see [[akka.stream.scaladsl.Source]]
    */
  def ingest(baseUrl: String,
             format: String,
             writeToken: Option[String] = None,
             method: String = "_in",
             replaceMode: Boolean = false,
             isPriority: Boolean = false,
             force: Boolean = false,
             source: Source[ByteString, _],
             within: FiniteDuration = 10.seconds,
             label: Option[String] = None)(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    val labelValue = label.map(l => s"[$l]").getOrElse("")

    /**
      * Creates HTTP Request for pushing data to CM-Well
      *
      * @param data data to be pushed
      * @return Http request for pushing data
      * @see [[akka.http.scaladsl.model.HttpRequest]]
      */
    def createRequest(data: Seq[ByteString], vars: Map[String,String]) = {
      val entity = HttpEntity(concatByteStrings(data, endl).utf8String)
        .withContentType(ContentTypes.`text/plain(UTF-8)`)

      val replaceModeValue = if (replaceMode) "&replace-mode" else ""
      val forceValue = if (force) "&force" else ""
      val priorityValue = if (isPriority) "&priority" else ""

      val uri = s"${formatHost(baseUrl)}/$method?format=$format$replaceModeValue$forceValue$priorityValue"
      val req = HttpRequest(uri = uri, method = HttpMethods.POST, entity = entity)

      writeToken match {
        case Some(token) => req.addHeader(RawHeader("X-CM-WELL-TOKEN", token))
        case None        => req
      }
    }

    implicit val labelId = label.map(LabelId.apply)

    val parallelism = config.getInt("akka.http.host-connection-pool.max-connections")

    val ingestFlow = Flow[Seq[ByteString]]
      .map(data => data -> None)
      .via(Retry.retryHttp(retryTimeout, parallelism, baseUrl)(createRequest))
      .map {
        case (Failure(ex), infotons, _) =>
          logger.error(s"$labelValue problem: ${ex}")
          badDataLogger.info(s"$labelValue data: ${infotons.map(_.utf8String).mkString("\n")}")
          IngestFailEvent(numInfotons = infotons.size)

        case (Success(res @ HttpResponse(s, h, e, p)), infotons, _) if s.isFailure() =>
          logger.error(s"$labelValue problem: host=${getHostname(h)} $e, $p")
          badDataLogger.info(s"$labelValue data: ${infotons.map(_.utf8String).mkString("\n")}")
          res.discardEntityBytes()
          IngestFailEvent(numInfotons = infotons.size)

        case (Success(res @ HttpResponse(s, h, e, p)), infotons, _) =>
          res.discardEntityBytes()

          val numBytes = infotons.foldLeft(0)(_ + _.size)
          IngestSuccessEvent(sizeInBytes = numBytes, numInfotons = infotons.size)
      }

    // ingest graph
    source
      .via(SizeChunker(chunkSize, within))
      .via(ingestFlow)
//    .via(balancer(ingestFlow, 20))
  }

  sealed trait IngestEvent
  case class IngestSuccessEvent(sizeInBytes: Int = 0, numInfotons: Int) extends IngestEvent
  case class IngestFailEvent(numInfotons: Int) extends IngestEvent
}
