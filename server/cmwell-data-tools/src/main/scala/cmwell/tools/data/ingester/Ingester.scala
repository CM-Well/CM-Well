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
package cmwell.tools.data.ingester

import java.io._

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpEncodings, RawHeader, `Content-Encoding`}
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import cmwell.tools.data.sparql.StpMetadata
import cmwell.tools.data.utils.chunkers.GroupChunker
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.ArgsManipulations._
import cmwell.tools.data.utils.akka.HeaderOps._
import cmwell.tools.data.utils.akka.Retry.State
import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import cmwell.tools.data.utils.akka.stats.IngesterStats.IngestStats

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe._

/**
  * Contains logic of pushing data to CM-Well
  */
object Ingester extends DataToolsLogging with DataToolsConfig {

  case class IngesterRuntimeConfig(force: Boolean, label: Option[String]=None)

  private[ingester] var retryTimeout = {
    val timeoutDuration = Duration(config.getString("cmwell.ingester.http-retry-timeout")).toCoarsest
    FiniteDuration(timeoutDuration.length, timeoutDuration.unit)
  }

  def ingestStatsFlow(tokenReporterOpt: Option[ActorRef], agentName: String,  initialIngestStatsOpt: Option[IngestStats] = None) = {

    import cmwell.tools.data.utils.akka.stats.IngesterStats.IngestStats._
    val emptyElement: Ingester.IngestEvent = Ingester.IngestSuccessEvent()

    def markStats(elem: Ingester.IngestEvent, ingestStats: IngestStats) = elem match {
      case IngestSuccessEvent(_, numInfotons) =>
        IngestStats(label = ingestStats.label, ingestedInfotons = ingestStats.ingestedInfotons + numInfotons )
      case IngestFailEvent(numInfotons) =>
        IngestStats(label = ingestStats.label, failedInfotons = ingestStats.failedInfotons + numInfotons )
    }

    Flow[Ingester.IngestEvent]
      .scan(initialStats(initialIngestStatsOpt, agentName, emptyElement))((stats, elem) =>
        (markStats(elem, stats._1), elem))
      .map {
        case (ingestStats, ingestEvent) =>
          tokenReporterOpt.foreach {  _ ! ingestStats }
          ingestEvent
      }

  }

  def ingesterFlow[T : TypeTag](baseUrl: String,
                   format: String,
                   writeToken: Option[String] = None,
                   method: String = "_in",
                   replaceMode: Boolean = false,
                   isPriority: Boolean = false,
                   extractContext: Option[T] => IngesterRuntimeConfig,
                   connectionPool: Option[Flow[(HttpRequest,State[T]), (Try[HttpResponse], State[T]), Http.HostConnectionPool]] = None)
                   (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    def getLabel(metadata: Option[T]) = extractContext(metadata).label.map(l => s"[$l]").getOrElse("")

    /**
      * Creates HTTP Request for pushing data to CM-Well
      *
      * @param data data to be pushed
      * @return Http request for pushing data
      * @see [[akka.http.scaladsl.model.HttpRequest]]
      */
    def createRequest(data: Seq[ByteString], vars: Map[String,String], context: Option[T]) = {
      val entity = HttpEntity(Gzip.encode(concatByteStrings(data, endl)))
        .withContentType(ContentTypes.`text/plain(UTF-8)`)

      val runtimeContext = extractContext(context)
      val force = runtimeContext.force

      val replaceModeValue = if (replaceMode) "&replace-mode" else ""
      val forceValue = if (force) "&force" else ""
      val priorityValue = if (isPriority) "&priority" else ""

      val gzipContentEncoding = `Content-Encoding`(HttpEncodings.gzip)

      val uri = s"${formatHost(baseUrl)}/$method?format=$format$replaceModeValue$forceValue$priorityValue"
      val req = HttpRequest(uri = uri, method = HttpMethods.POST, entity = entity,
        headers = scala.collection.immutable.Seq(gzipContentEncoding))

      writeToken match {
        case Some(token) => req.addHeader(RawHeader("X-CM-WELL-TOKEN", token))
        case None        => req
      }
    }

    val parallelism = config.getInt("akka.http.host-connection-pool.max-connections")

    Flow[Seq[(ByteString, Option[T])]]
      .map { elem => (elem.map(_._1), elem.head._2)}
      .via(Retry.retryHttp(retryTimeout, parallelism, managedConnection = connectionPool)(createRequest))
      .map {
        case (Failure(ex), infotons, stpMetadata) =>
          logger.error(s"${getLabel(stpMetadata)} problem: ${ex}")
          badDataLogger.info(s"${getLabel(stpMetadata)} data: ${infotons.map(_.utf8String).mkString("\n")}")
          (IngestFailEvent(numInfotons = infotons.size), stpMetadata)

        case (Success(res @ HttpResponse(s, h, e, p)), infotons, stpMetadata) if s.isFailure() =>
          e.toStrict(1.minute).map { strict =>
            logger.error(s"${getLabel(stpMetadata)} problem: host=${getHostname(h)} ${strict.data}, $p")
            badDataLogger.info(s"${getLabel(stpMetadata)} data: ${infotons.map(_.utf8String).mkString("\n")}")
            res.discardEntityBytes()
          }

          (IngestFailEvent(numInfotons = infotons.size), stpMetadata)

        case (Success(res @ HttpResponse(s, h, e, p)), infotons, stpMetadata) =>
          res.discardEntityBytes()

          val numBytes = infotons.foldLeft(0)(_ + _.size)
          (IngestSuccessEvent(sizeInBytes = numBytes, numInfotons = infotons.size), stpMetadata)
      }
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

    // $$$$ TEST DATA TOOLS!!!!
    // create akka source from input stream
    StreamConverters
      .fromInputStream(() => in)
      .via(lineSeparatorFrame)
      .filter(isInteresting)
      .via(GroupChunker(GroupChunker.formatToGroupExtractor(format)))
      .map(concatByteStrings(_, endl))
      .map(_ -> None)
      .groupedWeightedWithin((25*1024), 10.seconds)(_._1.size)
      .via(ingesterFlow[Option[Any]](
        baseUrl=baseUrl,
        format = format,
        writeToken = writeToken,
        replaceMode = replaceMode,
        isPriority = isPriority,
        extractContext = (_) => IngesterRuntimeConfig(false, label))
      )
      // Drop the context
      .map { d => d._1}

  }

  sealed trait IngestEvent
  case class IngestSuccessEvent(sizeInBytes: Int = 0, numInfotons: Int = 0) extends IngestEvent
  case class IngestFailEvent(numInfotons: Int) extends IngestEvent
}
