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
package cmwell.tools.data.downloader.consumer.japi

import java.io.InputStream
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.scaladsl.StreamConverters
import akka.stream.{ActorMaterializer, Materializer}
import cmwell.tools.data.downloader.consumer.Downloader
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.chunkers.GroupChunker

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object DownloaderUtils {

  /**
    * Downloads infotons' data according to given CM-Well token (position)
    * @param baseUrl address of destination CM-Well
    * @param path path in CM-Well (should start with "/", otherwise null)
    * @param params params in cm-well URI (or null)
    * @param qp query params in cm-well (or null)
    * @param format output data format (or null for trig format)
    * @param recursive is query recursive
    * @param token input token containing position in consumer API (otherwise, null)
    * @param onFinish code to run when completion event is fired
    * @return future of next token (position) and received data
    */
  def getChunkData(baseUrl: String,
                   path: String,
                   params: String,
                   qp: String,
                   format: String,
                   recursive: Boolean,
                   token: String,
                   onFinish: Runnable) = {

    implicit val system = ActorSystem("reactive-downloader")
    implicit val mat = ActorMaterializer()

    Downloader
      .getChunkData(
        baseUrl = baseUrl,
        path = if (path == null) "/" else path,
        params = if (params == null) "" else params,
        qp = if (qp == null) "" else qp,
        format = if (format == null) "trig" else format,
        recursive = recursive,
        token = Option(token)
      )
      .andThen { case _ => cleanup() }
      .andThen { case _ => onFinish.run() }
  }

  /**
    * Downloads infotons' paths according to given CM-Well token (position)
    * @param baseUrl address of destination CM-Well
    * @param path path in CM-Well (should start with "/", otherwise null)
    * @param params params in cm-well URI (or null)
    * @param qp query params in cm-well (or null)
    * @param recursive true if need to get records in a recursive way
    * @param token input token containing position in consumer API (otherwise, None)
    * @param onFinish code to run when completion event is fired
    * @return future of next token (position) and received paths
    */
  def getChunkPaths(baseUrl: String,
                    path: String,
                    params: String,
                    qp: String,
                    recursive: Boolean,
                    token: String,
                    onFinish: Runnable) = {

    implicit val system = ActorSystem("reactive-downloader")
    implicit val mat = ActorMaterializer()

    Downloader
      .getChunkPaths(
        baseUrl = baseUrl,
        path = if (path == null) "/" else path,
        params = if (params == null) "" else params,
        qp = if (qp == null) "" else qp,
        recursive = recursive,
        token = Option(token)
      )
      .andThen { case _ => cleanup() }
      .andThen { case _ => onFinish.run() }
  }

  def createInputStream(
    baseUrl: String,
    path: String = "/",
    params: String = "",
    qp: String = "",
    format: String = "trig",
    recursive: Boolean = false,
    isBulk: Boolean = false,
    token: Option[String] = None
  )(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {
    val downloader = new Downloader(baseUrl = baseUrl,
                                    path = path,
                                    params = params,
                                    format = format,
                                    qp = qp,
                                    isBulk = isBulk,
                                    recursive = recursive)

    downloader
      .createTsvSource(token)(ec)
      .async
      .map { case ((token, tsv), _, _) => token -> tsv.uuid }
      .via(downloader.downloadDataFromUuids()(ec))
      .map { case (token, data) => data }
      .via(cmwell.tools.data.utils.akka.lineSeparatorFrame)
      .via(GroupChunker(GroupChunker.formatToGroupExtractor(format)))
      .map(concatByteStrings(_, endl))
      .runWith(StreamConverters.asInputStream())
  }

  /**
    * Creates [[java.util.Iterator Iterator]] from g
    * @param baseUrl address of destination CM-Well
    * @param path path in CM-Well (should start with "/", otherwise null)
    * @param params params in cm-well URI (or null)
    * @param qp query params in cm-well (or null)
    * @param recursive true if need to get records in a recursive way
    * @param format output data format (or null for trig format)
    * @param lengthHint max number of records to receive (e.g., 1, 10, 50)
    * @param token input token containing position in consumer API (otherwise, null)
    * @return
    */
  def createIterator(baseUrl: String,
                     path: String,
                     params: String,
                     qp: String,
                     recursive: Boolean,
                     format: String,
                     lengthHint: Int,
                     timeoutMillis: Long,
                     token: String): Iterator[String] = {

    implicit val system = ActorSystem("reactive-downloader")
    implicit val mat = ActorMaterializer()

    val AWAIT_TIME = Duration(timeoutMillis, MILLISECONDS)

    /**
      * Implementation of [Iterator] interface using CM-Well consumer API
      *
      * @param token token (position) to iterate over
      * @param downloader cosumer-based downloader
      */
    class DownloaderIterator(token: String, downloader: Downloader) extends Iterator[String] {
      case class TokenAndData(token: String, data: String)

      private var currToken: String = token
      private var moreToIterate: Boolean = true

      override def toString: String = currToken

      override def hasNext: Boolean = moreToIterate

      override def next(): String = {
        val tokenAndDataFuture = downloader
          .getChunkData(currToken)
          .map { case (token, data) => TokenAndData(token = token, data = data) }

        val tokenAndData = Await.result(tokenAndDataFuture, AWAIT_TIME)

        if (currToken == tokenAndData.token) {
          moreToIterate = false

          // iteration has ended, clean resources
          cleanup()

        } else {
          currToken = tokenAndData.token
        }

        tokenAndData.data
      }
    }

    val downloader = new Downloader(
      baseUrl = baseUrl,
      path = if (path == null) "/" else path,
      params = if (params == null) "" else params,
      qp = if (qp == null) "" else qp,
      format = if (format == null) "trig" else format,
      recursive = recursive
    )

    // check if need to calculate token
    val tokenFuture =
      if (token == null)
        Downloader.getToken(baseUrl = baseUrl,
                            path = if (path == null) "/" else path,
                            params = if (params == null) "" else params,
                            qp = if (qp == null) "" else qp,
                            recursive = recursive,
                            isBulk = false)
      else Future.successful(token)

    val tokenValue = Await.result(tokenFuture, AWAIT_TIME)

    new DownloaderIterator(token = tokenValue, downloader = downloader)
  }

  def createJavaStream(baseUrl: String,
                       path: String,
                       params: String,
                       qp: String,
                       recursive: Boolean,
                       format: String,
                       timeoutMillis: Long,
                       token: String) = {
    implicit val system = ActorSystem("reactive-downloader")
    implicit val mat = ActorMaterializer()

    val dataSource = Downloader.createDataSource(baseUrl = baseUrl,
                                                 path = path,
                                                 params = params,
                                                 qp = qp,
                                                 recursive = recursive,
                                                 format = format,
                                                 token = Option(token))

    dataSource
      .map { case (token, data) => data }
      .via(GroupChunker(GroupChunker.formatToGroupExtractor("ntriples")))
      .map(concatByteStrings(_, endl))
      .runWith(StreamConverters.asJavaStream())
  }

  def createInputStream(baseUrl: String,
                        path: String,
                        params: String,
                        qp: String,
                        recursive: Boolean,
                        format: String,
                        timeout: Long,
                        token: String): InputStream = {
    implicit val system = ActorSystem("reactive-downloader")
    implicit val mat = ActorMaterializer()

    val dataSource = Downloader.createDataSource(baseUrl = baseUrl,
                                                 path = path,
                                                 params = params,
                                                 qp = qp,
                                                 recursive = recursive,
                                                 format = format,
                                                 token = Option(token))

    dataSource
      .map { case (token, data) => data }
      .via(GroupChunker(GroupChunker.formatToGroupExtractor("ntriples")))
      .map(concatByteStrings(_, endl))
      .runWith(StreamConverters.asInputStream(FiniteDuration(timeout, TimeUnit.MILLISECONDS)))
  }
}
