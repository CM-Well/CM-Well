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
package cmwell.tools.data.downloader.streams.japi

import java.io.InputStream

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import cmwell.tools.data.downloader.streams.Downloader
import cmwell.tools.data.utils.akka._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Java API containing wrapper methods for working
  * with akka-stream based CM-Well Downloader
  */
object DownloaderUtils {

  /**
    * Downloads data from cm-well
    *
    * @param host address of target cm-well
    * @param path path in cm-well
    * @param params uri params (i.e., &recursive)
    * @param qp cm-well query params
    * @param format desired cm-well data format
    * @param op operation type (stream, nstream etc.)
    * @param length max number of records to receive (e.g., 1, 10, all)
    * @param recursive true if need to get records in a recursive way
    */
  def fromQuery(host: String,
                path: String,
                params: String,
                qp: String,
                format: String,
                op: String,
                length: Option[Int],
                recursive: Boolean = false): Future[Done] = {
    fromQuery(
      host = host,
      path = path,
      params = params,
      qp = qp,
      format = format,
      op = op,
      length = length,
      recursive = recursive,
      onFinish = new Runnable { override def run(): Unit = {} }
    )
  }

  /**
    * Downloads data from cm-well
    *
    * @param host address of target cm-well
    * @param path path in cm-well
    * @param params uri params (i.e., &recursive)
    * @param qp cm-well query params
    * @param format desired cm-well data format
    * @param op operation type (stream, nstream etc.)
    * @param length max number of records to receive (e.g., 1, 10, all)
    * @param recursive true if need to get records in a recursive way
    * @param onFinish code to run when completion event is fired
    */
  def fromQuery(host: String,
                path: String,
                params: String,
                qp: String,
                format: String,
                op: String,
                length: Option[Int],
                recursive: Boolean,
                onFinish: Runnable): Future[Done] = {

    implicit val system = ActorSystem("reactive-downloader")
    implicit val mat = ActorMaterializer()

    Downloader
      .downloadFromQuery(
        baseUrl = host,
        path = path,
        params = params,
        qp = qp,
        format = format,
        op = op,
        length = length,
        recursive = recursive,
        // scalastyle:off
        outputHandler = println
        // scalastyle:on
      )
      .andThen { case _ => cleanup() }
      .andThen { case _ => onFinish.run() }
  }

  def fromUuidInputStream(host: String, format: String, op: String, in: InputStream, onFinish: Runnable) = {

    implicit val system = ActorSystem("reactive-downloader")
    implicit val mat = ActorMaterializer()

    Downloader
      .downloadFromUuidInputStream(
        baseUrl = host,
        format = format,
        // scalastyle:off
        outputHandler = println,
        // scalastyle:on
        in = in
      )
      .andThen { case _ => cleanup() }
      .andThen { case _ => onFinish.run() }
  }
}
