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
package cmwell.tools.data.ingester.japi

import java.io._

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer
import cmwell.tools.data.ingester.Ingester
import cmwell.tools.data.utils.akka._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Java API containing wrapper methods for working with CM-Well Ingester
  */
object IngesterUtils {

  /**
    * Performs data ingestion from [[java.io.PipedOutputStream PipedOutputStream]] to the target CM-Well
    *
    * @param host address of destination CM-Well
    * @param format format of data to be pushed
    * @param writeToken CM-Well write token permission (if needed)
    * @param pipe pipe of data
    * @param onFinish code to run when completion event is fired
    * @return future of [[akka.Done Done]] which signals when data ingestion is completed
    */
  def fromPipe(host: String,
               format: String,
               writeToken: String,
               pipe: PipedOutputStream,
               onFinish: Runnable): Future[Done] = {

    fromInputStream(
      host = host,
      format = format,
      writeToken = writeToken,
      in = new PipedInputStream(pipe),
      onFinish = onFinish
    )
  }

  /**
    * Performs data ingestion from [[java.io.InputStream InputStream]] to the target CM-Well
    *
    * @param host address of destination CM-Well
    * @param format format of data to be pushed
    * @param writeToken CM-Well write token permission (if needed)
    * @param in source of data
    * @param onFinish code to run when completion event is fired
    */
  def fromInputStream(host: String,
                      format: String,
                      writeToken: String,
                      in: InputStream,
                      onFinish: Runnable): Future[Done] = {

    implicit val system = ActorSystem("reactive-downloader")
    implicit val mat = ActorMaterializer()

    Ingester
      .fromInputStream(
        baseUrl = host,
        format = format,
        writeToken = Option(writeToken),
        in = in
      )
      .runWith(Sink.ignore)
      .andThen { case _ => cleanup() }
      .andThen { case _ => onFinish.run() }
  }
}
