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
package cmwell.tools.data.sparql.japi

import java.io.InputStream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, StreamConverters}
import cmwell.tools.data.sparql.SparqlProcessor
import cmwell.tools.data.utils.akka.{concatByteStrings, endl}
import cmwell.tools.data.utils.chunkers.GroupChunker
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration.FiniteDuration

object SparqlUtils {

  def createJavaStreamFromPaths(baseUrl: String,
                                parallelism: Int = 4,
                                isNeedWrapping: Boolean = true,
                                sparqlQuery: String,
                                in: InputStream) = {

    implicit val system = ActorSystem("reactive-sparql-processor")
    implicit val mat = ActorMaterializer()

    SparqlProcessor
      .createSourceFromPathsInputStream(
        baseUrl = baseUrl,
        spQueryParamsBuilder = (p: Seq[String], v: Map[String,String], q: Boolean) => "sp.pid=" + p.head.substring(p.head.lastIndexOf('-') + 1),
        parallelism = parallelism,
        isNeedWrapping = isNeedWrapping,
        sparqlQuery = sparqlQuery,
        in = in
      )
      .map { case (data, _) => data }
      .via(GroupChunker(GroupChunker.formatToGroupExtractor("ntriples")))
      .map(concatByteStrings(_, endl))
      .runWith(StreamConverters.asJavaStream())
  }

  def createJavaOutputStreamFromPaths(baseUrl: String,
                                      parallelism: Int = 4,
                                      isNeedWrapping: Boolean = true,
                                      sparqlQuery: String,
                                      in: InputStream,
                                      timeout: FiniteDuration) = {
    implicit val system = ActorSystem("reactive-sparql-processor")
    implicit val mat = ActorMaterializer()

    SparqlProcessor
      .createSourceFromPathsInputStream(
        baseUrl = baseUrl,
        spQueryParamsBuilder = (p: Seq[String], v: Map[String,String], q: Boolean) => "sp.pid=" + p.head.substring(p.head.lastIndexOf('-') + 1),
        parallelism = parallelism,
        isNeedWrapping = isNeedWrapping,
        sparqlQuery = sparqlQuery,
        in = in
      )
      .map { case (data, _) => data }
      .via(GroupChunker(GroupChunker.formatToGroupExtractor("ntriples")))
      .map(concatByteStrings(_, endl))
      .runWith(StreamConverters.asInputStream(timeout))
  }
}
