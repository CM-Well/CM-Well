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
package cmwell.tools.data.sparql

import java.io.FileInputStream
import java.nio.file.{Files, Paths}

import akka.stream.scaladsl.{Keep, Sink, Source}
import cmwell.tools.data.ingester.Ingester.IngesterRuntimeConfig
import cmwell.tools.data.ingester._
import cmwell.tools.data.utils.akka.Implicits._
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.akka.stats.{DownloaderStats, IngesterStats}
import cmwell.tools.data.utils.chunkers.GroupChunker
import cmwell.tools.data.utils.ops._
import com.typesafe.config.ConfigFactory
import org.rogach.scallop.ScallopConf

import scala.concurrent.ExecutionContext

object SparqlProcessorMain extends App {
  val metricRegistry = new com.codahale.metrics.MetricRegistry()

  object Opts extends ScallopConf(args) {
    version(s"cm-well sparql-processor ${getVersionFromManifest()} (c) 2016")

    val srcHost = opt[String]("source-host", descr = "source cm-well host name server", required = true)
    val dstHost = opt[String]("dest-host", descr = "destination cm-well host name server")
    val path = opt[String]("path", descr = "path in cm-well", default = Some("/"))
    val writeToken = opt[String]("write-token", descr = "cm-well write permission token")
    val positionToken = opt[String]("position-token", descr = "cm-well consume token")
    val qp = opt[String]("qp", descr = "query params in cm-well", default = Some(""))
    val sparqlQuery = opt[String]("sparql-query", descr = "cm-well sparql query body")
    val sparqlQueryFilePath = opt[String]("sparql-query-path", descr = "cm-well sparql query file path")
    val sparqlQueryWrap =
      opt[Boolean]("wrap-query", default = Some(false), descr = "wrap with 'PATHS...SPARQL...' the input sparql query")
    val state = opt[String]("state", descr = "position state file")
    val ingest = opt[Boolean]("ingest", descr = "ingest data to destination cm-well instance", default = Some(false))
    val numConnections = opt[Int]("num-connections", descr = "number of http connections to open")
    val parallelism = opt[Int]("parallelism", descr = "number of workers sending requests to _sp ", default = Some(4))
    val fromPaths = opt[Boolean]("from-paths",
                                 descr = "input paths received from input stream provided by stdin",
                                 default = Some(false))
    val fromQuery = opt[Boolean]("from-query", descr = "input paths received by cm-well query", default = Some(false))
    val fromPosition =
      opt[Boolean]("from-position", descr = "input paths received by consuming a given token", default = Some(false))
    val indexTime = opt[Long]("index-time", descr = "index-time lower bound", default = Some(0))

    requireOne(fromQuery, fromPaths, fromPosition)
    requireOne(sparqlQuery, sparqlQueryFilePath)
    conflicts(fromPaths, List(qp, positionToken))
    conflicts(fromPosition, List(qp))
    dependsOnAll(indexTime, List(fromQuery))
    dependsOnAll(ingest, List(dstHost))
    dependsOnAll(writeToken, List(ingest))
    dependsOnAll(fromPosition, List(positionToken))

    verify()
  }

  // resize akka http connection pool
  Opts.numConnections.map(
    numConnections => System.setProperty("akka.http.host-connection-pool.max-connections", numConnections.toString)
  )

  val sparqlQuery = if (Opts.sparqlQuery.isSupplied) {
    Opts.sparqlQuery()
  } else {
    scala.io.Source.fromFile(Opts.sparqlQueryFilePath()).mkString
  }

  import scala.concurrent.ExecutionContext.Implicits.global
//  implicit val executionContext: ExecutionContext = system.dispatchers.defaultGlobalDispatcher

  val start = System.currentTimeMillis()

  val source = if (Opts.fromQuery()) {
    SparqlProcessor.createSourceFromQuery(
      baseUrl = Opts.srcHost(),
      path = Opts.path(),
      qp = Opts.qp(),
      isNeedWrapping = Opts.sparqlQueryWrap(),
      parallelism = Opts.parallelism(),
      indexTime = Opts.indexTime(),
      spQueryParamsBuilder = (p: Seq[String], v: Map[String,String], q: Boolean) => "sp.pid=" + p.head.substring(p.head.lastIndexOf('-') + 1),
      sparqlQuery = sparqlQuery
    )
  } else if (Opts.fromPaths()) {
    SparqlProcessor.createSourceFromPathsInputStream(
      baseUrl = Opts.srcHost(),
      isNeedWrapping = Opts.sparqlQueryWrap(),
      spQueryParamsBuilder = (p: Seq[String], v: Map[String,String], q: Boolean) => "sp.pid=" + p.head.substring(p.head.lastIndexOf('-') + 1),
      sparqlQuery = sparqlQuery,
      parallelism = Opts.parallelism(),
      in = System.in
    )
  } else if (Opts.fromPosition()) {
    SparqlProcessor.createSourceFromToken(
      baseUrl = Opts.srcHost(),
      isNeedWrapping = Opts.sparqlQueryWrap(),
      spQueryParamsBuilder = (p: Seq[String], v: Map[String,String], q: Boolean) => "sp.pid=" + p.head.substring(p.head.lastIndexOf('-') + 1),
      sparqlQuery = sparqlQuery,
      parallelism = Opts.parallelism(),
      token = Opts.positionToken()
    )
  } else {
    Source.empty // should not arrive this line
  }

  val stateFilePath = Opts.state.toOption.map(Paths.get(_))
  var lastToken: Option[String] = None

  // group data bytes to infotons
  val infotonSource = source
    .map {
      case (data, tokenOpt) =>
        // write token to state file if needed
        if (tokenOpt != lastToken) {
          for {
            path <- stateFilePath
            token <- tokenOpt
          } {
            // save new token in state file
            Files.write(path, token.getBytes("UTF-8"))
          }
          lastToken = tokenOpt
        }
        data
    }
    .map { s =>
      s
    }
    .via(GroupChunker(GroupChunker.formatToGroupExtractor("ntriples")))
    .map(concatByteStrings(_, endl))
    .async


  val result = if (Opts.ingest()) {

    import scala.concurrent.duration._

    infotonSource.map(_ -> None).via(DownloaderStats(format = "ntriples")).map( _._1 -> None)
      .groupedWeightedWithin((25*1024), 10.seconds)(_._1.size)
      .via(Ingester.ingesterFlow(baseUrl = Opts.dstHost(),
        format = SparqlProcessor.format,
        writeToken = Opts.writeToken.toOption,
        extractContext = (a:Any) => IngesterRuntimeConfig(true)
      ))
      .map { d => d._1}
      .async
      .via(IngesterStats(isStderr = true))
      .runWith(Sink.ignore)

  } else {
    // display statistics of received infotons
    infotonSource
      .map { infoton =>
        // scalastyle:off
        println(infoton.utf8String); infoton
        // scalastyle:on
      } // print to stdout
      .map(_ -> None)
      .via(DownloaderStats(format = "ntriples", isStderr = true))
      .runWith(Sink.ignore)
  }

  result.onComplete { x =>
    val time = (System.currentTimeMillis() - start) / 1000.0
    System.err.println(s"\nended with status: $x")
    System.err.println(s"\ntotal execution time: $time seconds")
    cleanup()
  }
}
