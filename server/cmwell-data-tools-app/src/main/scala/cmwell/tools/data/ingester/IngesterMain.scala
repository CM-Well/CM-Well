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

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

import cmwell.tools.data.utils.akka.stats.IngesterStatsSink
//import cmwell.tools.data.sparql.SparqlProcessorMain.Opts.opt
import cmwell.tools.data.utils.ArgsManipulations._
import cmwell.tools.data.utils.akka.Implicits._
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.ops._
import com.typesafe.scalalogging.LazyLogging
import org.rogach.scallop.ScallopConf

import scala.concurrent.ExecutionContext.Implicits.global

object IngesterMain extends App with LazyLogging {
  object Opts extends ScallopConf(args) {
    version(s"cm-well ingester ${getVersionFromManifest()} (c) 2015")

    val host = opt[String]("host",      descr = "cm-well host name", required = true)
    val format = opt[String]("format",  descr = "input format (e.g. ntriples, nquads, jsonld)", required = true)
    val file = opt[String]("file",      descr = "input file path", default = None)
    val gzip  = opt[Boolean]("gzip",   descr = "is input file gzipped", default = Some(false))
    val token = opt[String]("token",    descr = "cm-well write permission token", default = None)
    val replaceMode = opt[Boolean]("with-replace-mode", descr = "replace-mode parameter in cm-well", default = Some(false))
    val force = opt[Boolean]("force", descr = "force parameter in cm-well", default = Some(false))
    val backpressure = opt[Int]("backpressure", descr = "max number of requests to be send (backpressure)", default = Some(100))
    val parallelism = opt[Int]("parallelism", descr = "number of workers sending parallel requests", default = Some(4))
    val numConnections = opt[Int]("num-connections", descr = "number of http connections to open")

    dependsOnAll(gzip, List(file))
    verify()
  }

  val start = System.currentTimeMillis()

  var totalIngestedBytes = 0L
  var ingestedBytesInWindow = 0
  var ingestedInfotonsInWindow = 0
  var totalIngestedInfotons = 0L
  var totalFailedInfotons = 0L
  var lastTime = start
  var nextPrint = 0L
  var lastMessageSize = 0
  val windowSizeMillis = 1000

  val formatter = java.text.NumberFormat.getNumberInstance

  // resize akka http connection pool
  Opts.numConnections.toOption.map { numConnections =>
    System.setProperty("akka.http.host-connection-pool.max-connections", numConnections.toString)
  }

  val inputStream = if (Opts.file.isSupplied) {
    val inputFile = new FileInputStream(Opts.file())
    if (Opts.gzip()) {
      new GZIPInputStream(inputFile)
    } else {
      inputFile
    }
  } else {
    System.in
  }

  val result = Ingester.fromInputStream(
    baseUrl = formatHost( Opts.host() ),
    format = Opts.format(),
    parallelism = Opts.parallelism(),
    writeToken = Opts.token.toOption,
    replaceMode = Opts.replaceMode(),
    force = Opts.force(),
    in = inputStream
  ).runWith(IngesterStatsSink(isStderr = true))


  // actor system is still alive, will be destroyed when finished
  result.onComplete { x =>
    System.err.println("\n")
    System.err.println(s"finished: $x")
    cleanup()
  }
}
