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
package cmwell.tools.data.downloader

import java.nio.file.{Files, Paths}

import akka.stream.scaladsl.Sink
import cmwell.tools.data.downloader.consumer.Downloader
import cmwell.tools.data.utils.ArgsManipulations._
import cmwell.tools.data.utils.akka.Implicits._
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.akka.stats.DownloaderStats
import cmwell.tools.data.utils.chunkers.GroupChunker
import cmwell.tools.data.utils.ops._
import nl.grons.metrics4.scala.InstrumentedBuilder
import org.rogach.scallop.ScallopConf

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

object ConsumerMain extends App with InstrumentedBuilder {

  object Opts extends ScallopConf(args) {
    version(s"cm-well downloader ${getVersionFromManifest()} (c) 2015")
    banner(""" |usage: --host <HOST> """.stripMargin)
    footer("..........................................")

    val host = opt[String]("host", descr = "cm-well host name server", required = true)
    val path = opt[String]("path", short = 'p', descr = "path in cm-well", default = Some("/"))
    val params = opt[String]("params", descr = "params string in cm-well", default = Some(""))
    val qp = opt[String]("qp", descr = "query params in cm-well", default = Some(""))
    val recursive =
      opt[Boolean]("recursive", short = 'r', descr = "flag to get download data recursively", default = Some(false))
    val format = opt[String](
      "format",
      descr = "desired record format (i.e., json, jsonld, jsonldq, n3, ntriples, nquads, trig, rdfxml)",
      default = Some("trig")
    )
    val state = opt[String]("state", short = 's', descr = "position state file")
    val follow = opt[String](
      "follow",
      short = 'f',
      descr = "continue consumption data after given update frequency (i.e., 5.seconds, 10.minutes etc.)"
    )
    val bulk = opt[Boolean]("bulk", default = Some(false), descr = "use bulk consumer mode in download")
    val numConnections = opt[Int]("num-connections", descr = "number of http connections to open")
    val indexTime = opt[Long]("index-time", descr = "index-time lower bound", default = Some(0))

    verify()
  }

  val stateFilePath = Opts.state.toOption.map(Paths.get(_))

  // resize akka http connection pool
  Opts.numConnections.toOption.map { numConnections =>
    System.setProperty("akka.http.host-connection-pool.max-connections", numConnections.toString)
  }

  val metricRegistry = new com.codahale.metrics.MetricRegistry()
  val metricDownloading = metrics.timer("consuming")
  val totalDownloadedBytes = metrics.counter("received-bytes")
  var bytesPerToken = 0L
  val bytesPerTokenMeter = metrics.meter("bytes-per-token")
  var bytesInWindow = 0L
  val metricRateBytes = metrics.meter("rate-bytes")
  var nextTimeToReport = 0L
  var lastTime = 0L
  var lastMessageSize = 0

  // check if input contains a valid state file which contains initial token
  val initToken = if (stateFilePath.isEmpty || !stateFilePath.get.toFile.exists()) {
    None
  } else {
    Option(scala.io.Source.fromFile(stateFilePath.get.toString).mkString)
  }

  var lastToken: Option[String] = None

  val start = System.currentTimeMillis()

  val tokenToQuery = lastToken match {
    case Some(t) => lastToken
    case None    => initToken
  }

  // extract update frequency if was requested to follow
  val updateFreq = Opts.follow.toOption.map { duration =>
    val d = Duration(duration)
    FiniteDuration(d.length, d.unit)
  }

  val graph = Downloader.createDataSource(
    baseUrl = formatHost(Opts.host()),
    path = formatPath(Opts.path()),
    params = Opts.params(),
    qp = Opts.qp(),
    recursive = Opts.recursive(),
    format = Opts.format(),
    isBulk = Opts.bulk(),
    token = tokenToQuery,
    updateFreq = updateFreq,
    indexTime = Opts.indexTime()
  )

  val result = graph
    .map {
      case (token, data) =>
        if (Some(token) != lastToken) {
          // save new token in state file
          stateFilePath.foreach { path =>
            Files.write(path, token.getBytes("UTF-8"))
          }
          lastToken = Some(token)
        }
        data
    }
    .via(GroupChunker(GroupChunker.formatToGroupExtractor(Opts.format())))
    .map(concatByteStrings(_, endl))
    .map { infoton =>
      // scalastyle:off
      println(infoton.utf8String)
      // scalastyle:on
      infoton
    } // print to stdout
    .map(_ -> None)
    .via(DownloaderStats(format = Opts.format(), isStderr = true))
    .runWith(Sink.ignore)

  result.onComplete { x =>
    System.err.println(s"finished: $x")
    cleanup()
  }
}
