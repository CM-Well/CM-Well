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

import cmwell.tools.data.downloader.streams.Downloader
import cmwell.tools.data.utils.ArgsManipulations._
import cmwell.tools.data.utils.akka.Implicits._
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.ops._
import cmwell.tools.data.utils.text.Files._
import nl.grons.metrics4.scala._
import org.apache.commons.lang3.time.DurationFormatUtils
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object StreamsMain extends App with InstrumentedBuilder {
  object Opts extends ScallopConf(args) {
    version(s"cm-well downloader ${getVersionFromManifest()} (c) 2015")

    val host = opt[String]("host", descr = "cm-well host name server", required = true)
    val path = opt[String]("path", short = 'p', descr = "path in cm-well", default = Some("/"))
    val recursive =
      opt[Boolean]("recursive", short = 'r', default = Some(false), descr = "flag to get download data recursively")
    val length = opt[String]("length",
                             short = 'l',
                             descr = "max number of records to download (i.e., 1, 100, all)",
                             default = Some("50"))
    val format = opt[String](
      "format",
      short = 'f',
      descr = "desired record format (i.e., json, jsonld, jsonldq, n3, ntriples, nquads, trig, rdfxml)",
      default = Some("trig")
    )
    val params = opt[String]("params", descr = "params string in cm-well", default = Some(""))
    val op = opt[String]("op", descr = "operation type (stream, nstream, mstream, sstream)", default = Some("stream"))
    val qp = opt[String]("qp", descr = "query params in cm-well", default = Some(""))
    val fromUuids = opt[Boolean]("from-uuids",
                                 descr = "download data from uuids input stream provided by stdin",
                                 default = Some(false))
    val fromPaths = opt[Boolean]("from-paths",
                                 descr = "download data from paths input stream provided by stdin",
                                 default = Some(false))
    val fromQuery = opt[Boolean]("from-query", descr = "download data from query to cm-well", default = Some(false))
    val numConnections = opt[Int]("num-connections", descr = "number of http connections to open")

    mutuallyExclusive(fromUuids, fromPaths)
    conflicts(fromUuids, List(path, recursive, length, params, op, qp))
    conflicts(fromPaths, List(path, recursive, length, params, op, qp))

    verify()
  }

  val allowedOps = Set("stream", "nstream", "mstream", "sstream")

  if (!allowedOps.contains(Opts.op())) {
    Opts.printHelp()
    System.exit(1)
  }

  // resize akka http connection pool
  Opts.numConnections.toOption.map { numConnections =>
    System.setProperty("akka.http.host-connection-pool.max-connections", numConnections.toString)
  }

  val length = Opts.length() match {
    case "ALL" | "all" | "All" => None
    case num                   => Some(num.toInt)
  }

  val metricRegistry = new com.codahale.metrics.MetricRegistry()

  val metricDownloading = metrics.timer("downloading")
  val totalDownloadedBytes = metrics.counter("received-bytes")
  var bytesInWindow = 0L
  val metricRateBytes = metrics.meter("rate-bytes")

  var nextTimeToReport = 0L

  val start = System.currentTimeMillis()

  // check which download function to execute
  val source = if (Opts.fromUuids()) {
    // download from uuids
    Downloader.createSourceFromUuidInputStream(baseUrl = formatHost(Opts.host()),
                                               format = Opts.format(),
                                               in = System.in)
  } else if (Opts.fromPaths()) {
    Downloader.createSourceFromPathsInputStream(baseUrl = formatHost(Opts.host()),
                                                format = Opts.format(),
                                                in = System.in)
  } else {
    // download from query
    Downloader.createSourceFromQuery(
      baseUrl = formatHost(Opts.host()),
      path = formatPath(Opts.path()),
      params = Opts.params(),
      qp = Opts.qp(),
      format = Opts.format(),
      op = Opts.op(),
      length = length,
      recursive = Opts.recursive()
    )
  }

  var lastTime = 0L
  var lastMessageSize = 0

  val result = source.runForeach { data =>
    // print data to standard output
    print(data.utf8String)

    // calculate download rate and statistics
    val bytesRead = data.size
    bytesInWindow += bytesRead
    totalDownloadedBytes += bytesRead
    metricRateBytes.mark(bytesRead)

    val now = System.currentTimeMillis()

    // throttle statistics report messages
    if (now > nextTimeToReport) {
      val rate = toHumanReadable(bytesInWindow * 1000 / (now - lastTime))
      val message =
        s"received=${toHumanReadable(totalDownloadedBytes.count)}".padTo(20, ' ') +
          s"mean rate=${toHumanReadable(metricRateBytes.meanRate)}/sec".padTo(30, ' ') +
//          s"rate=${toHumanReadable(metricRateBytes.oneMinuteRate)}/sec".padTo(30, ' ') +
          s"rate =${rate}/sec".padTo(30, ' ') +
          s"[${DurationFormatUtils.formatDurationWords(now - start, true, true)}]"

      System.err.print("\r" * lastMessageSize + message)
      nextTimeToReport = now + 1000
      bytesInWindow = 0
      lastTime = now
      lastMessageSize = message.size
    }
  }

  result.onComplete { x =>
    val time = (System.currentTimeMillis() - start) / 1000.0
    System.err.println(s"\ntotal execution time: $time seconds")
    System.err.println("status: " + x)
    cleanup()

    x match {
      case Success(v)   => LoggerFactory.getLogger(getClass).info("value: " + v)
      case Failure(err) => LoggerFactory.getLogger(getClass).error("error: ", err)
    }
  }
}
