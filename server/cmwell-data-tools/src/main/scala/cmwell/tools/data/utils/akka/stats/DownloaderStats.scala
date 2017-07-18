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


package cmwell.tools.data.utils.akka.stats

import akka.Done
import akka.actor.ActorRef
import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.util.ByteString
import cmwell.tools.data.utils.akka.stats.DownloaderStatsSink.DownloadStats
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.tools.data.utils.text.Files.toHumanReadable
import nl.grons.metrics.scala.InstrumentedBuilder
import org.apache.commons.lang3.time.DurationFormatUtils
import play.api.libs.json.{JsArray, Json}
import akka.actor._
import akka.pattern._

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.Try

object DownloaderStatsSink {
  case class DownloadStats(label: Option[String] = None,
                           receivedBytes: Long,
                           receivedInfotons: Long,
                           infotonRate: Double,
                           bytesRate: Double,
                           runningTime: Long,
                           statsTime: Long)

  def apply(isStderr: Boolean = false,
            format: String,
            label: Option[String] = None,
            reporter: Option[ActorRef] = None,
            initDelay: FiniteDuration = 1.second,
            interval: FiniteDuration = 1.second) = {

    new DownloaderStatsSink(isStderr, format, label, reporter, initDelay, interval)
  }
}

class DownloaderStatsSink(isStderr: Boolean,
                          format: String,
                          label: Option[String] = None,
                          reporter: Option[ActorRef] = None,
                          initDelay: FiniteDuration = 1.second,
                          interval: FiniteDuration = 1.second) extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Done]] with DataToolsLogging {

  val in: Inlet[ByteString] = Inlet("download-stats-sink")
  override val shape: SinkShape[ByteString] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()

    val logic = new GraphStageLogic(shape) with InstrumentedBuilder{
      val metricRegistry = new com.codahale.metrics.MetricRegistry()
      val totalDownloadedBytes = metrics.counter("received-bytes")
      val totalReceivedInfotons = metrics.meter("received-infotons")
      var bytesInWindow = 0L
      val metricRateBytes = metrics.meter("rate-bytes")
      var nextTimeToReport = 0L
      var lastTime = 0L
      var lastMessageSize = 0
      var timeOfLastStatistics = 0L

      var eventPoller: Option[Cancellable] = None

      val name = label.fold("")(name => s"[$name]")

      private var asyncCB: AsyncCallback[Unit] = _

      val start = System.currentTimeMillis()

      val formatter = java.text.NumberFormat.getNumberInstance

      override def preStart(): Unit = {
        asyncCB = getAsyncCallback{ _ =>
          displayStats()
          resetStatsInWindow()
        }

        eventPoller = Some(materializer.schedulePeriodically(initDelay, interval, new Runnable() {def run() = asyncCB.invoke(())}))

        pull(in)
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val element = grab(in)
          aggregateStats(element)
          pull(in)
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          eventPoller.foreach(_.cancel())
          promise.failure(ex)
          failStage(ex)
        }

        override def onUpstreamFinish(): Unit = {
          val now = System.currentTimeMillis()

          val message =
            s"received=${toHumanReadable(totalDownloadedBytes.count)}".padTo(20, ' ') +
              s"infotons=${formatter.format(totalReceivedInfotons.count)}".padTo(30, ' ') +
              s"infoton rate=${formatter.format(totalReceivedInfotons.meanRate)}/sec".padTo(30, ' ') +
              s"mean rate=${toHumanReadable(metricRateBytes.meanRate)}/sec".padTo(30, ' ') +
              s"[${DurationFormatUtils.formatDurationWords(now - start, true, true)}]"

          System.err.println("")
          System.err.println(message)

          promise.success(Done)
          eventPoller.foreach(_.cancel())
          completeStage()
        }

      })

      def resetStatsInWindow() = {
        bytesInWindow = 0
      }

      def countInfotonsInChunk(data: ByteString) = {
        def countInfotonsInJson(data: ByteString) = {
          val infotons = Json.parse(data.toArray) \\ "infotons"
          infotons.head match {
            case JsArray(arr) => arr.size
            case _            => 1
          }
        }

        format match {
          case "json" => countInfotonsInJson(data)
          case _      => 1
        }
      }

      def aggregateStats(data: ByteString) = {
        val bytesRead = data.size
        bytesInWindow += bytesRead
        totalDownloadedBytes += bytesRead
        metricRateBytes mark bytesRead
        totalReceivedInfotons mark countInfotonsInChunk(data)
        timeOfLastStatistics = System.currentTimeMillis()
      }

      def displayStats() = {
        val now = System.currentTimeMillis()
        if (now - lastTime > 0) {
          try {
            val rate = toHumanReadable(bytesInWindow * 1000 / (now - lastTime))
            val executionTime = now - start
            val message =
              s"[received=${toHumanReadable(totalDownloadedBytes.count)}]".padTo(20, ' ') +
                s"[infotons=${formatter.format(totalReceivedInfotons.count)}".padTo(30, ' ') +
                s"infoton rate=${formatter.format(totalReceivedInfotons.oneMinuteRate)}/sec]".padTo(30, ' ') +
                s"[mean rate=${toHumanReadable(metricRateBytes.meanRate)}/sec".padTo(25, ' ') +
                s"rate=${rate}/sec]".padTo(24, ' ') +
                s"[${DurationFormatUtils.formatDurationWords(executionTime, true, true)}]"

            if (isStderr) System.err.print("\r" * lastMessageSize + message)

            logger debug (s"$name $message")

            reporter.foreach {
              _ ! DownloadStats(
                label = label,
                receivedBytes = totalDownloadedBytes.count,
                receivedInfotons = totalReceivedInfotons.count,
                infotonRate = totalReceivedInfotons.oneMinuteRate,
                bytesRate = metricRateBytes.oneMinuteRate,
                runningTime = executionTime,
                statsTime = timeOfLastStatistics
              )
            }

            lastTime = now
            lastMessageSize = message.size
          } catch {
            case x => logger.error(s"error: $x", x)
          }
        }
      }
    }

    (logic, promise.future)
  }
}