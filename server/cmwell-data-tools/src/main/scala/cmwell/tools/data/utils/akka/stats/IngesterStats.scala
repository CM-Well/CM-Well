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
package cmwell.tools.data.utils.akka.stats

import akka.actor.{ActorRef, Cancellable}
import akka.stream._
import akka.stream.stage._
import cmwell.tools.data.ingester.Ingester._
import cmwell.tools.data.utils.akka.stats.IngesterStats.IngestStats
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.tools.data.utils.text.Files.toHumanReadable
import nl.grons.metrics4.scala.InstrumentedBuilder
import org.apache.commons.lang3.time.DurationFormatUtils

import scala.concurrent.duration._

object IngesterStats {

  case class IngestStats(label: Option[String] = None,
                         ingestedBytes: Long = 0,
                         ingestedInfotons: Long = 0,
                         failedInfotons: Long = 0)

  object IngestStats{
    def initialStats[T](initialIngestStatsOpt : Option[IngestStats], agentName: String, elem: T) = initialIngestStatsOpt match {
      case Some(stats) => (stats.copy(label = Some(agentName)), elem)
      case None => (IngestStats(label = Some(agentName)), elem)
    }
  }


  def apply(isStderr: Boolean = false,
            initDelay: FiniteDuration = 1.second,
            interval: FiniteDuration = 1.second,
            reporter: Option[ActorRef] = None,
            label: Option[String] = None,
            initialIngestStats: Option[IngestStats] = None) =
    new IngesterStats(isStderr, initDelay, interval, reporter, label, initialIngestStats)
}

class IngesterStats(isStderr: Boolean,
                    initDelay: FiniteDuration = 1.second,
                    interval: FiniteDuration = 1.second,
                    reporter: Option[ActorRef] = None,
                    label: Option[String] = None,
                    initialIngestStats: Option[IngestStats] = None)
    extends GraphStage[FlowShape[IngestEvent, IngestEvent]]
    with DataToolsLogging {
  val in = Inlet[IngestEvent]("ingest-stats.in")
  val out = Outlet[IngestEvent]("ingest-stats.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) with InstrumentedBuilder {
        val start = System.currentTimeMillis()
        val metricRegistry = new com.codahale.metrics.MetricRegistry()
        var totalIngestedBytes = metrics.meter("total-ingested-bytes")
        var totalIngestedInfotons = metrics.meter("total-ingested-infotons")
        var totalFailedInfotons = metrics.meter("total-failed-infotons")
        var ingestedBytesInWindow = 0L
        var lastTime = start
        var nextPrint = 0L
        var lastMessageSize = 0
        val windowSizeMillis = 1000

        var eventPoller: Option[Cancellable] = None

        val name = label.fold("")(name => s"[$name]")

        private var asyncCB: AsyncCallback[Unit] = _

        val formatter = java.text.NumberFormat.getNumberInstance

        override def preStart(): Unit = {

          // Initialise persisted statistics
          initialIngestStats.foreach { stats =>
            logger.debug(s"${name} Loading statistics initial state of Ingested Infotons: ${stats.ingestedInfotons}," +
              s" Failed Infotons ${stats.failedInfotons}")
            totalIngestedInfotons.mark(stats.ingestedInfotons)
            totalFailedInfotons.mark(stats.failedInfotons)
          }

          asyncCB = getAsyncCallback { _ =>
            displayStats()
            resetStats()
          }

          eventPoller = Some(materializer.schedulePeriodically(initDelay, interval, new Runnable() {
            def run() = asyncCB.invoke(())
          }))

          pull(in)
        }

        def resetStats() = {
          ingestedBytesInWindow = 0
        }

        def displayStats() = {
          try {
            val now = System.currentTimeMillis()

            // print output message
            val message =
              s"[ingested: ${toHumanReadable(totalIngestedBytes.count)}]    " +
                s"[ingested infotons: ${formatter.format(totalIngestedInfotons.count)}   " +
                s"${formatter.format(totalIngestedInfotons.oneMinuteRate)}/sec]   " +
                s"[failed infotons: ${formatter.format(totalFailedInfotons.count)}]    ".padTo(25, ' ') +
                s"[rate=${toHumanReadable(totalIngestedBytes.oneMinuteRate)}/sec    ".padTo(20, ' ') +
                s"average rate=${toHumanReadable(totalIngestedBytes.meanRate)}/sec]    ".padTo(30, ' ') +
                s"[${DurationFormatUtils.formatDurationWords(now - start, true, true)}]        "

            if (isStderr) System.err.print("\r" * lastMessageSize + message)

            reporter.foreach {
              _ ! IngestStats(
                label = label,
                ingestedBytes = totalIngestedBytes.count,
                ingestedInfotons = totalIngestedInfotons.count,
                failedInfotons = totalFailedInfotons.count
              )
            }

            logger.debug(s"$name $message")

            lastMessageSize = message.size
          } catch {
            case x :Throwable => logger.error(s"error: $x", x)
          }
        }

        def aggregateStats(ingestEvent: IngestEvent) = ingestEvent match {
          case IngestSuccessEvent(sizeInBytes, numInfotons) =>
            totalIngestedBytes.mark(sizeInBytes)
            totalIngestedInfotons.mark(numInfotons)
            ingestedBytesInWindow += sizeInBytes
          case IngestFailEvent(numInfotons) =>
            totalFailedInfotons.mark(numInfotons)
        }

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val element = grab(in)
              aggregateStats(element)
              pull(in)
            }

            override def onUpstreamFailure(ex: Throwable): Unit = {
              failStage(ex)
              eventPoller.foreach(_.cancel())
            }

            override def onUpstreamFinish(): Unit = {
              val now = System.currentTimeMillis()

              val message =
                s"ingested: ${toHumanReadable(totalIngestedBytes.count)}    " +
                  s"ingested infotons: ${formatter.format(totalIngestedInfotons.count)}".padTo(30, ' ') +
                  s"failed infotons: ${formatter.format(totalFailedInfotons.count)}".padTo(25, ' ') +
                  s" average rate=${totalIngestedBytes.meanRate}/sec".padTo(30, ' ') +
                  s"[${DurationFormatUtils.formatDurationWords(now - start, true, true)}]        "

              System.err.println("")
              System.err.println(message)

              completeStage()
              eventPoller.foreach(_.cancel())
            }
          }
        )

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            if (!hasBeenPulled(in)) pull(in)
          }
        })
      }
  }
}
