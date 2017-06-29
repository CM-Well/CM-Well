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
import akka.actor.Cancellable
import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import cmwell.tools.data.ingester.Ingester._
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.tools.data.utils.text.Files.toHumanReadable
import nl.grons.metrics.scala.InstrumentedBuilder
import org.apache.commons.lang3.time.DurationFormatUtils

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

object IngesterStatsSink {
  def apply(isStderr: Boolean = false,
            initDelay: FiniteDuration = 1.second,
            interval: FiniteDuration = 1.second,
            label: Option[String] = None) = new IngesterStatsSink(isStderr, initDelay, interval, label)
}

class IngesterStatsSink(isStderr: Boolean,
                        initDelay: FiniteDuration = 1.second,
                        interval: FiniteDuration = 1.second,
                        label: Option[String] = None) extends GraphStageWithMaterializedValue[SinkShape[IngestEvent], Future[Done]] with DataToolsLogging{
  val in: Inlet[IngestEvent] = Inlet("ingest-stats-sink")
  override val shape: SinkShape[IngestEvent] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()

    val logic = new GraphStageLogic(shape) with InstrumentedBuilder {
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
        asyncCB = getAsyncCallback{ _ =>
          displayStats()
          resetStats()
        }

        eventPoller = Some(materializer.schedulePeriodically(initDelay, interval, new Runnable() {def run() = asyncCB.invoke(())}))

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

          logger debug (s"$name $message")

          lastMessageSize = message.size
        } catch {
          case x => logger.error(s"error: $x", x)
        }
      }

      def aggregateStats(ingestEvent: IngestEvent) = ingestEvent match {
        case IngestSuccessEvent(sizeInBytes, numInfotons) =>
          totalIngestedBytes mark sizeInBytes
          totalIngestedInfotons mark numInfotons
          ingestedBytesInWindow += sizeInBytes
        case IngestFailEvent(numInfotons) =>
          totalFailedInfotons mark numInfotons
      }


      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val element = grab(in)
          aggregateStats(element)
          pull(in)
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          failStage(ex)
          eventPoller.foreach(_.cancel())
          promise.failure(ex)
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
          promise.success(Done)
          eventPoller.foreach(_.cancel())
        }
      })
    }

    (logic, promise.future)
  }
}
