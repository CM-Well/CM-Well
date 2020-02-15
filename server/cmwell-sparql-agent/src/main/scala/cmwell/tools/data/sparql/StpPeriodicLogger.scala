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

import akka.actor.ActorRef
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.pattern._
import akka.util.Timeout
import akka.stream.stage._
import cmwell.tools.data.ingester.Ingester.IngestEvent
import cmwell.tools.data.sparql.InfotonReporter.{RequestDownloadStats, RequestIngestStats, ResponseDownloadStats, ResponseIngestStats}
import cmwell.tools.data.utils.logging.DataToolsLogging

import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.concurrent.duration._

object StpPeriodicLogger {
  def apply(infotonReporter: ActorRef, logFrequency: FiniteDuration) =
    new StpPeriodicLogger(infotonReporter, logFrequency)
}

class StpPeriodicLogger(infotonReporter: ActorRef, logFrequency: FiniteDuration)
  extends GraphStage[FlowShape[IngestEvent, IngestEvent]] with DataToolsLogging {

  val in = Inlet[IngestEvent]("StpPeriodicLogger.in")
  val out = Outlet[IngestEvent]("StpPeriodicLogger.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    override def preStart(): Unit = schedulePeriodically(None, logFrequency)

    setHandler(in, new InHandler {
      override def onPush(): Unit = push(out, grab(in))
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = pull(in)
    })

    override protected def onTimer(timerKey: Any): Unit = {

      implicit val timeout : akka.util.Timeout = 5.seconds

      for {
        downloadStatsMap <- (infotonReporter ? RequestDownloadStats).mapTo[ResponseDownloadStats]
        ingestStatsObj <- (infotonReporter ? RequestIngestStats).mapTo[ResponseIngestStats]
        ingestStatsOption = ingestStatsObj.stats
      } yield {
        downloadStatsMap.stats.get(SparqlTriggeredProcessor.sparqlMaterializerLabel).foreach(materialized => {
          ingestStatsOption.foreach(ingestStats => {
            logger.info(s" *** STP Agent ${materialized.label}" +
              s" *** Materialized ${materialized.receivedInfotons}, Ingested: ${ingestStats.ingestedInfotons}, Failed: ${ingestStats.failedInfotons}")
          })
        })
      }
    }

  }

}

