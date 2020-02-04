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

package cmwell.crawler

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.typesafe.scalalogging.Logger

object CrawlerRatePrinter {
  def apply(crawlerId: String,
            printFrequency: Int,
            maxPrintRateMillis: Int)(logger: Logger): CrawlerRatePrinter =
    new CrawlerRatePrinter(crawlerId, printFrequency, maxPrintRateMillis)(logger)
}

class CrawlerRatePrinter(crawlerId: String,
                         printFrequency: Int,
                         maxPrintRateMillis: Int)(logger: Logger) extends GraphStage[FlowShape[Long, Long]] {
  val in = Inlet[Long]("CrawlerRatePrinter.in")
  val out = Outlet[Long]("CrawlerRatePrinter.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val startTime: Double = System.currentTimeMillis
      private var totalElementsGot: Long = 0L
      private var printedAtElementNo: Long = 0L
      private var printedAtTime: Double = startTime
      private var localStartTime: Double = startTime
      private var localTotalElementsGot: Long = 0L
      private var localRate: Double = 0

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            totalElementsGot += 1
            localTotalElementsGot += 1
            if (totalElementsGot - printedAtElementNo >= printFrequency) {
              val currentTime = System.currentTimeMillis
              if (currentTime - printedAtTime > maxPrintRateMillis) {
                val rate = totalElementsGot / (currentTime - startTime) * 1000
                if (currentTime - localStartTime > 15000) {
                  localRate = localTotalElementsGot / (currentTime - localStartTime) * 1000
                  localTotalElementsGot = 0
                  localStartTime = currentTime
                }
                logger.info(s"$crawlerId Current offset is $elem. Total $totalElementsGot offsets already processed. " +
                s"Read rate: avg: ${rate.formatted("%.2f")} current: ${localRate.formatted("%.2f")} offsets/second")
                printedAtElementNo = totalElementsGot
                printedAtTime = currentTime
              }
            }
            push(out, elem)
          }
        }
      )
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}
