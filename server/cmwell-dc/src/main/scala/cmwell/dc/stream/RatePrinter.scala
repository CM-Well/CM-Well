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
package cmwell.dc.stream

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import cmwell.dc.LazyLogging
import cmwell.dc.stream.MessagesTypesAndExceptions.{DcInfo, DcInfoKey}

/**
  * Created by eli on 21/08/16.
  */
object RatePrinter {
  def apply[A](dcKey: DcInfoKey,
               elementCount: A => Double,
               elementText: String,
               elementsText: String,
               printFrequency: Int): RatePrinter[A] =
    new RatePrinter(dcKey, elementCount, elementText, elementsText, printFrequency)
}

class RatePrinter[A](dcKey: DcInfoKey,
                     elementCount: A => Double,
                     elementText: String,
                     elementsText: String,
                     printFrequency: Int)
    extends GraphStage[FlowShape[A, A]]
    with LazyLogging {
  val in = Inlet[A]("RatePrinter.in")
  val out = Outlet[A]("RatePrinter.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val startTime: Double = System.currentTimeMillis
      private var totalElementsGot: Double = 0
      private var printedAtElementNo: Double = 0
      private var localStartTime: Double = startTime
      private var localTotalElementsGot: Double = 0
      private var localRate: Double = 0

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            val currentTime = System.currentTimeMillis
            val currentElementsGot = elementCount(elem)
            totalElementsGot += currentElementsGot
            localTotalElementsGot += currentElementsGot
            if (totalElementsGot - printedAtElementNo >= printFrequency) {
              val rate = totalElementsGot / (currentTime - startTime) * 1000
              logger.info(
                s"Sync $dcKey: Got ${currentElementsGot.formatted("%.2f")} $elementText. Total ${totalElementsGot
                  .formatted("%.2f")} $elementsText. Read rate: avg: ${rate.formatted("%.2f")} current: ${localRate
                  .formatted("%.2f")} $elementText/second"
              )
              if (currentTime - localStartTime > 15000) {
                localRate = localTotalElementsGot / (currentTime - localStartTime) * 1000
                localTotalElementsGot = 0
                localStartTime = currentTime
              }
              printedAtElementNo = totalElementsGot
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
