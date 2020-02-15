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
package cmwell.util.stream

import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import scala.util.control.NonFatal

case class TakeWeighted[T](maxWeight: Long, inclusive: Boolean = true, costFn: T => Long)
    extends GraphStage[FlowShape[T, T]] {
  val in: Inlet[T] = Inlet[T]("TakeWeighted.in")
  val out: Outlet[T] = Outlet[T]("TakeWeighted.out")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler with InHandler {

      private[this] var sum = 0L

      private def decider =
        inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

      override def onPull(): Unit = pull(in)

      override def onPush(): Unit = {
        try {
          val elem = grab(in)
          val cost = costFn(elem)
          if (cost + sum < maxWeight) {
            sum += cost
            push(out, elem)
          } else {
            if (inclusive || cost + sum == maxWeight) {
              push(out, elem)
            }
            completeStage()
          }
        } catch {
          case NonFatal(ex) ⇒
            decider(ex) match {
              case Supervision.Stop ⇒ failStage(ex)
              case _ ⇒ pull(in)
            }
        }
      }

      setHandlers(in, out, this)
    }
}
