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

class MapInitAndLast[In, Out](init: In ⇒ Out, last: In ⇒ Out) extends GraphStage[FlowShape[In, Out]] {
  val in: Inlet[In] = Inlet[In]("MapInitAndLast.in")
  val out: Outlet[Out] = Outlet[Out]("MapInitAndLast.out")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private var pending: In = null.asInstanceOf[In]

    private def decider =
      inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

    override def preStart(): Unit = tryPull(in)

    def pushWith(elem: In, f: In ⇒ Out): Unit = try { push(out, f(elem)) } catch {
      case NonFatal(ex) ⇒
        decider(ex) match {
          case Supervision.Stop ⇒ failStage(ex)
          case _ ⇒ pull(in)
        }
    }

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          if (pending != null) pushWith(pending, init)
          else if (isAvailable(out)) pull(in)

          pending = elem
        }

        override def onUpstreamFinish(): Unit = {
          if (pending == null) completeStage()
          else {
            if (isAvailable(out)) {
              pushWith(pending, last)
              completeStage()
            }
          }
        }
      }
    )

    setHandler(
      out,
      new OutHandler {
        override def onPull(): Unit = {
          if (!isClosed(in)) {
            if (!hasBeenPulled(in)) {
              pull(in)
            }
          } else {
            if (pending != null) {
              pushWith(pending, last)
            }
            completeStage()
          }
        }
      }
    )
  }
}
