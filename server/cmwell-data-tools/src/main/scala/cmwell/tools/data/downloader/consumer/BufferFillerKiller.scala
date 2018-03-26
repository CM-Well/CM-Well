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
package cmwell.tools.data.downloader.consumer

import akka.actor.{ActorRef, PoisonPill}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

/**
  * GraphStage needed to kill [[BufferFillerActor]].
  * Should be inserted at the end of the Graph.
  */
object BufferFillerKiller {
  def apply[T](bufferFiller: ActorRef) = new BufferFillerKiller[T](bufferFiller)
}

class BufferFillerKiller[T](bufferFiller: ActorRef) extends GraphStage[FlowShape[T, T]] {
  val in = Inlet[T]("Map.in")
  val out = Outlet[T]("Map.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            if (isAvailable(out))
              push(out, (grab(in)))
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            ex.printStackTrace()
            bufferFiller ! PoisonPill
            super.onUpstreamFailure(ex)
          }

          override def onUpstreamFinish(): Unit = {
            bufferFiller ! PoisonPill
            super.onUpstreamFinish()
          }

        }
      )

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            if (!hasBeenPulled(in))
              pull(in)
          }

          override def onDownstreamFinish(): Unit = {
            bufferFiller ! PoisonPill
            super.onDownstreamFinish()
          }
        }
      )
    }
  }
}
