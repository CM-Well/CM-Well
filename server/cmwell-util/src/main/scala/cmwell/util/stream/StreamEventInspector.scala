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

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
// format: off
class StreamEventInspector[Elem](onUpstreamFinishInspection:   ()        => Unit = () => {},
                                 onUpstreamFailureInspection:  Throwable => Unit = _  => {},
                                 onDownstreamFinishInspection: ()        => Unit = () => {},
                                 onPushInspection:             Elem      => Unit = (_: Elem)  => {},
                                 onPullInspection:             ()        => Unit = () => {}) extends GraphStage[FlowShape[Elem, Elem]] {
  // format: on
  private val in = Inlet[Elem]("StreamEventInspector.in")
  private val out = Outlet[Elem]("StreamEventInspector.out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          onPushInspection(elem)
          push(out, elem)
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          onUpstreamFailureInspection(ex)
          super.onUpstreamFailure(ex)
        }

        override def onUpstreamFinish(): Unit = {
          onUpstreamFinishInspection()
          super.onUpstreamFinish()
        }
      }
    )

    setHandler(
      out,
      new OutHandler {
        override def onPull(): Unit = {
          onPullInspection()
          pull(in)
        }

        override def onDownstreamFinish(): Unit = {
          onDownstreamFinishInspection()
          super.onDownstreamFinish()
        }
      }
    )
  }
}
