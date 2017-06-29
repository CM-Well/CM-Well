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


package cmwell.util.stream

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

class DetectCompletion[Elem](onComplete: () => Unit, onFailure: Throwable => Unit, onDownstreamComplete: () => Unit) extends GraphStage[FlowShape[Elem, Elem]] {

  private val in = Inlet[Elem]("DetectCompletion.in")
  private val out = Outlet[Elem]("DetectCompletion.out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(in, new InHandler {
      override def onPush(): Unit = push(out, grab(in))

      override def onUpstreamFailure(ex: Throwable): Unit = {
        onFailure(ex)
        super.onUpstreamFailure(ex)
      }

      override def onUpstreamFinish(): Unit = {
        onComplete()
        super.onUpstreamFinish()
      }
    })

    setHandler(out, new OutHandler{
      override def onPull(): Unit = pull(in)

      override def onDownstreamFinish(): Unit = {
        onDownstreamComplete()
        super.onDownstreamFinish()
      }
    })
  }
}
