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

package akka.stream.contrib

import akka.stream.scaladsl.GraphDSL
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, BidiShape, FlowShape, Graph, Inlet, Outlet}

/**
 *
 */
object BufferLimiter {

  /**
   * Creates a flow that will limit the maximum available elements inside the inner flow
   * NOTE: The inner flow must be 1:1 flow. Which means for each element got into it there will be exactly one element emitted.
   *       Failing to follow this will result in an unexpected behaviour.
   * @param maxBuffer The maximum allowed elements in the inner flow
   * @param flow The inner flow wrapped
   * @return The inner flow wrapped with the limiter
   */
  def apply[I, O, M](maxBuffer: Int, flow: Graph[FlowShape[I, O], M]): Graph[FlowShape[I, O], M] =
    GraphDSL.create(flow) { implicit b =>
      flow =>
        import GraphDSL.Implicits._
        val limiter = b.add(new BufferLimiterCoordinator[I, O](maxBuffer))
        limiter.out2 ~> flow ~> limiter.in2
        FlowShape(limiter.in1, limiter.out1)
    }

  private[akka] class BufferLimiterCoordinator[I, O](maxBuffer: Int)
    extends GraphStage[BidiShape[I, O, O, I]] {
    val mainIn: Inlet[I] = Inlet[I]("BufferLimiter.mainIn")
    val mainOut: Outlet[O] = Outlet[O]("BufferLimiter.mainOut")
    val innerOut: Inlet[O] = Inlet[O]("BufferLimiter.innerOut")
    val innerIn: Outlet[I] = Outlet[I]("BufferLimiter.innerIn")
    override val shape: BidiShape[I, O, O, I] = BidiShape[I, O, O, I](mainIn, mainOut, innerOut, innerIn)

    override def createLogic(attributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      var bufferedElementCount = 0

      //input from the main upstream
      val mainInHandler: InHandler = new InHandler {
        override def onPush(): Unit = {
          push(innerIn, grab(mainIn))
          bufferedElementCount += 1
        }

        override def onUpstreamFinish(): Unit = {
          //When upstream finishes don't complete the stage yet. Just complete the input port of the inner flow.
          //Only when the inner flow finishes complete the whole stage.
          complete(innerIn)
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          //In case of an upstream failure - complete the whole stage. It might be that buffered elements in the inner flow will be lost.
          super.onUpstreamFailure(ex)
        }
      }
      //output to the main downstream
      val mainOutHandler: OutHandler = new OutHandler {
        override def onPull(): Unit =
          if(!hasBeenPulled(innerOut) && !isClosed(innerOut))
            pull(innerOut)

        override def onDownstreamFinish(): Unit = super.onDownstreamFinish()
      }
      //output of the inner flow
      val innerFlowOutHandler: InHandler = new InHandler {
        override def onPush(): Unit = {
          //Element will be pushed only if there was already a pull from out1. Hence, PUSHED state of out1 is not reasonable.
          push(mainOut, grab(innerOut))
          //In case the inner (which is buffer like) flow asked an element and it reached the maximum the limiter won't ask another element from the upstream.
          //Now, one element is sent to the downstream so another element can be pulled from the upstream.
          //But it's only if the inner stream asked for an element (hence the isAvailable(out2)) and the upstream is willing to accept requests
          //(hence the !hasBeenPulled and the !isClosed). The PUSHED state of in1 (see https://doc.akka.io/docs/akka/current/stream/stream-customize.html)
          //is not reasonable because for each push event there is a grab without any condition.
          if(isAvailable(innerIn) && !hasBeenPulled(mainIn) && !isClosed(mainIn))
            pull(mainIn)
          bufferedElementCount -= 1
        }

        override def onUpstreamFinish(): Unit = super.onUpstreamFinish()

        override def onUpstreamFailure(ex: Throwable): Unit = super.onUpstreamFailure(ex)
      }

      //input of the internal flow
      val innerFlowInHandler: OutHandler = new OutHandler {
        override def onPull(): Unit = {
          //If the maximum hadn't reached and the upstream is ready to accept elements - pull from the upstream.
          //The upstream might finish already but not the inner flow (that has buffer in it) so this stage is not finished
          //and still the inner flow can ask for more elements (hence the checks of in1)
          if (bufferedElementCount < maxBuffer && !hasBeenPulled(mainIn) && !isClosed(mainIn))
            pull(mainIn)
        }

        override def onDownstreamFinish(): Unit = {
          //The inner stream won't accept any more elements but it can still flush its buffer - do nothing.
        }
      }

      setHandler(mainIn, mainInHandler)
      setHandler(mainOut, mainOutHandler)
      setHandler(innerOut, innerFlowOutHandler)
      setHandler(innerIn, innerFlowInHandler)
    }

  }

}
