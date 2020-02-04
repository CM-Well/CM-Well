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
package cmwell.tools.data.utils.akka

/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._

/**
  * Source factory methods are placed here
  */
object SourceGen {

  /**
    * Create a `Source` that will unfold a value of type `S` by
    * passing it through a flow. The flow should emit a
    * pair of the next state `S` and output elements of type `E`.
    * Source completes when the flow completes.
    */
  def unfoldFlow[S, E, M](seed: S)(flow: Graph[FlowShape[S, (S, E)], M]): Source[E, M] = {

    val fanOut2Stage = fanOut2unfoldingStage[(S, E), S, E](seed, { (handleStateElement, grabIn, completeStage) =>
      new InHandler {
        override def onPush() = {
          val (s, e) = grabIn()
          handleStateElement(s, e)
        }
      }
    })

    unfoldFlowGraph(fanOut2Stage, flow)
  }

  /**
    * Create a `Source` that will unfold a value of type `S` by
    * passing it through a flow. The flow should emit an output
    * value of type `O`, that when fed to the unfolding function,
    * generates a pair of the next state `S` and output elements of type `E`.
    */
  def unfoldFlowWith[E, S, O, M](seed: S,
                                 flow: Graph[FlowShape[S, O], M])(unfoldWith: O => Option[(S, E)]): Source[E, M] = {

    val fanOut2Stage = fanOut2unfoldingStage[O, S, E](
      seed, { (handleStateElement, grabIn, completeStage) =>
        new InHandler {
          override def onPush() = {
            val o = grabIn()
            unfoldWith(o) match {
              case None         => completeStage()
              case Some((s, e)) => handleStateElement(s, e)
            }
          }
        }
      }
    )

    unfoldFlowGraph(fanOut2Stage, flow)
  }

  private[akka] def unfoldFlowGraph[E, S, O, M](
    fanOut2Stage: GraphStage[FanOutShape2[O, S, E]],
    flow: Graph[FlowShape[S, O], M]
  ): Source[E, M] =
    Source.fromGraph(GraphDSL.create(flow) { implicit b =>
      { f =>
        {
          import GraphDSL.Implicits._

          val fo2 = b.add(fanOut2Stage)
          fo2.out0 ~> f ~> fo2.in
          SourceShape(fo2.out1)
        }
      }
    })

  private[akka] def fanOut2unfoldingStage[O, S, E](seed: S,
                                                   withInHandler: ((S, E) => Unit, () => O, () => Unit) => InHandler) =
    new GraphStage[FanOutShape2[O, S, E]] {

      override val shape = new FanOutShape2[O, S, E]("unfoldFlow")

      override def createLogic(attributes: Attributes) = {

        new GraphStageLogic(shape) {

          import shape._

          var pending: S = seed
          var pushedToCycle = false

          setHandler(in, withInHandler((s, e) => {
            pending = s
            push(out1, e)
            pushedToCycle = false
          }, () => grab(in), completeStage))

          setHandler(
            out0,
            new OutHandler {
              override def onPull() = if (!pushedToCycle && isAvailable(out1)) {
                push(out0, pending)
                pending = null.asInstanceOf[S]
                pushedToCycle = true
              }

              override def onDownstreamFinish() = {
                //Do Nothing, intercept completion as downstream
              }
            }
          )

          setHandler(
            out1,
            new OutHandler {
              override def onPull() = {
                pull(in)
                if (!pushedToCycle && isAvailable(out0)) {
                  push(out0, pending)
                  pending = null.asInstanceOf[S]
                  pushedToCycle = true
                }
              }
            }
          )
        }
      }
    }
}
