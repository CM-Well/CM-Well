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

import akka.stream._
import akka.stream.scaladsl.GraphDSL
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable
import scala.util.{Success, Try}

/**
  * Created by matan on 27/2/17.
  */
object GoodRetry {
  def concat[I, O, S, M](limit: Long, flow: Graph[FlowShape[(I, S), (Try[O], S)], M])(
    retryWith: S => Option[immutable.Iterable[(I, S)]]
  ): Graph[FlowShape[(I, S), (Try[O], S)], M] = {
    GraphDSL.create(flow) { implicit b => origFlow =>
      import GraphDSL.Implicits._

      val retry = b.add(new RetryConcatCoordinator[I, S, O](limit, retryWith))

      retry.out2 ~> origFlow ~> retry.in2

      FlowShape(retry.in1, retry.out1)
    }
  }

  class RetryConcatCoordinator[I, S, O](limit: Long, retryWith: S => Option[immutable.Iterable[(I, S)]])
      extends GraphStage[BidiShape[(I, S), (Try[O], S), (Try[O], S), (I, S)]] {
    val in1 = Inlet[(I, S)]("RetryConcat.ext.in")
    val out1 = Outlet[(Try[O], S)]("RetryConcat.ext.out")
    val in2 = Inlet[(Try[O], S)]("RetryConcat.int.in")
    val out2 = Outlet[(I, S)]("RetryConcat.int.out")
    override val shape = BidiShape[(I, S), (Try[O], S), (Try[O], S), (I, S)](in1, out1, in2, out2)
    override def createLogic(attributes: Attributes) = new GraphStageLogic(shape) {
      var numElementsInCycle = 0
      val queue = scala.collection.mutable.Queue.empty[(I, S)]

      setHandler(
        in1,
        new InHandler {
          override def onPush() = {
            val is = grab(in1)
            if (isAvailable(out2)) {
              push(out2, is)
              numElementsInCycle += 1
            } else queue.enqueue(is)
          }

          override def onUpstreamFinish() = {
            if (numElementsInCycle == 0 && queue.isEmpty) {
              completeStage()
            }
          }
        }
      )

      setHandler(out1, new OutHandler {
        override def onPull() = {
          pull(in2)
        }
      })

      setHandler(
        in2,
        new InHandler {
          override def onPush() = {
            numElementsInCycle -= 1
            grab(in2) match {
              case s @ (_: Success[O], _) => pushAndCompleteIfLast(s)
              case failure @ (_, s) =>
                retryWith(s).fold(pushAndCompleteIfLast(failure)) {
                  xs =>
                    if (xs.size + queue.size > limit)
                      failStage(
                        new IllegalStateException(
                          s"Queue limit of $limit has been exceeded. Trying to append ${xs.size} elements to a queue that has ${queue.size} elements."
                        )
                      )
                    else {
                      xs.foreach(queue.enqueue(_))
                      if (queue.isEmpty) {
                        if (isClosed(in1)) {
                          completeStage()
                        }
                        else {
                          pull(in2)
                        }
                      } else {
                        pull(in2)
                        if (isAvailable(out2)) {
                          val elem = queue.dequeue()
                          push(out2, elem)
                          numElementsInCycle += 1
                        }
                      }
                    }
                }
            }
          }
        }
      )

      def pushAndCompleteIfLast(elem: (Try[O], S)): Unit = {
        push(out1, elem)
        if (isClosed(in1) && queue.isEmpty && numElementsInCycle == 0) {
          completeStage()
        }
      }

      setHandler(
        out2,
        new OutHandler {
          override def onPull() = {
            if (queue.isEmpty) {
              if (!hasBeenPulled(in1) && !isClosed(in1)) {
                pull(in1)
              }
            } else {
              push(out2, queue.dequeue())
              numElementsInCycle += 1
            }
//          if (!elementInCycle && isAvailable(out1)) {
//            if (queue.isEmpty) {
//              pull(in1)
//            }
//            else {
//              push(out2, queue.dequeue())
//              elementInCycle = true
//              if (!hasBeenPulled(in2)) pull(in2)
//            }
//          }
          }

          override def onDownstreamFinish() = {
            //Do Nothing, intercept completion as downstream
          }
        }
      )
    }
  }
}
