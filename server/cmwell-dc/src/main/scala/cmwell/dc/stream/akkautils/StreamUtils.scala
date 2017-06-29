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


package cmwell.dc.stream.akkautils

import akka.Done
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import akka.stream.stage._

import scala.concurrent.{Future, Promise}

/**
  * Created by eli on 20/07/16.
  */
object StreamUtils {

  //todo: remove this class
    def unfoldFlowOldNotUsed[E, I, O, M](seed: I, flow: Graph[FlowShape[I, O], M])(
    unfoldWith: O => Option[(E, I)]
  ): Source[E, (Future[I], M)] = {

    val fanOut2Shape = new GraphStageWithMaterializedValue[FanOutShape2[O, I, E], Future[I]] {
      override val shape = new FanOutShape2[O, I, E]("unfoldFlow")

      override def createLogicAndMaterializedValue(attributes: Attributes): (GraphStageLogic, Future[I]) = {
        val donePromise = Promise[I]()

        val logic = new GraphStageLogic(shape) {

          import shape._

          private var ePending: E = null.asInstanceOf[E]
          private var iPending: I = seed
          private var lastIGot: I = seed

          override def preStart() = pull(in)

          setHandler(in, new InHandler {
            override def onPush() = {
              val o = grab(in)
              unfoldWith(o) match {
                case None => {
                  donePromise.trySuccess(lastIGot)
                  completeStage()
                }
                case Some((e, i)) => {
                  lastIGot = i
//                  pull(in)
//
//                  if (isAvailable(out0)) {
//                    push(out0, i)
//                    iPending = null.asInstanceOf[I]
//                  } else iPending = i

                  if (isAvailable(out1)) {
                    push(out1, e)
                    ePending = null.asInstanceOf[E]
                    if (isAvailable(out0)) {
                      push(out0, i)
                      pull(in)
                      iPending = null.asInstanceOf[I]
                    } else {
                      iPending = i
                    }
                  }
                  else {
                    ePending = e
                    iPending = i
                  }
                }
              }
            }

            override def onUpstreamFailure(ex: Throwable): Unit = {
              donePromise.tryFailure(ex)
              super.onUpstreamFailure(ex)
            }

            override def onUpstreamFinish(): Unit = {
              donePromise.trySuccess(lastIGot)
              super.onUpstreamFinish()
            }
          })

          setHandler(out0, new OutHandler {
            override def onPull() = {
              if (iPending != null) {
                push(out0, iPending)
                iPending = null.asInstanceOf[I]
              }
            }

            override def onDownstreamFinish(): Unit = {
              //no need to do here nothing. the same finish will be got in the inlet (it's the same flow for both) but more accurate (success or error)
            }
          })

          setHandler(out1, new OutHandler {
            override def onPull() = {
              if (ePending != null) {
                push(out1, ePending)
                ePending = null.asInstanceOf[E]
                pull(in)
              }
              if (isAvailable(out0) && iPending != null) {
                push(out0, iPending)
                iPending = null.asInstanceOf[I]
              }
            }

            override def onDownstreamFinish(): Unit = {
              //todo: why doesn't it work as in the remark below
              //don't do anything here just pass the message to the flow to decide
              complete(out0)
//              donePromise.trySuccess(lastIGot)
//              super.onDownstreamFinish()
            }
          })
        }
        val materializedValue = donePromise.future
        (logic, materializedValue)
      }
    }

    Source.fromGraph(GraphDSL.create(fanOut2Shape, flow)(Keep.both) { implicit b => (fo2, f) =>
      import GraphDSL.Implicits._

      fo2.out0 ~> f ~> fo2.in

      SourceShape(fo2.out1)
    })
  }
}
