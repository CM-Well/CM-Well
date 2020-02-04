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
/*
 * Copyright (c) 2016 Gilad Hoch
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package cmwell.tools.data.downloader.consumer

import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._

object StreamUtils {

  def unfoldFlow[E, I, O, M](seed: I, flow: Graph[FlowShape[I, O], M])(
    unfoldWith: O => Option[(E, I)]
  ): Source[E, M] = {

    val fanOut2Shape = new GraphStage[FanOutShape2[O, I, E]] {
      override val shape = new FanOutShape2[O, I, E]("unfoldFlow")

      override def createLogic(attributes: Attributes) = {
        new GraphStageLogic(shape) {
          import shape._
          var ePending: E = null.asInstanceOf[E]
          var iPending: I = seed

          override def preStart() = pull(in)

          setHandler(
            in,
            new InHandler {
              override def onPush() = {
                val o = grab(in)
                unfoldWith(o) match {
                  case None => completeStage()
                  case Some((e, i)) => {
                    pull(in)

                    if (isAvailable(out0)) {
                      push(out0, i)
                      iPending = null.asInstanceOf[I]
                    } else iPending = i

                    if (isAvailable(out1)) push(out1, e)
                    else ePending = e
                  }
                }
              }
            }
          )

          setHandler(out0, new OutHandler {
            override def onPull() = {
              if (iPending != null) {
                push(out0, iPending)
                iPending = null.asInstanceOf[I]
              }
            }
          })

          setHandler(
            out1,
            new OutHandler {
              override def onPull() = {
                if (ePending != null) {
                  push(out1, ePending)
                  ePending = null.asInstanceOf[E]
                }
                if (isAvailable(out0) && iPending != null) {
                  push(out0, iPending)
                  iPending = null.asInstanceOf[I]
                }
              }
            }
          )
        }
      }
    }

    Source.fromGraph(GraphDSL.create(flow) { implicit b => f =>
      import GraphDSL.Implicits._

      val fo2 = b.add(fanOut2Shape)

      fo2.out0 ~> f ~> fo2.in

      SourceShape(fo2.out1)
    })

  }
}
