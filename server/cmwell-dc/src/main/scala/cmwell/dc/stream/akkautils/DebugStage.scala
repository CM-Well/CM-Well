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
package cmwell.dc.stream.akkautils

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by eli on 01/08/16.
  */
object DebugStage {
  def apply[A](name: String): DebugStage[A] = new DebugStage(name)
}

class DebugStage[A](name: String) extends GraphStage[FlowShape[A, A]] with LazyLogging {
  val in = Inlet[A]("DebugStage.in")
  val out = Outlet[A]("DebugStage.out")
  override val shape = FlowShape.of(in, out)
  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            logger.info(s"[$name]: grabbing element")
            val elem = grab(in)
            logger.info(s"[$name]: pushing the grabbed element $elem")
            push(out, elem)
          }
          override def onUpstreamFinish(): Unit = {
            logger.info(s"[$name]: onUpstreamFinish")
            super.onUpstreamFinish()
          }
          override def onUpstreamFailure(ex: Throwable): Unit = {
            logger.info(s"[$name]: onUpstreamFailure")
            super.onUpstreamFailure(ex)
          }
        }
      )
      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            logger.info(s"[$name]: pulling element")
            pull(in)
          }
          override def onDownstreamFinish(): Unit = {
            logger.info(s"[$name]: onDownstreamFinish")
            super.onDownstreamFinish()
          }
        }
      )
    }
}
