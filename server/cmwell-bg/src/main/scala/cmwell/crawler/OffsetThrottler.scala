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

package cmwell.crawler

import akka.kafka.scaladsl.Consumer
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

object OffsetThrottler {


}

class OffsetThrottler()
  extends GraphStage[FanInShape2[Long, ConsumerRecord[Array[Byte], Array[Byte]], ConsumerRecord[Array[Byte], Array[Byte]]]] with LazyLogging {
  val offsetIn = Inlet[Long]("OffsetThrottler.offsetIn")
  val messageIn = Inlet[ConsumerRecord[Array[Byte], Array[Byte]]]("OffsetThrottler.messageIn")
  val messageOut = Outlet[ConsumerRecord[Array[Byte], Array[Byte]]]("OffsetThrottler.messageOut")
  override val shape = new FanInShape2(offsetIn, messageIn, messageOut)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var pending: ConsumerRecord[Array[Byte], Array[Byte]] = _
    private var maxAllowedOffset: Long = -1L

    setHandler(
      offsetIn,
      new InHandler {
        override def onPush(): Unit = {
          maxAllowedOffset = grab(offsetIn)
          if (pending.offset > maxAllowedOffset && isAvailable(messageOut))
            push(messageOut, pending)
        }

        override def onUpstreamFinish(): Unit = {
          super.onUpstreamFinish()
          completeStage()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          super.onUpstreamFailure(ex)
          failStage(ex)
        }
      }
    )
    setHandler(
      messageIn,
      new InHandler {
        override def onPush(): Unit = {
          val elem = grab(messageIn)
          pending = elem
          if (maxAllowedOffset == -1 || elem.offset > maxAllowedOffset)
            pull(offsetIn)
          else if (isAvailable(messageOut))
            push(messageOut, pending)
        }

        override def onUpstreamFinish(): Unit = {
          super.onUpstreamFinish()
          completeStage()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          super.onUpstreamFailure(ex)
          failStage(ex)
        }
      }
    )
    setHandler(
      messageOut,
      new OutHandler {
        override def onPull(): Unit = {
          pull(messageIn)
        }

        override def onDownstreamFinish(): Unit = {
          super.onDownstreamFinish()
          completeStage()
        }
      }
    )
  }
}
