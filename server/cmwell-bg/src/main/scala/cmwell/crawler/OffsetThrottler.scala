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

package cmwell.crawler

import akka.kafka.scaladsl.Consumer
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

object OffsetThrottler {

  def apply(crawlerId: String): OffsetThrottler = new OffsetThrottler(crawlerId)
}

class OffsetThrottler(crawlerId: String)
  extends GraphStage[FanInShape2[Long, ConsumerRecord[Array[Byte], Array[Byte]], ConsumerRecord[Array[Byte], Array[Byte]]]] with LazyLogging {
  private val offsetIn = Inlet[Long]("OffsetThrottler.offsetIn")
  private val messageIn = Inlet[ConsumerRecord[Array[Byte], Array[Byte]]]("OffsetThrottler.messageIn")
  private val messageOut = Outlet[ConsumerRecord[Array[Byte], Array[Byte]]]("OffsetThrottler.messageOut")
  override val shape = new FanInShape2(offsetIn, messageIn, messageOut)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var pending: ConsumerRecord[Array[Byte], Array[Byte]] = _
    private var maxAllowedOffset: Long = -1L

    private val offsetInHandler: InHandler = new InHandler {
      override def onPush(): Unit = {
        maxAllowedOffset = grab(offsetIn)
        //checking that the port isn't closed isn't necessary because the whole stage will be finished by then
        //also, no need for isAvailable check because:
        //messageOut-onPull->pull(messageIn)->pull(offsetIn)=>isAvailable(messageOut)==true
        if (pending.offset <= maxAllowedOffset /* && !isClosed(messageOut)*/ ) {
          logger.info(s"$crawlerId Got a new max allowed offset $maxAllowedOffset. Releasing the back pressure.")
          if (pending.offset() == maxAllowedOffset)
            logger.info(s"$crawlerId The current element's offset $maxAllowedOffset is the same as the max allowed one. " +
              s"This means the crawler is going to handle the last infoton before horizon.")
          push(messageOut, pending)
          pending = null
          if (isClosed(messageIn))
            completeStage()
        }
        else {
          logger.info(s"$crawlerId Got a new max allowed offset $maxAllowedOffset but the pending message has offset ${pending.offset}. " +
            s"Pulling again from another max allowed offset.")
          pull(offsetIn)
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (isClosed(messageIn) || pending.offset > maxAllowedOffset)
          completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        super.onUpstreamFailure(ex)
        failStage(ex)
      }
    }
    private val initialMessageInHandler: InHandler = new InHandler {
      override def onPush(): Unit = {
        val elem = grab(messageIn)
        pending = elem
        logger.info(s"$crawlerId Initial message with offset ${pending.offset()} received - " +
          s"pulling the offset source for the max allowed offset (setting back pressure)")
        pull(offsetIn)
        //from now on, each message we get should be checked against the maxAllowedOffset - set a new handler for the newly got messages
        setHandler(messageIn, ongoingMessageInHandler)
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
    private val ongoingMessageInHandler: InHandler = new InHandler {
      override def onPush(): Unit = {
        val elem = grab(messageIn)
        pending = elem
        if (pending.offset > maxAllowedOffset) {
          logger.info(s"$crawlerId Got a message with offset ${pending.offset} that is larger than the current max allowed $maxAllowedOffset. " +
            s"Pulling the offset source for a newer maxAllowedOffset (setting back pressure)")
          pull(offsetIn)
        }
        //checking that the port isn't closed isn't necessary because the whole stage will be finished by then
        else {
          /* if (!isClosed(messageOut))*/
          if (pending.offset() == maxAllowedOffset)
            logger.info(s"$crawlerId The current element's offset $maxAllowedOffset is the same as the max allowed one. " +
              s"This means the crawler is going to handle the last infoton before horizon.")
          push(messageOut, pending)
          pending = null
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (pending == null || isClosed(offsetIn))
          completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        super.onUpstreamFailure(ex)
        failStage(ex)
      }
    }
    private val messageOutHandler: OutHandler = new OutHandler {
      override def onPull(): Unit = {
        pull(messageIn)
      }

      override def onDownstreamFinish(): Unit = {
        super.onDownstreamFinish()
        completeStage()
      }
    }
    //initial handler allocation
    setHandler(offsetIn, offsetInHandler)
    setHandler(messageIn, initialMessageInHandler)
    setHandler(messageOut, messageOutHandler)
  }
}
