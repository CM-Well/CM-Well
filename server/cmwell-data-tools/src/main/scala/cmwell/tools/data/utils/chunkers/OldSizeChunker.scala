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
package cmwell.tools.data.utils.chunkers

import akka.actor.Cancellable
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage._
import akka.util.ByteString

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

object OldSizeChunker {
  type In = ByteString
  type Out = Seq[ByteString]

  def apply(maxSize: Int, within: Duration = Duration.Undefined) = new OldSizeChunker(maxSize, within)
}

import OldSizeChunker._

class OldSizeChunker(maxSize: Int, within: Duration) extends GraphStage[FlowShape[In, Out]] {
  private[this] val BUFFER_SIZE = (maxSize * 1.2).toInt
  private var buffer: ArrayBuffer[In] = _
  private var dataSize: Long = _

  val in = Inlet[In]("SizeChunker.in")
  val out = Outlet[Out]("SizeChunker.out")

  resetBuffer()

  /**
    * Converts the buffer content to a single element
    *
    * @return single elements composed from buffered contents
    */
  private def bufferToElement() = buffer.toSeq

  /**
    * Clears buffered content
    */
  private def resetBuffer(): Unit = {
    dataSize = 0
    buffer = new ArrayBuffer[In](BUFFER_SIZE)
  }

  /**
    * Clears buffered content and stores new element
    *
    * @param elem new element to be stored
    */
  private def resetBuffer(elem: In): Unit = {
    resetBuffer()
    addToBuffer(elem)
  }

  /**
    * Stores new element in buffer
    *
    * @param elem element to be stored
    * @return buffer with new element
    */
  private def addToBuffer(elem: In) = {
    dataSize += elem.size
    buffer += elem
  }

  /**
    * Checks if buffered data size exceeded limit
    *
    * @return true if data size exceeded limit, otherwise false
    */
  private def isFlushBufferedData = { dataSize > maxSize }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var outPendingElem: Option[Out] = None
    private var asyncCB: AsyncCallback[Unit] = _
    private var forcePushEvent: Option[Cancellable] = None
    private var isWaitingForEvent: Boolean = false

    override def preStart() = {
      pull(in)
      asyncCB = getAsyncCallback(_ => {
        if (isWaitingForEvent) {
          isWaitingForEvent = false

          if (buffer.nonEmpty) {
            push(out, bufferToElement())
            resetBuffer()
          }
        }
      })
    }

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)

          if (isFlushBufferedData) {
            isWaitingForEvent = false
            forcePushEvent.foreach(_.cancel)

            val elemToPush = bufferToElement()

            if (isAvailable(out)) {
              push(out, elemToPush)
              pull(in)
            } else {
              outPendingElem = Some(elemToPush)
            }

            resetBuffer(elem)
          } else {
            addToBuffer(elem)
            pull(in)
          }
        }

        override def onUpstreamFinish(): Unit = {
          if (buffer.isEmpty) {
            completeStage()
          } else if (outPendingElem.isEmpty && isAvailable(out)) {
            push(out, bufferToElement())
            completeStage()
          }
        }
      }
    )

    setHandler(
      out,
      new OutHandler {
        override def onPull(): Unit = {
          outPendingElem match {
            case Some(elem) =>
              push(out, elem)

              if (isClosed(in)) {
                if (buffer.nonEmpty) {
                  outPendingElem = Some(bufferToElement())
                  resetBuffer()
                } else {
                  completeStage()
                }
              } else {
                outPendingElem = None
                pull(in)
              }

            case None => { // there is no element to push yet
              if (isClosed(in)) {
                if (buffer.nonEmpty) {
                  push(out, bufferToElement())
                }
                completeStage()
              } else if (within.isFinite) {
                isWaitingForEvent = true
                // schedule a force push if no push is arrived in duration
                forcePushEvent = Some(
                  materializer.scheduleOnce(FiniteDuration(within._1, within._2), new Runnable() {
                    def run() = asyncCB.invoke(())
                  })
                )
              }
            }
          }
        }
      }
    )
  }

  override val shape: FlowShape[In, Out] = FlowShape.of(in, out)
}
