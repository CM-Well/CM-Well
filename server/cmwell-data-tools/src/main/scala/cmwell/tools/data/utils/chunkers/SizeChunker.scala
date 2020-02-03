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

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

import scala.collection.immutable
import scala.collection.immutable.VectorBuilder
import scala.concurrent.duration._

/**
  * Chunk up this stream into groups of elements received within a time window,
  * or limited by the given total size of elements, whatever happens first.
  * Empty groups will not be emitted if no elements are received from upstream.
  * The last group before end-of-stream will contain the buffered elements
  * since the previously emitted group.
  *
  * `maxSize` must be positive, and `within` must be greater than 0 seconds, otherwise
  * IllegalArgumentException is thrown.
  */
object SizeChunker {
  def apply[T](maxSize: Int = 25 * 1024, within: FiniteDuration = 3.seconds) = new SizeChunker[T](maxSize, within)
}

/**
  * Groups [[akka.util.ByteString]] elements according to a given size threshold or a timed window
  * (whicever comes first)
  * @param maxSize size threshold
  * @param within timed window length
  */
class SizeChunker[T](maxSize: Int, within: FiniteDuration)
  extends GraphStage[FlowShape[(ByteString,T), (immutable.Seq[ByteString],T) ]] {
  require(maxSize > 0, "maxSize must be greater than 0")
  require(within > Duration.Zero)

  val in = Inlet[(ByteString,T)]("in")
  val out = Outlet[(scala.collection.immutable.Seq[ByteString],T)]("out")

  override def initialAttributes = Attributes.name("size-chunker")

  val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      private var buffer: VectorBuilder[(ByteString,T)] = new VectorBuilder
      private var totalSizeInBuffer = 0L

      // True if:
      // - buffer is nonEmpty
      //       AND
      // - timer fired OR group is full
      private var groupClosedSoft = false
      private var groupClosedHard = false
      private var groupEmitted = false
      private var finished = false

      private val ChunkerWithinTimer = "SizeChunkerTimer"

      def isNeedToCloseGroup(): Boolean = totalSizeInBuffer >= maxSize

      override def preStart() = {
        schedulePeriodically(ChunkerWithinTimer, within)
        pull(in)
      }

      private def nextElement(elem: (ByteString,T)): Unit = {
        groupEmitted = false
        buffer += elem
        totalSizeInBuffer += elem._1.size

        if (isNeedToCloseGroup) {
          schedulePeriodically(ChunkerWithinTimer, within)
          closeGroupHard()
        } else pull(in)
      }

      private def closeGroupHard(): Unit = {
        groupClosedHard = true
        if (isAvailable(out)) emitGroup()
      }

      private def closeGroupSoft(): Unit = {
        groupClosedSoft = true
        if (isAvailable(out)) emitGroup()
      }

      private def emitGroup(): Unit = {
        groupEmitted = true

        val bufferContents = buffer.result()
        val emit = bufferContents.map { _._1}
        val lastState = bufferContents.last._2

        push(out, (emit,lastState))
        buffer.clear()
        if (!finished) startNewGroup()
        else completeStage()
      }

      private def startNewGroup(): Unit = {
        groupClosedSoft = false
        groupClosedHard = false
        totalSizeInBuffer = 0L

        if (isAvailable(in)) nextElement(grab(in))
        else if (!hasBeenPulled(in)) pull(in)
      }

      override def onPush(): Unit = {
        schedulePeriodically(ChunkerWithinTimer, within)

        if (!groupClosedHard) nextElement(grab(in))
      }

      override def onPull(): Unit = if (groupClosedSoft || groupClosedHard) emitGroup()

      override def onUpstreamFinish(): Unit = {
        finished = true
        if (groupEmitted) completeStage()
        else closeGroupHard()
      }

      private def isBufferNonEmpty() = totalSizeInBuffer > 0

      override protected def onTimer(timerKey: Any) = if (isBufferNonEmpty) closeGroupSoft()

      setHandlers(in, out, this)
    }
}
