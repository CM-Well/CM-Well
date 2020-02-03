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

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage._
import akka.util.ByteString

import scala.collection.immutable
import scala.collection.immutable.VectorBuilder
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

object GroupChunker {

  /**
    * Extracts subject part of n-tuple data (ntriples, nquads)
    * @param data input data
    * @return extracted subject in data
    */
  def extractSubject(data: ByteString): ByteString = data.splitAt(data.indexOf(' '))._1
  def identity(data: ByteString): ByteString = data

  /**
    * Gets group extractor according to input format string
    * @param format input format string
    * @return subject extractor function
    */
  def formatToGroupExtractor(format: String) = {
    format match {
      case "ntriples" | "nquads" => extractSubject(_)
      case "json"                => identity(_)
      case _                     => identity(_)
    }
  }

  def apply(groupExtractor: ByteString => ByteString, within: FiniteDuration = 3.seconds) =
    new GroupChunker(groupExtractor, within)
}

class GroupChunker(extractGroup: ByteString => ByteString, within: FiniteDuration)
    extends GraphStage[FlowShape[ByteString, immutable.Seq[ByteString]]] {
  val in = Inlet[ByteString]("in")
  val out = Outlet[scala.collection.immutable.Seq[ByteString]]("out")

  override def initialAttributes = Attributes.name("group-chunker")

  val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      private var buffer: VectorBuilder[ByteString] = new VectorBuilder
      private var numElementsInBuffer = 0
      private var lastGroup: ByteString = _

      // True if:
      // - buffer is nonEmpty
      //       AND
      // - timer fired OR group is full
      private var groupClosed = false
      private var groupEmitted = false
      private var finished = false
      private var pendingElement: immutable.Seq[ByteString] = _

      private val ChunkerWithinTimer = "GroupChunkerTimer"

      def isNeedToCloseGroup(newElement: ByteString): Boolean = extractGroup(newElement) != lastGroup

      override def preStart() = {
        schedulePeriodically(ChunkerWithinTimer, within)
        pull(in)
      }

      private def bufferToElement() = {
        val element = buffer.result()
        buffer.clear()
        element
      }

      private def nextElement(elem: ByteString): Unit = {
        groupEmitted = false

        if (isNeedToCloseGroup(elem)) {
          schedulePeriodically(ChunkerWithinTimer, within)
          closeGroup()
        } else pull(in)

        buffer += elem
        numElementsInBuffer += 1
        lastGroup = extractGroup(elem)
      }

      private def closeGroup(): Unit = {
        groupClosed = true
        pendingElement = bufferToElement()

        if (isAvailable(out)) emitGroup()
      }

      private def emitGroup(): Unit = {
        groupEmitted = true
        if (pendingElement.nonEmpty) push(out, pendingElement)
        pendingElement = immutable.Seq.empty[ByteString]

        if (!finished) startNewGroup()
        else completeStage()
      }

      private def startNewGroup(): Unit = {
        groupClosed = false
        numElementsInBuffer = 0

        if (isAvailable(in)) nextElement(grab(in))
        else if (!hasBeenPulled(in)) pull(in)
      }

      override def onPush(): Unit = {
        schedulePeriodically(ChunkerWithinTimer, within)

        if (!groupClosed) nextElement(grab(in))
      }

      override def onPull(): Unit = if (groupClosed) emitGroup()

      override def onUpstreamFinish(): Unit = {
        finished = true
        if (groupEmitted && isBufferEmpty) completeStage()
        else closeGroup()
      }

      private def isBufferEmpty() = numElementsInBuffer == 0

      override protected def onTimer(timerKey: Any) = if (!isBufferEmpty) closeGroup()

      setHandlers(in, out, this)
    }
}
