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
package cmwell.tools.data.utils

import _root_.akka.actor.ActorSystem
import _root_.akka.http.scaladsl.Http
import _root_.akka.http.scaladsl.model.HttpHeader
import _root_.akka.stream.FlowShape
import _root_.akka.stream.scaladsl._
import _root_.akka.util.ByteString
import com.typesafe.config.ConfigFactory

import scala.annotation.tailrec
import scala.concurrent._

/**
  * Functions related to Akka integration
  */
package object akka extends DataToolsConfig {
  val blank = ByteString("", "UTF-8")
  val endl = ByteString("\n", "UTF-8")
  val lineSeparatorFrame = Framing.delimiter(delimiter = endl,
                                             maximumFrameLength = config.getInt("cmwell.akka.max-frame-length"),
                                             allowTruncation = true)

  /**
    * Dispose Akka resources after given future is complete
    *
    * @param system actor system to be terminated
    * @param ec execution context
    */
  def cleanup()(implicit system: ActorSystem, ec: ExecutionContext) = {
    Http().shutdownAllConnectionPools().onComplete { _ =>
      system.terminate()
    }
  }

  def balancer[In, Out](worker: Flow[In, Out, Any], workerCount: Int): Flow[In, Out, _] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val balancer = b.add(Balance[In](workerCount, waitForAllDownstreams = false))
      val merge = b.add(Merge[Out](workerCount))

      for (_ <- 1 to workerCount) {
        // for each worker, add an edge from the balancer to the worker, then wire
        // it to the merge element
        balancer ~> worker.async ~> merge
      }

      FlowShape(balancer.in, merge.out)
    })
  }

  /**
    * Builds a single [[akka.util.ByteString ByteString]] from a sequence,
    * separated by `\n` character
    * @param lines input sequence containing `ByteString` data
    * @return single ByteString containing concatenation of data elements separated with `\n`
    */
  def concatByteStrings(lines: Seq[ByteString], sep: ByteString): ByteString =
    concatByteStrings(lines, blank, endl, blank)

  def concatByteStrings(lines: Seq[ByteString], start: String, sep: String, end: String): ByteString = {
    concatByteStrings(lines, ByteString(start), ByteString(sep), ByteString(end))
  }

  /**
    * Builds a single [[akka.util.ByteString ByteString]] from a sequence of ByteString elements.
    * @param lines input sequence of ByteString elements
    * @param start starting element
    * @param sep separator element
    * @param end ending element
    *
    * @return
    */
  def concatByteStrings(lines: Seq[ByteString], start: ByteString, sep: ByteString, end: ByteString): ByteString = {
    val builder = ByteString.newBuilder
    var first = true

    builder.append(start)
    for (line <- lines) {
      if (first) {
        builder.append(line)
        first = false
      } else {
        builder.append(sep)
        builder.append(line)
      }
    }

    builder.append(end)

    builder.result()
  }

  implicit class ByteStringUtils(b: ByteString) {
    def trim() = {
      @tailrec
      def iterateRight(b: ByteString): ByteString = {
        if (b.isEmpty) b
        else if (b.last <= ' ') iterateRight(b.init)
        else b
      }

      iterateRight(b.dropWhile(_ <= ' '))
    }

    def split(delimiter: Byte) = {
      @tailrec
      def splitByteString(bytes: ByteString, delimiter: Byte, acc: Seq[ByteString]): Seq[ByteString] = {
        bytes.splitAt(bytes.indexOf(delimiter)) match {
          case (split, rest) if split.isEmpty => acc :+ rest
          case (split, rest)                  => splitByteString(rest.tail, delimiter, acc :+ split)
        }
      }

      splitByteString(b, delimiter, Seq.empty[ByteString])
    }
  }
}
