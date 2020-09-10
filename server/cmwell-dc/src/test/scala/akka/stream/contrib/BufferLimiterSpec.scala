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

package akka.stream.contrib

import akka.stream.{KillSwitches, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep}
import akka.stream.testkit.GraphStageMessages.{Pull, Push}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestProbe

import scala.concurrent.duration.DurationInt

class BufferLimiterSpec extends BufferLimiterBaseSpec {
  private val expectDuration = 150.millis

  private def createAndRunTestGraph(maxBuffer: Int, flow: Flow[Int, Int, _]) = {
    val intSrc = TestSource.probe[Int]
    val intSnk = TestSink.probe[Int]
    val preFlowProbe = TestProbe()
    val preFlowTestStage = StageInspector[Int](preFlowProbe)
    val postFlowProbe = TestProbe()
    val postFlowTestStage = StageInspector[Int](postFlowProbe)
    val limitedFlow = BufferLimiter(maxBuffer, flow)
    val (src, snk) = intSrc
      .via(preFlowTestStage)
      .via(limitedFlow)
      .via(postFlowTestStage)
      .toMat(intSnk)(Keep.both).run()
    src.ensureSubscription()
    snk.ensureSubscription()
    (src, preFlowProbe, postFlowProbe, snk)
  }

  "BufferLimiter" should "pass no requests in case of size 0" in {
    val bufferFlow = Flow[Int].buffer(17, OverflowStrategy.backpressure)
    val (src, preFlowProbe, postFlowProbe, snk) = createAndRunTestGraph(0, bufferFlow)
    snk.request(10)
    preFlowProbe.expectNoMessage(expectDuration)
  }
  it should "request only one element in case of a buffer of 1" in {
    val bufferFlow = Flow[Int].buffer(17, OverflowStrategy.backpressure)
    val (src, preFlowProbe, postFlowProbe, snk) = createAndRunTestGraph(1, bufferFlow)
    snk.request(10)
    preFlowProbe.expectMsg(expectDuration, Pull)
    preFlowProbe.expectNoMessage(expectDuration)
  }
  it should "request only one element without downstream demand" in {
    val bufferFlow = Flow[Int].buffer(17, OverflowStrategy.backpressure)
    val (src, preFlowProbe, postFlowProbe, snk) = createAndRunTestGraph(1, bufferFlow)
    preFlowProbe.expectMsg(expectDuration, Pull)
    preFlowProbe.expectNoMessage(expectDuration)
  }
  it should "request exactly one another element after sending the first one" in {
    val bufferFlow = Flow[Int].buffer(17, OverflowStrategy.backpressure)
    val (src, preFlowProbe, postFlowProbe, snk) = createAndRunTestGraph(1, bufferFlow)
    snk.request(10)
    preFlowProbe.expectMsg(expectDuration, Pull)
    src.sendNext(42)
    preFlowProbe.expectMsg(expectDuration, Push)
    snk.expectNext(42)
    preFlowProbe.expectMsg(expectDuration, Pull)
    preFlowProbe.expectNoMessage(expectDuration)
  }
  it should "request two elements in case of no downstream demand" in {
    val bufferFlow = Flow[Int].buffer(2, OverflowStrategy.backpressure)
    val (src, preFlowProbe, postFlowProbe, snk) = createAndRunTestGraph(100, bufferFlow)
    preFlowProbe.expectMsg(expectDuration, Pull)
    src.sendNext(11)
    preFlowProbe.expectMsg(expectDuration, Push)
    preFlowProbe.expectMsg(expectDuration, Pull)
    src.sendNext(22)
    preFlowProbe.expectMsg(expectDuration, Push)
    preFlowProbe.expectNoMessage(expectDuration)
  }
  it should "request two elements in case of no downstream demand - opposite direction" in {
    val bufferFlow = Flow[Int].buffer(100, OverflowStrategy.backpressure)
    val (src, preFlowProbe, postFlowProbe, snk) = createAndRunTestGraph(2, bufferFlow)
    preFlowProbe.expectMsg(expectDuration, Pull)
    src.sendNext(11)
    preFlowProbe.expectMsg(expectDuration, Push)
    preFlowProbe.expectMsg(expectDuration, Pull)
    src.sendNext(22)
    preFlowProbe.expectMsg(expectDuration, Push)
    preFlowProbe.expectNoMessage(expectDuration)
  }
  it should "handle stream completion from the source" in {
    val bufferFlow = Flow[Int].buffer(100, OverflowStrategy.backpressure)
    val (src, preFlowProbe, postFlowProbe, snk) = createAndRunTestGraph(2, bufferFlow)
    src.sendComplete()
    snk.expectComplete()
  }
  it should "handle stream error from the source" in {
    val bufferFlow = Flow[Int].buffer(100, OverflowStrategy.backpressure)
    val (src, preFlowProbe, postFlowProbe, snk) = createAndRunTestGraph(2, bufferFlow)
    src.sendError(new Exception("test!"))
    snk.expectError()
  }
  it should "handle stream cancellation from the sink" in {
    val bufferFlow = Flow[Int].buffer(100, OverflowStrategy.backpressure)
    val (src, preFlowProbe, postFlowProbe, snk) = createAndRunTestGraph(2, bufferFlow)
    snk.cancel()
    src.expectCancellation()
  }
  it should "handle stream completion from the flow" in {
    val intSrc = TestSource.probe[Int]
    val intSnk = TestSink.probe[Int]
    val flow = Flow[Int].viaMat(KillSwitches.single)(Keep.right)
    val limitedFlow = BufferLimiter(100, flow)
    val ((src, killSwitch), snk) = intSrc
      .viaMat(limitedFlow)(Keep.both)
      .toMat(intSnk)(Keep.both)
      .run()
    src.ensureSubscription()
    snk.ensureSubscription()
    killSwitch.shutdown()
    src.expectCancellation()
    snk.expectComplete()
  }
  it should "handle stream error from the flow" in {
    val intSrc = TestSource.probe[Int]
    val intSnk = TestSink.probe[Int]
    val flow = Flow[Int].viaMat(KillSwitches.single)(Keep.right)
    val limitedFlow = BufferLimiter(100, flow)
    val ((src, killSwitch), snk) = intSrc
      .viaMat(limitedFlow)(Keep.both)
      .toMat(intSnk)(Keep.both)
      .run()
    src.ensureSubscription()
    snk.ensureSubscription()
    killSwitch.abort(new Exception("Boom!"))
    src.expectCancellation()
    snk.expectError()
  }
}
