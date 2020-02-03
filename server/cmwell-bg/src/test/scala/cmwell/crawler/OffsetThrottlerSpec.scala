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

import java.nio.charset.StandardCharsets

import akka.stream.ClosedShape
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Source}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration.DurationInt
import scala.util.Try

class OffsetThrottlerSpec extends CrawlerStreamSpec {
  private val expectDuration = 150.millis
  private def createAndRunOffsetThrottlerTestGraph = {
    val offsetSrcProbe = TestSource.probe[Long]
    val messageSrcProbe = TestSource.probe[ConsumerRecord[Array[Byte], Array[Byte]]]
    val messageSinkProbe = TestSink.probe[ConsumerRecord[Array[Byte], Array[Byte]]]

    val (offsetSrc, messageSrc, messageSnk) =
      RunnableGraph.fromGraph(GraphDSL.create(offsetSrcProbe, messageSrcProbe, messageSinkProbe)((a, b, c) => (a, b, c)) {
        implicit b => {
          (offstSource, msgSource, msgSink) => {
            import akka.stream.scaladsl.GraphDSL.Implicits._
            val ot = b.add(OffsetThrottler("testCrawler"))
            offstSource ~> ot.in0
            msgSource ~> ot.in1
            ot.out ~> msgSink
            ClosedShape
          }
        }
      }).run()
    offsetSrc.ensureSubscription()
    messageSrc.ensureSubscription()
    messageSnk.ensureSubscription()
    (offsetSrc, messageSrc, messageSnk)
  }

  "OffsetThrottler" should "shouldn't emit an element before the sink demands" in {
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    offsetSrc.sendNext(17)
    val element = new ConsumerRecord("testTopic", 8, 0, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    messageSrc.sendNext(element)
    offsetSrc.sendNext(100)
    messageSnk.expectNoMessage(expectDuration)
  }

  it should "back pressue before initial offset" in {
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    val element = new ConsumerRecord("testTopic", 8, 0, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    messageSnk.request(1)
    messageSrc.sendNext(element)
    messageSnk.expectNoMessage(expectDuration)
  }

  it should "should emit an element when sink demand came before the sources emitted" in {
    val element = new ConsumerRecord("testTopic", 8, 0, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(1)
    offsetSrc.sendNext(17)
    messageSrc.sendNext(element)
    messageSnk.expectNext(element)
  }

  it should "should emit an element when sink demand came after the sources emitted" in {
    val element = new ConsumerRecord("testTopic", 8, 0, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    offsetSrc.sendNext(17)
    messageSrc.sendNext(element)
    messageSnk.request(1)
    messageSnk.expectNext(element)
  }

  it should "should emit an element when sink demand came between the sources emitted elements" in {
    val element = new ConsumerRecord("testTopic", 8, 0, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSrc.sendNext(element)
    messageSnk.request(1)
    offsetSrc.sendNext(17)
    messageSnk.expectNext(element)
  }

  it should "should back pressure if the offset is too early" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(1)
    messageSrc.sendNext(element)
    offsetSrc.sendNext(17)
    messageSnk.expectNoMessage(expectDuration)
  }

  it should "emit element when proper offset finally arrives" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSrc.sendNext(element)
    messageSnk.request(1)
    offsetSrc.sendNext(17)
    offsetSrc.sendNext(20)
    messageSnk.expectNext(element)
  }

  it should "emit several elements when proper offset finally arrives" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val element2 = new ConsumerRecord("testTopic", 8, 25, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val element3 = new ConsumerRecord("testTopic", 8, 28, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(3)
    messageSrc.sendNext(element)
    offsetSrc.sendNext(17)
    offsetSrc.sendNext(20)
    messageSrc.sendNext(element2)
    messageSnk.expectNext(element)
    messageSnk.expectNoMessage(expectDuration)
    //check that if the offset got is too early, pull again for another max offset
    offsetSrc.sendNext(24)
    offsetSrc.sendNext(30)
    messageSrc.sendNext(element3)
    messageSnk.expectNext(element2)
    messageSnk.expectNext(element3)
  }

  it should "emit several elements when proper offset finally arrives2" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val element2 = new ConsumerRecord("testTopic", 8, 25, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val element3 = new ConsumerRecord("testTopic", 8, 28, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val element4 = new ConsumerRecord("testTopic", 8, 30, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val element5 = new ConsumerRecord("testTopic", 8, 35, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(2)
    messageSrc.sendNext(element)
    messageSrc.sendNext(element2)
    offsetSrc.sendNext(22)
    messageSnk.expectNext(element)
    messageSnk.expectNoMessage(expectDuration)
    offsetSrc.sendNext(32)
    messageSrc.sendNext(element3)
    messageSrc.sendNext(element4)
    messageSrc.sendNext(element5)
    messageSnk.request(3)
    messageSnk.expectNext(element2)
    messageSnk.expectNext(element3)
    messageSnk.expectNext(element4)
    messageSnk.expectNoMessage(expectDuration)
    offsetSrc.sendNext(100)
    messageSnk.expectNext(element5)
  }

  it should "not request another offset if not needed (don't spam zStore with unnecessary reads)" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val element2 = new ConsumerRecord("testTopic", 8, 25, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val element3 = new ConsumerRecord("testTopic", 8, 28, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val element4 = new ConsumerRecord("testTopic", 8, 30, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val element5 = new ConsumerRecord("testTopic", 8, 35, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(10)
    messageSrc.sendNext(element)
    offsetSrc.sendNext(40)
    messageSrc.sendNext(element2)
    messageSrc.sendNext(element3)
    messageSrc.sendNext(element4)
    messageSrc.sendNext(element5)
    messageSnk.expectNext(element)
    messageSnk.expectNext(element2)
    messageSnk.expectNext(element3)
    messageSnk.expectNext(element4)
    messageSnk.expectNext(element5)
    offsetSrc.expectNoMessage(expectDuration)
  }

  it should "complete stage if messageSrc finishes before having any message or offset" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(1)
    messageSrc.sendComplete()
    messageSnk.expectComplete()
  }

  it should "not complete stage if messageSrc finishes after sending an element before having any offset" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(1)
    messageSrc.sendNext(element)
    messageSrc.sendComplete()
    messageSnk.expectNoMessage(expectDuration)
  }

  it should "not complete stage if messageSrc finishes after sending an element and having too early offset" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(1)
    messageSrc.sendNext(element)
    offsetSrc.sendNext(3)
    messageSrc.sendComplete()
    messageSnk.expectNoMessage(expectDuration)
  }

  it should "complete stage if messageSrc finishes after sending an element and having too early offset and offsetSrc finished" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(1)
    messageSrc.sendNext(element)
    offsetSrc.sendNext(3)
    messageSrc.sendComplete()
    offsetSrc.sendComplete()
    messageSnk.expectComplete()
  }

  it should "emit element and complete stage if messageSrc finishes after sending an element and having too early offset " +
    "and later good offset and offsetSrc finished" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(1)
    messageSrc.sendNext(element)
    offsetSrc.sendNext(3)
    messageSrc.sendComplete()
    offsetSrc.sendNext(20)
    offsetSrc.sendComplete()
    messageSnk.expectNext(element)
    messageSnk.expectComplete()
  }

  it should "complete stage if messageSrc finishes after sending an element and having too early offset but offsetSrc is already finished" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(1)
    messageSrc.sendNext(element)
    offsetSrc.sendNext(3)
    offsetSrc.sendComplete()
    messageSrc.sendComplete()
    messageSnk.expectComplete()
  }

  it should "complete stage there is a pending element but offsetSrc has finished" in {
    val element = new ConsumerRecord("testTopic", 8, 20, "testPath".getBytes(StandardCharsets.UTF_8), "testCommand".getBytes(StandardCharsets.UTF_8))
    val (offsetSrc, messageSrc, messageSnk) = createAndRunOffsetThrottlerTestGraph
    messageSnk.request(1)
    messageSrc.sendNext(element)
    offsetSrc.sendNext(3)
    offsetSrc.sendComplete()
    messageSnk.expectComplete()
  }
}
