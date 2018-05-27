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

import akka.stream.ClosedShape
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import org.apache.kafka.clients.consumer.ConsumerRecord

class OffsetThrottlerSpec extends CrawlerStreamSpec {

  "OffsetThrottler" should "blah" in {
    val sourceUnderTest = Source(1 to 4).filter(_ % 2 == 0).map(_ * 2)

    sourceUnderTest
      .runWith(TestSink.probe[Int])
      .request(2)
      .expectNext(4, 8)
      .expectComplete()
  }

  it should "fsdlj" in {
    val offsetSrcProbe = TestSource.probe[Long]
    val messageSrcProbe = TestSource.probe[ConsumerRecord[Array[Byte], Array[Byte]]]
    val messageSinkProbe = TestSink.probe[ConsumerRecord[Array[Byte], Array[Byte]]]

    val (offsetSrc, messageSrc, messageSnk) =
      RunnableGraph.fromGraph(GraphDSL.create(offsetSrcProbe, messageSrcProbe, messageSinkProbe)((a, b, c) => (a, b, c)) {
      implicit b => {
        (offstSource, msgSource, msgSink) => {
          import akka.stream.scaladsl.GraphDSL.Implicits._
          val ot = b.add(OffsetThrottler())
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

    messageSnk.

//    offsetSrc.
  }
}
