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


package cmwell.util.streams.test

import akka.stream._
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.TestPublisher.{Probe => SrcProbe}
import akka.stream.testkit.TestSubscriber.{Probe => SnkProbe}
import cmwell.util.stream.MapInitAndLast
import scala.concurrent.duration.DurationInt

class MapInitAndLastTests extends StreamSpec {

  def generateGraph[In](): (SrcProbe[In],SnkProbe[(In,Boolean)]) = {
    val src = TestSource.probe[In]
    val snk = TestSink.probe[(In,Boolean)]
    RunnableGraph.fromGraph(GraphDSL.create(src, snk)((a, b) => (a, b)) {
      implicit b => {
        (s1, s2) => {
          import GraphDSL.Implicits._

          val mial = b.add(new MapInitAndLast[In, (In,Boolean)](_ -> false, _ -> true))

          s1 ~> mial ~> s2

          ClosedShape
        }
      }
    }).run()
  }

  describe("MapInitAndLast Stage"){
    it("should buffer a single element"){
      val (src,snk) = generateGraph[Int]()
      snk.request(99)
      src.sendNext(1)
      snk.expectNoMessage(300.millis)
      src.sendComplete()
      snk.expectNext((1,true))
      snk.expectComplete()
    }

    it("should treat last element differently") {
      val (src,snk) = generateGraph[Int]()
      snk.request(99)
      src.sendNext(1)
      snk.expectNoMessage(300.millis)
      src.sendNext(2)
      snk.expectNext((1,false))
      src.sendNext(3)
      snk.expectNext((2,false))
      src.sendComplete()
      snk.expectNext((3,true))
      snk.expectComplete()
    }

    it("should propagate back-pressure"){
      val (src,snk) = generateGraph[Int]()
      snk.ensureSubscription()
      src.sendNext(1)
      snk.expectNoMessage(300.millis)
      src.sendNext(1)
      snk.expectNoMessage(300.millis)
      src.sendComplete()
      snk.expectNoMessage(300.millis)
      snk.request(1)
      snk.expectNext((1,false))
      snk.request(1)
      snk.expectNext((1,true))
      snk.expectComplete()
    }
  }
}
