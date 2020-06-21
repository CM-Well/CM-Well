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

import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.util.ByteString
import cmwell.tools.data.helpers.BaseStreamSpec

import scala.concurrent.duration._

class GroupSpecAutoFusingOn  extends { val autoFusing = true  } with GroupChunkerSpec
class GroupSpecAutoFusingOff extends { val autoFusing = false } with GroupChunkerSpec

trait GroupChunkerSpec extends BaseStreamSpec {
  "GroupChunker" should "emit elements when new group has arrived" in {
    val (pub, sub) = TestSource.probe[String]
      .map(x => ByteString(x.toString))
      .via(GroupChunker(b => ByteString(b.size), 2.seconds)) // group byte-strings by size
      .map(_.map(_.utf8String))
      .toMat(TestSink.probe[Seq[String]])(Keep.both)
      .run()

    sub.request(100)
    pub.sendNext("hello")
    pub.sendNext("world")
    pub.sendNext("nba")
    pub.sendNext("ibm")
    pub.sendNext("what")
    pub.sendNext("is")
    pub.sendNext("life")
    pub.sendComplete()
    sub.expectNext(Seq("hello", "world"))
    sub.expectNext(Seq("nba", "ibm"))
    sub.expectNext(Seq("what"))
    sub.expectNext(Seq("is"))
    sub.expectNext(Seq("life"))
    sub.expectComplete()
  }

  it should "emit elements when time threshold has reached" in {
    val (pub, sub) = TestSource.probe[String]
      .map(x => ByteString(x.toString))
      .via(GroupChunker(b => ByteString(b.size), 2.seconds)) // group byte-strings by size
      .map(_.map(_.utf8String))
      .toMat(TestSink.probe[Seq[String]])(Keep.both)
      .run()

    sub.request(4)

    pub.sendNext("one")
    sub.expectNext(Seq("one"))

    pub.sendNext("two")
    sub.expectNext(Seq("two"))

    pub.sendNext("four")
    pub.sendNext("five")
    pub.sendComplete()
    sub.expectNext(Seq("four","five"))
    sub.expectComplete()
  }
}
