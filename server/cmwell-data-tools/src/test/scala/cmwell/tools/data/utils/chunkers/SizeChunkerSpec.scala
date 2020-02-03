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
/*
class SizeChunkerSpecAutoFusingOn  extends { val autoFusing = true  } with SizeChunkerSpec
class SizeChunkerSpecAutoFusingOff extends { val autoFusing = false } with SizeChunkerSpec

trait SizeChunkerSpec extends BaseStreamSpec {
  "SizeChunker" should "emit elements when size threshold has reached" in {
    val (pub, sub) = TestSource.probe[Int]
      .map(x => ByteString(x.toString))
      .via(SizeChunker(2, 2.seconds))
      .map(_.map(_.utf8String.toInt))
      .toMat(TestSink.probe[Seq[Int]])(Keep.both)
      .run()

    sub.request(4)
    pub.sendNext(1)
    pub.sendNext(2)
    pub.sendNext(3)
    pub.sendNext(4)
    pub.sendComplete()
    sub.expectNext(Seq(1, 2))
    sub.expectNext(Seq(3, 4))
    sub.expectComplete()
  }

  it should "emit elements when time threshold has reached" in {
    val (pub, sub) = TestSource.probe[Int]
      .map(x => ByteString(x.toString))
      .via(SizeChunker(2, 1.seconds))
      .map(_.map(_.utf8String.toInt))
      .toMat(TestSink.probe[Seq[Int]])(Keep.both)
      .run()

    sub.request(4)

    pub.sendNext(1)
    sub.expectNext(Seq(1))

    pub.sendNext(2)
    sub.expectNext(Seq(2))

    pub.sendNext(3)
    pub.sendNext(4)
    pub.sendComplete()
    sub.expectNext(Seq(3,4))
    sub.expectComplete()
  }
}
*/
