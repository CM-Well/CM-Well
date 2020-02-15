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

import akka.stream.{ClosedShape, Inlet, Outlet}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import cmwell.util.stream.GlobalVarStateHandler

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}

/**
  * Proj: server
  * User: gilad
  * Date: 9/4/17
  * Time: 2:05 PM
  */
class GlobalVarStateHandlerTests extends StreamSpec {
  describe("GlobalVarStateHandler Stage") {
    describe("should handle single setter & single getter") {
      it("where state is initially available") {

        val src = TestSource.probe[Int]
        val snk = TestSink.probe[Int]

        val (uStream, dStream) = RunnableGraph.fromGraph(GraphDSL.create(src, snk)((a, b) => (a, b)) {
          implicit b => {
            (s1, s2) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](1, 1)(() => Future.successful(42))(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              gvsh.outlets.head ~> s2

              ClosedShape
            }
          }
        }).run()

        uStream.ensureSubscription()
        dStream.ensureSubscription()

        uStream.expectRequest()
        dStream.requestNext(42)
        dStream.requestNext(42)
        dStream.requestNext(42)
        uStream.sendNext(12345)
        dStream.requestNext(12345)
      }

      it("where state is not initially available") {

        val src = TestSource.probe[Int]
        val snk = TestSink.probe[Int]
        val p = Promise[Int]()

        val (uStream, dStream) = RunnableGraph.fromGraph(GraphDSL.create(src, snk)((a, b) => (a, b)) {
          implicit b => {
            (s1, s2) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](1, 1)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              gvsh.outlets.head ~> s2

              ClosedShape
            }
          }
        }).run()

        uStream.ensureSubscription()
        dStream.ensureSubscription()

        uStream.expectRequest()
        dStream.request(1)
        dStream.expectNoMessage(300.millis)
        p.success(42)
        dStream.expectNext(42)
        dStream.requestNext(42)
        uStream.sendNext(12345)
        dStream.requestNext(12345)
      }

      it("where state is not initially available, and is overridden before initialization is complete") {

        val src = TestSource.probe[Int]
        val snk = TestSink.probe[Int]
        val p = Promise[Int]()

        val (uStream, dStream) = RunnableGraph.fromGraph(GraphDSL.create(src, snk)((a, b) => (a, b)) {
          implicit b => {
            (s1, s2) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](1, 1)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              gvsh.outlets.head ~> s2

              ClosedShape
            }
          }
        }).run()

        uStream.ensureSubscription()
        dStream.ensureSubscription()

        uStream.expectRequest()
        dStream.request(1)
        dStream.expectNoMessage(300.millis)
        uStream.sendNext(12345)
        dStream.expectNext(12345)
        p.success(42)
        dStream.expectNoMessage(300.millis)
        dStream.requestNext(12345)
        uStream.sendNext(786)
        dStream.requestNext(786)
      }

      it("where state is not initially available, and is overridden before initialization is failed") {

        val src = TestSource.probe[Int]
        val snk = TestSink.probe[Int]
        val p = Promise[Int]()

        val (uStream, dStream) = RunnableGraph.fromGraph(GraphDSL.create(src, snk)((a, b) => (a, b)) {
          implicit b => {
            (s1, s2) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](1, 1)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              gvsh.outlets.head ~> s2

              ClosedShape
            }
          }
        }).run()

        uStream.ensureSubscription()
        dStream.ensureSubscription()

        uStream.expectRequest()
        dStream.request(1)
        dStream.expectNoMessage(300.millis)
        uStream.sendNext(12345)
        dStream.expectNext(12345)
        p.failure(new Exception("initialization failed!"))
        dStream.requestNext(12345)
        uStream.sendNext(786)
        dStream.requestNext(786)
      }
    }

    describe("should handle single setter & multiple getters") {
      it("where state is initially available") {

        val src = TestSource.probe[Int]
        val snk1 = TestSink.probe[Int]
        val snk2 = TestSink.probe[Int]

        val (upStream, d1Stream, d2Stream) = RunnableGraph.fromGraph(GraphDSL.create(src, snk1, snk2)((a, b, c) => (a, b, c)) {
          implicit b => {
            (s1, s2, s3) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](1, 2)(() => Future.successful(42))(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              gvsh.outlets.head ~> s2
              gvsh.outlets.last ~> s3

              ClosedShape
            }
          }
        }).run()

        upStream.ensureSubscription()
        d1Stream.ensureSubscription()

        upStream.expectRequest()
        d1Stream.requestNext(42)
        d2Stream.requestNext(42)
        d1Stream.requestNext(42)
        d1Stream.requestNext(42)
        d2Stream.requestNext(42)
        d2Stream.requestNext(42)
        upStream.sendNext(12345)
        d1Stream.requestNext(12345)
        d2Stream.requestNext(12345)
      }

      it("where state is not initially available") {

        val src = TestSource.probe[Int]
        val snk1 = TestSink.probe[Int]
        val snk2 = TestSink.probe[Int]
        val p = Promise[Int]()

        val (upStream, d1Stream, d2Stream) = RunnableGraph.fromGraph(GraphDSL.create(src, snk1, snk2)((a, b, c) => (a, b, c)) {
          implicit b => {
            (s1, s2, s3) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](1, 2)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              gvsh.outlets.head ~> s2
              gvsh.outlets.last ~> s3

              ClosedShape
            }
          }
        }).run()

        upStream.ensureSubscription()
        d1Stream.ensureSubscription()
        d2Stream.ensureSubscription()

        upStream.expectRequest()
        d1Stream.request(1)
        d2Stream.request(1)
        d1Stream.expectNoMessage(300.millis)
        d2Stream.expectNoMessage(300.millis)
        p.success(42)
        d1Stream.expectNext(42)
        d2Stream.expectNext(42)
        d1Stream.requestNext(42)
        d2Stream.requestNext(42)
        upStream.sendNext(12345)
        d1Stream.requestNext(12345)
        d2Stream.requestNext(12345)
      }

      it("where state is not initially available, and is overridden before initialization is complete") {

        val src = TestSource.probe[Int]
        val snk1 = TestSink.probe[Int]
        val snk2 = TestSink.probe[Int]
        val p = Promise[Int]()

        val (upStream, d1Stream, d2Stream) = RunnableGraph.fromGraph(GraphDSL.create(src, snk1, snk2)((a, b, c) => (a, b, c)) {
          implicit b => {
            (s1, s2, s3) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](1, 2)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              gvsh.outlets.head ~> s2
              gvsh.outlets.last ~> s3

              ClosedShape
            }
          }
        }).run()

        upStream.ensureSubscription()
        d1Stream.ensureSubscription()
        d2Stream.ensureSubscription()

        upStream.expectRequest()
        d1Stream.request(1)
        d2Stream.request(1)
        d1Stream.expectNoMessage(300.millis)
        d2Stream.expectNoMessage(300.millis)
        upStream.sendNext(12345)
        d1Stream.expectNext(12345)
        d2Stream.expectNext(12345)
        p.success(42)
        d1Stream.expectNoMessage(300.millis)
        d2Stream.expectNoMessage(300.millis)
        d1Stream.requestNext(12345)
        d2Stream.requestNext(12345)
        upStream.sendNext(786)
        d1Stream.requestNext(786)
        d2Stream.requestNext(786)
      }

      it("where state is not initially available, and is overridden before initialization is failed") {

        val src = TestSource.probe[Int]
        val snk1 = TestSink.probe[Int]
        val snk2 = TestSink.probe[Int]
        val p = Promise[Int]()

        val (upStream, d1Stream, d2Stream) = RunnableGraph.fromGraph(GraphDSL.create(src, snk1, snk2)((a, b, c) => (a, b, c)) {
          implicit b => {
            (s1, s2, s3) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](1, 2)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              gvsh.outlets.head ~> s2
              gvsh.outlets.last ~> s3

              ClosedShape
            }
          }
        }).run()

        upStream.ensureSubscription()
        d1Stream.ensureSubscription()
        d2Stream.ensureSubscription()

        upStream.expectRequest()
        d1Stream.request(1)
        d2Stream.request(1)
        d1Stream.expectNoMessage(300.millis)
        d2Stream.expectNoMessage(300.millis)
        upStream.sendNext(12345)
        d1Stream.expectNext(12345)
        d2Stream.expectNext(12345)
        p.failure(new Exception("initialization failed!"))
        d1Stream.requestNext(12345)
        d2Stream.requestNext(12345)
        upStream.sendNext(786)
        d1Stream.requestNext(786)
        d2Stream.requestNext(786)
      }
    }

    describe("should handle multiple setters & single getter") {
      it("where state is initially available") {

        val src1 = TestSource.probe[Int]
        val src2 = TestSource.probe[Int]
        val snk = TestSink.probe[Int]

        val (u1Stream, u2Stream, dnStream) = RunnableGraph.fromGraph(GraphDSL.create(src1, src2, snk)((a, b, c) => (a, b, c)) {
          implicit b => {
            (s1, s2, s3) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](2, 1)(() => Future.successful(42))(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              s2 ~> gvsh.inlets.last
              gvsh.outlets.head ~> s3

              ClosedShape
            }
          }
        }).run()

        u1Stream.ensureSubscription()
        dnStream.ensureSubscription()

        u1Stream.expectRequest()
        u2Stream.expectRequest()
        dnStream.requestNext(42)
        dnStream.requestNext(42)
        dnStream.requestNext(42)
        u1Stream.sendNext(12345)
        dnStream.requestNext(12345)
        u2Stream.sendNext(786)
        dnStream.requestNext(786)
      }

      it("where state is not initially available") {

        val src1 = TestSource.probe[Int]
        val src2 = TestSource.probe[Int]
        val snk = TestSink.probe[Int]
        val p = Promise[Int]()

        val (u1Stream, u2Stream, dnStream) = RunnableGraph.fromGraph(GraphDSL.create(src1, src2, snk)((a, b, c) => (a, b, c)) {
          implicit b => {
            (s1, s2, s3) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](2, 1)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              s2 ~> gvsh.inlets.last
              gvsh.outlets.head ~> s3

              ClosedShape
            }
          }
        }).run()

        u1Stream.ensureSubscription()
        u2Stream.ensureSubscription()
        dnStream.ensureSubscription()

        u1Stream.expectRequest()
        u2Stream.expectRequest()
        dnStream.request(1)
        dnStream.expectNoMessage(300.millis)
        p.success(42)
        dnStream.expectNext(42)
        dnStream.requestNext(42)
        u1Stream.sendNext(12345)
        dnStream.requestNext(12345)
        u2Stream.sendNext(786)
        dnStream.requestNext(786)
      }

      it("where state is not initially available, and is overridden before initialization is complete") {

        val src1 = TestSource.probe[Int]
        val src2 = TestSource.probe[Int]
        val snk = TestSink.probe[Int]
        val p = Promise[Int]()

        val (u1Stream, u2Stream, dnStream) = RunnableGraph.fromGraph(GraphDSL.create(src1, src2, snk)((a, b, c) => (a, b, c)) {
          implicit b => {
            (s1, s2, s3) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](2, 1)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              s2 ~> gvsh.inlets.last
              gvsh.outlets.head ~> s3

              ClosedShape
            }
          }
        }).run()

        u1Stream.ensureSubscription()
        u2Stream.ensureSubscription()
        dnStream.ensureSubscription()

        u1Stream.expectRequest()
        u2Stream.expectRequest()
        dnStream.request(1)
        dnStream.expectNoMessage(300.millis)
        u1Stream.sendNext(12345)
        dnStream.expectNext(12345)
        u2Stream.sendNext(786)
        dnStream.requestNext(786)
        p.success(42)
        dnStream.expectNoMessage(300.millis)
        dnStream.requestNext(786)
        u1Stream.sendNext(1729)
        dnStream.requestNext(1729)
        u2Stream.sendNext(1234567)
        dnStream.requestNext(1234567)
      }

      it("where state is not initially available, and is overridden before initialization is failed") {

        val src1 = TestSource.probe[Int]
        val src2 = TestSource.probe[Int]
        val snk = TestSink.probe[Int]
        val p = Promise[Int]()

        val (u1Stream, u2Stream, dnStream) = RunnableGraph.fromGraph(GraphDSL.create(src1, src2, snk)((a, b, c) => (a, b, c)) {
          implicit b => {
            (s1, s2, s3) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](2, 1)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              s2 ~> gvsh.inlets.last
              gvsh.outlets.head ~> s3

              ClosedShape
            }
          }
        }).run()

        u1Stream.ensureSubscription()
        u2Stream.ensureSubscription()
        dnStream.ensureSubscription()

        u1Stream.expectRequest()
        u2Stream.expectRequest()
        dnStream.request(1)
        dnStream.expectNoMessage(300.millis)
        u1Stream.sendNext(12345)
        dnStream.expectNext(12345)
        u2Stream.sendNext(786)
        p.failure(new Exception("initialization failed!"))
        dnStream.requestNext(786)
        u1Stream.sendNext(1729)
        dnStream.requestNext(1729)
        u2Stream.sendNext(12345)
        dnStream.requestNext(12345)
      }
    }

    describe("should handle multiple setters & multiple getters") {
      it("where state is initially available") {

        val src1 = TestSource.probe[Int]
        val src2 = TestSource.probe[Int]
        val snk1 = TestSink.probe[Int]
        val snk2 = TestSink.probe[Int]

        val (u1Stream, u2Stream, d1Stream, d2Stream) = RunnableGraph.fromGraph(GraphDSL.create(src1, src2, snk1, snk2)((a, b, c, d) => (a, b, c, d)) {
          implicit b => {
            (s1, s2, s3, s4) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](2, 2)(() => Future.successful(42))(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              s2 ~> gvsh.inlets.last
              gvsh.outlets.head ~> s3
              gvsh.outlets.last ~> s4

              ClosedShape
            }
          }
        }).run()

        u1Stream.ensureSubscription()
        u2Stream.ensureSubscription()
        d1Stream.ensureSubscription()
        d2Stream.ensureSubscription()

        u1Stream.expectRequest()
        u2Stream.expectRequest()
        d1Stream.requestNext(42)
        d2Stream.requestNext(42)
        d1Stream.requestNext(42)
        d1Stream.requestNext(42)
        d2Stream.requestNext(42)
        d2Stream.requestNext(42)
        u1Stream.sendNext(12345)
        d1Stream.requestNext(12345)
        d2Stream.requestNext(12345)
        u2Stream.sendNext(786)
        d1Stream.requestNext(786)
        d2Stream.requestNext(786)
      }

      it("where state is not initially available") {

        val src1 = TestSource.probe[Int]
        val src2 = TestSource.probe[Int]
        val snk1 = TestSink.probe[Int]
        val snk2 = TestSink.probe[Int]
        val p = Promise[Int]()

        val (u1Stream, u2Stream, d1Stream, d2Stream) = RunnableGraph.fromGraph(GraphDSL.create(src1, src2, snk1, snk2)((a, b, c, d) => (a, b, c, d)) {
          implicit b => {
            (s1, s2, s3, s4) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](2, 2)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              s2 ~> gvsh.inlets.last
              gvsh.outlets.head ~> s3
              gvsh.outlets.last ~> s4

              ClosedShape
            }
          }
        }).run()

        u1Stream.ensureSubscription()
        u2Stream.ensureSubscription()
        d1Stream.ensureSubscription()
        d2Stream.ensureSubscription()

        u1Stream.expectRequest()
        u2Stream.expectRequest()
        d1Stream.request(1)
        d2Stream.request(1)
        d1Stream.expectNoMessage(300.millis)
        d2Stream.expectNoMessage(300.millis)
        p.success(42)
        d1Stream.expectNext(42)
        d2Stream.expectNext(42)
        d1Stream.requestNext(42)
        d2Stream.requestNext(42)
        u1Stream.sendNext(12345)
        d1Stream.requestNext(12345)
        d2Stream.requestNext(12345)
        u2Stream.sendNext(786)
        d1Stream.requestNext(786)
        d2Stream.requestNext(786)
      }

      it("where state is not initially available, and is overridden before initialization is complete") {

        val src1 = TestSource.probe[Int]
        val src2 = TestSource.probe[Int]
        val snk1 = TestSink.probe[Int]
        val snk2 = TestSink.probe[Int]
        val p = Promise[Int]()

        val (u1Stream, u2Stream, d1Stream, d2Stream) = RunnableGraph.fromGraph(GraphDSL.create(src1, src2, snk1, snk2)((a, b, c, d) => (a, b, c, d)) {
          implicit b => {
            (s1, s2, s3, s4) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](2, 2)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              s2 ~> gvsh.inlets.last
              gvsh.outlets.head ~> s3
              gvsh.outlets.last ~> s4

              ClosedShape
            }
          }
        }).run()

        u1Stream.ensureSubscription()
        u2Stream.ensureSubscription()
        d1Stream.ensureSubscription()
        d2Stream.ensureSubscription()

        u1Stream.expectRequest()
        u2Stream.expectRequest()
        d1Stream.request(1)
        d2Stream.request(1)
        d1Stream.expectNoMessage(300.millis)
        d2Stream.expectNoMessage(300.millis)
        u1Stream.sendNext(12345)
        d1Stream.expectNext(12345)
        d2Stream.expectNext(12345)
        u2Stream.sendNext(786)
        d1Stream.requestNext(786)
        d2Stream.requestNext(786)
        p.success(42)
        d1Stream.expectNoMessage(300.millis)
        d2Stream.expectNoMessage(300.millis)
        d1Stream.requestNext(786)
        d2Stream.requestNext(786)
        u1Stream.sendNext(1729)
        d1Stream.requestNext(1729)
        d2Stream.requestNext(1729)
        u2Stream.sendNext(1234567)
        d1Stream.requestNext(1234567)
        d2Stream.requestNext(1234567)
      }

      it("where state is not initially available, and is overridden before initialization is failed") {

        val src1 = TestSource.probe[Int]
        val src2 = TestSource.probe[Int]
        val snk1 = TestSink.probe[Int]
        val snk2 = TestSink.probe[Int]
        val p = Promise[Int]()

        val (u1Stream, u2Stream, d1Stream, d2Stream) = RunnableGraph.fromGraph(GraphDSL.create(src1, src2, snk1, snk2)((a, b, c, d) => (a, b, c, d)) {
          implicit b => {
            (s1, s2, s3, s4) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](2, 2)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              s2 ~> gvsh.inlets.last
              gvsh.outlets.head ~> s3
              gvsh.outlets.last ~> s4

              ClosedShape
            }
          }
        }).run()

        u1Stream.ensureSubscription()
        u2Stream.ensureSubscription()
        d1Stream.ensureSubscription()
        d2Stream.ensureSubscription()

        u1Stream.expectRequest()
        u2Stream.expectRequest()
        d1Stream.request(1)
        d2Stream.request(1)
        d1Stream.expectNoMessage(300.millis)
        d2Stream.expectNoMessage(300.millis)
        u1Stream.sendNext(12345)
        d1Stream.expectNext(12345)
        d2Stream.expectNext(12345)
        u2Stream.sendNext(786)
        p.failure(new Exception("initialization failed!"))
        d1Stream.requestNext(786)
        d2Stream.requestNext(786)
        u1Stream.sendNext(1729)
        d1Stream.requestNext(1729)
        d2Stream.requestNext(1729)
        u2Stream.sendNext(12345)
        d1Stream.requestNext(12345)
        d2Stream.requestNext(12345)
      }
    }

    describe("should handle completion properly") {
      it("when all upstreams finishes after initialization complete") {
        val src1 = TestSource.probe[Int]
        val src2 = TestSource.probe[Int]
        val snk1 = TestSink.probe[Int]
        val snk2 = TestSink.probe[Int]
        val p = Promise[Int]()

        val (u1Stream, u2Stream, d1Stream, d2Stream) = RunnableGraph.fromGraph(GraphDSL.create(src1, src2, snk1, snk2)((a, b, c, d) => (a, b, c, d)) {
          implicit b => {
            (s1, s2, s3, s4) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](2, 2)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              s2 ~> gvsh.inlets.last
              gvsh.outlets.head ~> s3
              gvsh.outlets.last ~> s4

              ClosedShape
            }
          }
        }).run()

        u1Stream.ensureSubscription()
        u2Stream.ensureSubscription()
        d1Stream.ensureSubscription()
        d2Stream.ensureSubscription()

        u1Stream.expectRequest()
        u2Stream.expectRequest()
        d1Stream.request(1)
        d2Stream.request(1)
        p.success(42)
        d1Stream.expectNext(42)
        d2Stream.expectNext(42)

        u1Stream.sendComplete()
        d1Stream.expectNoMessage(100.millis)
        d2Stream.expectNoMessage(100.millis)

        u2Stream.sendComplete()
        d1Stream.expectNoMessage(100.millis)
        d2Stream.expectNoMessage(100.millis)

        d1Stream.requestNext(42)
        d2Stream.requestNext(42)

        d1Stream.cancel()
        d2Stream.requestNext(42)
      }

      it("when all upstream finishes before initialization complete") {
        val src1 = TestSource.probe[Int]
        val src2 = TestSource.probe[Int]
        val snk1 = TestSink.probe[Int]
        val snk2 = TestSink.probe[Int]
        val p = Promise[Int]()

        val (u1Stream, u2Stream, d1Stream, d2Stream) = RunnableGraph.fromGraph(GraphDSL.create(src1, src2, snk1, snk2)((a, b, c, d) => (a, b, c, d)) {
          implicit b => {
            (s1, s2, s3, s4) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](2, 2)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              s2 ~> gvsh.inlets.last
              gvsh.outlets.head ~> s3
              gvsh.outlets.last ~> s4

              ClosedShape
            }
          }
        }).run()

        u1Stream.ensureSubscription()
        u2Stream.ensureSubscription()
        d1Stream.ensureSubscription()
        d2Stream.ensureSubscription()

        u1Stream.expectRequest()
        u2Stream.expectRequest()
        d1Stream.request(1)
        d2Stream.request(1)
        d1Stream.expectNoMessage(100.millis)
        d2Stream.expectNoMessage(100.millis)

        u1Stream.sendComplete()
        d1Stream.expectNoMessage(100.millis)
        d2Stream.expectNoMessage(100.millis)

        u2Stream.sendComplete()
        d1Stream.expectNoMessage(100.millis)
        d2Stream.expectNoMessage(100.millis)

        p.success(42)
        d1Stream.expectNext(42)
        d2Stream.expectNext(42)

        d1Stream.cancel()
        d2Stream.requestNext(42)
      }

      it("when all upstream finishes before initialization fails") {
        val src1 = TestSource.probe[Int]
        val src2 = TestSource.probe[Int]
        val snk1 = TestSink.probe[Int]
        val snk2 = TestSink.probe[Int]
        val p = Promise[Int]()

        val (u1Stream, u2Stream, d1Stream, d2Stream) = RunnableGraph.fromGraph(GraphDSL.create(src1, src2, snk1, snk2)((a, b, c, d) => (a, b, c, d)) {
          implicit b => {
            (s1, s2, s3, s4) => {
              import akka.stream.scaladsl.GraphDSL.Implicits._

              val gvsh = b.add(new GlobalVarStateHandler[Int](2, 2)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

              s1 ~> gvsh.inlets.head
              s2 ~> gvsh.inlets.last
              gvsh.outlets.head ~> s3
              gvsh.outlets.last ~> s4

              ClosedShape
            }
          }
        }).run()

        u1Stream.ensureSubscription()
        u2Stream.ensureSubscription()
        d1Stream.ensureSubscription()
        d2Stream.ensureSubscription()

        u1Stream.expectRequest()
        u2Stream.expectRequest()
        d1Stream.request(1)
        d2Stream.request(1)
        d1Stream.expectNoMessage(100.millis)
        d2Stream.expectNoMessage(100.millis)

        u1Stream.sendComplete()
        d1Stream.expectNoMessage(100.millis)
        d2Stream.expectNoMessage(100.millis)

        u2Stream.sendComplete()
        d1Stream.expectNoMessage(100.millis)
        d2Stream.expectNoMessage(100.millis)

        val ex = new Exception("pre initialization failure!")
        p.failure(ex)
        d1Stream.expectError(ex)
        d2Stream.expectError(ex)
      }
    }
  }
}
