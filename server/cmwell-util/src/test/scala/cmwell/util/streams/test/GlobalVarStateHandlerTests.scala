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
  describe("GlobalVerStateHandler Stage") {
    it("should handle single setter & single getter where state is initially available") {

      val src = TestSource.probe[Int]
      val snk = TestSink.probe[Int]

      val (uStream,dStream) = RunnableGraph.fromGraph(GraphDSL.create(src, snk)((a, b) => (a, b)) {
        implicit b => {
          (s1, s2) => {
            import akka.stream.scaladsl.GraphDSL.Implicits._

            val gvsh = b.add(new GlobalVarStateHandler[Int](1,1)(() => Future.successful(42))(scala.concurrent.ExecutionContext.Implicits.global))

            s1 ~> gvsh.inlets.head.asInstanceOf[Inlet[Int]]
            gvsh.outlets.head.asInstanceOf[Outlet[Int]] ~> s2

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

    it("should handle single setter & single getter where state is not initially available") {

      val src = TestSource.probe[Int]
      val snk = TestSink.probe[Int]
      val p = Promise[Int]()

      val (uStream,dStream) = RunnableGraph.fromGraph(GraphDSL.create(src, snk)((a, b) => (a, b)) {
        implicit b => {
          (s1, s2) => {
            import akka.stream.scaladsl.GraphDSL.Implicits._

            val gvsh = b.add(new GlobalVarStateHandler[Int](1,1)(() => p.future)(scala.concurrent.ExecutionContext.Implicits.global))

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
      dStream.expectNoMsg(300.millis)
      p.success(42)
      dStream.expectNext(42)
      dStream.requestNext(42)
      uStream.sendNext(12345)
      dStream.requestNext(12345)
    }
  }
}