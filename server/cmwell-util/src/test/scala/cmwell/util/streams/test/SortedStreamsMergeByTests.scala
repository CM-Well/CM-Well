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
package cmwell.util.streams.test

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.TestPublisher.{Probe => SrcProbe}
import akka.stream.testkit.TestSubscriber.{Probe => SnkProbe}
import cmwell.util.stream.SortedStreamsMergeBy
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

trait SortedStreamsMergeByTests extends StreamSpec {

  type Out[T] = (T, Vector[T], Vector[T])

  def generateGraph[T: Ordering]()
    : (SrcProbe[T], SrcProbe[T], SnkProbe[Out[T]]) = {
    val src1 = TestSource.probe[T]
    val src2 = TestSource.probe[T]
    val sink = TestSink.probe[Out[T]]
    RunnableGraph
      .fromGraph(GraphDSL.create(src1, src2, sink)((a, b, c) => (a, b, c)) {
        implicit b =>
          { (s1, s2, s3) =>
            {
              import GraphDSL.Implicits._

              val ssmb =
                b.add(new SortedStreamsMergeBy[T, T, T](identity, identity))
              s1 ~> ssmb.in0
              s2 ~> ssmb.in1
              ssmb.out ~> s3

              ClosedShape
            }
          }
      })
      .run()
  }

  describe("SortedStreamsMergeBy Stage") {
    it("should accumulate element properly") {
      val (lSrc, rSrc, sink) = generateGraph[Int]()
      sink.request(3)
      lSrc.sendNext(2)
      rSrc.sendNext(1)
      sink.expectNoMessage(300.millis)
      rSrc.sendComplete()
      sink.expectNext((1, Vector.empty[Int], Vector(1)))
      lSrc.sendNext(2)
      sink.expectNoMessage(300.millis)
      lSrc.sendComplete()
      sink.expectNext((2, Vector(2, 2), Vector.empty[Int]))
      sink.expectComplete()
    }

    it("should handle empty input streams") {
      val (lSrc, rSrc, sink) = generateGraph[Int]()
      sink.request(3)
      lSrc.sendComplete()
      sink.expectNoMessage(300.millis)
      rSrc.sendComplete()
      sink.expectComplete()
    }

    it("should handle single side accumulations") {
      val (lSrc, rSrc, sink) = generateGraph[Int]()
      sink.request(99)
      lSrc.sendComplete()
      rSrc.sendNext(1)
      rSrc.sendNext(1)
      sink.expectNoMessage(300.millis)
      rSrc.sendNext(1)
      rSrc.sendNext(2)
      sink.expectNext((1, Vector.empty[Int], Vector(1, 1, 1)))
      rSrc.sendNext(2)
      rSrc.sendNext(2)
      sink.expectNoMessage(300.millis)
      rSrc.sendNext(3)
      sink.expectNext((2, Vector.empty[Int], Vector(2, 2, 2)))
      rSrc.sendComplete()
      sink.expectNext((3, Vector.empty[Int], Vector(3)))
      sink.expectComplete()
    }

    it("should fail on upstream failures") {
      val (lSrc, rSrc, sink) = generateGraph[Int]()
      sink.ensureSubscription()
      lSrc.sendError(new Exception("Game Over!"))
      sink.expectError()
      rSrc.expectCancellation()
    }

    it("should accumulate from both sides") {
      val (lSrc, rSrc, sink) = generateGraph[Int]()
      sink.request(1)
      rSrc.sendNext(1)
      lSrc.sendNext(1)
      lSrc.sendNext(1)
      lSrc.sendNext(2)
      rSrc.sendNext(1)
      sink.expectNoMessage(300.millis)
      rSrc.sendNext(2)
      sink.expectNext((1, Vector(1, 1), Vector(1, 1)))
      lSrc.sendNext(3)
      rSrc.sendNext(3)
      sink.expectNoMessage(300.millis)
      sink.requestNext((2, Vector(2), Vector(2)))
      lSrc.sendComplete()
      rSrc.sendComplete()
      sink.expectNoMessage(300.millis)
      sink.requestNext((3, Vector(3), Vector(3)))
      sink.expectComplete()
    }

    it("should handle a failed case we've encountered (simple)") {
      val ts1 = scala.io.Source
        .fromURL(this.getClass.getResource("/timestamps_1"))
        .getLines()
        .map(_.toLong)
        .toVector
        .sorted
      val ts2 = scala.io.Source
        .fromURL(this.getClass.getResource("/timestamps_2"))
        .getLines()
        .map(_.toLong)
        .toVector
        .sorted

      val sink = RunnableGraph
        .fromGraph(
          GraphDSL.create(TestSink.probe[(Long, Vector[Long], Vector[Long])]) {
            implicit b =>
              { s =>
                {
                  import GraphDSL.Implicits._

                  val ssmb = b.add(
                    new SortedStreamsMergeBy[Long, Long, Long](identity,
                                                               identity)
                  )
                  val src1 = b.add(Source(ts1))
                  val src2 = b.add(Source(ts2))

                  src1 ~> ssmb.in0
                  src2 ~> ssmb.in1
                  ssmb.out ~> s

                  ClosedShape
                }
              }
          }
        )
        .run()

      sink.request(999)
      var last = 0L
      for (i <- 1 to 155) {
        val curr = sink.expectNext()._1
        curr should be > last
        last = curr
      }
      sink.expectComplete()
    }

    it("should handle a failed case we've encountered (explicit)") {
      val (lSrc, rSrc, sink) = generateGraph[Long]()
      sink.ensureSubscription()
      lSrc.sendNext(1446773718700L)
      sink.request(1)
      rSrc.sendNext(1446773718700L)
      rSrc.sendNext(1446773720411L)
      lSrc.sendNext(1446773720411L)
      sink.expectNext(
        (1446773718700L, Vector(1446773718700L), Vector(1446773718700L))
      )
      rSrc.sendNext(1484640095182L)
      lSrc.sendNext(1484640095182L)
      sink.request(1)
      sink.expectNext(
        (1446773720411L, Vector(1446773720411L), Vector(1446773720411L))
      )
      rSrc.sendNext(1484640224584L)
      lSrc.sendNext(1484640224584L)
      sink.request(1)
      sink.expectNext(
        (1484640095182L, Vector(1484640095182L), Vector(1484640095182L))
      )
      rSrc.sendNext(1484640225583L)
      lSrc.sendNext(1484640225583L)
      sink.request(1)
      sink.expectNext(
        (1484640224584L, Vector(1484640224584L), Vector(1484640224584L))
      )
      rSrc.sendNext(1484640226541L)
      lSrc.sendNext(1484640226541L)
      sink.request(1)
      sink.expectNext(
        (1484640225583L, Vector(1484640225583L), Vector(1484640225583L))
      )
      rSrc.sendNext(1484640226663L)
      lSrc.sendNext(1484640226663L)
      sink.request(1)
      sink.expectNext(
        (1484640226541L, Vector(1484640226541L), Vector(1484640226541L))
      )
      rSrc.sendNext(1484640227641L)
      lSrc.sendNext(1484640227641L)
      sink.request(1)
      sink.expectNext(
        (1484640226663L, Vector(1484640226663L), Vector(1484640226663L))
      )
      rSrc.sendNext(1484640228638L)
      lSrc.sendNext(1484640228638L)
      sink.request(1)
      sink.expectNext(
        (1484640227641L, Vector(1484640227641L), Vector(1484640227641L))
      )
      rSrc.sendNext(1484640228769L)
      lSrc.sendNext(1484640228769L)
      sink.request(1)
      sink.expectNext(
        (1484640228638L, Vector(1484640228638L), Vector(1484640228638L))
      )
      rSrc.sendNext(1484640229745L)
      lSrc.sendNext(1484640229745L)
      sink.request(1)
      sink.expectNext(
        (1484640228769L, Vector(1484640228769L), Vector(1484640228769L))
      )
      rSrc.sendNext(1484640230708L)
      lSrc.sendNext(1484640230708L)
      sink.request(1)
      sink.expectNext(
        (1484640229745L, Vector(1484640229745L), Vector(1484640229745L))
      )
      rSrc.sendNext(1484640230836L)
      lSrc.sendNext(1484640230836L)
      sink.request(1)
      sink.expectNext(
        (1484640230708L, Vector(1484640230708L), Vector(1484640230708L))
      )
      rSrc.sendNext(1484640231826L)
      lSrc.sendNext(1484640231826L)
      sink.request(1)
      sink.expectNext(
        (1484640230836L, Vector(1484640230836L), Vector(1484640230836L))
      )
      rSrc.sendNext(1484640232803L)
      lSrc.sendNext(1484640232803L)
      sink.request(1)
      sink.expectNext(
        (1484640231826L, Vector(1484640231826L), Vector(1484640231826L))
      )
      rSrc.sendNext(1484640232929L)
      lSrc.sendNext(1484640232929L)
      sink.request(1)
      sink.expectNext(
        (1484640232803L, Vector(1484640232803L), Vector(1484640232803L))
      )
      rSrc.sendNext(1484640233911L)
      lSrc.sendNext(1484640233911L)
      sink.request(1)
      sink.expectNext(
        (1484640232929L, Vector(1484640232929L), Vector(1484640232929L))
      )
      rSrc.sendNext(1484640234881L)
      lSrc.sendNext(1484640234881L)
      sink.request(1)
      sink.expectNext(
        (1484640233911L, Vector(1484640233911L), Vector(1484640233911L))
      )
      rSrc.sendNext(1484640235972L)
      lSrc.sendNext(1484640235972L)
      sink.request(1)
      sink.expectNext(
        (1484640234881L, Vector(1484640234881L), Vector(1484640234881L))
      )
      rSrc.sendNext(1484640236923L)
      lSrc.sendNext(1484640236923L)
      sink.request(1)
      sink.expectNext(
        (1484640235972L, Vector(1484640235972L), Vector(1484640235972L))
      )
      rSrc.sendNext(1484640238037L)
      lSrc.sendNext(1484640238037L)
      sink.request(1)
      sink.expectNext(
        (1484640236923L, Vector(1484640236923L), Vector(1484640236923L))
      )
      rSrc.sendNext(1484640239007L)
      lSrc.sendNext(1484640239007L)
      sink.request(1)
      sink.expectNext(
        (1484640238037L, Vector(1484640238037L), Vector(1484640238037L))
      )
      rSrc.sendNext(1484640240110L)
      lSrc.sendNext(1484640240110L)
      sink.request(1)
      sink.expectNext(
        (1484640239007L, Vector(1484640239007L), Vector(1484640239007L))
      )
      rSrc.sendNext(1484640241096L)
      lSrc.sendNext(1484640241096L)
      sink.request(1)
      sink.expectNext(
        (1484640240110L, Vector(1484640240110L), Vector(1484640240110L))
      )
      rSrc.sendNext(1484640242163L)
      lSrc.sendNext(1484640242163L)
      sink.request(1)
      sink.expectNext(
        (1484640241096L, Vector(1484640241096L), Vector(1484640241096L))
      )
      rSrc.sendNext(1484640243126L)
      lSrc.sendNext(1484640243126L)
      sink.request(1)
      sink.expectNext(
        (1484640242163L, Vector(1484640242163L), Vector(1484640242163L))
      )
      rSrc.sendNext(1484640244227L)
      lSrc.sendNext(1484640244227L)
      sink.request(1)
      sink.expectNext(
        (1484640243126L, Vector(1484640243126L), Vector(1484640243126L))
      )
      rSrc.sendNext(1484640245208L)
      lSrc.sendNext(1484640245208L)
      sink.request(1)
      sink.expectNext(
        (1484640244227L, Vector(1484640244227L), Vector(1484640244227L))
      )
      rSrc.sendNext(1484640246197L)
      lSrc.sendNext(1484640246197L)
      sink.request(1)
      sink.expectNext(
        (1484640245208L, Vector(1484640245208L), Vector(1484640245208L))
      )
      rSrc.sendNext(1484640247285L)
      lSrc.sendNext(1484640247285L)
      sink.request(1)
      sink.expectNext(
        (1484640246197L, Vector(1484640246197L), Vector(1484640246197L))
      )
      rSrc.sendNext(1484640248265L)
      lSrc.sendNext(1484640248265L)
      sink.request(1)
      sink.expectNext(
        (1484640247285L, Vector(1484640247285L), Vector(1484640247285L))
      )
      rSrc.sendNext(1484640249364L)
      lSrc.sendNext(1484640249364L)
      sink.request(1)
      sink.expectNext(
        (1484640248265L, Vector(1484640248265L), Vector(1484640248265L))
      )
      rSrc.sendNext(1484640250386L)
      lSrc.sendNext(1484640250386L)
      sink.request(1)
      sink.expectNext(
        (1484640249364L, Vector(1484640249364L), Vector(1484640249364L))
      )
      rSrc.sendNext(1484640251371L)
      lSrc.sendNext(1484640251371L)
      sink.request(1)
      sink.expectNext(
        (1484640250386L, Vector(1484640250386L), Vector(1484640250386L))
      )
      rSrc.sendNext(1484640252356L)
      lSrc.sendNext(1484640252356L)
      sink.request(1)
      sink.expectNext(
        (1484640251371L, Vector(1484640251371L), Vector(1484640251371L))
      )
      rSrc.sendNext(1484640252485L)
      lSrc.sendNext(1484640252485L)
      sink.request(1)
      sink.expectNext(
        (1484640252356L, Vector(1484640252356L), Vector(1484640252356L))
      )
      rSrc.sendNext(1484640253445L)
      lSrc.sendNext(1484640253445L)
      sink.request(1)
      sink.expectNext(
        (1484640252485L, Vector(1484640252485L), Vector(1484640252485L))
      )
      rSrc.sendNext(1484640254549L)
      lSrc.sendNext(1484640254549L)
      sink.request(1)
      sink.expectNext(
        (1484640253445L, Vector(1484640253445L), Vector(1484640253445L))
      )
      rSrc.sendNext(1484640255523L)
      lSrc.sendNext(1484640255523L)
      sink.request(1)
      sink.expectNext(
        (1484640254549L, Vector(1484640254549L), Vector(1484640254549L))
      )
      rSrc.sendNext(1484640256515L)
      lSrc.sendNext(1484640256515L)
      sink.request(1)
      sink.expectNext(
        (1484640255523L, Vector(1484640255523L), Vector(1484640255523L))
      )
      rSrc.sendNext(1484640256639L)
      lSrc.sendNext(1484640256639L)
      sink.request(1)
      sink.expectNext(
        (1484640256515L, Vector(1484640256515L), Vector(1484640256515L))
      )
      rSrc.sendNext(1484640257608L)
      lSrc.sendNext(1484640257608L)
      sink.request(1)
      sink.expectNext(
        (1484640256639L, Vector(1484640256639L), Vector(1484640256639L))
      )
      rSrc.sendNext(1484640258648L)
      lSrc.sendNext(1484640258648L)
      sink.request(1)
      sink.expectNext(
        (1484640257608L, Vector(1484640257608L), Vector(1484640257608L))
      )
      rSrc.sendNext(1484640259700L)
      lSrc.sendNext(1484640259700L)
      sink.request(1)
      sink.expectNext(
        (1484640258648L, Vector(1484640258648L), Vector(1484640258648L))
      )
      rSrc.sendNext(1484640260633L)
      lSrc.sendNext(1484640260633L)
      sink.request(1)
      sink.expectNext(
        (1484640259700L, Vector(1484640259700L), Vector(1484640259700L))
      )
      rSrc.sendNext(1484640260754L)
      lSrc.sendNext(1484640260754L)
      sink.request(1)
      sink.expectNext(
        (1484640260633L, Vector(1484640260633L), Vector(1484640260633L))
      )
      rSrc.sendNext(1484640261728L)
      lSrc.sendNext(1484640261728L)
      sink.request(1)
      sink.expectNext(
        (1484640260754L, Vector(1484640260754L), Vector(1484640260754L))
      )
      rSrc.sendNext(1484640262721L)
      lSrc.sendNext(1484640262721L)
      sink.request(1)
      sink.expectNext(
        (1484640261728L, Vector(1484640261728L), Vector(1484640261728L))
      )
      rSrc.sendNext(1484640262843L)
      lSrc.sendNext(1484640262843L)
      sink.request(1)
      sink.expectNext(
        (1484640262721L, Vector(1484640262721L), Vector(1484640262721L))
      )
      rSrc.sendNext(1484640263791L)
      lSrc.sendNext(1484640263791L)
      sink.request(1)
      sink.expectNext(
        (1484640262843L, Vector(1484640262843L), Vector(1484640262843L))
      )
      rSrc.sendNext(1484640264902L)
      lSrc.sendNext(1484640264902L)
      sink.request(1)
      sink.expectNext(
        (1484640263791L, Vector(1484640263791L), Vector(1484640263791L))
      )
      rSrc.sendNext(1484640265854L)
      lSrc.sendNext(1484640265854L)
      sink.request(1)
      sink.expectNext(
        (1484640264902L, Vector(1484640264902L), Vector(1484640264902L))
      )
      rSrc.sendNext(1484640266919L)
      lSrc.sendNext(1484640266919L)
      sink.request(1)
      sink.expectNext(
        (1484640265854L, Vector(1484640265854L), Vector(1484640265854L))
      )
      rSrc.sendNext(1484640267893L)
      lSrc.sendNext(1484640267893L)
      sink.request(1)
      sink.expectNext(
        (1484640266919L, Vector(1484640266919L), Vector(1484640266919L))
      )
      rSrc.sendNext(1484640268015L)
      lSrc.sendNext(1484640268015L)
      sink.request(1)
      sink.expectNext(
        (1484640267893L, Vector(1484640267893L), Vector(1484640267893L))
      )
      rSrc.sendNext(1484640268972L)
      lSrc.sendNext(1484640268972L)
      sink.request(1)
      sink.expectNext(
        (1484640268015L, Vector(1484640268015L), Vector(1484640268015L))
      )
      rSrc.sendNext(1484640269967L)
      lSrc.sendNext(1484640269967L)
      sink.request(1)
      sink.expectNext(
        (1484640268972L, Vector(1484640268972L), Vector(1484640268972L))
      )
      rSrc.sendNext(1484640270089L)
      lSrc.sendNext(1484640270089L)
      sink.request(1)
      sink.expectNext(
        (1484640269967L, Vector(1484640269967L), Vector(1484640269967L))
      )
      rSrc.sendNext(1484640271032L)
      lSrc.sendNext(1484640271032L)
      sink.request(1)
      sink.expectNext(
        (1484640270089L, Vector(1484640270089L), Vector(1484640270089L))
      )
      rSrc.sendNext(1484640272079L)
      lSrc.sendNext(1484640272079L)
      sink.request(1)
      sink.expectNext(
        (1484640271032L, Vector(1484640271032L), Vector(1484640271032L))
      )
      rSrc.sendNext(1484640273142L)
      lSrc.sendNext(1484640273142L)
      sink.request(1)
      sink.expectNext(
        (1484640272079L, Vector(1484640272079L), Vector(1484640272079L))
      )
      rSrc.sendNext(1484640274181L)
      lSrc.sendNext(1484640274181L)
      sink.request(1)
      sink.expectNext(
        (1484640273142L, Vector(1484640273142L), Vector(1484640273142L))
      )
      rSrc.sendNext(1484640274887L)
      lSrc.sendNext(1484640274887L)
      sink.request(1)
      sink.expectNext(
        (1484640274181L, Vector(1484640274181L), Vector(1484640274181L))
      )
      rSrc.sendNext(1484640276124L)
      lSrc.sendNext(1484640276124L)
      sink.request(1)
      sink.expectNext(
        (1484640274887L, Vector(1484640274887L), Vector(1484640274887L))
      )
      rSrc.sendNext(1484640276253L)
      lSrc.sendNext(1484640276253L)
      sink.request(1)
      sink.expectNext(
        (1484640276124L, Vector(1484640276124L), Vector(1484640276124L))
      )
      rSrc.sendNext(1484640277211L)
      lSrc.sendNext(1484640277211L)
      sink.request(1)
      sink.expectNext(
        (1484640276253L, Vector(1484640276253L), Vector(1484640276253L))
      )
      rSrc.sendNext(1484640278301L)
      lSrc.sendNext(1484640278301L)
      sink.request(1)
      sink.expectNext(
        (1484640277211L, Vector(1484640277211L), Vector(1484640277211L))
      )
      rSrc.sendNext(1484640279240L)
      lSrc.sendNext(1484640279240L)
      sink.request(1)
      sink.expectNext(
        (1484640278301L, Vector(1484640278301L), Vector(1484640278301L))
      )
      rSrc.sendNext(1484640279363L)
      lSrc.sendNext(1484640279363L)
      sink.request(1)
      sink.expectNext(
        (1484640279240L, Vector(1484640279240L), Vector(1484640279240L))
      )
      rSrc.sendNext(1484640280330L)
      lSrc.sendNext(1484640280330L)
      sink.request(1)
      sink.expectNext(
        (1484640279363L, Vector(1484640279363L), Vector(1484640279363L))
      )
      rSrc.sendNext(1484640280451L)
      lSrc.sendNext(1484640280451L)
      sink.request(1)
      sink.expectNext(
        (1484640280330L, Vector(1484640280330L), Vector(1484640280330L))
      )
      rSrc.sendNext(1484640281414L)
      lSrc.sendNext(1484640281414L)
      sink.request(1)
      sink.expectNext(
        (1484640280451L, Vector(1484640280451L), Vector(1484640280451L))
      )
      rSrc.sendNext(1484640282459L)
      lSrc.sendNext(1484640282459L)
      sink.request(1)
      sink.expectNext(
        (1484640281414L, Vector(1484640281414L), Vector(1484640281414L))
      )
      rSrc.sendNext(1484640283495L)
      lSrc.sendNext(1484640283495L)
      sink.request(1)
      sink.expectNext(
        (1484640282459L, Vector(1484640282459L), Vector(1484640282459L))
      )
      rSrc.sendNext(1484640284472L)
      lSrc.sendNext(1484640284472L)
      sink.request(1)
      sink.expectNext(
        (1484640283495L, Vector(1484640283495L), Vector(1484640283495L))
      )
      rSrc.sendNext(1484640284592L)
      lSrc.sendNext(1484640284592L)
      sink.request(1)
      sink.expectNext(
        (1484640284472L, Vector(1484640284472L), Vector(1484640284472L))
      )
      rSrc.sendNext(1484640285550L)
      lSrc.sendNext(1484640285550L)
      sink.request(1)
      sink.expectNext(
        (1484640284592L, Vector(1484640284592L), Vector(1484640284592L))
      )
      rSrc.sendNext(1484640286661L)
      lSrc.sendNext(1484640286661L)
      sink.request(1)
      sink.expectNext(
        (1484640285550L, Vector(1484640285550L), Vector(1484640285550L))
      )
      rSrc.sendNext(1484640287600L)
      lSrc.sendNext(1484640287600L)
      sink.request(1)
      sink.expectNext(
        (1484640286661L, Vector(1484640286661L), Vector(1484640286661L))
      )
      rSrc.sendNext(1484640288695L)
      lSrc.sendNext(1484640288695L)
      sink.request(1)
      sink.expectNext(
        (1484640287600L, Vector(1484640287600L), Vector(1484640287600L))
      )
      rSrc.sendNext(1484640289696L)
      lSrc.sendNext(1484640289696L)
      sink.request(1)
      sink.expectNext(
        (1484640288695L, Vector(1484640288695L), Vector(1484640288695L))
      )
      rSrc.sendNext(1484640290797L)
      lSrc.sendNext(1484640290797L)
      sink.request(1)
      sink.expectNext(
        (1484640289696L, Vector(1484640289696L), Vector(1484640289696L))
      )
      rSrc.sendNext(1484640291738L)
      lSrc.sendNext(1484640291738L)
      sink.request(1)
      sink.expectNext(
        (1484640290797L, Vector(1484640290797L), Vector(1484640290797L))
      )
      rSrc.sendNext(1484640291859L)
      lSrc.sendNext(1484640291859L)
      sink.request(1)
      sink.expectNext(
        (1484640291738L, Vector(1484640291738L), Vector(1484640291738L))
      )
      rSrc.sendNext(1484640292815L)
      lSrc.sendNext(1484640292815L)
      sink.request(1)
      sink.expectNext(
        (1484640291859L, Vector(1484640291859L), Vector(1484640291859L))
      )
      rSrc.sendNext(1484640293896L)
      lSrc.sendNext(1484640293896L)
      sink.request(1)
      sink.expectNext(
        (1484640292815L, Vector(1484640292815L), Vector(1484640292815L))
      )
      rSrc.sendNext(1484640294830L)
      lSrc.sendNext(1484640294830L)
      sink.request(1)
      sink.expectNext(
        (1484640293896L, Vector(1484640293896L), Vector(1484640293896L))
      )
      rSrc.sendNext(1484640294946L)
      lSrc.sendNext(1484640294946L)
      sink.request(1)
      sink.expectNext(
        (1484640294830L, Vector(1484640294830L), Vector(1484640294830L))
      )
      rSrc.sendNext(1484640295905L)
      lSrc.sendNext(1484640295905L)
      sink.request(1)
      sink.expectNext(
        (1484640294946L, Vector(1484640294946L), Vector(1484640294946L))
      )
      rSrc.sendNext(1484640296022L)
      lSrc.sendNext(1484640296022L)
      sink.request(1)
      sink.expectNext(
        (1484640295905L, Vector(1484640295905L), Vector(1484640295905L))
      )
      rSrc.sendNext(1484640296950L)
      lSrc.sendNext(1484640296950L)
      sink.request(1)
      sink.expectNext(
        (1484640296022L, Vector(1484640296022L), Vector(1484640296022L))
      )
      rSrc.sendNext(1484640297067L)
      lSrc.sendNext(1484640297067L)
      sink.request(1)
      sink.expectNext(
        (1484640296950L, Vector(1484640296950L), Vector(1484640296950L))
      )
      rSrc.sendNext(1484640298044L)
      lSrc.sendNext(1484640298044L)
      sink.request(1)
      sink.expectNext(
        (1484640297067L, Vector(1484640297067L), Vector(1484640297067L))
      )
      rSrc.sendNext(1484640299113L)
      lSrc.sendNext(1484640299113L)
      sink.request(1)
      sink.expectNext(
        (1484640298044L, Vector(1484640298044L), Vector(1484640298044L))
      )
      rSrc.sendNext(1484640300085L)
      lSrc.sendNext(1484640300085L)
      sink.request(1)
      sink.expectNext(
        (1484640299113L, Vector(1484640299113L), Vector(1484640299113L))
      )
      rSrc.sendNext(1484640300209L)
      lSrc.sendNext(1484640300209L)
      sink.request(1)
      sink.expectNext(
        (1484640300085L, Vector(1484640300085L), Vector(1484640300085L))
      )
      rSrc.sendNext(1484640301170L)
      lSrc.sendNext(1484640301170L)
      sink.request(1)
      sink.expectNext(
        (1484640300209L, Vector(1484640300209L), Vector(1484640300209L))
      )
      rSrc.sendNext(1484640302255L)
      lSrc.sendNext(1484640302255L)
      sink.request(1)
      sink.expectNext(
        (1484640301170L, Vector(1484640301170L), Vector(1484640301170L))
      )
      rSrc.sendNext(1484640303215L)
      lSrc.sendNext(1484640303215L)
      sink.request(1)
      sink.expectNext(
        (1484640302255L, Vector(1484640302255L), Vector(1484640302255L))
      )
      rSrc.sendNext(1484640303335L)
      lSrc.sendNext(1484640303335L)
      sink.request(1)
      sink.expectNext(
        (1484640303215L, Vector(1484640303215L), Vector(1484640303215L))
      )
      rSrc.sendNext(1484640304294L)
      lSrc.sendNext(1484640304294L)
      sink.request(1)
      sink.expectNext(
        (1484640303335L, Vector(1484640303335L), Vector(1484640303335L))
      )
      rSrc.sendNext(1484640304416L)
      lSrc.sendNext(1484640304416L)
      sink.request(1)
      sink.expectNext(
        (1484640304294L, Vector(1484640304294L), Vector(1484640304294L))
      )
      rSrc.sendNext(1484640305368L)
      lSrc.sendNext(1484640305368L)
      sink.request(1)
      sink.expectNext(
        (1484640304416L, Vector(1484640304416L), Vector(1484640304416L))
      )
      rSrc.sendNext(1484640305491L)
      lSrc.sendNext(1484640305491L)
      sink.request(1)
      sink.expectNext(
        (1484640305368L, Vector(1484640305368L), Vector(1484640305368L))
      )
      rSrc.sendNext(1484640306465L)
      lSrc.sendNext(1484640306465L)
      sink.request(1)
      sink.expectNext(
        (1484640305491L, Vector(1484640305491L), Vector(1484640305491L))
      )
      rSrc.sendNext(1484640307545L)
      lSrc.sendNext(1484640307545L)
      sink.request(1)
      sink.expectNext(
        (1484640306465L, Vector(1484640306465L), Vector(1484640306465L))
      )
      rSrc.sendNext(1484640308522L)
      lSrc.sendNext(1484640308522L)
      sink.request(1)
      sink.expectNext(
        (1484640307545L, Vector(1484640307545L), Vector(1484640307545L))
      )
      rSrc.sendNext(1484640309616L)
      lSrc.sendNext(1484640309616L)
      sink.request(1)
      sink.expectNext(
        (1484640308522L, Vector(1484640308522L), Vector(1484640308522L))
      )
      rSrc.sendNext(1484640310578L)
      lSrc.sendNext(1484640310578L)
      sink.request(1)
      sink.expectNext(
        (1484640309616L, Vector(1484640309616L), Vector(1484640309616L))
      )
      rSrc.sendNext(1484640310700L)
      lSrc.sendNext(1484640310700L)
      sink.request(1)
      sink.expectNext(
        (1484640310578L, Vector(1484640310578L), Vector(1484640310578L))
      )
      rSrc.sendNext(1484640311689L)
      lSrc.sendNext(1484640311689L)
      sink.request(1)
      sink.expectNext(
        (1484640310700L, Vector(1484640310700L), Vector(1484640310700L))
      )
      rSrc.sendNext(1484640312791L)
      lSrc.sendNext(1484640312791L)
      sink.request(1)
      sink.expectNext(
        (1484640311689L, Vector(1484640311689L), Vector(1484640311689L))
      )
      rSrc.sendNext(1484640313765L)
      lSrc.sendNext(1484640313765L)
      sink.request(1)
      sink.expectNext(
        (1484640312791L, Vector(1484640312791L), Vector(1484640312791L))
      )
      rSrc.sendNext(1484640314850L)
      lSrc.sendNext(1484640314850L)
      sink.request(1)
      sink.expectNext(
        (1484640313765L, Vector(1484640313765L), Vector(1484640313765L))
      )
      rSrc.sendNext(1484640315826L)
      lSrc.sendNext(1484640315826L)
      sink.request(1)
      sink.expectNext(
        (1484640314850L, Vector(1484640314850L), Vector(1484640314850L))
      )
      rSrc.sendNext(1484640316923L)
      lSrc.sendNext(1484640316923L)
      sink.request(1)
      sink.expectNext(
        (1484640315826L, Vector(1484640315826L), Vector(1484640315826L))
      )
      rSrc.sendNext(1484640317883L)
      lSrc.sendNext(1484640317883L)
      sink.request(1)
      sink.expectNext(
        (1484640316923L, Vector(1484640316923L), Vector(1484640316923L))
      )
      rSrc.sendNext(1484640318988L)
      lSrc.sendNext(1484640318988L)
      sink.request(1)
      sink.expectNext(
        (1484640317883L, Vector(1484640317883L), Vector(1484640317883L))
      )
      rSrc.sendNext(1484640319955L)
      lSrc.sendNext(1484640319955L)
      sink.request(1)
      sink.expectNext(
        (1484640318988L, Vector(1484640318988L), Vector(1484640318988L))
      )
      rSrc.sendNext(1484640320075L)
      lSrc.sendNext(1484640320075L)
      sink.request(1)
      sink.expectNext(
        (1484640319955L, Vector(1484640319955L), Vector(1484640319955L))
      )
      rSrc.sendNext(1484640321003L)
      lSrc.sendNext(1484640321003L)
      sink.request(1)
      sink.expectNext(
        (1484640320075L, Vector(1484640320075L), Vector(1484640320075L))
      )
      rSrc.sendNext(1484640321122L)
      lSrc.sendNext(1484640321122L)
      sink.request(1)
      sink.expectNext(
        (1484640321003L, Vector(1484640321003L), Vector(1484640321003L))
      )
      rSrc.sendNext(1484640322060L)
      lSrc.sendNext(1484640322060L)
      sink.request(1)
      sink.expectNext(
        (1484640321122L, Vector(1484640321122L), Vector(1484640321122L))
      )
      rSrc.sendNext(1484640322179L)
      lSrc.sendNext(1484640322179L)
      sink.request(1)
      sink.expectNext(
        (1484640322060L, Vector(1484640322060L), Vector(1484640322060L))
      )
      rSrc.sendNext(1484640323133L)
      lSrc.sendNext(1484640323133L)
      sink.request(1)
      sink.expectNext(
        (1484640322179L, Vector(1484640322179L), Vector(1484640322179L))
      )
      rSrc.sendNext(1484640324149L)
      lSrc.sendNext(1484640324149L)
      sink.request(1)
      sink.expectNext(
        (1484640323133L, Vector(1484640323133L), Vector(1484640323133L))
      )
      rSrc.sendNext(1484640324275L)
      lSrc.sendNext(1484640324275L)
      sink.request(1)
      sink.expectNext(
        (1484640324149L, Vector(1484640324149L), Vector(1484640324149L))
      )
      rSrc.sendNext(1484640325232L)
      lSrc.sendNext(1484640325232L)
      sink.request(1)
      sink.expectNext(
        (1484640324275L, Vector(1484640324275L), Vector(1484640324275L))
      )
      rSrc.sendNext(1484640325365L)
      lSrc.sendNext(1484640325365L)
      sink.request(1)
      sink.expectNext(
        (1484640325232L, Vector(1484640325232L), Vector(1484640325232L))
      )
      rSrc.sendNext(1484640326331L)
      lSrc.sendNext(1484640326331L)
      sink.request(1)
      sink.expectNext(
        (1484640325365L, Vector(1484640325365L), Vector(1484640325365L))
      )
      rSrc.sendNext(1484640326455L)
      lSrc.sendNext(1484640326455L)
      sink.request(1)
      sink.expectNext(
        (1484640326331L, Vector(1484640326331L), Vector(1484640326331L))
      )
      rSrc.sendNext(1484640327462L)
      lSrc.sendNext(1484640327462L)
      sink.request(1)
      sink.expectNext(
        (1484640326455L, Vector(1484640326455L), Vector(1484640326455L))
      )
      rSrc.sendNext(1484640328426L)
      lSrc.sendNext(1484640328426L)
      sink.request(1)
      sink.expectNext(
        (1484640327462L, Vector(1484640327462L), Vector(1484640327462L))
      )
      rSrc.sendNext(1484640329510L)
      lSrc.sendNext(1484640329510L)
      sink.request(1)
      sink.expectNext(
        (1484640328426L, Vector(1484640328426L), Vector(1484640328426L))
      )
      rSrc.sendNext(1484640330477L)
      lSrc.sendNext(1484640330477L)
      sink.request(1)
      sink.expectNext(
        (1484640329510L, Vector(1484640329510L), Vector(1484640329510L))
      )
      rSrc.sendNext(1484640330603L)
      lSrc.sendNext(1484640330603L)
      sink.request(1)
      sink.expectNext(
        (1484640330477L, Vector(1484640330477L), Vector(1484640330477L))
      )
      rSrc.sendNext(1484640331570L)
      lSrc.sendNext(1484640331570L)
      sink.request(1)
      sink.expectNext(
        (1484640330603L, Vector(1484640330603L), Vector(1484640330603L))
      )
      rSrc.sendNext(1484640332650L)
      lSrc.sendNext(1484640332650L)
      sink.request(1)
      sink.expectNext(
        (1484640331570L, Vector(1484640331570L), Vector(1484640331570L))
      )
      rSrc.sendNext(1484640333631L)
      lSrc.sendNext(1484640333631L)
      sink.request(1)
      sink.expectNext(
        (1484640332650L, Vector(1484640332650L), Vector(1484640332650L))
      )
      rSrc.sendNext(1484640333756L)
      lSrc.sendNext(1484640333756L)
      sink.request(1)
      sink.expectNext(
        (1484640333631L, Vector(1484640333631L), Vector(1484640333631L))
      )
      rSrc.sendNext(1484640334709L)
      lSrc.sendNext(1484640334709L)
      sink.request(1)
      sink.expectNext(
        (1484640333756L, Vector(1484640333756L), Vector(1484640333756L))
      )
      rSrc.sendNext(1484640335797L)
      lSrc.sendNext(1484640335797L)
      sink.request(1)
      sink.expectNext(
        (1484640334709L, Vector(1484640334709L), Vector(1484640334709L))
      )
      rSrc.sendNext(1484640336755L)
      lSrc.sendNext(1484640336755L)
      sink.request(1)
      sink.expectNext(
        (1484640335797L, Vector(1484640335797L), Vector(1484640335797L))
      )
      rSrc.sendNext(1484640336878L)
      lSrc.sendNext(1484640336878L)
      sink.request(1)
      sink.expectNext(
        (1484640336755L, Vector(1484640336755L), Vector(1484640336755L))
      )
      rSrc.sendNext(1484640337850L)
      lSrc.sendNext(1484640337850L)
      sink.request(1)
      sink.expectNext(
        (1484640336878L, Vector(1484640336878L), Vector(1484640336878L))
      )
      rSrc.sendNext(1484640338946L)
      lSrc.sendNext(1484640338946L)
      sink.request(1)
      sink.expectNext(
        (1484640337850L, Vector(1484640337850L), Vector(1484640337850L))
      )
      rSrc.sendNext(1484640339908L)
      lSrc.sendNext(1484640339908L)
      sink.request(1)
      sink.expectNext(
        (1484640338946L, Vector(1484640338946L), Vector(1484640338946L))
      )
      rSrc.sendNext(1484640340998L)
      lSrc.sendNext(1484640340998L)
      sink.request(1)
      sink.expectNext(
        (1484640339908L, Vector(1484640339908L), Vector(1484640339908L))
      )
      rSrc.sendNext(1484640341978L)
      lSrc.sendNext(1484640341978L)
      sink.request(1)
      sink.expectNext(
        (1484640340998L, Vector(1484640340998L), Vector(1484640340998L))
      )
      rSrc.sendNext(1484640342098L)
      lSrc.sendNext(1484640342098L)
      sink.request(1)
      sink.expectNext(
        (1484640341978L, Vector(1484640341978L), Vector(1484640341978L))
      )
      rSrc.sendNext(1484640343051L)
      lSrc.sendNext(1484640343051L)
      sink.request(1)
      sink.expectNext(
        (1484640342098L, Vector(1484640342098L), Vector(1484640342098L))
      )
      rSrc.sendNext(1484640344143L)
      lSrc.sendNext(1484640344143L)
      sink.request(1)
      sink.expectNext(
        (1484640343051L, Vector(1484640343051L), Vector(1484640343051L))
      )
      rSrc.sendNext(1484640345114L)
      lSrc.sendNext(1484640345114L)
      sink.request(1)
      sink.expectNext(
        (1484640344143L, Vector(1484640344143L), Vector(1484640344143L))
      )
      rSrc.sendNext(1484640345353L)
      lSrc.sendNext(1484640345353L)
      sink.request(1)
      sink.expectNext(
        (1484640345114L, Vector(1484640345114L), Vector(1484640345114L))
      )
      lSrc.sendNext(1484640908898L)
      rSrc.sendNext(1484640908898L)
      lSrc.sendComplete()
      rSrc.sendComplete()
      sink.request(1)
      sink.expectNext(
        (1484640345353L, Vector(1484640345353L), Vector(1484640345353L))
      )
      sink.request(1)
      sink.expectNext(
        (1484640908898L, Vector(1484640908898L), Vector(1484640908898L))
      )
      sink.expectComplete()
    }
  }
}

class SortedStreamsMergeByTestsAutoFusingOn extends { val autoFusing = true }
with SortedStreamsMergeByTests
class SortedStreamsMergeByTestsAutoFusingOff extends { val autoFusing = false }
with SortedStreamsMergeByTests
