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


package cmwell.it

import cmwell.util.exceptions.stackTraceToString
import cmwell.util.concurrent.SimpleScheduler._
import cmwell.util.concurrent._
import cmwell.util.http.SimpleResponse
import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Inspectors, Matchers}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success, Try}

class ConsumeTests extends AsyncFunSpec with Inspectors with Matchers with Helpers with fixture.NSHashesAndPrefixes with LazyLogging {
  describe("consume API") {

    type ConsumeResults = (Int, Option[String], String)

    val requestHandler: SimpleResponse[String] => ConsumeResults = { res =>
      val sta = res.status
      val pos = res.headers.find(_._1 == "X-CM-WELL-POSITION").map(_._2)
      val bod = res.payload
      (sta, pos, bod)
    }

    def ingest(nt: String): Future[SimpleResponse[String]] = Http.post(_in, nt, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader)

    val firstConsumableChunk: Future[ConsumeResults] = {
      val file = Source.fromURL(this.getClass.getResource("/consumeable-data1.nt")).mkString
      ingest(file).map(requestHandler)
    }

    val secondConsumableChunk: Future[ConsumeResults] = {
      val file = Source.fromURL(this.getClass.getResource("/consumeable-data2.nt")).mkString
      //2 phase ingest to ensure different indexTime values
      firstConsumableChunk.flatMap(_ => SimpleScheduler.scheduleFuture(10.millis){
        ingest(file).map(requestHandler)
      })
    }

    // we wait 40 seconds because bulk consume streams only results with indexTime up to [event-horizon] - [30 seconds],
    // and 10 extra seconds to be sure indexing in ES occurred >30 seconds ago
    val waitForIngestPlus40Seconds: Future[Unit] = secondConsumableChunk.flatMap(_ => schedule(40.seconds)(()))

    val path = cmw / "example.net" / "Consumeable"
    val badReq1 = Http.get(path, List("op" -> "consume")).map(_.status should be(400))
    val badReq2 = Http.get(path, List("op" -> "bulk-consume")).map(_.status should be(400))

    // scalastyle:off
    val f1 = waitForIngestPlus40Seconds.flatMap(_ => Http.get(path, List("op" -> "create-consumer")).map(requestHandler))
    val f2 = f1.flatMap(t => Http.get(path, List("op" -> "consume", "position" -> t._2.get, "length-hint" -> "7")).map(requestHandler))
    val f3 = f2.flatMap(t => Http.get(path, List("op" -> "consume", "position" -> t._2.get, "length-hint" -> "613")).map(requestHandler))
    val f4 = f3.flatMap(t => Http.get(path, List("op" -> "consume", "position" -> t._2.get)).map(requestHandler))

    val g0 = waitForIngestPlus40Seconds.flatMap(_ => Http.post(path, "qp=collaboratesWith.rel::http://example.net/Consumeable/RK",Some("application/x-www-form-urlencoded"),queryParams = List("op" -> "create-consumer"),headers = tokenHeader).map(requestHandler))
    val g1 = waitForIngestPlus40Seconds.flatMap(_ => Http.get(path, List("op" -> "create-consumer", "qp" -> "collaboratesWith.rel::http://example.net/Consumeable/RK")).map(requestHandler))
    val g2 = g1.flatMap(t => Http.get(path, List("op" -> "consume", "position" -> t._2.get)).map(requestHandler))

    val h1 = waitForIngestPlus40Seconds.flatMap(_ => Http.get(path, List("op" -> "create-consumer")).map(requestHandler))
    val h2 = h1.flatMap {
      case (_,Some(p),_) => Http.get(cmw / "_bulk-consume", List("format" -> "tsv", "position" -> p)).map(requestHandler)
      case (_, None, _) => Future.failed[(Int,Option[String],String)](new IllegalStateException("No Position supplied from previous session"))
    }

    val i1 = f1.flatMap ( t => Http.get(_consume, List("position" -> t._2.get, "length-hint" -> "613", "format" -> "json", "with-data" -> "", "gqp" -> "<parentOf.rel")))
    val i2 = f1.flatMap ( t => Http.get(_consume, List("position" -> t._2.get, "length-hint" -> "613", "format" -> "json", "with-data" -> "", "gqp" -> "<parentOf.rel[friendOf.rel:]")))
    val i3 = f1.flatMap ( t => Http.get(_consume, List("position" -> t._2.get, "length-hint" -> "613", "format" -> "json", "with-data" -> "", "gqp" -> ">parentOf.rel")))
    val i4 = f1.flatMap ( t => Http.get(_consume, List("position" -> t._2.get, "length-hint" -> "613", "format" -> "json", "with-data" -> "", "gqp" -> ">parentOf.rel[childOf.rel:]")))

    val j1 = f1.flatMap ( t => Http.get(_consume, List("position" -> t._2.get, "length-hint" -> "613", "format" -> "ntriples", "with-data" -> "", "fields" -> "siblingOf.rel")))

    // scalastyle:on
    it("should ingest data to consume later") {

      firstConsumableChunk.zip(secondConsumableChunk).map {
        case clue@((s1,_,b1),(s2,_,b2)) => withClue(clue){
          s1 should be(200)
          s1 should be(200)
          Try {
            val j1 = Json.parse(b1)
            val j2 = Json.parse(b2)
            j1 -> j2
          } match {
            case Failure(error) => fail(stackTraceToString(error))
            case Success((j1, j2)) => {
              j1 shouldEqual jsonSuccess
              j2 shouldEqual jsonSuccess
            }
          }
        }
      }
    }

    describe("without qp") {

      it("should create an initial position") {
        f1.map { case (sta, pos, bod) =>
          sta should be(200)
          pos.isDefined should be(true)
          bod shouldEqual ""
        }
      }

      it("should receive BAD REQUEST if no position is supplied")(badReq1)

      val pathCount = Promise[Int]()

      it("should receive the first chunk") {
        f2.map {
          case t@(sta, pos, bod) => withClue(t) {
            sta should be(200)
            pos.isDefined should be(true)
            val indexTimes = bod.trim.lines.map { s =>
              (Json.parse(s) \ "system" \ "indexTime").as[Long]
            }.toVector
            pathCount.success(indexTimes.length)
            // either there are 7 infotons, and the last ones is dropped,
            // or all share same indexTime, which in this case all infotons with same indexTime are returned.
            if(indexTimes.length < 7) succeed
            else forAll(indexTimes)(_ shouldEqual indexTimes.head)
          }
        }
      }

      it("should receive the second chunk") {
        pathCount.future.flatMap { pCount =>
          f3.map {
            case (sta, pos, bod) =>
              sta should be(200)
              pos.isDefined should be(true)
              val pathCount = bod.trim.lines.count(_.trim.nonEmpty)
              pathCount should be(11 - pCount)
          }
        }
      }

      it("should receive empty content if queried for further elements") {
        f4.map {
          case (sta, pos, bod) =>
            sta should be(204)
            pos.isDefined should be(true)
            bod shouldEqual ""
        }
      }
    }

    describe("with qp") {

      it("should create an initial position") {
        g1.map {
          case (sta, pos, bod) =>
            sta should be(200)
            pos.isDefined should be(true)
            bod shouldEqual ""
        }
      }

      it("should create same position from a urlencoded POST request") {
        g0.zip(g1).map {
          case (t0,t1) =>
            t0 shouldEqual t1
        }
      }

      it("should receive the first and only chunk") {
        g2.map {
          case (sta, pos, bod) => withClue(s"body: '${bod.take(25)}...'") {
            sta should be(200)
            pos.isDefined should be(true)
            bod.trim.lines.count(_.trim.nonEmpty) should be(1)
          }
        }
      }
    }

    describe("with gqp") {
      it("should filter only infotons which is pointed by parentOf.rel attributes") {
        i1.map { r =>
          withClue(r) {
            r.status should be(200)
            r.payload.trim.lines.size should be(2)
          }
        }
      }

      it("should filter only infotons which is pointed by parentOf.rel attributes and has childOf.rel attributes") {
        i2.map { r =>
          withClue(r) {
            r.status should be(200)
            r.payload.trim.lines.size should be(1)
          }
        }
      }

      it("should filter only infotons which points to parentOf.rel attributes") {
        i3.map { r =>
          withClue(r) {
            r.status should be(200)
            r.payload.trim.lines.size should be(2)
          }
        }
      }

      it("should filter only infotons which points to parentOf.rel attributes and has childOf.rel attributes") {
        i4.map { r =>
          withClue(r) {
            r.status should be(200)
            r.payload.trim.lines.size should be(1)
          }
        }
      }
    }

    it("should mask fields when fields parameter is supplied") {
      j1.map { r =>
        withClue(r) {
          r.status should be(200)
          r.payload.trim.lines.filterNot(_.contains("/meta/sys#")).size should be(2)
        }
      }
    }

    describe("bulk consume") {

      it("should create an initial position") {
        h1.map {
          case (sta, pos, bod) =>
            sta should be(200)
            pos.isDefined should be(true)
            bod shouldEqual ""
        }
      }

      it("should receive BAD REQUEST if no position is supplied")(badReq2)

      it("should stream the first and only chunk") {
        h2.map {
          case (sta, pos, bod) => withClue {
            val b = if (bod.nonEmpty) s"body: '${bod.take(320)}...'" else "empty body"
            val p = "position: " + pos.fold("N/A")(e => cmwell.util.string.Zip.decompress(cmwell.util.string.Base64.decodeBase64(e)))
            b + "," + p + ", current time: " + System.currentTimeMillis()
          } {
            sta should be(200)
            pos.isDefined should be(true)
            bod.trim.lines.count(_.trim.nonEmpty) should be(11)
          }
        }
      }
    }
  }
}
