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


package cmwell.it

import cmwell.util.concurrent._
import cmwell.util.http.SimpleResponse
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.json._
import org.scalatest._

import scala.concurrent.{ExecutionContext, Future, duration}
import duration.DurationInt
import scala.util.Try

class IterationsTests extends AsyncFunSpec with Matchers with Inspectors with Helpers with fixture.NSHashesAndPrefixes with LazyLogging {
  describe("Iterator API") {
    type ScrollResults = (Int,Option[String],Option[Long],Option[(Boolean,Int)],Option[String])
    val requestHandler: SimpleResponse[Array[Byte]] => ScrollResults = { res =>
      val s = res.status
      val j = Json.parse(res.payload)
      (
        s,
        Try(j.\("iteratorId").as[String]).toOption,
        Try(j.\("totalHits").as[Long]).toOption,
        Try(((j \ "infotons").get : @unchecked) match {
          case JsArray(iSeq) => iSeq.forall{
            case obj: JsObject => true
            case _ => false
          } -> iSeq.size
        }).toOption,
        Try(res.toString()).toOption //Try(j.\("searchQueryStr").as[String]).toOption
      )
    }

    val scrollDataFuture = Future.traverse(1 to 70){ i =>
      val path = cmw / "scroll" / s"InfoObj$i"
      val body = Json.obj("name" -> ("scroller" + i), "family" -> ("froller" + 1))
      Http.post(path, Json.stringify(body), None, Nil, ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
    }.map { seq =>
      forAll(seq) { res =>
        res.status should be(200)
      }
    }
    val ldScrollDataFuture = {
      val triples = (1 to 70).map { i =>
        s"""<http://ld-scroll.com/s-$i> <http://purl.org/dc/elements/1.1/identifier> "$i"^^<http://www.w3.org/2001/XMLSchema#int> ."""
      }.mkString("\n")
      Http.post(_in, triples, Some("application/n-quads;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader).map { res =>
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }

    val path = cmw / "scroll"

    val allIngestedSuccessfully = spinCheck[Array[Byte]](1.second)(Http.get(path, List("op" -> "search", "length" -> "1", "format" -> "json"))){ r =>
      r.status == 200 && {
        val j = Json.parse(r.payload)
        (j \ "results" \ "total" : @unchecked) match {
          case JsDefined(JsNumber(bigDec)) => bigDec.intValue() == 70
        }
      }
    }

    def executeAfterIndexing[T](body: =>Future[T]): Future[T] = allIngestedSuccessfully.flatMap(_ => body)

    val f1 = executeAfterIndexing(Http.get(path, List("op" -> "create-iterator", "session-ttl" -> "60", "length" -> "20", "format" -> "json")).map(requestHandler))
    val f2 = f1.flatMap(t => Http.get(path, List("op" -> "next-chunk", "iterator-id" -> t._2.get, "session-ttl" -> "60", "format" -> "json")).map(requestHandler))
    val f3 = f2.flatMap(t => Http.get(path, List("op" -> "next-chunk", "iterator-id" -> t._2.get, "session-ttl" -> "60", "format" -> "json")).map(requestHandler))
    val f4 = f3.flatMap(t => Http.get(path, List("op" -> "next-chunk", "iterator-id" -> t._2.get, "session-ttl" -> "60", "format" -> "json")).map(requestHandler))
    val f5 = f4.flatMap(t => Http.get(path, List("op" -> "next-chunk", "iterator-id" -> t._2.get, "session-ttl" -> "60", "format" -> "json")).map(requestHandler))

    val deleteSomeValues = executeAfterCompletion(f5){
      Future.traverse(1 to 70 by 2 : IndexedSeq[Int]){ i =>
        Http.delete(path / s"InfoObj$i", Nil, ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
      }.map { seq =>
        forAll(seq) { res =>
          res.status should be(200)
        }
      }
    }

    val allDeletedSuccessfully = executeAfterCompletion(deleteSomeValues)(spinCheck[Array[Byte]](1.second)(Http.get(path, List("op" -> "search", "length" -> "1", "format" -> "json"))){ r =>
      r.status == 200 && {
        val j = Json.parse(r.payload)
        (j \ "results" \ "total" : @unchecked) match {
          case JsDefined(JsNumber(bigDec)) => bigDec.intValue() == 35
        }
      }
    })

    val d1 = executeAfterCompletion(allDeletedSuccessfully)(Http.get(path, List("op" -> "create-iterator", "session-ttl" -> "60", "length" -> "20", "format" -> "json", "with-history" -> "", "qp" -> "system.kind:DeletedInfoton")).map(requestHandler))
    val d2 = d1.flatMap(t => Http.get(path, List("op" -> "next-chunk", "iterator-id" -> t._2.get, "session-ttl" -> "60", "format" -> "json")).map(requestHandler))
    val d3 = d2.flatMap(t => Http.get(path, List("op" -> "next-chunk", "iterator-id" -> t._2.get, "session-ttl" -> "60", "format" -> "json")).map(requestHandler))

    val ldPath = cmw / "ld-scroll.com"

    val allLdIngestedSuccessfully = spinCheck[Array[Byte]](1.second)(Http.get(ldPath, List("op" -> "search", "qp" -> "identifier.dc<<50", "length" -> "1", "format" -> "json"))){ r =>
      r.status == 200 && {
        val j = Json.parse(r.payload)
        (j \ "results" \ "total" : @unchecked) match {
          case JsDefined(JsNumber(bigDec)) => bigDec.intValue() == 50
        }
      }
    }

    def executeAfterIndexing2[T](body: =>Future[T]): Future[T] = allLdIngestedSuccessfully.flatMap(_ => body).recoverWith{
      case _: Throwable => body
    }

    val g1 = executeAfterIndexing2(spinCheck[Array[Byte]](1.second,true)(Http.get(ldPath, List("op" -> "create-iterator", "session-ttl" -> "60", "length" -> "20", "format" -> "json", "qp" -> "identifier.dc<<50", "debug-info" -> ""))){ r =>
      r.status == 200 && {
        val j = Json.parse(r.payload)

        val b1 = (j \ "totalHits" : @unchecked) match {
          case JsDefined(JsNumber(bigDec)) => bigDec.intValue() == 50
        }

        // PassiveFieldTypesCache refreshes only once in every 30 seconds.
        // we need to make sure the right type is used, and it could be only after a cache refresh
        val b2 = scala.util.Try((j \ "searchQueryStr" : @unchecked) match {
          case JsDefined(JsString(sQuery)) => (Json.parse(sQuery) \\ "range" : @unchecked) match {
            case xs => xs.exists {
              case JsObject(m) => m.keysIterator.exists(_.contains('$'))
              case _ => false
            }
          }
        }).getOrElse(false)

        b1 && b2
      }
    }.map(requestHandler))
    val g2 = g1.flatMap(t => Http.get(ldPath, List("op" -> "next-chunk", "iterator-id" -> t._2.get, "session-ttl" -> "60", "format" -> "json")).map(requestHandler))
    val g3 = g2.flatMap(t => Http.get(ldPath, List("op" -> "next-chunk", "iterator-id" -> t._2.get, "session-ttl" -> "60", "format" -> "json")).map(requestHandler))
    val g4 = g3.flatMap(t => Http.get(ldPath, List("op" -> "next-chunk", "iterator-id" -> t._2.get, "session-ttl" -> "60", "format" -> "json")).map(requestHandler))

    val h0 = executeAfterIndexing2(spinCheck[Array[Byte]](1.second,true)(Http.get(ldPath, List("op" -> "create-iterator", "session-ttl" -> "60", "length" -> "20", "format" -> "json", "qp" -> s"identifier.$$${ns.dc}<50", "debug-info" -> ""))){ r =>
      r.status == 200 && {
        val j = Json.parse(r.payload)
        val b1 = (j \ "totalHits" : @unchecked) match {
          case JsDefined(JsNumber(bigDec)) => bigDec.intValue() == 49
        }

        // PassiveFieldTypesCache refreshes only once in every 30 seconds.
        // we need to make sure the right type is used, and it could be only after a cache refresh
        val b2 = scala.util.Try((j \ "searchQueryStr" : @unchecked) match {
          case JsDefined(JsString(sQuery)) => (Json.parse(sQuery) \\ "range" : @unchecked) match {
            case xs => xs.exists {
              case JsObject(m) => m.keysIterator.exists(_.contains('$'))
              case _ => false
            }
          }
        }).getOrElse(false)

        b1 && b2
      }
    }.map(requestHandler))
    val h1 = h0.flatMap {
      case (_, None, _, _, debug) => fail("no iterator id from previous request" + debug.fold("")("; ".+))
      case (_, Some(iteratorID), _, _, _) => Http.get(ldPath, List("op" -> "next-chunk", "iterator-id" -> iteratorID, "session-ttl" -> "60", "format" -> "json")).map(requestHandler)
    }
    val h2 = h1.flatMap {
      case (_, None, _, _, debug) => fail("no iterator id from previous request" + debug.fold("")("; ".+))
      case (_, Some(iteratorID), _, _, _) => Http.get(ldPath, List("op" -> "next-chunk", "iterator-id" -> iteratorID, "session-ttl" -> "60", "format" -> "json")).map(requestHandler)
    }
    val h3 = h2.flatMap {
      case (_, None, _, _, debug) => fail("no iterator id from previous request" + debug.fold("")("; ".+))
      case (_, Some(iteratorID), _, _, _) => Http.get(ldPath, List("op" -> "next-chunk", "iterator-id" -> iteratorID, "session-ttl" -> "60", "format" -> "json")).map(requestHandler)
    }

    it("should insert infotons to scroll on")(scrollDataFuture)

    describe("simple iteration") {

      it("should create iterator-id") {
        f1.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(70))
            infotons should be(Some(true, 0))
          }
        }
      }

      it("should get 1st chunk") {
        f2.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(70))
            infotons should be(Some(true, 20))
          }
        }
      }

      it("should get 2nd chunk") {
        f3.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(70))
            infotons should be(Some(true, 20))
          }
        }
      }

      it("should get 3rd chunk") {
        f4.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(70))
            infotons should be(Some(true, 20))
          }
        }
      }

      it("should get 4th chunk") {
        f5.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(70))
            infotons should be(Some(true, 10))
          }
        }
      }
    }

    it("should delete some values from previous iteration")(deleteSomeValues)

    describe("over deleted infotons") {

      it("should create iterator-id") {
        d1.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(35))
            infotons should be(Some(true, 0))
          }
        }
      }

      it("should get 1st chunk") {
        d2.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(35))
            infotons should be(Some(true, 20))
          }
        }
      }

      it("should get 2nd chunk") {
        d3.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(35))
            infotons should be(Some(true, 15))
          }
        }
      }
    }

    it("should prepare LD infotons to scroll on")(ldScrollDataFuture)

    describe("with RDF range qp") {

      it("should create iterator-id") {
        g1.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(50))
            infotons should be(Some(true, 0))
          }
        }
      }

      it("should get 1st chunk") {
        g2.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(50))
            infotons should be(Some(true, 20))
          }
        }
      }

      it("should get 2nd chunk") {
        g3.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(50))
            infotons should be(Some(true, 20))
          }
        }
      }

      it("should get 3rd chunk") {
        g4.map {
          case (status, _, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(50))
            infotons should be(Some(true, 10))
          }
        }
      }
    }

    describe("with RDF range qp & explicit $dc query") {

      it("should create iterator-id") {
        h0.map {
          case (status, _, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(49))
            infotons should be(Some(true, 0))
          }
        }
      }

      it("should get 1st chunk") {
        h1.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(49))
            infotons should be(Some(true, 20))
          }
        }
      }

      it("should get 2nd chunk") {
        h2.map {
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(49))
            infotons should be(Some(true, 20))
          }
        }
      }

      it("should get 3rd chunk") {
        h3.map{
          case (status, id, hits, infotons, debug) => withClue(debug) {
            status should be(200)
            hits should be(Some(49))
            infotons should be(Some(true, 9))
          }
        }
      }
    }
  }
}
