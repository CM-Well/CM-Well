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

import cmwell.util.concurrent.SimpleScheduler.{schedule, scheduleFuture}
import cmwell.util.http.SimpleResponse
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.json._

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import org.scalatest.{AsyncFunSpec, Inspectors, Matchers}

import scala.util.{Failure, Success, Try}

class SearchTests extends AsyncFunSpec with Matchers with Inspectors with Helpers with fixture.NSHashesAndPrefixes with LazyLogging {

  def orderingFor[A : Numeric](rootSegment: String)(field: String, ascending: Boolean)(implicit rds: Reads[A]): Ordering[JsValue] = new Ordering[JsValue] {
    override def compare(x: JsValue, y: JsValue): Int = {
      val num = implicitly[Numeric[A]]
      val xf = Try((x \ rootSegment \ field).head.as[A])
      val yf = Try((y \ rootSegment \ field).head.as[A])
      (xf,yf) match {
        case (Failure(_), Failure(_)) => 0
        case (Failure(_), _) => -1
        case (_, Failure(_)) => 1
        case (Success(xv), Success(yv)) =>
          if (ascending) num.compare(xv, yv)
          else num.compare(yv, xv)
      }
    }
  }

  def orderingForField[A : Numeric](field: String, ascending: Boolean)(implicit rds: Reads[A]): Ordering[JsValue] = orderingFor("fields")(field,ascending)

  val orderingForScore = orderingFor[Float]("extra")("score",ascending=false)

  describe("Search API should") {
    //Assertions
    val ingestGeonames = Future.traverse(Seq(293846, 293918, 294640, 294904, 5052287, 6342919, 6468007)) { n =>
      val geoN3 = scala.io.Source.fromURL(this.getClass.getResource(s"/geo/geonames_$n.n3")).mkString
      Http.post(_in, geoN3, Some("text/n3;charset=UTF-8"), List("format" -> "n3"), tokenHeader)
    }.map { results =>
      forAll(results) { res =>
        withClue(res) {
          res.status should be(200)
          Json.parse(res.payload) shouldEqual jsonSuccess
        }
      }
    }

    val boxedErrorOnRootWithoutQP = Http.get(cmw, List("op"->"search","recursive"->"","with-history"->"","format"->"json")).map { res =>
      withClue(res) {
        res.status should be(200)
      }
    }


    val path = cmw / "sws.geonames.org"


    val f0 = executeAfterCompletion(ingestGeonames)(spinCheck(250.millis)(Http.get(path, List("op" -> "search", "recursive" -> "", "length" -> "14", "format" -> "json"))){ r =>
      (Json.parse(r.payload) \ "results" \ "total" : @unchecked) match {
        case JsDefined(JsNumber(n)) => n.intValue() == 14
      }
    })

    val f1 = f0.flatMap(_ => scheduleFuture(10.seconds){
      spinCheck(250.millis,true)(Http.get(cmw / "meta" / "ns" / "2_fztg" / "alt", List("format"->"json"))){ r =>
        ((Json.parse(r.payload) \ "fields" \ "mang").head : @unchecked) match {
          case JsDefined(JsString(char)) => char == "f"
        }
      }
    })

    val searchForExistence = executeAfterCompletion(f1){
      spinCheck(250.millis,true)(Http.get(path, List("op" -> "search", "qp" -> "alt.wgs84_pos:", "format" -> "json", "debug-info" -> ""))) { r =>
        val j = Json.parse(r.payload)
        (j \ "results" \ "total" : @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue() == 2
        }
      }
    }

    val postSearchForExistence = executeAfterCompletion(f1){
      spinCheck(250.millis,true)(Http.post(path,
                                           "qp=alt.wgs84_pos:&format=json&debug-info=",
                                           Some("application/x-www-form-urlencoded"),
                                           queryParams = List("op" -> "search"),
                                           headers = tokenHeader)) { r =>
        val j = Json.parse(r.payload) \ "results"
        (j \ "total" : @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue() == 2
        }
      }
    }

    val testSearchForExistence = searchForExistence.map { res =>
      withClue(res) {
        val jResults = Json.parse(res.payload) \ "results"
        (jResults \ "total").as[Long] should be(2L)
      }
    }

    val testPostSameAsGet = searchForExistence.zip(postSearchForExistence).map{
      case (res1,res2) => withClue(res1,res2){
        val j1 = Json.parse(res1.payload)
        val j2 = Json.parse(res2.payload)
        (j2 \ "results" \ "total").as[Long] should be(2L)
        j1 shouldEqual j2
      }
    }

    val sortByLatitude = executeAfterCompletion(f1){
      spinCheck(250.millis,true)(Http.get(
        uri = path,
        queryParams = List("op" -> "search","sort-by" -> "-lat.wgs84_pos","format" -> "json","with-data" -> "","pretty" -> "","debug-info" -> "")
      )){ r =>
        val j = Json.parse(r.payload) \ "results"
        (j \ "total": @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue() >= 2
        }
      }.map { res =>
        withClue(res) {
          val jInfotonsArr = (Json.parse(res.payload) \ "results" \ "infotons").get.asInstanceOf[JsArray].value
          implicit val ord = orderingForField[Float]("lat.wgs84_pos", ascending = false)
          jInfotonsArr shouldBe sorted
        }
      }
    }

    val sortByAltitude = executeAfterCompletion(f1){
      spinCheck(250.millis,true)(Http.get(
        uri = path,
        queryParams = List("op" -> "search","sort-by" -> "-alt.wgs84_pos","format" -> "json","with-data" -> "","pretty" -> "","debug-info" -> "")
      )){ r =>
        val j = Json.parse(r.payload) \ "results"
        (j \ "total": @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue() >= 2
        }
      }.map { res =>
        withClue(res){
          val jInfotonsArr = (Json.parse(res.payload) \ "results" \ "infotons").get.asInstanceOf[JsArray].value
          implicit val ord = orderingForField[Float]("alt.wgs84_pos", ascending = false)
          jInfotonsArr.size should be(7)
          jInfotonsArr.take(2) shouldBe sorted
          forAll(jInfotonsArr.drop(2)) { jInfoton =>
            (jInfoton \ "fields" \ "alt.wgs84_pos").toOption shouldBe empty
          }
        }
      }
    }

    val sortByScoreFilterByIL = executeAfterCompletion(f1){
      spinCheck(250.millis,true)(Http.get(
        uri = path,
        queryParams = List("op" -> "search","sort-by" -> "system.score","format" -> "json","qp" -> "*alternateName.geonames:israel,*countryCode.geonames:il","pretty" -> "","debug-info" -> "")
      )){ r =>
        val j = Json.parse(r.payload) \ "results"
        (j \ "total": @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue() == 4
        }
      }.map { res =>
        withClue(res){
          val jInfotonsArr = (Json.parse(res.payload) \ "results" \ "infotons").get.asInstanceOf[JsArray].value
          implicit val ord = orderingForScore
          jInfotonsArr.size should be(4)
          jInfotonsArr.take(2) shouldBe sorted
        }
      }
    }

    val recursiveSearch = executeAfterCompletion(f1){
      spinCheck(250.millis,true)(Http.get(
        uri = path,
        queryParams = List("op" -> "search","format" -> "json","pretty" -> "","debug-info" -> "","recursive" -> ""))){ r =>
        (Json.parse(r.payload) \ "results" \ "total": @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue() == 14
        }
      }.map { res =>
        withClue(res) {
          val total = (Json.parse(res.payload) \ "results" \ "total").as[Int]
          total should be(14)
        }
      }
    }

    val ex2unitPF: PartialFunction[Throwable,Unit] = {
      case _: Throwable => ()
    }

    val deleteAbouts = (for {
      _ <- searchForExistence.recover(ex2unitPF)
      _ <- postSearchForExistence.recover(ex2unitPF)
      _ <- sortByLatitude.recover(ex2unitPF)
      _ <- sortByAltitude.recover(ex2unitPF)
      _ <- sortByScoreFilterByIL.recover(ex2unitPF)
      _ <- recursiveSearch.recover(ex2unitPF)
    } yield Seq(293846, 293918, 294640, 294904, 5052287, 6342919, 6468007)).flatMap { seq =>
      Future.traverse(seq){ n =>
        val aboutPath =  path / n.toString / "about.rdf"
        Http.delete(uri = aboutPath,headers = tokenHeader)
      }.map { results =>
        forAll(results){ res =>
          withClue(res){
            res.status should be(200)
            Json.parse(res.payload) should be(jsonSuccess)
          }
        }
      }
    }

    def recSearch(path: String, queryParams: Seq[(String,String)], expectedTotal: Int) = executeAfterCompletion(deleteAbouts)(spinCheck(250.millis,true)(Http.get(path, queryParams)){ r =>
      (Json.parse(r.payload) \ "results" \ "total" : @unchecked) match {
        case JsDefined(JsNumber(n)) => n.intValue() == expectedTotal
      }
    })

    val recursiveSearch2 = recSearch(path, List("op" -> "search", "recursive" -> "", "debug-info" -> "", "length" -> "14", "format" -> "json"), 7).map { res =>
      withClue(res) {
        val total = (Json.parse(res.payload) \ "results" \ "total").as[Int]
        total should be(7)
      }
    }

    val recursiveSearch3 =  executeAfterCompletion(recursiveSearch2)(recSearch(path,List("op" -> "search","format" -> "json","pretty" -> "","debug-info" -> "","recursive" -> "","with-deleted"->"","length" -> "14"), 14).map { res =>
      withClue(res) {
        val results = Json.parse(res.payload) \ "results"
        val total = (results \ "total").as[Int]
        total should be(14)
        results \ "infotons" match {
          case JsDefined(JsArray(infotons)) => {
            val m = infotons.groupBy(jv => (jv \ "type").as[String])
            m should have size (2)
            forAll(m.values) { seqByType =>
              seqByType should have size (7)
            }
          }
          case somethingElse => fail(s"was expecting an array, but got: $somethingElse")
        }
      }
    })

    val recursiveSearch4 = executeAfterCompletion(recursiveSearch2)(recSearch(path,List("op" -> "search","format" -> "json","pretty" -> "","debug-info" -> "","recursive" -> "","with-history"->"","length" -> "42"),21).map { res =>
      withClue(res) {
        val results = Json.parse(res.payload) \ "results"
        val total = (results \ "total").as[Int]
        total should be(21)
        results \ "infotons" match {
          case JsDefined(JsArray(infotons)) => {
            val m = infotons.groupBy(jv => (jv \ "type").as[String])
            m should have size (2)
            forAll(m) {
              case ("DeletedInfoton", deletedInfotons) => deletedInfotons should have size (7)
              case ("ObjectInfoton", deletedInfotons) => deletedInfotons should have size (14)
            }
          }
          case somethingElse => fail(s"was expecting an array, but got: $somethingElse")
        }
      }
    })

    it("verify boxed error bug on root search without qp is solved")(boxedErrorOnRootWithoutQP)
    it("ingest geonames data successfully")(ingestGeonames)
    it("retrieve only infotons with `alt.wgs84_pos` field")(testSearchForExistence)
    it("retrieve only infotons with `alt.wgs84_pos` field with POST")(testPostSameAsGet)
    it("get sorted by latitude (all share property)")(sortByLatitude)
    it("get sorted by altitude (some share property)")(sortByAltitude)
    it("get sorted by system.score (filtered by qp)")(sortByScoreFilterByIL)
    it("get nested object using recursive query param")(recursiveSearch)
    describe("delete infotons and search for in") {
      it("succeed deleting nested objects")(deleteAbouts)
      it("not get nested objects using recursive query param after deletes")(recursiveSearch2)
      it("get nested deleted objects using recursive and with-deleted after deletes")(recursiveSearch3)
      it("get nested deleted & historic objects using recursive and with-history after deletes")(recursiveSearch4)
    }
    //TODO: replicate sort-by tests to old it?
    //TODO: dcSync style search
  }
}