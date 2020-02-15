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

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Inspectors, Matchers}
import play.api.libs.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
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
          jsonSuccessPruner(Json.parse(res.payload)) shouldEqual jsonSuccess
        }
      }
    }

    val boxedErrorOnRootWithoutQP = Http.get(cmw, List("op"->"search","recursive"->"","with-history"->"","format"->"json")).map { res =>
      withClue(res) {
        res.status should be(200)
      }
    }


    val path = cmw / "sws.geonames.org"


    val f0 = executeAfterCompletion(ingestGeonames) {
      spinCheck(100.millis)(Http.get(
        path,
        List("op" -> "search", "recursive" -> "", "length" -> "14", "format" -> "json"))) { r =>
          (Json.parse(r.payload) \ "results" \ "total": @unchecked) match {
            case JsDefined(JsNumber(n)) => n.intValue == 14
          }
        }
    }

    val f1 = f0.flatMap(_ => {
      spinCheck(100.millis,true)(Http.get(cmw / "meta" / "ns" / "2_fztg" / "alt", List("format"->"json"))){ r =>
        ((Json.parse(r.payload) \ "fields" \ "mang").head : @unchecked) match {
          case JsDefined(JsString(char)) => char == "f"
        }
      }
    })

    val searchForExistence = executeAfterCompletion(f1){
      spinCheck(100.millis,true)(Http.get(path, List("op" -> "search", "qp" -> "alt.wgs84_pos:", "format" -> "json", "debug-info" -> ""))) { r =>
        val j = Json.parse(r.payload)
        (j \ "results" \ "total" : @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue == 2
        }
      }
    }

    val postSearchForExistence = executeAfterCompletion(f1){
      spinCheck(100.millis,true)(Http.post(path,
                                           "qp=alt.wgs84_pos:&format=json&debug-info=",
                                           Some("application/x-www-form-urlencoded"),
                                           queryParams = List("op" -> "search"),
                                           headers = tokenHeader)) { r =>
        val j = Json.parse(r.payload) \ "results"
        (j \ "total" : @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue == 2
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


    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    def getTypesCache = Http.get[String](cmw / "_types-cache").map(_.payload)

    val typesCache = executeAfterCompletion(f1)(getTypesCache)

    val sortByLatitude = executeAfterCompletion(typesCache){

      spinCheck(100.millis,true)(Http.get(
        uri = path,
        queryParams = List("op" -> "search","sort-by" -> "-lat.wgs84_pos","format" -> "json","with-data" -> "","pretty" -> "","debug-info" -> "")
      )){ r =>
        val j = Json.parse(r.payload) \ "results"
        (j \ "total": @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue >= 7
        }
      }.map { res =>
        withClue {
          val sb = new StringBuilder
          sb ++= res.toString()
          sb ++= "\ntypes cache before: "
          sb ++= typesCache.value.toString
          sb ++= "\ntypes cache after: "
          sb ++= Try(Await.result(getTypesCache,60.seconds)).getOrElse("getTypesCache failed!!!")
          sb.result()
        } {
            val jInfotonsArr = (Json.parse(res.payload) \ "results" \ "infotons").get.asInstanceOf[JsArray].value
            implicit val ord = orderingForField[Float]("lat.wgs84_pos", ascending = false)
            jInfotonsArr shouldBe sorted
          }
      }
    }

    val sortByAltitude = executeAfterCompletion(f1){
      spinCheck(100.millis,true)(Http.get(
        uri = path,
        queryParams = List("op" -> "search","sort-by" -> "-alt.wgs84_pos","format" -> "json","with-data" -> "","pretty" -> "","debug-info" -> "")
      )){ r =>
        val j = Json.parse(r.payload) \ "results"
        (j \ "total": @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue >= 2
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
      spinCheck(100.millis,true)(Http.get(
        uri = path,
        queryParams = List(
          "op" -> "search",
          "sort-by" -> "system.score",
          "format" -> "json",
          "qp" -> "*alternateName.geonames:israel,*countryCode.geonames:il",
          "pretty" -> "",
          "debug-info" -> "")
      )){ r =>
        val j = Json.parse(r.payload) \ "results"
        (j \ "total": @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue == 4
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
      spinCheck(100.millis,true)(Http.get(
        uri = path,
        queryParams = List("op" -> "search","format" -> "json","pretty" -> "","debug-info" -> "","recursive" -> ""))){ r =>
        (Json.parse(r.payload) \ "results" \ "total": @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue == 14
        }
      }.map { res =>
        withClue(res) {
          val total = (Json.parse(res.payload) \ "results" \ "total").as[Int]
          total should be(14)
        }
      }
    }

    val lastModifiedSearch = executeAfterCompletion(ingestGeonames){
      val currentTime = System.currentTimeMillis()
      spinCheck(100.millis,true)(Http.get(
        uri = path,
        queryParams = List("op" -> "search","format" -> "json","pretty" -> "","debug-info" -> "",
          "recursive"-> "", "qp" -> ("system.lastModified<<" + currentTime)))){ r =>
        (Json.parse(r.payload) \ "results" \ "total": @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue == 14
        }
      }.map { res =>
        withClue(res) {
          val total = (Json.parse(res.payload) \ "results" \ "total").as[Int]
          total should be(14)
        }
      }
    }

    ///sws.geonames.org/?op=search&recursive&qp=type.rdf::http://xmlns.com/foaf/0.1/Document&gqp=<isDefinedBy.rdfs[countryCode.geonames::US]
    val gqpFiltering = executeAfterCompletion(f1){
      spinCheck(100.millis,true)(Http.get(
        uri = path,
        queryParams = List(
          "op" -> "search",
          "format" -> "json",
          "pretty" -> "",
          "debug-info" -> "",
          "recursive" -> "",
          "qp" -> "type.rdf::http://xmlns.com/foaf/0.1/Document",
          "gqp" -> "<isDefinedBy.rdfs[countryCode.geonames::US]"))){ r =>
        (Json.parse(r.payload) \ "results" \ "total": @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue == 7
        }
      }.map { res =>
        withClue(res) {
          val j = Json.parse(res.payload)
          val total = (j \ "results" \ "total").as[Int]
          total should be(7)
          val length = (j \ "results" \ "length").as[Int]
          length should be(2)
          (j \ "results" \ "infotons").toOption.fold(fail("son.results.infotons was not found")) {
            case JsArray(infotons) => infotons should have size(2)
            case notArray => fail(s"json.results.infotons was not an array[$notArray]")
          }
        }
      }
    }

    val ingestGOT = {
      val gotN3 = scala.io.Source.fromURL(this.getClass.getResource("/got/data.n3")).mkString
      Http.post(_in, gotN3, Some("text/n3;charset=UTF-8"), List("format" -> "n3"), tokenHeader).map { res =>
        withClue(res) {
          res.status should be(200)
          jsonSuccessPruner(Json.parse(res.payload)) shouldEqual jsonSuccess
        }
      }
    }

    val awoiaf = cmw / "awoiaf.westeros.org"

    val awoiafSearch = executeAfterCompletion(ingestGOT){
      spinCheck(100.millis,true)(Http.get(
        awoiaf,
        List("op" -> "search", "format" -> "json", "length" -> "1")
      )) { r =>
        (Json.parse(r.payload) \ "results" \ "total": @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue == 11
        }
      }.map { r =>
        withClue(r) {
          r.status should be (200)
          val j = Json.parse(r.payload)
          val total = (j \ "results" \ "total").as[Int]
          total should be(11)
        }
      }
    }

    val searchPersons = executeAfterCompletion(awoiafSearch) {
      Http.get(awoiaf, List(
        "op" -> "search",
        "format" -> "json",
        "qp" -> "type.rdf::http://xmlns.com/foaf/0.1/Person")).map { r =>
        withClue(r) {
          r.status should be(200)
          val j = Json.parse(r.payload)
          val total = (j \ "results" \ "total").as[Int]
          total should be(4)
        }
      }
    }

    val gqpFilterByHomeType = executeAfterCompletion(awoiafSearch) {
      Http.get(awoiaf, List(
        "op" -> "search",
        "format" -> "json",
        "qp" -> "type.rdf::http://xmlns.com/foaf/0.1/Person",
        "gqp" -> "<hasTenant.gotns>homeLocation.schema[homeType.gotns::Castle]")).map { r =>
        withClue(r) {
          r.status should be(200)
          val j = Json.parse(r.payload) \ "results"
          val total = (j \ "total").as[Int]
          total should be(4)
          val length = (j \ "length").as[Int]
          length should be(3)
        }
      }
    }

    val gqpFilterBySkippingGhostNed = executeAfterCompletion(awoiafSearch) {
      Http.get(awoiaf, List(
        "op" -> "search",
        "format" -> "json",
        "with-data" -> "",
        "qp" -> "type.rdf::http://xmlns.com/foaf/0.1/Person",
        "gqp" -> ">childOf.rel<childOf.rel")).map { r =>
        withClue(r) {
          r.status should be(200)
          val j = Json.parse(r.payload) \ "results"
          val total = (j \ "total").as[Int]
          total should be(4)
          val length = (j \ "length").as[Int]
          length should be(2)
        }
      }
    }

    val gqpFilterAllAlthoughSkippingGhostNed = executeAfterCompletion(awoiafSearch) {
      Http.get(awoiaf, List(
        "op" -> "search",
        "format" -> "json",
        "with-data" -> "",
        "qp" -> "type.rdf::http://xmlns.com/foaf/0.1/Person",
        "gqp" -> ">childOf.rel[type.rdf::http://xmlns.com/foaf/0.1/Person]<childOf.rel")).map { r =>
        withClue(r) {
          r.status should be(200)
          val j = Json.parse(r.payload) \ "results"
          val total = (j \ "total").as[Int]
          total should be(4)
          val length = (j \ "length").as[Int]
          length should be(0)
        }
      }
    }

//    TODO: empty filter can mean existence -> i.e: no ghost skips w/o explicit filter. not yet implemented though...
//    val gqpFilterAllAlthoughSkippingGhostNedWithEmptyFilter = executeAfterCompletion(awoiafSearch) {
//      Http.get(awoiaf, List(
//        "op" -> "search",
//        "format" -> "json",
//        "with-data" -> "",
//        "qp" -> "type.rdf::http://xmlns.com/foaf/0.1/Person",
//        "gqp" -> ">childOf.rel[]<childOf.rel")).map { r =>
//        withClue(r) {
//          r.status should be(200)
//          val j = Json.parse(r.payload) \ "results"
//          val total = (j \ "total").as[Int]
//          total should be(4)
//          val length = (j \ "length").as[Int]
//          length should be(0)
//        }
//      }
//    }

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
      _ <- gqpFiltering.recover(ex2unitPF)
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

    def recSearch(path: String, queryParams: Seq[(String,String)], expectedTotal: Int) =
      executeAfterCompletion(deleteAbouts)(spinCheck(500.millis,true)(Http.get(path, queryParams)){ r =>
        (Json.parse(r.payload) \ "results" \ "total" : @unchecked) match {
          case JsDefined(JsNumber(n)) => n.intValue == expectedTotal
        }
      })

    val recursiveSearch2 = recSearch(path, List("op" -> "search", "recursive" -> "", "debug-info" -> "", "length" -> "14", "format" -> "json"), 7).map { res =>
      withClue(res) {
        val total = (Json.parse(res.payload) \ "results" \ "total").as[Int]
        total should be(7)
      }
    }

    val recursiveSearch3 =  executeAfterCompletion(
      recursiveSearch2)(
      recSearch(
        path,
        List("op" -> "search","format" -> "json","pretty" -> "","debug-info" -> "","recursive" -> "","with-deleted"->"","length" -> "14"),
        14).map { res =>
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

    val recursiveSearch4 = executeAfterCompletion(
      recursiveSearch2)(
      recSearch(
        path,
        List("op" -> "search","format" -> "json","pretty" -> "","debug-info" -> "","recursive" -> "","with-history"->"","length" -> "42"),
        21).map { res =>
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
                  case x @ (_, _) => logger.error(s"Unexpected input. Received: $x"); ???
                }
              }
              case somethingElse => fail(s"was expecting an array, but got: $somethingElse")
            }
          }
        })

    val searchUnderscoreAll = recSearch(cmw / "", Seq("op"->"search", "recursive"->"", "qp"->"_all:Tikva", "format"->"json"), 1).map { r =>
      (Json.parse(r.payload) \ "results" \ "total").as[Int] should be(1)
    }

    it("verify boxed error bug on root search without qp is solved")(boxedErrorOnRootWithoutQP)
    it("ingest geonames data successfully")(ingestGeonames)
    it("retrieve only infotons with `alt.wgs84_pos` field")(testSearchForExistence)
    it("retrieve only infotons with `alt.wgs84_pos` field with POST")(testPostSameAsGet)
    it("get sorted by latitude (all share property)")(sortByLatitude)
    it("get sorted by altitude (some share property)")(sortByAltitude)
    it("get sorted by system.score (filtered by qp)")(sortByScoreFilterByIL)
    it("get nested object using recursive query param")(recursiveSearch)
    it("filter results with gqp indirect properties")(gqpFiltering)
    it("ingest GOT data successfully")(ingestGOT)
    it("verify all GOT data is searcheable")(awoiafSearch)
    it("retrieve only infotons of type person from GOT")(searchPersons)
    it("filter person results by 'homeType' indirect property")(gqpFilterByHomeType)
    it("filter person results with 'ghost skipping' an intermediate missing infoton")(gqpFilterBySkippingGhostNed)
    it("filter all person results with 'ghost skipping' an intermediate missing infoton if it also contains a filter")(gqpFilterAllAlthoughSkippingGhostNed)
    it("search by timestamp last modified")(lastModifiedSearch)
// TODO: uncomment when implemented:
    // scalastyle:off
//  it("filter all person results with 'ghost skipping' an intermediate missing infoton if it also contains an empty filter")(gqpFilterAllAlthoughSkippingGhostNedWithEmptyFilter)
    // scalastyle:on
    describe("delete infotons and search for in") {
      it("succeed deleting nested objects")(deleteAbouts)
      it("not get nested objects using recursive query param after deletes")(recursiveSearch2)
      it("get nested deleted objects using recursive and with-deleted after deletes")(recursiveSearch3)
      it("get nested deleted & historic objects using recursive and with-history after deletes")(recursiveSearch4)
    }
    //TODO: dcSync style search

    it("search using _all API")(searchUnderscoreAll)
  }
}
