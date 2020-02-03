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


import cmwell.it.fixture.NSHashesAndPrefixes
import cmwell.util.concurrent.SimpleScheduler._
import cmwell.util.concurrent._
import cmwell.util.http.SimpleResponse
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Matchers, Inspectors}
import play.api.libs.json._

import scala.concurrent.duration._
import scala.concurrent._
import scala.util.Success

class RDFTests extends AsyncFunSpec with Matchers with Helpers with Inspectors with NSHashesAndPrefixes with LazyLogging {

  val clf = cmw / "clearforest.com"

  def executeAfterFuture[T](futureToWaitFor: Future[_])(body: =>Future[T]) = futureToWaitFor.flatMap(_ => body)

  def waitForIt[T](req: => Future[SimpleResponse[T]])(decide: SimpleResponse[T] => Boolean): Future[Unit] = {
    def innerLoop(): Future[Unit] = {
      val p = Promise[Unit]()
      req.onComplete {
        case Success(v) if decide(v) => p.success(())
        case _ => p.completeWith(scheduleFuture(999.millis)(innerLoop()))
      }
      p.future
    }
    timeoutFuture(innerLoop(),30.seconds)
  }


  //Assertions
  val uploadYaakov = {
    val data = """
                 |@prefix vcard: <http://www.w3.org/2006/vcard/ns#> .
                 |@prefix rdfa: <http://www.w3.org/ns/rdfa#> .
                 |<http://clearforest.com/ce/YB> a vcard:Individual;
                 |  vcard:hasEmail <mailto:yaakov.breuer@thomsonteuters.com>;
                 |  vcard:FN "Yaakov Breuer";
                 |  vcard:hasAddress <http://clearforest.com/address> .
                 |<http://clearforest.com/address> a vcard:ADR ;
                 |  vcard:country-name "Israel";
                 |  vcard:locality "PetachTikva";
                 |  vcard:postal-code "7131";
                 |  vcard:street-address "94 Em HaMoshavot st." .
               """.stripMargin

    Http.post(_in,data,Some("text/rdf+turtle;charset=UTF-8"),List("format"->"ttl"), tokenHeader).map { res =>
      withClue(res) {
        res.status should be(200)
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }
  }

  val weirdTypeData = """<http://clearforest.com/weird> <http://ont.clearforest.com/weirdTypedValue> "x"^^<WEIRD> ."""
  val emptyTypeData = """<http://clearforest.com/empty> <http://ont.clearforest.com/emptyTypedValue> ""^^<http://empty.val/TYPE> ."""

  val uploadWeirdType = {
    Http.post(_in,weirdTypeData,Some("text/plain;charset=UTF-8"),List("format"->"ntriples"), tokenHeader).map { res =>
      withClue(res) {
        res.status should be(200)
        jsonSuccessPruner(Json.parse(res.payload)) should be(jsonSuccess)
      }
    }
  }

  val verifyWeirdIngest = executeAfterFuture(uploadWeirdType){
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler

    waitForIt(Http.get(clf / "weird", List("format" -> "json")))(_.status == 200).flatMap { _ =>

      spinCheck(100.millis, true)(Http.get(clf / "weird", List("format" -> "ntriples"))){
        res => (res.status == 200) && res.payload.lines.toList.contains(weirdTypeData)
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          res.payload.lines.toList should contain(weirdTypeData)
        }
      }
    }
  }

  val uploadEmptyType = {
    Http.post(_in,emptyTypeData,Some("text/plain;charset=UTF-8"),List("format"->"ntriples"), tokenHeader).map { res =>
      withClue(res) {
        res.status should be(200)
        jsonSuccessPruner(Json.parse(res.payload)) should be(jsonSuccess)
      }
    }
  }

  val verifyEmptyIngest = executeAfterFuture(uploadEmptyType){
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler

    waitForIt(Http.get(clf / "empty", List("format" -> "json")))(_.status == 200).flatMap { _ =>
      spinCheck(100.millis, true)(Http.get(clf / "empty", List("format" -> "ntriples"))) { res =>
        (res.status == 200) && res.payload.lines.toList.contains(emptyTypeData)
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          res.payload.lines.toList should contain(emptyTypeData)
        }
      }
    }
  }

  val waitForIngest = executeAfterFuture(uploadYaakov){
    waitForIt(Http.get(clf / "ce" / "YB", List("format" -> "json")))(_.status == 200)
  }

  def executeAfterIngesting[T](body: =>Future[T]): Future[T] = waitForIngest.flatMap(_ => body)

  val verifyIngest = executeAfterIngesting {
    Http.get(clf / "ce" / "YB", List("format" -> "json")).map { res =>
      withClue(res)(res.status shouldBe 200)
    }
  }

  val markDeleteData = executeAfterIngesting {
    val data = """
                 |@prefix vcard: <http://www.w3.org/2006/vcard/ns#> .
                 |@prefix rdfa:  <http://www.w3.org/ns/rdfa#> .
                 |@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                 |@prefix sys:   <cmwell://meta/sys#> .
                 |<http://clearforest.com/ce/YB>
                 |  sys:markDelete [
                 |    vcard:hasEmail <mailto:yaakov.breuer@thomsonteuters.com> ;
                 |    vcard:FN "Yaakov Breuer"
                 |  ];
                 |  vcard:EMAIL <http://clearforest.com/email_accounts/YB> .
                 |<http://clearforest.com/email_accounts/YB> a vcard:Internet ;
                 |  rdf:value  "yaakov.breuer@thomsonteuters.com" .
               """.stripMargin

    Http.post(_in, data, None, List("format" -> "turtle"), tokenHeader).map { res =>
      withClue(res) {
        res.status should be(200)
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }
  }

  val waitForChange = executeAfterFuture(markDeleteData){
    waitForIt(Http.get(clf/"ce"/"YB", List("format"->"json","with-history"->""))){ res =>
      res.status == 200 && {
        Json.parse(res.payload) \ "versions" match {
          case JsDefined(arr: JsArray) => arr.value.size == 2
          case _ => false
        }
      }
    }
  }
  def executeAfterChanging[T](body: =>Future[T]): Future[T] = waitForChange.flatMap(_ => body)

  val verifyChange = executeAfterChanging {
    val expected = Json.parse(s"""
                                 |{
                                 |  "type" : "ObjectInfoton",
                                 |  "system" : {
                                 |    "lastModifiedBy" : "pUser",
                                 |    "path" : "/clearforest.com/ce/YB",
                                 |    "parent" : "/clearforest.com/ce",
                                 |    "dataCenter" : "$dcName"
                                 |  },
                                 |  "fields" : {
                                 |    "EMAIL.vcard" : [ "http://clearforest.com/email_accounts/YB" ],
                                 |    "hasAddress.vcard" : [ "http://clearforest.com/address" ],
                                 |    "type.rdf" : [ "http://www.w3.org/2006/vcard/ns#Individual" ]
                                 |  }
                                 |}
      """.stripMargin)

    Http.get(clf / "ce" / "YB", List("format" -> "json")).map{res =>
      withClue(res) {
        Json.parse(res.payload).transform(uuidDateEraser) match {
          case JsSuccess(j, _) => j shouldEqual expected
          case err: JsError => fail(Json.prettyPrint(JsError.toJson(err)))
        }
      }
    }
  }

  val blankNodesPath = cmw / "refinitiv.com" / "eliel"

  val blankNodesIngestAndRender = {
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    val qps = List("format" -> "ntriples", "xg" -> "1")
    val data =
      """<http://refinitiv.com/eliel> <http://refinitiv.com/ont/f1> _:CAFEBABE .
        |_:CAFEBABE <http://refinitiv.com/ont/f2> "value" .""".stripMargin
    Http.post(_in, data, queryParams = List("format" -> "ntriples"), headers = tokenHeader).flatMap { _ =>
      spinCheck(100.millis, true)(Http.get(blankNodesPath, queryParams = qps))(_.status == 200).map { resp =>
        val triples = resp.payload.lines.filterNot(_ contains "/meta/sys").map(_.split(' ')).toSeq
        //
        // Output should be something like that:
        //
        //   <http://refinitiv.com/eliel> <http://refinitiv.com/ont/f1> _:BA61b22281X2dX811bX2dX47c2X2dX9405X2dX5e65f992a599 .
        //   _:BA61b22281X2dX811bX2dX47c2X2dX9405X2dX5e65f992a599 <http://refinitiv.com/ont/f2> "value" .
        //
        forExactly(1, triples)(_ (2) should startWith("_:B"))
        forExactly(1, triples)(_ (0) should startWith("_:B"))
      }
    }
  }

  val blankNodesIngestAndRenderRaw = {
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    val qps = List("format" -> "ntriples", "xg" -> "1", "raw" -> "")
    spinCheck(100.millis, true)(Http.get(blankNodesPath, queryParams = qps))(_.status == 200).map { resp =>
      val triples = resp.payload.lines.filterNot(_ contains "/meta/sys").map(_.split(' ')).toSeq
      forExactly(1, triples)(_ (2) should startWith(s"<${cmw.url}/blank_node"))
      forExactly(1, triples)(_ (0) should startWith(s"<${cmw.url}/blank_node"))
    }
  }

  describe("RDF API") {
    it("should upload turtle document with Yaakov's data")(uploadYaakov)
    it("should verify Yaakov's data has been ingested OK")(verifyIngest)
    it("should change Yaakov's email data with cmwell://meta/sys#markDelete predicate")(markDeleteData)
    it("should verify that the email was changed")(verifyChange)
    it("should upload weird unlabeled type")(uploadWeirdType)
    it("should verify weird output is same as ingested")(verifyWeirdIngest)
    it("should upload empty typed value")(uploadEmptyType)
    it("should verify empty output is same as ingested")(verifyEmptyIngest)
  }

  describe("Blank Nodes") {
    it("should render blank nodes as blank nodes")(blankNodesIngestAndRender)
    it("should render blank nodes as URIs when raw is supplied")(blankNodesIngestAndRenderRaw)
  }
}
