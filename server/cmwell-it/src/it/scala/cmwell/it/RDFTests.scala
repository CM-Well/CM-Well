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


import cmwell.it.fixture.NSHashesAndPrefixes
import cmwell.util.concurrent.SimpleScheduler._
import cmwell.util.concurrent._
import cmwell.util.http.SimpleResponse
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Matchers}
import play.api.libs.json._

import scala.concurrent.duration._
import scala.concurrent._
import scala.util.Success

class RDFTests extends AsyncFunSpec with Matchers with Helpers with NSHashesAndPrefixes with LazyLogging {

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


  describe("RDF API") {

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

    it("should upload turtle document with Yaakov's data")(uploadYaakov)
    it("should verify Yaakov's data has been ingested OK")(verifyIngest)
    it("should change Yaakov's email data with cmwell://meta/sys#markDelete predicate")(markDeleteData)
    it("should verify that the email was changed")(verifyChange)
  }
}
