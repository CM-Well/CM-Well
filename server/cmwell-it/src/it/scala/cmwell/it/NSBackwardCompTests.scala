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

import cmwell.util.concurrent.SimpleScheduler.scheduleFuture
import cmwell.util.concurrent._
import cmwell.util.http.SimpleResponse
import cmwell.util.string.Hash._
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Matchers}
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future, duration}
import duration.DurationInt
import scala.io.Source
import scala.util._

class NSBackwardCompTests extends AsyncFunSpec with Matchers with Helpers with fixture.NSHashesAndPrefixes with LazyLogging {
  describe("CM-Well") {

    //Assertions
    val indexingBugSampleIngest = {
      val data =
        """
          |@prefix vcard: <http://www.w3.org/2006/vcard/ns#> .
          |@prefix rdfa: <http://www.w3.org/ns/rdfa#> .
          |<http://www.example.net/Individuals/JohnSmith> a vcard:Individual;
          |  vcard:EMAIL <mailto:john.smith@example.net>, <mailto:jsmith@gmail.com> ;
          |  vcard:FN "John Smith";
          |  vcard:NOTE "1st note", "some other note" ;
          |  vcard:ADR <http://www.example.net/Addresses/c9ca3047> .
          |<http://www.example.net/Addresses/c9ca3047> a vcard:HOME ;
          |  vcard:NOTE "1st note", "some other note", "note to self";
          |  vcard:COUNTRY-NAME "USA";
          |  vcard:LOCALITY "Springfield;IL";
          |  vcard:POSTAL-CODE "12345";
          |  vcard:STREET-ADDRESS "123 Main St." .
          | """.stripMargin
      Http.post(_in, data, Some(
        "text/rdf+turtle;charset=UTF-8"), List("format" -> "ttl"), tokenHeader).map { res =>
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }
    val oldStyleNSDataIngest = {
      val _cmd = cmw / "_cmd"
      val data = """
                   |<cmwell://meta/ns/common-7270120a> <cmwell://meta/nn#url_hash> "7270120a" .
                   |<cmwell://meta/ns/common-7270120a> <cmwell://meta/nn#url> <http://permid.org/ontology/common/> .
                 """.stripMargin
      Http.post(_cmd, data, Some("text/plain;charset=UTF-8"), List("op" -> "init", "format" -> "ntriples"), tokenHeader).map { res =>
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }
    val wrappedAPIUploadOldStyle = {
      val oldStyleInfoton = Json.obj(
        "type" -> "ObjectInfoton",
        "system" -> Json.obj(
          "lastModified" -> "2015-07-03T22:09:03.780Z",
          "path" -> "/data.thomsonreuters.com/4-bd6d205c9e5f926f5f1b64ced180d1b3b7d7d4bae4632588d885c4e70585c00b"),
        "fields" -> Json.obj(
          s"street-address.${ns.vcard}" -> Json.arr("710 N Post Oak Rd # 400"),
          "hasPhoneNumber.common-7270120a" -> Json.arr("(800) 447-0528", "(713) 613-2927"),
          "hasFaxNumber.common-7270120a" -> Json.arr("Fax: (713) 613-2908"),
          s"postal-code.${ns.vcard}" -> Json.arr("Houston, Texas 77024-3812"),
          s"country-name.${ns.vcard}" -> Json.arr("U.S.A.")))

      Http.post(_in, Json.stringify(oldStyleInfoton), None, List("format" -> "jsonw"), tokenHeader).map { res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      }
    }
    val oldVcardOntologyDataIngest = {
      val oldVcardData = Source.fromURL(this.getClass.getResource("/vcard_old_ns.xml")).mkString
      Http.post(_in, oldVcardData, Some("application/rdf+xml;charset=UTF-8"), List("format" -> "rdfxml"), tokenHeader).map { res =>
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }

    describe("ingests") {
      describe("ensuring indexing bug is gone") {
        it("should upload the problematic infotons successfully")(indexingBugSampleIngest)
      }
      describe("data to test ns backward compatibility to old-style ns data") {
        it("should upload a /meta/ns old-style infoton to _cmd")(oldStyleNSDataIngest)
        it("should use wrapped API to upload old style infoton using old-style ns")(wrappedAPIUploadOldStyle)
        it("should post data with old VCARD onthology")(oldVcardOntologyDataIngest)
      }
    }

    describe("verifying") {

      //waiting helper
      val indexingWaitingFuture = {
        for {
          _ <- indexingBugSampleIngest
          _ <- oldStyleNSDataIngest
          _ <- wrappedAPIUploadOldStyle
          _ <- oldVcardOntologyDataIngest
        } yield SimpleScheduler.schedule[Unit](indexingDuration)(())(implicitly[ExecutionContext])
      }
      def executeAfterIndexing[T](body: =>Future[T]): Future[T] = indexingWaitingFuture.flatMap(_.flatMap(_ => body))

      //CONSTS
      val pathForOldNS = cmw / "data.thomsonreuters.com" / "4-bd6d205c9e5f926f5f1b64ced180d1b3b7d7d4bae4632588d885c4e70585c00b"
      val exampleNetPath = cmw / "www.example.net"
      val expectedJsonForOldNS = Json.obj(
        "type" -> "ObjectInfoton",
        "system" -> Json.obj(
          "uuid" -> "b83ca6ce7ee546c2136b6f30ddc1e75a",
          "lastModified" -> "2015-07-03T22:09:03.780Z",
          "path" -> "/data.thomsonreuters.com/4-bd6d205c9e5f926f5f1b64ced180d1b3b7d7d4bae4632588d885c4e70585c00b",
          "dataCenter" -> dcName,
          "parent" -> "/data.thomsonreuters.com"),
        "fields" -> Json.obj(
          "street-address.vcard" -> Json.arr("710 N Post Oak Rd # 400"),
          "hasPhoneNumber.common-7270120a" -> Json.arr("(800) 447-0528", "(713) 613-2927"),
          "hasFaxNumber.common-7270120a" -> Json.arr("Fax: (713) 613-2908"),
          "postal-code.vcard" -> Json.arr("Houston, Texas 77024-3812"),
          "country-name.vcard" -> Json.arr("U.S.A."))).transform(fieldsSorter).get
      val expectedN3BeforeRel2 = """
                       |@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
                       |@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                       |@prefix vcard: <http://www.w3.org/2006/vcard/ns#> .
                       |
                       |<http://www.example.net/Addresses/c9ca3047>
                       |        a                     <http://www.w3.org/2006/vcard/ns#HOME> ;
                       |        vcard:COUNTRY-NAME    "USA" ;
                       |        vcard:LOCALITY        "Springfield;IL" ;
                       |        vcard:NOTE            "some other note" , "note to self" , "1st note" ;
                       |        vcard:POSTAL-CODE     "12345" ;
                       |        vcard:STREET-ADDRESS  "123 Main St." .
                       |
                       |<http://www.example.net/Individuals/JohnSmith>
                       |        a                 <http://www.w3.org/2006/vcard/ns#Individual> ;
                       |        vcard:ADR         <http://www.example.net/Addresses/c9ca3047> ;
                       |        vcard:EMAIL       <mailto:john.smith@example.net> , <mailto:jsmith@gmail.com> ;
                       |        vcard:FN          "John Smith" ;
                       |        vcard:NOTE        "some other note" , "1st note" .
                     """.stripMargin

      def futureToWaitFor(httpReq: =>Future[SimpleResponse[Array[Byte]]], expectedTotal: Int) = {
        val timestamp = System.currentTimeMillis()
        def recurse(): Future[SimpleResponse[Array[Byte]]] = {
          httpReq.flatMap { res =>
            Json.parse(res.payload) \ "results" \ "total" match {
              case JsDefined(JsNumber(n)) if n.intValue() == expectedTotal => Future.successful(res)
              case _ if System.currentTimeMillis()-timestamp > 24000L => Future.failed(new Exception(s"failed to wait within timeout. last response: $res"))
              case _ => scheduleFuture(990.millis)(recurse())
            }
          }
        }

        recurse()
      }

      //Assertions
      val verifyingIndexingBugFixed = executeAfterIndexing {
        val req = () => Http.get(cmw / "www.example.net", List("op" -> "search", "with-descendants" -> "true", "format" -> "json"))
        futureToWaitFor(req(),4).map { res =>
          withClue(s"got response:\n$res") {
            (Json.parse(res.payload) \ "results" \ "total").as[Long] should be(4L)
          }
        }.recover {
          case e@FutureTimeout(_) => fail("failed to get 4 results (2 infotons + 2 parent dirs) within the specified timeout",e)
        }
      }

      val verifyingOldNSAsJson = executeAfterIndexing {
        Http.get(pathForOldNS, List("format" -> "json")).map { res =>
          Json.parse(res.payload)
            .transform((__ \ 'system \ 'indexTime).json.prune andThen fieldsSorter)
            .get shouldEqual expectedJsonForOldNS
        }
      }

      val verifyingOldNSAsNtriples = {
        val ntriples =
          """
<http://data.thomsonreuters.com/4-bd6d205c9e5f926f5f1b64ced180d1b3b7d7d4bae4632588d885c4e70585c00b> <http://permid.org/ontology/common/hasPhoneNumber> "(713) 613-2927" .
<http://data.thomsonreuters.com/4-bd6d205c9e5f926f5f1b64ced180d1b3b7d7d4bae4632588d885c4e70585c00b> <http://permid.org/ontology/common/hasPhoneNumber> "(800) 447-0528" .
<http://data.thomsonreuters.com/4-bd6d205c9e5f926f5f1b64ced180d1b3b7d7d4bae4632588d885c4e70585c00b> <http://permid.org/ontology/common/hasFaxNumber> "Fax: (713) 613-2908" .
<http://data.thomsonreuters.com/4-bd6d205c9e5f926f5f1b64ced180d1b3b7d7d4bae4632588d885c4e70585c00b> <http://www.w3.org/2006/vcard/ns#country-name> "U.S.A." .
<http://data.thomsonreuters.com/4-bd6d205c9e5f926f5f1b64ced180d1b3b7d7d4bae4632588d885c4e70585c00b> <http://www.w3.org/2006/vcard/ns#postal-code> "Houston, Texas 77024-3812" .
<http://data.thomsonreuters.com/4-bd6d205c9e5f926f5f1b64ced180d1b3b7d7d4bae4632588d885c4e70585c00b> <http://www.w3.org/2006/vcard/ns#street-address> "710 N Post Oak Rd # 400" .
          """.trim.lines.toSeq.map(_.trim).sorted.mkString("\n")
        executeAfterIndexing {
          Http.get(pathForOldNS, List("format" -> "ntriples")).map { res =>

            //status should be OK
            res.status should be >= 200
            res.status should be < 400

            new String(res.payload, "UTF-8")
              .lines
              .filterNot(_.contains("/meta/sys#"))
              .toSeq
              .map(_.trim)
              .sorted
              .mkString("\n") shouldEqual ntriples
          }
        }
      }

      val failedSearchDueToAmbiguity = executeAfterIndexing {
        Http.get(
          exampleNetPath,
          List("op" -> "search", "qp" -> "NOTE.vcard:note", "with-descendants" -> "true", "with-data" -> "true", "format" -> "n3")
        ).map(_.status shouldEqual 422)
      }

      val explicitNSSearchSuccess = executeAfterIndexing {
        Http.get(exampleNetPath, List("op" -> "search", "qp" -> s"NOTE.$$${ns.vcard}:note", "with-descendants" -> "true", "with-data" -> "true", "format" -> "n3")).map { res =>
          val s = new String(res.payload, "UTF-8")
          withClue(s) {
            res.status should be >= 200
            res.status should be < 400 //status should be OK
            compareRDFwithoutSys(expectedN3BeforeRel2, s, "N3") should be(true)
          }
        }
      }

      val fullNSByURISearchSuccess = executeAfterIndexing {
        Http.get(exampleNetPath, List("op" -> "search", "qp" -> "$http://www.w3.org/2006/vcard/ns#NOTE$:note", "with-descendants" -> "true", "with-data" -> "true","format" -> "n3")).map{res =>
          res.status should be >= 200
          res.status should be < 400 //status should be OK
          compareRDFwithoutSys(expectedN3BeforeRel2, new String(res.payload, "UTF-8"), "N3") should be(true)
        }
      }

      val nestedQueriesSearch = executeAfterIndexing {
        val f = Http.get(exampleNetPath, List(
          "op" -> "search",
          "qp" -> "*[$http://www.w3.org/2006/vcard/ns#NOTE$:note,$http://www.w3.org/2006/vcard/ns#FN$:John],*[$http://www.w3.org/2006/vcard/ns#POSTAL-CODE$::12345,$http://www.w3.org/2006/vcard/ns#COUNTRY-NAME$::USA]",
          "with-descendants" -> "true",
          "with-data" -> "true",
          "format" -> "n3"))

        f.map(res => compareRDFwithoutSys(expectedN3BeforeRel2, new String(res.payload,"UTF-8"), "N3") should be(true)).recoverWith {
          case t: Throwable => f.map(res => new String(res.payload,"UTF-8")).map { s =>
            fail(s)
          }
        }
      }

      val oldStyleQPFailure = executeAfterIndexing {
        Http.get(exampleNetPath, List(
          "op" -> "search",
          "qp" -> "*[$http://www.w3.org/2006/vcard/ns#NOTE$:note,$http://www.w3.org/2006/vcard/ns#FN$:John]*[$http://www.w3.org/2006/vcard/ns#POSTAL-CODE$::12345,$http://www.w3.org/2006/vcard/ns#COUNTRY-NAME$::USA]",
          "with-descendants" -> "true",
          "with-data" -> "true",
          "format" -> "n3"))
          .map(_.status should be(400))
      }

      val getJohnSmithThroughII = executeAfterIndexing {
        val path = cmw / "www.example.net" / "Individuals" / "JohnSmith"
        Http.get(path, List("format" -> "json")).flatMap(res =>  {
          val json1 = Json.parse(res.payload)
          val uuid = (json1 \ "system" \ "uuid").as[String]
          Http.get(cmw / "ii" / uuid, List("format" -> "json")).map(res => Json.parse(res.payload)).map{json2 =>
            json1 shouldEqual json2
          }
        })
      }

      val checkVPOldVcardNS = executeAfterIndexing {
        val oldVcardHash = crc32("http://www.w3.org/2001/old-vcard-rdf/3.0#")
        val expected = Json.parse(
          s"""
             |{
             |  "type":"ObjectInfoton",
             |  "system":{
             |    "path":"/clearforest.com/pe/VP",
             |    "parent":"/clearforest.com/pe",
             |    "dataCenter" : "$dcName"
             |  },
             |  "fields":{
             |    "FN.vcard":["V P"]
             |  }
             |}
        """.stripMargin)
        val vp = cmw / "clearforest.com" / "pe" / "VP"
        Http.get(vp, List("format" -> "json")).map { res =>
          val json = Json
            .parse(res.payload)               //blank node is unknown
            .transform(uuidDateEraser andThen (__ \ 'fields \ s"N.vcard").json.prune)

          withClue(new String(res.payload,"UTF-8") + res.headers.mkString("\n[",",","] : status=") + res.status) {
            json.isSuccess should be(true)
            json.get shouldEqual expected
          }
        }
      }

      val checkOldVcardMetaNS = executeAfterIndexing {
        val hash = crc32base64("http://www.w3.org/2001/old-vcard-rdf/3.0#")
        val expected = Json.parse(s"""
                                     |{
                                     |  "type":"ObjectInfoton",
                                     |  "system":{
                                     |    "path":"/meta/ns/$hash",
                                     |    "parent":"/meta/ns",
                                     |    "dataCenter":"$dcName"
                                     |  },
                                     |  "fields":{
                                     |    "url":["http://www.w3.org/2001/old-vcard-rdf/3.0#"],
                                     |    "prefix":["vcard"]
                                     |  }
                                     |}
                                  """.stripMargin)
        val path = metaNs / hash
        Http.get(path, List("format" -> "json")).map { res =>
          Json
            .parse(res.payload)
            .transform(uuidDateEraser)
            .get shouldEqual expected
        }
      }

      it("4 infotons were ingested (indexing bug)")(verifyingIndexingBugFixed)

      describe("ns backward compability to old-style ns data") {
        it("should GET the infoton as regular json")(verifyingOldNSAsJson)
        it("should GET the infoton as RDF ntriples")(verifyingOldNSAsNtriples)
        it("should verify data from old VCARD onthology (but same prefix)")(checkVPOldVcardNS)
        it("should check hashed vcard")(checkOldVcardMetaNS)
      }

      describe("Search API on this data") {
        it("should fail to search for 'note' under www.example.net using implicit ambiguous namespace")(failedSearchDueToAmbiguity)
        it("should search for 'note' under www.example.net in n3 explicitly using $")(explicitNSSearchSuccess)
        it("should search for 'note' under www.example.net in n3 using the full NS URI")(fullNSByURISearchSuccess)
        it("should search with nested queries")(nestedQueriesSearch)
        it("should not respect old-style qp any more")(oldStyleQPFailure)
      }

      describe("/ii/<uuid> infoton retrieval") {
        it("should get /www.example.net/Individuals/JohnSmith as json and compare to /ii/<json.uuid>")(getJohnSmithThroughII)
      }

      //CONSTS
      val i1 = Json.obj("type" -> "ObjectInfoton",
                        "system" -> Json.obj("path" -> "/www.example.net/Addresses/c9ca3047",
                          "parent" -> "/www.example.net/Addresses",
                          "dataCenter" -> dcName),
                        "fields" -> Json.obj("type.rdf" -> Json.arr("http://www.w3.org/2006/vcard/ns#HOME"),
                          "COUNTRY-NAME.vcard" -> Json.arr("USA"),
                          "LOCALITY.vcard" -> Json.arr("Springfield;IL"),
                          "POSTAL-CODE.vcard" -> Json.arr("12345"),
                          "NOTE.vcard" -> Json.arr("1st note","some other note","note to self"),//Json.arr("one note to rule them all!"),
                          "STREET-ADDRESS.vcard" -> Json.arr("123 Main St.")))
      val i2 = Json.obj("type" -> "ObjectInfoton",
                        "system" -> Json.obj("path" -> "/www.example.net/Individuals/JohnSmith",
                          "parent" -> "/www.example.net/Individuals",
                          "dataCenter" -> dcName),
                        "fields" -> Json.obj("type.rdf" -> Json.arr("http://www.w3.org/2006/vcard/ns#Individual"),
                          "FN.vcard" -> Json.arr("John Smith"),
                          "ADR.vcard" -> Json.arr("http://www.example.net/Addresses/c9ca3047"),
                          "EMAIL.vcard" -> Json.arr("mailto:jsmith@gmail.com","mailto:john.smith@example.net"),//Json.arr("mailto:jsmith@yahoo.com", "mailto:john.smith@example.net"),
                          "NOTE.vcard" -> Json.arr("1st note","some other note"))) //Json.arr("1st note", "2nd note")
      val jSmith = cmw / "www.example.net" / "Individuals" / "JohnSmith"

      def jSmithUnderscoreOut(): Future[SimpleResponse[Array[Byte]]] =
        Http.post(_out, "/www.example.net/Individuals/JohnSmith", Some("text/plain;charset=UTF-8"), List("format" -> "json", "xg" -> "*.vcard"), tokenHeader)

      //Assertions
      val jSmithExplicitXg = executeAfterIndexing {
        Http.get(jSmith, List("format" -> "json", "xg" -> s"ADR.$$${ns.vcard}")).map { res =>
          withClue(res) {
            Json
              .parse(res.payload)
              .transform(bagUuidDateEraserAndSorter)
              .get shouldEqual Json.obj("type" -> "BagOfInfotons", "infotons" -> Json.arr(i1, i2)).transform(bagUuidDateEraserAndSorter).get
          }
        }
      }
      val jSmithFullNsURIXg = executeAfterIndexing {
        Http.get(jSmith, List("format" -> "json", "xg" -> "$http://www.w3.org/2006/vcard/ns#ADR$")).map { res =>
          withClue(res) {
            Json
              .parse(res.payload)
              .transform(bagUuidDateEraserAndSorter)
              .get shouldEqual Json.obj("type" -> "BagOfInfotons", "infotons" -> Json.arr(i1, i2)).transform(bagUuidDateEraserAndSorter).get
          }
        }
      }
      val jSmithImplicitXg = executeAfterIndexing {
        Http.get(jSmith, List("format" -> "json", "xg" -> "ADR.vcard")).map(_.status shouldEqual 422)
      }
      val jSmithExplicitBulkXg = executeAfterIndexing {
        Http.post(_out, "/www.example.net/Individuals/JohnSmith", Some("text/plain;charset=UTF-8"), List("format" -> "json", "xg" -> s"*.$$${ns.vcard}"), tokenHeader).map { res =>
          withClue(res) {
            Json
              .parse(res.payload)
              .transform(bagUuidDateEraserAndSorter)
              .get shouldEqual Json.obj("type" -> "RetrievablePaths", "infotons" -> Json.arr(i1, i2), "irretrievablePaths" -> Json.arr()).transform(bagUuidDateEraserAndSorter).get
          }
        }
      }
      val jSmithImplicitBulkXg = executeAfterIndexing {
        jSmithUnderscoreOut().map(_.status shouldEqual 422)
      }

      //changing the data
      val renamingOldVcardPrefix = for {
        _ <- jSmithExplicitXg
        _ <- jSmithFullNsURIXg
        _ <- jSmithImplicitXg
        _ <- jSmithExplicitBulkXg
        _ <- jSmithImplicitBulkXg
        body = """<> <cmwell://meta/ns#old-vcard> "http://www.w3.org/2001/old-vcard-rdf/3.0#" . """
        res <- Http.post(_in, body, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader)
      } yield {
        Try(Json.parse(res.payload)) match {
          case Success(j) => withClue(s"prefix renaming failed with response: $j")(j shouldEqual jsonSuccess)
          case Failure(e) => fail("failed to parse: '" + new String(res.payload,"UTF-8") + "'\n\n" + cmwell.util.exceptions.stackTraceToString(e))
        }
      }

      // new waiting helper
      val indexingWaitingFuture2 = renamingOldVcardPrefix.flatMap(_ => SimpleScheduler.schedule[Unit](indexingDuration)(())(implicitly[ExecutionContext]))
      def executeAfterIndexing2[T](body: =>Future[T]): Future[T] = indexingWaitingFuture2.flatMap(_ => body)

      val jSmithImplicitXgSuccess = executeAfterIndexing2 {
        Http.get(jSmith, List("format" -> "json", "xg" -> "ADR.vcard")).map { res =>
          lazy val clue = new String(res.payload, "UTF-8")
          withClue(res -> clue) {
            Try {
              Json
              .parse(res.payload)
              .transform(bagUuidDateEraserAndSorter)
              .get} match {
              case Success(j) => j shouldEqual Json.obj("type" -> "BagOfInfotons", "infotons" -> Json.arr(i1, i2)).transform(bagUuidDateEraserAndSorter).get
              case Failure(e) => fail(clue)
            }
          }
        }
      }
      val jSmithImplicitBulkXgSuccess = executeAfterIndexing2 {
        jSmithUnderscoreOut().map { res =>
          lazy val clue = new String(res.payload, "UTF-8")
          withClue(res -> new String(res.payload, "UTF-8")) {
            Try {
              Json
                .parse(res.payload)
                .transform(bagUuidDateEraserAndSorter)
                .get
            } match {
              case Success(j) => j shouldEqual Json.obj("type" -> "RetrievablePaths", "infotons" -> Json.arr(i1, i2), "irretrievablePaths" -> Json.arr()).transform(bagUuidDateEraserAndSorter).get
              case Failure(e) => fail(clue)
            }
          }
        }
      }

      describe("expand graph API") {
        it("should expand JohnSmith with address on regular read with explicit $ namespace")(jSmithExplicitXg)
        it("should expand JohnSmith with address on regular read using full NS URI")(jSmithFullNsURIXg)
        it("should fail to expand JohnSmith with address on regular read with implicit ambiguous namespace")(jSmithImplicitXg)
        it("should expand JohnSmith with any vcard on bulk read through _out with explicit $ namespace")(jSmithExplicitBulkXg)
        it("should fail to expand JohnSmith with any vcard on bulk read through _out with implicit ambiguous namespace")(jSmithImplicitBulkXg)

        //after renaming vcard prefix
        it("should change prefix for ambiguous vcard namespace")(renamingOldVcardPrefix)
        it("should succeed previously failed request to expand JohnSmith with address on regular read with implicit ambiguous namespace")(jSmithImplicitXgSuccess)
        it("should succeed previously failed request to expand JohnSmith with any vcard on bulk read through _out with implicit ambiguous namespace")(jSmithImplicitBulkXgSuccess)
      }
    }
  }
}