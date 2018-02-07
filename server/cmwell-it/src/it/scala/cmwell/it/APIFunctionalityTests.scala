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

import java.io.ByteArrayInputStream
import java.time.Instant

import cmwell.util.concurrent.delayedTask
import cmwell.util.concurrent.SimpleScheduler.scheduleFuture
import cmwell.util.formats.JsonEncoder
import cmwell.util.http.SimpleResponse
import cmwell.util.formats.JsonEncoder
import cmwell.util.string.Hash.crc32base64
import cmwell.common.build.JsonSerializer
import cmwell.util.formats.JsonEncoder
import com.typesafe.scalalogging.LazyLogging
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{ModelFactory, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import play.api.libs.json._
import scala.concurrent._
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Created by:
 * User: Gilad
 * Date: 11/10/14
 * Time: 6:59 AM
 */
import org.scalatest._

class APIFunctionalityTests extends AsyncFunSpec
  with Matchers
  with Inspectors
  with OptionValues
  with EitherValues
  with Helpers
  with fixture.NSHashesAndPrefixes
  with LazyLogging {

  describe("CM-Well REST API"){

    val antiJenaVcardRFFIngest = {
      val vcardRdf = Source.fromURL(this.getClass.getResource("/anti_jena_ns.xml")).mkString
      Http.post(_in, vcardRdf, Some("application/rdf+xml;charset=UTF-8"), List("format" -> "rdfxml"), tokenHeader)
    }

    val complicatedInferenceIngest = {
      val zzz = Source.fromURL(this.getClass.getResource("/zzz.rdf")).mkString
      Http.post(_in, zzz, Some("application/rdf+xml;charset=UTF-8"), List("format" -> "rdfxml"), tokenHeader)
    }

    describe("post sem-web doc to _in") {
      describe("with RDF format") {

        it("should post with new VCARD onthology") {
          val vcardRdf = Source.fromURL(this.getClass.getResource("/vcard_new_ns.xml")).mkString
          Http.post(_in, vcardRdf, Some("application/rdf+xml;charset=UTF-8"), List("format" -> "rdfxml"), tokenHeader).map { res =>
            withClue(res) {
              Json.parse(res.payload) should be(jsonSuccess)
            }
          }
        }

        it("should post with anti jena new onthology"){
          antiJenaVcardRFFIngest.map { res =>
            withClue(res) {
              Json.parse(res.payload) should be(jsonSuccess)
            }
          }
        }

        it("should post with complicated inference logic"){
          complicatedInferenceIngest.map { res =>
            withClue(res) {
              Json.parse(res.payload) should be(jsonSuccess)
            }
          }
        }
      }

      describe("with N-Triple format"){
        it("should post with new VCARD onthology") {
          val vcardNTriple = Source.fromURL(this.getClass.getResource("/vcard_new_ns.nt")).mkString
          Http.post(_in, vcardNTriple, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader).map { res =>
            withClue(res) {
              Json.parse(res.payload) should be(jsonSuccess)
            }
          }
        }

        it("should insert JaneSmith triples"){
          val janeSmith = """
                            |<http://example.org/JaneSmith> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Individual> .
                            |<http://example.org/JaneSmith> <http://www.w3.org/2006/vcard/ns#GENDER> "Female" .
                            |<http://example.org/JaneSmith> <http://www.w3.org/2006/vcard/ns#FN> "Jane Smith" .
                          """.stripMargin
          Http.post(_in, janeSmith, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader).map { res =>
            withClue(res) {
              Json.parse(res.payload) should be(jsonSuccess)
            }
          }
        }
      }

      describe("with N3 format"){
        it("should post with new VCARD onthology") {
          val vcardN3 = Source.fromURL(this.getClass.getResource("/vcard_new_ns.n3")).mkString
          Http.post(_in, vcardN3, Some("text/n3;charset=UTF-8"), List("format" -> "n3"), tokenHeader).map { res =>
            withClue(res) {
              Json.parse(res.payload) should be(jsonSuccess)
            }
          }
        }
      }

      describe("with JSON-LD format"){}
    }

    it("should put object infoton"){
      val jsonObjIn = Json.obj("header" -> "TestHeader","title" -> "TestTitle")
      val path = cmt / "InfoObj2"
      Http.post(path, Json.stringify(jsonObjIn), Some("application/json;charset=UTF-8"), Nil, ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader).map { res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      }
    }

    it("get object infoton in json format") {
      val expected = JsObject(
        Seq("type" -> JsString("ObjectInfoton"),
          "system" -> JsObject(
            Seq("path" -> JsString("/cmt/cm/test/InfoObj2"),
              "parent" -> JsString("/cmt/cm/test"),
              "dataCenter" -> JsString(dcName))),
          "fields" -> JsObject(
            Seq("title" -> JsArray(Seq(JsString("TestTitle"))),
              "header" -> JsArray(Seq(JsString("TestHeader")))))))
      scheduleFuture(indexingDuration) {
        spinCheck(1.second, true)(Http.get(cmt / "InfoObj2", List("format" -> "json")))(_.status).map{ res =>
          Json.parse(res.payload)
            .transform(uuidDateEraser)
            .get shouldEqual expected
        }
      }
    }

    describe("get object infoton in rdf formats") {
      val path = cmt / "InfoObjForRdfGet"

      it("should put object infoton for RDF GET") {
        val jsonObjIn = Json.obj("company" -> "microsoft", "head" -> "seatle", s"type.${ns.rdf}" -> "http://ont.example.org/types#someType")
        Http.post(path, Json.stringify(jsonObjIn), Some("application/json;charset=UTF-8"), Nil, ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader).map{res =>
          withClue(res) {
            Json.parse(res.payload) should be(jsonSuccess)
          }
        }
      }

      val subject = ResourceFactory.createResource(cmw.url + "/cmt/cm/test/InfoObjForRdfGet")
      val predHead = ResourceFactory.createProperty("http://localhost:9000/meta/nn#", "head")
      val predComp = ResourceFactory.createProperty("http://localhost:9000/meta/nn#", "company")

      it("in RDF N3 format") {
        scheduleFuture(indexingDuration) {
          Http.get(path, List("format" -> "n3")).map { body =>
            body.status should be >= 200
            body.status should be < 400 //status should be OK
            val graphResult = ModelFactory.createDefaultModel()
            RDFDataMgr.read(graphResult, new java.io.ByteArrayInputStream(body.payload), Lang.N3)
            graphResult.getProperty(subject, predHead).getLiteral.getLexicalForm should be("seatle")
            graphResult.getProperty(subject, predComp).getLiteral.getLexicalForm should be("microsoft")
          }
        }
      }

      it("in RDF NTriple format") {
        Http.get(path, List("format" -> "ntriples")).map{ body =>
          body.status should be >=200
          body.status should be <400 //status should be OK
          val graphResult = ModelFactory.createDefaultModel()
          RDFDataMgr.read(graphResult, new java.io.ByteArrayInputStream(body.payload), Lang.NTRIPLES)
          graphResult.getProperty(subject,predHead).getLiteral.getLexicalForm should be("seatle")
          graphResult.getProperty(subject,predComp).getLiteral.getLexicalForm should be("microsoft")
        }
      }

      it("in RDF XML format"){
        Http.get(path, List("format" -> "rdfxml")).map{ body =>
          body.status should be >=200
          body.status should be <400 //status should be OK
          val graphResult = ModelFactory.createDefaultModel()
          RDFDataMgr.read(graphResult, new java.io.ByteArrayInputStream(body.payload), Lang.RDFXML)
          graphResult.getProperty(subject,predHead).getLiteral.getLexicalForm should be("seatle")
          graphResult.getProperty(subject,predComp).getLiteral.getLexicalForm should be("microsoft")
        }
      }
    }

    describe("wrapped mode") {
      val wrapped = cmt / "wrapped"

      describe("in put/get Object Infoton") {
        val jsonw = Json.parse(s"""
          {
            "type":"ObjectInfoton",
            "system": {
              "path":"/cmt/cm/test/wrapped/ObjectInfoton1",
              "parent":"/cmt/cm/test/wrapped",
              "dataCenter":"$dcName"
            },
            "fields": {
                "title": [
                    "E.T"
                ],
                "year": [
                    "1986"
                ],
                "intVal" : [1],
                "floatVal" : [1.1],
                "boolVal" : [true],
                "dateVal" : ["2014-05-29T05:56:56.612Z"],
                "combined" : [2, false, "2013-04-29T05:56:56.612Z", 2.1]
            }
          }""").transform(fieldsSorter).get

        val f = Http.post(_in, Json.stringify(jsonw), None, List("format" -> "jsonw"), tokenHeader)

        it("should post the infoton") {
          f.map(res => Json.parse(res.payload) should be(jsonSuccess))
        }

        it("should get the infoton") {
          f.flatMap(_ => scheduleFuture(indexingDuration) {
            Http.get(wrapped./("ObjectInfoton1"), List("format" -> "json")).map { res =>
              withClue(new String(res.payload,"UTF-8")) {
                Json
                  .parse(res.payload)
                  .transform(uuidDateEraser andThen fieldsSorter)
                  .get shouldEqual jsonw
              }
            }
          })
        }
      }

      describe("in put/get Link Infoton") {
        val obj = Json.parse(
          s"""
          {
            "type":"ObjectInfoton",
            "system": {
              "path":"/cmt/cm/test/wrapped/ObjToBeLinkedTo",
              "parent":"/cmt/cm/test/wrapped",
              "dataCenter":"$dcName"
            },
            "fields": {
                "title": [
                    "E.T Come home"
                ],
                "year": [
                    "1986"
                ]
            }
          }
        """)

        val lnk = Json.parse(
          s"""
          {
            "type":"LinkInfoton",
            "system": {
              "path":"/cmt/cm/test/wrapped/LinkToObj",
              "parent":"/cmt/cm/test/wrapped",
              "dataCenter":"$dcName"
            },
            "linkTo":"/cmt/cm/test/wrapped/ObjToBeLinkedTo",
            "linkType":1
          }
         """)

        val f = {
          val f1 = Http.post(_in, Json.stringify(obj), None, List("format" -> "jsonw"), tokenHeader)
          val f2 = Http.post(_in, Json.stringify(lnk), None, List("format" -> "jsonw"), tokenHeader)
          f1.zip(f2)
        }

        it("should post obj and link") {
          f.map {
            case (r1, r2) => {
              withClue(r1) {
                Json.parse(r1.payload) should be(jsonSuccess)
              }
              withClue(r2) {
                Json.parse(r2.payload) should be(jsonSuccess)
              }
            }
          }
        }

        it("should get the obj through the link") {
          f.flatMap(_ => scheduleFuture(indexingDuration) {
            Http.get(uri = wrapped / "LinkToObj").flatMap { res =>
              withClue(res) {
                res.status should be(307)
                val loc = res.headers.find(_._1 == "Location").map(_._2.split('/').filter(_.nonEmpty).foldLeft(cmw)(_ / _))
                loc shouldBe defined
                Http.get(loc.get, queryParams = List("format" -> "json")).map { result =>
                  withClue(s"response for GET '${loc.get.url}': $result") {
                    result.status should be(200)
                    Json
                      .parse(result.payload)
                      .transform(uuidDateEraser)
                      .get shouldEqual obj
                  }
                }
              }
            }
          })
        }
      }

      describe("in put get Text File Infoton in jsonw") {
        val data = "Hello!! this is text content"
        val json = Json.parse(s"""
          {
            "type":"FileInfoton",
            "system": {
              "path":"/cmt/cm/test/wrapped/FileInfoton.txt",
              "parent":"/cmt/cm/test/wrapped",
              "dataCenter":"$dcName"
            },
            "content": {
              "mimeType": "text/plain",
              "data": "$data"
            }
          }""")

        val f = Http.post(_in, Json.stringify(json), None, List("format" -> "jsonw"), tokenHeader)

        it("should post the file") {
          f.map(res => Json.parse(res.payload) should be(jsonSuccess))
        }

        it("should get the file") {
          f.flatMap(_ => scheduleFuture(indexingDuration) {
            Http.get(wrapped./("FileInfoton.txt"), List("format" -> "json")).map { res =>
              withClue(res) {
                Json
                  .parse(res.payload)
                  .transform(uuidDateEraser)
                  .get shouldEqual json.transform((__ \ 'content).json.update {
                  Reads.JsObjectReads.map { case JsObject(xs) => JsObject(xs + ("length" -> JsNumber(data.length))) }
                }).get
              }
            }
          })
        }
      }

      describe("in put get Binary File Infoton") {
        val json = Json.parse(s"""
          {
            "type": "FileInfoton",
            "system": {
                "path": "/cmt/cm/test/wrapped/BinaryFileInfoton.png",
                "parent":"/cmt/cm/test/wrapped",
                "dataCenter":"$dcName"
            },
            "content": {
                "mimeType": "image/png",
                "base64-data": "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAALEgAACxIB0t1+/AAAABV0RVh0Q3JlYXRpb24gVGltZQA2LzI0LzA59sFr4wAAABx0RVh0U29mdHdhcmUAQWRvYmUgRmlyZXdvcmtzIENTNAay06AAAAIgSURBVDiNlZI9aFNRGIaf+2PSVJvc5PZHJaRxCsZCW52iUap0KChYB3E0jkUQ0cVB7FVR10JRcEtnh2booKCYgOCg6Q/FQqHiLTQh2sZ7g0rU5PY4JK2J1lI/+Dh8h/M+533hk3L3CegKU26I8R/1A14XHc6ogXWm3J3RGHsPgbRDtQB34V0sUFiYUluqxJAh+/IJAG0toHlAbwVF3gbijdJSJSaJmwj8wDUBwJf3Gez5FPbbJLpqs9+7DcQCSdxAoAMX0uALg7cbAOd7iU/pMawXBpGOf7gpgiSuI+houPSFIW5Az0UAyrk5lsYHiPrsvyGrIIkrCLogmwOXCroHuvaA4g/D+RR09uKUSyzdCRNps5sBH0GmAjhw5KEgcstEOm6wYGvYeRMe98HcBIrHR+hymrxde7vZlQYAZgbXbo19p0eJ3jUpBoexvwGTCYSZwRPsRT5h4FSbAZJIIAg22DplwMlRAD48Okcon0Lxh3FGZhCAde8AHXI9ygrIG7R8CeYLkJ80YLwfgNClJKsVDYomTE+gtmrQl2iKIVO3pA4aRB6YqIMGrMzC89soHh/ysavggPJqDAB3z9nfgGoDQI8ncLV3o8frPzw1WC+X0I7W5zWTytoy3oMDfwAc4Csob5JgLdfODXv5WVzt3ZvzrpJZy17X4ID0eZisX+UwGuDZYtu2qjJgg1VlWl2UGYr85Jm/QP8O5QBYMjOLKkO/ABjzzMAyxYbTAAAAAElFTkSuQmCC",
                "length": 711
            }
           }""")
        val f = Http.post(_in, Json.stringify(json), None, List("format" -> "jsonw"), tokenHeader)

        it("should post the file") {
          f.map(res => Json.parse(res.payload) should be(jsonSuccess))
        }

        it("should get the file") {
          f.flatMap(_ => scheduleFuture(indexingDuration) {
            Http.get(wrapped./("BinaryFileInfoton.png"), List("format" -> "json")).map { res =>
              withClue(res) {
                Json
                  .parse(res.payload)
                  .transform(uuidDateEraser)
                  .get shouldEqual json
              }
            }
          })
        }
      }
    }

    describe("bag of infotons") {
      val infotons = Json.parse(s""" {
        "type":"BagOfInfotons",
        "infotons":[
          {
            "type": "ObjectInfoton",
            "system": {
                "path": "/cmt/cm/test/bag/InfoObj1",
                "parent":"/cmt/cm/test/bag",
                "dataCenter":"$dcName"
            },
            "fields": {
                "name": ["sabaab"],
                "title": ["the return of the jedi"]
            }
          },
          {
            "type": "ObjectInfoton",
            "system": {
                "path": "/cmt/cm/test/bag/InfoObj2",
                "parent":"/cmt/cm/test/bag",
                "dataCenter":"$dcName"
            },
            "fields": {
                "title": ["Gladiator"],
                "name": ["just another name"]
            }
          },
          {
            "type": "FileInfoton",
            "system": {
                "path": "/cmt/cm/test/bag/InfoTextFile1",
                "parent":"/cmt/cm/test/bag",
                "dataCenter":"$dcName"
            },
            "content": {
                "mimeType": "text/plain",
                "data": "this is the data!!!",
                "length": 16

            }
          }
        ]
      }""")

      val postReq = Http.post(_in, Json.stringify(infotons), None, List("format" -> "jsonw"), tokenHeader)

      it("should post the bag of infotons to _in") {
        postReq.map(res => Json.parse(res.payload) should be(jsonSuccess))
      }

      it("should read the bag of infotons from _out") {
        val bulkReq = Json.obj("type" -> "InfotonPaths", "paths" -> Json.arr("/cmt/cm/test/bag/InfoObj1", "/cmt/cm/test/bag/InfoObj2", "/cmt/cm/test/bag/InfoTextFile1"))
        postReq.flatMap { _ =>
          scheduleFuture(indexingDuration) {
            Http.post(_out, Json.stringify(bulkReq), Some("application/json;charset=UTF-8"), List("format" -> "json"), tokenHeader).map { res =>
              res.status should be >= 200
              res.status should be < 400 //status should be OK
              JsonEncoder.decodeBagOfInfotons(res.payload).value.infotons.length should be(3)
            }
          }
        }
      }

      it("should read  the bag of infotons from _out in CSV format") {
        val bulkReq = Json.obj("type" -> "InfotonPaths", "paths" -> Json.arr("/cmt/cm/test/bag/InfoObj1", "/cmt/cm/test/bag/InfoObj2", "/cmt/cm/test/bag/InfoTextFile1"))
        val expectedHeaderPattern =
          """path,lastModified,type,uuid,parent,dataCenter,indexTime,mimeType,length,data,(name|title),(name|title)"""
        val expectedContentPattern = {
          val pathRegex = """"(/cmt/cm/test/bag/Info(Obj1|Obj2|TextFile1))""""
          val lastModifiedRegex = """([0-9]{4}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{0,3})?Z)"""
          val typeRegex = """(ObjectInfoton|FileInfoton)"""
          val uuidRegex = """([0-9a-f]{32})"""
          val parent = "(/cmt/cm/test/bag)"
          val dataCenterRegex = s"($dcName)"
          val indexTimeRegex = "([0-9]+)"
          val mimeTypeRegex = "(text/plain)?"
          val lengthRegex = "([0-9]+)?"
          val dataRegex = "(this is the data!!!)?"
          val nameRegex = "((just another name)|(sabaab))?"
          val titleRegex = "((Gladiator)|(the return of the jedi))?"
          s"$pathRegex,$lastModifiedRegex,$typeRegex,$uuidRegex,$parent,$dataCenterRegex,$indexTimeRegex,$mimeTypeRegex,$lengthRegex,$dataRegex,(($titleRegex,$nameRegex)|($nameRegex,$titleRegex))"
        }
        postReq.flatMap(_ => scheduleFuture(indexingDuration) {
          Http.post(_out, Json.stringify(bulkReq), Some("application/json;charset=UTF-8"), List("format" -> "csv"), tokenHeader).map { res =>

            res.status should be >= 200
            res.status should be < 400 //status should be OK

            val l = new String(res.payload, "UTF-8").lines.toList
            val (header, lines) = (l.head, l.tail)

            header should fullyMatch regex expectedHeaderPattern
            forAll(lines)(_ should fullyMatch regex expectedContentPattern)
          }
        })
      }
    }

    describe("_in sem-web docs") {
      val exampleOrg = cmw / "example.org"
      val clf = cmw / "clearforest.com"


      describe("JSONL meta/ns bookeeping") {
        it("should add .nn for lastName not in meta/ns") {
          val path = cmt / "InfoObjForRdfGet"
          Http.get(path, List("format" -> "jsonl")).map{res =>
            val resp = Json.parse(res.payload)
            Try((resp \ "company.nn").as[List[Map[String,String]]].head("value")).getOrElse("") should be("microsoft")
          }
        }
        it("should not add .nn for lastName in meta/ns") {
          val path = cmt / "InfoObjForRdfGet"
          Http.get(path, List("format" -> "jsonl")).map { res =>
            val resp = Json.parse(res.payload)
            Try((resp \ "type.rdf").as[List[Map[String,String]]].head("value")).getOrElse("") should be("http://ont.example.org/types#someType")
          }
        }
      }

      describe("cmwell://meta/sys#markReplace predicate") {
        it("should change Jane Smith name") {
          val jenniferSmith =
            """
            |<http://example.org/JaneSmith> <cmwell://meta/sys#markReplace> <http://www.w3.org/2006/vcard/ns#FN> .
            |<http://example.org/JaneSmith> <http://www.w3.org/2006/vcard/ns#FN> "Jennifer Smith" .
          """.stripMargin
          Http.post(_in, jenniferSmith, None, List("format" -> "nquads"), tokenHeader).map { res =>
            withClue(res) {
              Json.parse(res.payload) should be(jsonSuccess)
            }
          }
        }

        it("should verify that the name was changed") {
          indexingDuration.fromNow.block
          Http.get(exampleOrg./("JaneSmith"), List("format" -> "json")).map { res =>
            val jv = Json.parse(res.payload).transform((__ \ 'fields \ "FN.vcard").json.pick).get
            jv shouldEqual JsArray(Seq(JsString("Jennifer Smith")))
          }
        }
      }

      it("should deal with anti-jena namespace") {
        val expected = Json.parse(
        s"""
          |{
          |  "type":"ObjectInfoton",
          |  "system":{
          |    "path":"/clearforest.com/ce/GH",
          |    "parent":"/clearforest.com/ce",
          |    "dataCenter" : "$dcName"
          |  },
          |  "fields":{
          |    "ide.cmwtest":["IntelliJ IDEA"],
          |    "browser.cmwtest":["FireFox"],
          |    "lang.cmwtest":["Scala"]
          |  }
          |}
        """.stripMargin)
        val g = clf / "ce" / "GH"
        executeAfterCompletion(antiJenaVcardRFFIngest)(scheduleFuture(10.seconds){
          Http.get(g, List("format" -> "json")).map { res =>
            withClue(res) {
              val jv = Json.parse(res.payload).transform(uuidDateEraser).get
              jv shouldEqual expected
            }
          }
        })
      }

      it("should deal with complicated prefix inference logic") {
        val expectedNt = Set[String](
          s"""<http://example.org/hochgi> <${cmw.url}/meta/sys#type> "ObjectInfoton" .""",
          """<http://example.org/hochgi> <http://zzz.me/2014/ZzZzz/2014-06-24#FN> "G H" .""",
          """<http://example.org/hochgi> <http://zzz.me/2014/ZzZzz/2014-06-24#GENDER> "Male" .""",
          """<http://example.org/hochgi> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://zzz.me/2014/ZzZzz/2014-06-24#Individual> .""",
          s"""<http://example.org/hochgi> <http://localhost:9000/meta/sys#dataCenter> "$dcName" .""",
          """<http://example.org/hochgi> <http://localhost:9000/meta/sys#parent> "/example.org" .""",
          """<http://example.org/hochgi> <http://localhost:9000/meta/sys#path> "/example.org/hochgi" ."""
        )

        executeAfterCompletion(complicatedInferenceIngest)(scheduleFuture(10.seconds) {
          Http.get(exampleOrg./("hochgi"), List("format" -> "ntriples")).map { body =>
            body.status should be >= 200
            body.status should be < 400 //status should be OK
          def mSys(s: String) = s"/meta/sys#$s"

            val res = new String(body.payload, "UTF-8").split('\n').filterNot { t =>
              t.contains(mSys("lastModified")) ||
                t.contains(mSys("uuid"))       ||
                t.contains(mSys("indexTime"))
            }
            res.toSet shouldEqual expectedNt
          }
        })
      }

      it("should verify ZzZzz meta creation") {
        val hash = crc32base64("http://zzz.me/2014/ZzZzz/2014-06-24#")
        val expected = Json.parse(
          s"""
            |{
            |  "type":"ObjectInfoton",
            |  "system":{
            |    "path":"/meta/ns/$hash",
            |    "parent":"/meta/ns",
            |    "dataCenter" : "$dcName"
            |  },
            |  "fields":{
            |    "url":["http://zzz.me/2014/ZzZzz/2014-06-24#"],
            |    "prefix":["ZzZzz"]
            |  }
            |}
          """.stripMargin)
        val zzz = metaNs / hash
        Http.get(zzz, List("format" -> "json")).map { res =>
          withClue(res) {
            Json.parse(res.payload).transform(uuidDateEraser).get shouldEqual expected
          }
        }
      }

      describe("with inserted N-Triple docs") {
        val ik = clf / "ce" / "IK"
        it("should verify I's data as json") {
          val expected = Json.parse(
            s"""
            |{
            |  "type":"ObjectInfoton",
            |  "system":{
            |    "path":"/clearforest.com/ce/IK",
            |    "parent":"/clearforest.com/ce",
            |    "dataCenter" : "$dcName"
            |  },
            |  "fields":{
            |    "NOTE.vcard":["I Hate SBT!"],
            |    "FN.vcard":["I K"]
            |  }
            |}
          """.stripMargin)
          Http.get(ik, List("format" -> "json")).map { res =>
            withClue(res) {
              val jv = Json.parse(res.payload).transform(uuidDateEraser andThen (__ \ 'fields \ "N.vcard").json.prune).get
              jv shouldEqual expected
            }
          }
        }

        it("should verify language tag support") {
          Http.get(ik, List("format" -> "ntriples")).map{ res =>
            res.status should be >=200
            res.status should be <400 //status should be OK
            import scala.collection.JavaConversions._
            val m = ModelFactory.createDefaultModel()
            m.read(new ByteArrayInputStream(res.payload),null,"N-TRIPLE")
            val it = m.listStatements()
            it.collect{
              case stmt if stmt.getPredicate.getLocalName.toUpperCase == "NOTE" => {
                stmt.getObject.asLiteral.getLanguage
              }
            }.next shouldEqual "en"
          }
        }
      }

      describe("handle FileInfoton ingested as LD content") {

        val icon = cmw / "test" / "imgs" / "icon.png"
        val fp = {
          val fileNTriple = Source.fromURL(this.getClass.getResource("/file_ntriple")).mkString
          Http.post(_in, fileNTriple, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader)
        }
        val f0 = fp.flatMap(_ => scheduleFuture(indexingDuration)(spinCheck(1.second)(Http.get(icon, List("format" -> "text")))(_.status)))
        val f1 = f0.flatMap(_ => Http.get(icon, List("format" -> "json")))
        val f2 = f0.flatMap(_ => Http.get(icon, List("format" -> "n3")))
        val f3 = f0.flatMap(_ => Http.get(icon, List("format" -> "rdfxml")))

        it("should post N-Triple file with file infoton content") {
          fp.map { res =>
            withClue(s"uploading [/file_ntriple] failed") {
              Json.parse(res.payload) should be(jsonSuccess)
            }
          }
        }

        it("it should retrieve textual file from ntriples as JSON") {
          val expected = Json.parse(s"""
                                      |{
                                      |  "type": "FileInfoton",
                                      |  "system": {
                                      |    "path": "/test/imgs/icon.png",
                                      |    "parent": "/test/imgs",
                                      |    "dataCenter" : "$dcName"
                                      |  },
                                      |  "content": {
                                      |    "mimeType": "image/png",
                                      |    "base64-data": "iVBORw0KGgoAAAANSUhEUgAAABoAAAAaCAYAAACpSkzOAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAA2lpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMC1jMDYwIDYxLjEzNDc3NywgMjAxMC8wMi8xMi0xNzozMjowMCAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wUmlnaHRzPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvcmlnaHRzLyIgeG1sbnM6eG1wTU09Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9tbS8iIHhtbG5zOnN0UmVmPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvc1R5cGUvUmVzb3VyY2VSZWYjIiB4bWxuczp4bXA9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC8iIHhtcFJpZ2h0czpNYXJrZWQ9IkZhbHNlIiB4bXBNTTpEb2N1bWVudElEPSJ4bXAuZGlkOkM3MUJFMDdFOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXBNTTpJbnN0YW5jZUlEPSJ4bXAuaWlkOkM3MUJFMDdEOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXA6Q3JlYXRvclRvb2w9IkFkb2JlIFBob3Rvc2hvcCBDUzIgV2luZG93cyI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ1dWlkOjgyRTY0QjM0QzU5QkRGMTFBODA4OTIwNUU4Mjg0ODVBIiBzdFJlZjpkb2N1bWVudElEPSJ1dWlkOkI2NjNCOUQyOTk5OERGMTE4MzE1RDE2MUYyMTQyRDUxIi8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+4K+uiwAABPFJREFUeNq0lQ1MlVUYx//vey8X7gcfl9D4agjdqIjbkumtUTA+drlQsZRqy6YL52wa4ow0ZqKORRHCamprS5ljQS3D5pBcCrKgXftyc+VMEVACkpRvhAvcj/d9O+e87wXcbl228tkeODvnPL//ec7z3PNykiTBa9mHdGjbPoPFRuZMPKfqhh9rLZ7i/m1dDf8WSv+8m9+I0ake8DwHjpMQqAIZ8wgMSMCeUwV+IUsRYjY9/Rs6B45Db1BBGyjgkft4BAXoEBNetKR4JnSj/kl2f0eNZjq+a0NZiAnvT/VicmYQI9PX4eTV0IsezLp4smqAy3MLixm+LGHDz5z6el2K1DMcDOuLhT43uSf7AXuN3xOveHqL79p9XQeJaPAQ3LCuWY/WE8fAhSSh7AOy4Bic977RziVdzeIYyqAsyqRsqsFLHhckMsi2ZqHlyJuoqKgEm1P88njf0oQWxVAGZVEmZdM5ng0ED/Z+3IqsnAy01JaAj7bOuznOtiShxTGUQVmUSdlUQ+3NqHxzKlO2bqzC6bK15AQzUJFWuR1EKA/yfoXO7rOB9AikoHDklTVA6GuWmUpGakkgQiLNyqXcgYgHolwIfzQdAToj/nDfAf7q8Cv0RF4+XNMjmOy9whjzPIVNMnISRarqVHQEaPRaaLUqaIJ1CHJ62LzTPeVTQJQc7L/WoAXv4TGjkRjDy4PCZlcHJT0POUn+ybWAjmwYkLOIDAzBa5a3MDze5lNo1nUN+cmFePn3enmCxlIGsVNJL4HzXp3onIPomoXodIBWoi77MLa3bsOBV39Ax5VKROiTcXP0PIbv9PgUmnFexhpzKjITDyBcn42zVxtw6cY3KDfGMyY0lD0HnhZQJKoiUaW+LNCIIncYShozkZW8F/auKvSO2skL4PAp5BEn4HBeRFSIiPbuE+j45SOUr94FtSDITMYGbW960XILUqeFjBQ4WHTLsenzVXjDdh7m2AKo+UCfQipej1CdDe09c6j7qQIZbg30qqAFHmOTt05gJXJCcM3JxSUnkUQRWyMexzTHYfMXqTjyyveY80ygb8xO22Xht8PrYNQ/i4s3jfjUXop1oSZY+m8zhpcHxlYykgTajgJzmhFpJUyOjGNb2FNI0ITj9S/TkflYKWKNKXdlY9Tb0D2SgIPflSLHYEKmK4zEikp7C4qLLCOezZORKMpOP4TUI8w2LDfn4mBODWL092PrVzmwmvcvZMNpMORIQfW5HUiLtmBHWimLEZX4BZ7AtHmJHUBi1yXJqmyj688fmYuDF/DhQ/kw6SKw5Xgu1lsasTIyiWRTgP2ni5FmfBhvR1nm97MvtrTAk9lEyEPSEgQJHsXpiUSyIpAiekjXCOQHF0A27ovPQ5QmmDTIC1i54hhKmhqQEhyDoph0tkcgb5pAO4zAKcPLk9mkGUS5JCw99jKQ8cCQA5OXOqE2DJPP9sJHeB3iUKu+ioKjFsSKWjw3tgydY7/KcaTirqkhTAyLSFzE87JlIeJVJ/vxXmU1mg/txPPFtf/4psXP3MIn1z7D7uQiGAL0PvfIjBrs2b0L72w0MT7XvglSYu4G6BPz0Hx4J55ZHYf/w+wX+pBPxBxd36LrTD3pOpoaWWgip0hdFYfqpn453f/glEFZlCkq18edK4Q0yEUjIzcb98Laz7QhWhoER9uxpZCTcA8tp07i/hZgAMNRD8XVs1vdAAAAAElFTkSuQmCC",
                                      |    "length": 2244
                                      |  }
                                      |}
                                    """.stripMargin)

          f1.map(res => withClue(new String(res.payload,"UTF-8") + "\n" + res.toString) {
            Json
              .parse(res.payload)
              .transform(uuidDateEraser)
              .get shouldEqual expected
          })
        }

        it("it should retrieve textual file from ntriples as N3") {
          val expected = s"""
                            |@prefix sys:   <http://localhost:9000/meta/sys#> .
                            |@prefix nn:    <http://localhost:9000/meta/nn#> .
                            |@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
                            |@prefix o:     <http://localhost:9000/test/imgs/> .
                            |
                            |o:icon.png
                            |      sys:base64-data "iVBORw0KGgoAAAANSUhEUgAAABoAAAAaCAYAAACpSkzOAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAA2lpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMC1jMDYwIDYxLjEzNDc3NywgMjAxMC8wMi8xMi0xNzozMjowMCAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wUmlnaHRzPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvcmlnaHRzLyIgeG1sbnM6eG1wTU09Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9tbS8iIHhtbG5zOnN0UmVmPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvc1R5cGUvUmVzb3VyY2VSZWYjIiB4bWxuczp4bXA9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC8iIHhtcFJpZ2h0czpNYXJrZWQ9IkZhbHNlIiB4bXBNTTpEb2N1bWVudElEPSJ4bXAuZGlkOkM3MUJFMDdFOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXBNTTpJbnN0YW5jZUlEPSJ4bXAuaWlkOkM3MUJFMDdEOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXA6Q3JlYXRvclRvb2w9IkFkb2JlIFBob3Rvc2hvcCBDUzIgV2luZG93cyI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ1dWlkOjgyRTY0QjM0QzU5QkRGMTFBODA4OTIwNUU4Mjg0ODVBIiBzdFJlZjpkb2N1bWVudElEPSJ1dWlkOkI2NjNCOUQyOTk5OERGMTE4MzE1RDE2MUYyMTQyRDUxIi8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+4K+uiwAABPFJREFUeNq0lQ1MlVUYx//vey8X7gcfl9D4agjdqIjbkumtUTA+drlQsZRqy6YL52wa4ow0ZqKORRHCamprS5ljQS3D5pBcCrKgXftyc+VMEVACkpRvhAvcj/d9O+e87wXcbl228tkeODvnPL//ec7z3PNykiTBa9mHdGjbPoPFRuZMPKfqhh9rLZ7i/m1dDf8WSv+8m9+I0ake8DwHjpMQqAIZ8wgMSMCeUwV+IUsRYjY9/Rs6B45Db1BBGyjgkft4BAXoEBNetKR4JnSj/kl2f0eNZjq+a0NZiAnvT/VicmYQI9PX4eTV0IsezLp4smqAy3MLixm+LGHDz5z6el2K1DMcDOuLhT43uSf7AXuN3xOveHqL79p9XQeJaPAQ3LCuWY/WE8fAhSSh7AOy4Bic977RziVdzeIYyqAsyqRsqsFLHhckMsi2ZqHlyJuoqKgEm1P88njf0oQWxVAGZVEmZdM5ng0ED/Z+3IqsnAy01JaAj7bOuznOtiShxTGUQVmUSdlUQ+3NqHxzKlO2bqzC6bK15AQzUJFWuR1EKA/yfoXO7rOB9AikoHDklTVA6GuWmUpGakkgQiLNyqXcgYgHolwIfzQdAToj/nDfAf7q8Cv0RF4+XNMjmOy9whjzPIVNMnISRarqVHQEaPRaaLUqaIJ1CHJ62LzTPeVTQJQc7L/WoAXv4TGjkRjDy4PCZlcHJT0POUn+ybWAjmwYkLOIDAzBa5a3MDze5lNo1nUN+cmFePn3enmCxlIGsVNJL4HzXp3onIPomoXodIBWoi77MLa3bsOBV39Ax5VKROiTcXP0PIbv9PgUmnFexhpzKjITDyBcn42zVxtw6cY3KDfGMyY0lD0HnhZQJKoiUaW+LNCIIncYShozkZW8F/auKvSO2skL4PAp5BEn4HBeRFSIiPbuE+j45SOUr94FtSDITMYGbW960XILUqeFjBQ4WHTLsenzVXjDdh7m2AKo+UCfQipej1CdDe09c6j7qQIZbg30qqAFHmOTt05gJXJCcM3JxSUnkUQRWyMexzTHYfMXqTjyyveY80ygb8xO22Xht8PrYNQ/i4s3jfjUXop1oSZY+m8zhpcHxlYykgTajgJzmhFpJUyOjGNb2FNI0ITj9S/TkflYKWKNKXdlY9Tb0D2SgIPflSLHYEKmK4zEikp7C4qLLCOezZORKMpOP4TUI8w2LDfn4mBODWL092PrVzmwmvcvZMNpMORIQfW5HUiLtmBHWimLEZX4BZ7AtHmJHUBi1yXJqmyj688fmYuDF/DhQ/kw6SKw5Xgu1lsasTIyiWRTgP2ni5FmfBhvR1nm97MvtrTAk9lEyEPSEgQJHsXpiUSyIpAiekjXCOQHF0A27ovPQ5QmmDTIC1i54hhKmhqQEhyDoph0tkcgb5pAO4zAKcPLk9mkGUS5JCw99jKQ8cCQA5OXOqE2DJPP9sJHeB3iUKu+ioKjFsSKWjw3tgydY7/KcaTirqkhTAyLSFzE87JlIeJVJ/vxXmU1mg/txPPFtf/4psXP3MIn1z7D7uQiGAL0PvfIjBrs2b0L72w0MT7XvglSYu4G6BPz0Hx4J55ZHYf/w+wX+pBPxBxd36LrTD3pOpoaWWgip0hdFYfqpn453f/glEFZlCkq18edK4Q0yEUjIzcb98Laz7QhWhoER9uxpZCTcA8tp07i/hZgAMNRD8XVs1vdAAAAAElFTkSuQmCC"^^xsd:base64Binary ;
                            |      sys:length      "2244"^^xsd:long ;
                            |      sys:mimeType    "image/png" ;
                            |      sys:parent      "/test/imgs" ;
                            |      sys:path        "/test/imgs/icon.png" ;
                            |      sys:dataCenter  "$dcName" ;
                            |      sys:type        "FileInfoton" .
                          """.stripMargin

          f2.map { res =>
            val body = new String(res.payload, "UTF-8")
            withClue(body + "\n" + res.toString) {
              res.status should be >= 200
              res.status should be < 400 //status should be OK
              val errOrOk = compareRDF(expected, body, "N3", Array("lastModified", "uuid", "indexTime"))
              withClue("\n" + errOrOk.left.getOrElse("") + "\n") {
                errOrOk.right.value should be(())
              }
            }
          }
        }

        it("it should retrieve textual file from ntriples as RDF/XML") {
          val expected = s"""
                            |<rdf:RDF
                            |    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                            |    xmlns:sys="http://localhost:9000/meta/sys#"
                            |    xmlns:nn="http://localhost:9000/meta/nn#"
                            |    xmlns:xsd="http://www.w3.org/2001/XMLSchema#"
                            |    xmlns:o="http://localhost:9000/test/imgs/" >
                            |  <rdf:Description rdf:about="http://localhost:9000/test/imgs/icon.png">
                            |    <sys:type>FileInfoton</sys:type>
                            |    <sys:path>/test/imgs/icon.png</sys:path>
                            |    <sys:parent>/test/imgs</sys:parent>
                            |    <sys:dataCenter>$dcName</sys:dataCenter>
                            |    <sys:mimeType>image/png</sys:mimeType>
                            |    <sys:length rdf:datatype="http://www.w3.org/2001/XMLSchema#long">2244</sys:length>
                            |    <sys:base64-data rdf:datatype="http://www.w3.org/2001/XMLSchema#base64Binary">iVBORw0KGgoAAAANSUhEUgAAABoAAAAaCAYAAACpSkzOAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAA2lpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMC1jMDYwIDYxLjEzNDc3NywgMjAxMC8wMi8xMi0xNzozMjowMCAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wUmlnaHRzPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvcmlnaHRzLyIgeG1sbnM6eG1wTU09Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9tbS8iIHhtbG5zOnN0UmVmPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvc1R5cGUvUmVzb3VyY2VSZWYjIiB4bWxuczp4bXA9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC8iIHhtcFJpZ2h0czpNYXJrZWQ9IkZhbHNlIiB4bXBNTTpEb2N1bWVudElEPSJ4bXAuZGlkOkM3MUJFMDdFOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXBNTTpJbnN0YW5jZUlEPSJ4bXAuaWlkOkM3MUJFMDdEOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXA6Q3JlYXRvclRvb2w9IkFkb2JlIFBob3Rvc2hvcCBDUzIgV2luZG93cyI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ1dWlkOjgyRTY0QjM0QzU5QkRGMTFBODA4OTIwNUU4Mjg0ODVBIiBzdFJlZjpkb2N1bWVudElEPSJ1dWlkOkI2NjNCOUQyOTk5OERGMTE4MzE1RDE2MUYyMTQyRDUxIi8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+4K+uiwAABPFJREFUeNq0lQ1MlVUYx//vey8X7gcfl9D4agjdqIjbkumtUTA+drlQsZRqy6YL52wa4ow0ZqKORRHCamprS5ljQS3D5pBcCrKgXftyc+VMEVACkpRvhAvcj/d9O+e87wXcbl228tkeODvnPL//ec7z3PNykiTBa9mHdGjbPoPFRuZMPKfqhh9rLZ7i/m1dDf8WSv+8m9+I0ake8DwHjpMQqAIZ8wgMSMCeUwV+IUsRYjY9/Rs6B45Db1BBGyjgkft4BAXoEBNetKR4JnSj/kl2f0eNZjq+a0NZiAnvT/VicmYQI9PX4eTV0IsezLp4smqAy3MLixm+LGHDz5z6el2K1DMcDOuLhT43uSf7AXuN3xOveHqL79p9XQeJaPAQ3LCuWY/WE8fAhSSh7AOy4Bic977RziVdzeIYyqAsyqRsqsFLHhckMsi2ZqHlyJuoqKgEm1P88njf0oQWxVAGZVEmZdM5ng0ED/Z+3IqsnAy01JaAj7bOuznOtiShxTGUQVmUSdlUQ+3NqHxzKlO2bqzC6bK15AQzUJFWuR1EKA/yfoXO7rOB9AikoHDklTVA6GuWmUpGakkgQiLNyqXcgYgHolwIfzQdAToj/nDfAf7q8Cv0RF4+XNMjmOy9whjzPIVNMnISRarqVHQEaPRaaLUqaIJ1CHJ62LzTPeVTQJQc7L/WoAXv4TGjkRjDy4PCZlcHJT0POUn+ybWAjmwYkLOIDAzBa5a3MDze5lNo1nUN+cmFePn3enmCxlIGsVNJL4HzXp3onIPomoXodIBWoi77MLa3bsOBV39Ax5VKROiTcXP0PIbv9PgUmnFexhpzKjITDyBcn42zVxtw6cY3KDfGMyY0lD0HnhZQJKoiUaW+LNCIIncYShozkZW8F/auKvSO2skL4PAp5BEn4HBeRFSIiPbuE+j45SOUr94FtSDITMYGbW960XILUqeFjBQ4WHTLsenzVXjDdh7m2AKo+UCfQipej1CdDe09c6j7qQIZbg30qqAFHmOTt05gJXJCcM3JxSUnkUQRWyMexzTHYfMXqTjyyveY80ygb8xO22Xht8PrYNQ/i4s3jfjUXop1oSZY+m8zhpcHxlYykgTajgJzmhFpJUyOjGNb2FNI0ITj9S/TkflYKWKNKXdlY9Tb0D2SgIPflSLHYEKmK4zEikp7C4qLLCOezZORKMpOP4TUI8w2LDfn4mBODWL092PrVzmwmvcvZMNpMORIQfW5HUiLtmBHWimLEZX4BZ7AtHmJHUBi1yXJqmyj688fmYuDF/DhQ/kw6SKw5Xgu1lsasTIyiWRTgP2ni5FmfBhvR1nm97MvtrTAk9lEyEPSEgQJHsXpiUSyIpAiekjXCOQHF0A27ovPQ5QmmDTIC1i54hhKmhqQEhyDoph0tkcgb5pAO4zAKcPLk9mkGUS5JCw99jKQ8cCQA5OXOqE2DJPP9sJHeB3iUKu+ioKjFsSKWjw3tgydY7/KcaTirqkhTAyLSFzE87JlIeJVJ/vxXmU1mg/txPPFtf/4psXP3MIn1z7D7uQiGAL0PvfIjBrs2b0L72w0MT7XvglSYu4G6BPz0Hx4J55ZHYf/w+wX+pBPxBxd36LrTD3pOpoaWWgip0hdFYfqpn453f/glEFZlCkq18edK4Q0yEUjIzcb98Laz7QhWhoER9uxpZCTcA8tp07i/hZgAMNRD8XVs1vdAAAAAElFTkSuQmCC</sys:base64-data>
                            |  </rdf:Description>
                            |</rdf:RDF>
                            |
                          """.stripMargin

          f3.map { res =>
            val body = new String(res.payload, "UTF-8")
            withClue(body + "\n" + res.toString) {
              res.status should be >= 200
              res.status should be < 400 //status should be OK
              val errOrOk = compareRDF(expected, body, "RDF/XML-ABBREV", Array(
                "lastModified", "uuid", "indexTime"))
              withClue("\n" + errOrOk.left.getOrElse("") + "\n") {
                errOrOk.right.value should be(())
              }
            }
          }
        }

        describe("retrieving through _out") {
          val input = Json.parse("""{"type":"InfotonPaths","paths":["/test/imgs/icon.png"]}""")

          val f4 = f0.flatMap(_ => Http.post(_out, Json.stringify(input), Some("application/json;charset=UTF-8"), List("format" -> "json"), tokenHeader))
          val f5 = f0.flatMap(_ => Http.post(_out, Json.stringify(input), Some("application/json;charset=UTF-8"), List("format" -> "n3"), tokenHeader))
          val f6 = f0.flatMap(_ => Http.post(_out, Json.stringify(input), Some("application/json;charset=UTF-8"), List("format" -> "rdfxml"), tokenHeader))

          it("should get: application/json -> json") {
            val expected = Json.parse(s"""
                                        |{
                                        |    "type": "RetrievablePaths",
                                        |    "infotons": [{
                                        |        "type": "FileInfoton",
                                        |        "system": {
                                        |            "path": "/test/imgs/icon.png",
                                        |            "parent": "/test/imgs",
                                        |            "dataCenter" : "$dcName"
                                        |        },
                                        |        "content": {
                                        |            "mimeType": "image/png",
                                        |            "base64-data": "iVBORw0KGgoAAAANSUhEUgAAABoAAAAaCAYAAACpSkzOAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAA2lpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMC1jMDYwIDYxLjEzNDc3NywgMjAxMC8wMi8xMi0xNzozMjowMCAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wUmlnaHRzPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvcmlnaHRzLyIgeG1sbnM6eG1wTU09Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9tbS8iIHhtbG5zOnN0UmVmPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvc1R5cGUvUmVzb3VyY2VSZWYjIiB4bWxuczp4bXA9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC8iIHhtcFJpZ2h0czpNYXJrZWQ9IkZhbHNlIiB4bXBNTTpEb2N1bWVudElEPSJ4bXAuZGlkOkM3MUJFMDdFOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXBNTTpJbnN0YW5jZUlEPSJ4bXAuaWlkOkM3MUJFMDdEOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXA6Q3JlYXRvclRvb2w9IkFkb2JlIFBob3Rvc2hvcCBDUzIgV2luZG93cyI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ1dWlkOjgyRTY0QjM0QzU5QkRGMTFBODA4OTIwNUU4Mjg0ODVBIiBzdFJlZjpkb2N1bWVudElEPSJ1dWlkOkI2NjNCOUQyOTk5OERGMTE4MzE1RDE2MUYyMTQyRDUxIi8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+4K+uiwAABPFJREFUeNq0lQ1MlVUYx//vey8X7gcfl9D4agjdqIjbkumtUTA+drlQsZRqy6YL52wa4ow0ZqKORRHCamprS5ljQS3D5pBcCrKgXftyc+VMEVACkpRvhAvcj/d9O+e87wXcbl228tkeODvnPL//ec7z3PNykiTBa9mHdGjbPoPFRuZMPKfqhh9rLZ7i/m1dDf8WSv+8m9+I0ake8DwHjpMQqAIZ8wgMSMCeUwV+IUsRYjY9/Rs6B45Db1BBGyjgkft4BAXoEBNetKR4JnSj/kl2f0eNZjq+a0NZiAnvT/VicmYQI9PX4eTV0IsezLp4smqAy3MLixm+LGHDz5z6el2K1DMcDOuLhT43uSf7AXuN3xOveHqL79p9XQeJaPAQ3LCuWY/WE8fAhSSh7AOy4Bic977RziVdzeIYyqAsyqRsqsFLHhckMsi2ZqHlyJuoqKgEm1P88njf0oQWxVAGZVEmZdM5ng0ED/Z+3IqsnAy01JaAj7bOuznOtiShxTGUQVmUSdlUQ+3NqHxzKlO2bqzC6bK15AQzUJFWuR1EKA/yfoXO7rOB9AikoHDklTVA6GuWmUpGakkgQiLNyqXcgYgHolwIfzQdAToj/nDfAf7q8Cv0RF4+XNMjmOy9whjzPIVNMnISRarqVHQEaPRaaLUqaIJ1CHJ62LzTPeVTQJQc7L/WoAXv4TGjkRjDy4PCZlcHJT0POUn+ybWAjmwYkLOIDAzBa5a3MDze5lNo1nUN+cmFePn3enmCxlIGsVNJL4HzXp3onIPomoXodIBWoi77MLa3bsOBV39Ax5VKROiTcXP0PIbv9PgUmnFexhpzKjITDyBcn42zVxtw6cY3KDfGMyY0lD0HnhZQJKoiUaW+LNCIIncYShozkZW8F/auKvSO2skL4PAp5BEn4HBeRFSIiPbuE+j45SOUr94FtSDITMYGbW960XILUqeFjBQ4WHTLsenzVXjDdh7m2AKo+UCfQipej1CdDe09c6j7qQIZbg30qqAFHmOTt05gJXJCcM3JxSUnkUQRWyMexzTHYfMXqTjyyveY80ygb8xO22Xht8PrYNQ/i4s3jfjUXop1oSZY+m8zhpcHxlYykgTajgJzmhFpJUyOjGNb2FNI0ITj9S/TkflYKWKNKXdlY9Tb0D2SgIPflSLHYEKmK4zEikp7C4qLLCOezZORKMpOP4TUI8w2LDfn4mBODWL092PrVzmwmvcvZMNpMORIQfW5HUiLtmBHWimLEZX4BZ7AtHmJHUBi1yXJqmyj688fmYuDF/DhQ/kw6SKw5Xgu1lsasTIyiWRTgP2ni5FmfBhvR1nm97MvtrTAk9lEyEPSEgQJHsXpiUSyIpAiekjXCOQHF0A27ovPQ5QmmDTIC1i54hhKmhqQEhyDoph0tkcgb5pAO4zAKcPLk9mkGUS5JCw99jKQ8cCQA5OXOqE2DJPP9sJHeB3iUKu+ioKjFsSKWjw3tgydY7/KcaTirqkhTAyLSFzE87JlIeJVJ/vxXmU1mg/txPPFtf/4psXP3MIn1z7D7uQiGAL0PvfIjBrs2b0L72w0MT7XvglSYu4G6BPz0Hx4J55ZHYf/w+wX+pBPxBxd36LrTD3pOpoaWWgip0hdFYfqpn453f/glEFZlCkq18edK4Q0yEUjIzcb98Laz7QhWhoER9uxpZCTcA8tp07i/hZgAMNRD8XVs1vdAAAAAElFTkSuQmCC",
                                        |            "length": 2244
                                        |        }
                                        |    }],
                                        |    "irretrievablePaths":[]
                                        |}
                                      """.stripMargin)

            f4.map { res =>
              val body = new String(res.payload, "UTF-8")
              withClue(body + "\n" + res.toString) {
              Json
                .parse(res.payload)
                .transform(bagUuidDateEraserAndSorter)
                .get shouldEqual expected
              }
            }
          }

          it("should get: application/json -> n3") {
            val expected = s"""
                            |@prefix nn:    <http://localhost:9000/meta/nn#> .
                            |@prefix sys:   <http://localhost:9000/meta/sys#> .
                            |@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
                            |
                            |<http://localhost:9000/test/imgs/icon.png>
                            |      sys:base64-data   "iVBORw0KGgoAAAANSUhEUgAAABoAAAAaCAYAAACpSkzOAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAA2lpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMC1jMDYwIDYxLjEzNDc3NywgMjAxMC8wMi8xMi0xNzozMjowMCAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wUmlnaHRzPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvcmlnaHRzLyIgeG1sbnM6eG1wTU09Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9tbS8iIHhtbG5zOnN0UmVmPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvc1R5cGUvUmVzb3VyY2VSZWYjIiB4bWxuczp4bXA9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC8iIHhtcFJpZ2h0czpNYXJrZWQ9IkZhbHNlIiB4bXBNTTpEb2N1bWVudElEPSJ4bXAuZGlkOkM3MUJFMDdFOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXBNTTpJbnN0YW5jZUlEPSJ4bXAuaWlkOkM3MUJFMDdEOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXA6Q3JlYXRvclRvb2w9IkFkb2JlIFBob3Rvc2hvcCBDUzIgV2luZG93cyI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ1dWlkOjgyRTY0QjM0QzU5QkRGMTFBODA4OTIwNUU4Mjg0ODVBIiBzdFJlZjpkb2N1bWVudElEPSJ1dWlkOkI2NjNCOUQyOTk5OERGMTE4MzE1RDE2MUYyMTQyRDUxIi8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+4K+uiwAABPFJREFUeNq0lQ1MlVUYx//vey8X7gcfl9D4agjdqIjbkumtUTA+drlQsZRqy6YL52wa4ow0ZqKORRHCamprS5ljQS3D5pBcCrKgXftyc+VMEVACkpRvhAvcj/d9O+e87wXcbl228tkeODvnPL//ec7z3PNykiTBa9mHdGjbPoPFRuZMPKfqhh9rLZ7i/m1dDf8WSv+8m9+I0ake8DwHjpMQqAIZ8wgMSMCeUwV+IUsRYjY9/Rs6B45Db1BBGyjgkft4BAXoEBNetKR4JnSj/kl2f0eNZjq+a0NZiAnvT/VicmYQI9PX4eTV0IsezLp4smqAy3MLixm+LGHDz5z6el2K1DMcDOuLhT43uSf7AXuN3xOveHqL79p9XQeJaPAQ3LCuWY/WE8fAhSSh7AOy4Bic977RziVdzeIYyqAsyqRsqsFLHhckMsi2ZqHlyJuoqKgEm1P88njf0oQWxVAGZVEmZdM5ng0ED/Z+3IqsnAy01JaAj7bOuznOtiShxTGUQVmUSdlUQ+3NqHxzKlO2bqzC6bK15AQzUJFWuR1EKA/yfoXO7rOB9AikoHDklTVA6GuWmUpGakkgQiLNyqXcgYgHolwIfzQdAToj/nDfAf7q8Cv0RF4+XNMjmOy9whjzPIVNMnISRarqVHQEaPRaaLUqaIJ1CHJ62LzTPeVTQJQc7L/WoAXv4TGjkRjDy4PCZlcHJT0POUn+ybWAjmwYkLOIDAzBa5a3MDze5lNo1nUN+cmFePn3enmCxlIGsVNJL4HzXp3onIPomoXodIBWoi77MLa3bsOBV39Ax5VKROiTcXP0PIbv9PgUmnFexhpzKjITDyBcn42zVxtw6cY3KDfGMyY0lD0HnhZQJKoiUaW+LNCIIncYShozkZW8F/auKvSO2skL4PAp5BEn4HBeRFSIiPbuE+j45SOUr94FtSDITMYGbW960XILUqeFjBQ4WHTLsenzVXjDdh7m2AKo+UCfQipej1CdDe09c6j7qQIZbg30qqAFHmOTt05gJXJCcM3JxSUnkUQRWyMexzTHYfMXqTjyyveY80ygb8xO22Xht8PrYNQ/i4s3jfjUXop1oSZY+m8zhpcHxlYykgTajgJzmhFpJUyOjGNb2FNI0ITj9S/TkflYKWKNKXdlY9Tb0D2SgIPflSLHYEKmK4zEikp7C4qLLCOezZORKMpOP4TUI8w2LDfn4mBODWL092PrVzmwmvcvZMNpMORIQfW5HUiLtmBHWimLEZX4BZ7AtHmJHUBi1yXJqmyj688fmYuDF/DhQ/kw6SKw5Xgu1lsasTIyiWRTgP2ni5FmfBhvR1nm97MvtrTAk9lEyEPSEgQJHsXpiUSyIpAiekjXCOQHF0A27ovPQ5QmmDTIC1i54hhKmhqQEhyDoph0tkcgb5pAO4zAKcPLk9mkGUS5JCw99jKQ8cCQA5OXOqE2DJPP9sJHeB3iUKu+ioKjFsSKWjw3tgydY7/KcaTirqkhTAyLSFzE87JlIeJVJ/vxXmU1mg/txPPFtf/4psXP3MIn1z7D7uQiGAL0PvfIjBrs2b0L72w0MT7XvglSYu4G6BPz0Hx4J55ZHYf/w+wX+pBPxBxd36LrTD3pOpoaWWgip0hdFYfqpn453f/glEFZlCkq18edK4Q0yEUjIzcb98Laz7QhWhoER9uxpZCTcA8tp07i/hZgAMNRD8XVs1vdAAAAAElFTkSuQmCC"^^xsd:base64Binary ;
                            |      sys:length        "2244"^^xsd:long ;
                            |      sys:mimeType      "image/png" ;
                            |      sys:parent        "/test/imgs" ;
                            |      sys:path          "/test/imgs/icon.png" ;
                            |      sys:dataCenter    "$dcName" ;
                            |      sys:type          "FileInfoton" .
                            |
                            |[ sys:infotons  <http://localhost:9000/test/imgs/icon.png> ;
                            |  sys:size      "1"^^xsd:int ;
                            |  sys:type      "RetrievablePaths"
                            |] .
                            """.stripMargin

            f5.map { res =>
              val body = new String(res.payload, "UTF-8")
              withClue(body + "\n" + res.toString) {
                res.status should be >= 200
                res.status should be < 400 //status should be OK
                val errOrOk = compareRDF(expected, body, "N3", Array(
                  "lastModified", "uuid", "indexTime"))
                withClue("\n" + errOrOk.left.getOrElse("") + "\n") {
                  errOrOk.right.value should be(())
                }
              }
            }
          }

          it("should get: application/json -> rdfxml") {
            val expected = s"""
                              |<rdf:RDF
                              |    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                              |    xmlns:sys="http://localhost:9000/meta/sys#"
                              |    xmlns:nn="http://localhost:9000/meta/nn#"
                              |    xmlns:xsd="http://www.w3.org/2001/XMLSchema#">
                              |  <rdf:Description>
                              |    <sys:type>RetrievablePaths</sys:type>
                              |    <sys:size rdf:datatype="http://www.w3.org/2001/XMLSchema#int">1</sys:size>
                              |    <sys:infotons>
                              |      <rdf:Description rdf:about="http://localhost:9000/test/imgs/icon.png">
                              |        <sys:type>FileInfoton</sys:type>
                              |        <sys:path>/test/imgs/icon.png</sys:path>
                              |        <sys:parent>/test/imgs</sys:parent>
                              |        <sys:dataCenter>$dcName</sys:dataCenter>
                              |        <sys:mimeType>image/png</sys:mimeType>
                              |        <sys:length rdf:datatype="http://www.w3.org/2001/XMLSchema#long">2244</sys:length>
                              |        <sys:base64-data rdf:datatype="http://www.w3.org/2001/XMLSchema#base64Binary">
                              |          iVBORw0KGgoAAAANSUhEUgAAABoAAAAaCAYAAACpSkzOAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAA2lpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMC1jMDYwIDYxLjEzNDc3NywgMjAxMC8wMi8xMi0xNzozMjowMCAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wUmlnaHRzPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvcmlnaHRzLyIgeG1sbnM6eG1wTU09Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9tbS8iIHhtbG5zOnN0UmVmPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvc1R5cGUvUmVzb3VyY2VSZWYjIiB4bWxuczp4bXA9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC8iIHhtcFJpZ2h0czpNYXJrZWQ9IkZhbHNlIiB4bXBNTTpEb2N1bWVudElEPSJ4bXAuZGlkOkM3MUJFMDdFOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXBNTTpJbnN0YW5jZUlEPSJ4bXAuaWlkOkM3MUJFMDdEOEU4QzExREZCMjU5OEFEMjE5QjA1MDRDIiB4bXA6Q3JlYXRvclRvb2w9IkFkb2JlIFBob3Rvc2hvcCBDUzIgV2luZG93cyI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ1dWlkOjgyRTY0QjM0QzU5QkRGMTFBODA4OTIwNUU4Mjg0ODVBIiBzdFJlZjpkb2N1bWVudElEPSJ1dWlkOkI2NjNCOUQyOTk5OERGMTE4MzE1RDE2MUYyMTQyRDUxIi8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+4K+uiwAABPFJREFUeNq0lQ1MlVUYx//vey8X7gcfl9D4agjdqIjbkumtUTA+drlQsZRqy6YL52wa4ow0ZqKORRHCamprS5ljQS3D5pBcCrKgXftyc+VMEVACkpRvhAvcj/d9O+e87wXcbl228tkeODvnPL//ec7z3PNykiTBa9mHdGjbPoPFRuZMPKfqhh9rLZ7i/m1dDf8WSv+8m9+I0ake8DwHjpMQqAIZ8wgMSMCeUwV+IUsRYjY9/Rs6B45Db1BBGyjgkft4BAXoEBNetKR4JnSj/kl2f0eNZjq+a0NZiAnvT/VicmYQI9PX4eTV0IsezLp4smqAy3MLixm+LGHDz5z6el2K1DMcDOuLhT43uSf7AXuN3xOveHqL79p9XQeJaPAQ3LCuWY/WE8fAhSSh7AOy4Bic977RziVdzeIYyqAsyqRsqsFLHhckMsi2ZqHlyJuoqKgEm1P88njf0oQWxVAGZVEmZdM5ng0ED/Z+3IqsnAy01JaAj7bOuznOtiShxTGUQVmUSdlUQ+3NqHxzKlO2bqzC6bK15AQzUJFWuR1EKA/yfoXO7rOB9AikoHDklTVA6GuWmUpGakkgQiLNyqXcgYgHolwIfzQdAToj/nDfAf7q8Cv0RF4+XNMjmOy9whjzPIVNMnISRarqVHQEaPRaaLUqaIJ1CHJ62LzTPeVTQJQc7L/WoAXv4TGjkRjDy4PCZlcHJT0POUn+ybWAjmwYkLOIDAzBa5a3MDze5lNo1nUN+cmFePn3enmCxlIGsVNJL4HzXp3onIPomoXodIBWoi77MLa3bsOBV39Ax5VKROiTcXP0PIbv9PgUmnFexhpzKjITDyBcn42zVxtw6cY3KDfGMyY0lD0HnhZQJKoiUaW+LNCIIncYShozkZW8F/auKvSO2skL4PAp5BEn4HBeRFSIiPbuE+j45SOUr94FtSDITMYGbW960XILUqeFjBQ4WHTLsenzVXjDdh7m2AKo+UCfQipej1CdDe09c6j7qQIZbg30qqAFHmOTt05gJXJCcM3JxSUnkUQRWyMexzTHYfMXqTjyyveY80ygb8xO22Xht8PrYNQ/i4s3jfjUXop1oSZY+m8zhpcHxlYykgTajgJzmhFpJUyOjGNb2FNI0ITj9S/TkflYKWKNKXdlY9Tb0D2SgIPflSLHYEKmK4zEikp7C4qLLCOezZORKMpOP4TUI8w2LDfn4mBODWL092PrVzmwmvcvZMNpMORIQfW5HUiLtmBHWimLEZX4BZ7AtHmJHUBi1yXJqmyj688fmYuDF/DhQ/kw6SKw5Xgu1lsasTIyiWRTgP2ni5FmfBhvR1nm97MvtrTAk9lEyEPSEgQJHsXpiUSyIpAiekjXCOQHF0A27ovPQ5QmmDTIC1i54hhKmhqQEhyDoph0tkcgb5pAO4zAKcPLk9mkGUS5JCw99jKQ8cCQA5OXOqE2DJPP9sJHeB3iUKu+ioKjFsSKWjw3tgydY7/KcaTirqkhTAyLSFzE87JlIeJVJ/vxXmU1mg/txPPFtf/4psXP3MIn1z7D7uQiGAL0PvfIjBrs2b0L72w0MT7XvglSYu4G6BPz0Hx4J55ZHYf/w+wX+pBPxBxd36LrTD3pOpoaWWgip0hdFYfqpn453f/glEFZlCkq18edK4Q0yEUjIzcb98Laz7QhWhoER9uxpZCTcA8tp07i/hZgAMNRD8XVs1vdAAAAAElFTkSuQmCC
                              |	       </sys:base64-data>
                              |      </rdf:Description>
                              |    </sys:infotons>
                              |  </rdf:Description>
                              |</rdf:RDF>
                            """.stripMargin

            f6.map { res =>
              val body = new String(res.payload, "UTF-8")
              withClue(body + "\n" + res.toString) {
                res.status should be >= 200
                res.status should be < 400 //status should be OK
                val errOrOk = compareRDF(expected, body, "RDF/XML", Array("lastModified", "uuid", "indexTime"))
                withClue("\n" + errOrOk.left.getOrElse("") + "\n") {
                  errOrOk.right.value should be(())
                }
              }
            }
          }

          it("should get: wrong/type -> error") {
            Http.post(_out, Json.stringify(input), Some("wrong/type;charset=UTF-8"), List("format" -> "rdfxml"), tokenHeader).map(_.status should be(400))
          }
        }
      }

      it("should retrieve JohnSmith via web sockets") {
        val expected =
          s"""
            |<rdf:RDF
            |    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            |    xmlns:sys="http://localhost:9000/meta/sys#"
            |    xmlns:nn="http://localhost:9000/meta/nn#"
            |    xmlns:xsd="http://www.w3.org/2001/XMLSchema#"
            |    xmlns:vcard="http://www.w3.org/2006/vcard/ns#">
            |  <rdf:Description>
            |    <sys:type>BagOfInfotons</sys:type>
            |    <sys:size rdf:datatype="http://www.w3.org/2001/XMLSchema#int">1</sys:size>
            |    <sys:infotons>
            |      <vcard:Individual rdf:about="http://example.org/JohnSmith">
            |        <sys:type>ObjectInfoton</sys:type>
            |        <sys:path>/example.org/JohnSmith</sys:path>
            |        <sys:parent>/example.org</sys:parent>
            |        <sys:dataCenter>$dcName</sys:dataCenter>
            |        <vcard:FN>John Smith</vcard:FN>
            |        <vcard:GENDER>Male</vcard:GENDER>
            |      </vcard:Individual>
            |    </sys:infotons>
            |  </rdf:Description>
            |</rdf:RDF>
          """.stripMargin

        val p = Promise[String]()

        Http.ws(uri = "ws://localhost:9000/ws/_out",
                initiationMessage = "/example.org/JohnSmith",
                queryParams = List("format"->"rdfxml")) { msg =>
          p.success(msg)
          None
        }

        p.future.map { res =>
          val errOrOk = compareRDF(expected, res, "RDF/XML-ABBREV", Array("lastModified", "uuid", "indexTime"))
          withClue("\n" + errOrOk.left.getOrElse("") + "\n") {
            errOrOk.right.value should be(())
          }
        }
      }

      it("should retrieve inserted N3 docs as JSON") {
        val expected = Json.parse(
          s"""
            |{
            |  "type":"ObjectInfoton",
            |  "system":{
            |    "path":"/clearforest.com/ce/MZ",
            |    "parent":"/clearforest.com/ce",
            |    "dataCenter":"$dcName"
            |  },
            |  "fields":{
            |    "FN.vcard":["M Z"]
            |  }
            |}
          """.stripMargin)

        val path = clf / "ce" / "MZ"
        Http.get(path, List("format" -> "json")).map { res =>
          withClue(res) {
            val jv = Json.parse(res.payload).transform(uuidDateEraser andThen (__ \ 'fields \ "N.vcard").json.prune).get
            jv shouldEqual expected
          }
        }
      }

      describe("checking /meta/ns data") {

        it("should check generated anti-jena prefix") {
          val hash = crc32base64("http://made.up.property.org/2013/cmwtest#")
          val expected = Json.parse(
            s"""
              |{
              |  "type":"ObjectInfoton",
              |  "system":{
              |    "path":"/meta/ns/$hash",
              |    "parent":"/meta/ns",
              |    "dataCenter":"$dcName"
              |  },
              |  "fields":{
              |    "url":["http://made.up.property.org/2013/cmwtest#"],
              |    "prefix":["cmwtest"]
              |  }
              |}
            """.stripMargin)
          val path = metaNs / hash
          Http.get(path, List("format" -> "json")).map { res =>
            withClue(res) {
              val jv = Json.parse(res.payload).transform(uuidDateEraser).get
              jv shouldEqual expected
            }
          }
        }
      }
    }

    describe("expand graph API") {

      val mZ = cmw / "example.net" / "Individuals" / "M_Z"
      val cKent = cmw / "example.org" / "Individuals" / "ClarkKent"


      val ingestOfNtriplesFromFilesPromise = Promise[Unit]()

      def jSmithUnderscoreOut(): Future[SimpleResponse[Array[Byte]]] =
        Http.post(_out, "/example.net/Individuals/JohnSmith", Some("text/plain;charset=UTF-8"), List("format" -> "json", "xg" -> "*.vcard"), tokenHeader)

      val f00 = ingestOfNtriplesFromFilesPromise.future
      val f01 = f00.flatMap(_ => Http.get(cmw / "example.org" / "BugChecks" / "xgBugCheck", List("format" -> "json", "xg" -> s"dislikes.rel")))
      val f08 = f00.flatMap(_ => Http.get(mZ, List("xg" -> s"colleagueOf.$$${ns.rel}>employedBy.$$${ns.rel}>mentorOf.$$${ns.rel}>friendOf.$$${ns.rel}>worksWith.$$${ns.rel}>parentOf.$$${ns.rel}","format" -> "json")))
      val f09 = f00.flatMap(_ => Http.get(mZ, List("xg" -> "$http://purl.org/vocab/relationship/colleagueOf$>$http://purl.org/vocab/relationship/employedBy$>$http://purl.org/vocab/relationship/mentorOf$>$http://purl.org/vocab/relationship/friendOf$>$http://purl.org/vocab/relationship/worksWith$>$http://purl.org/vocab/relationship/parentOf$","format" -> "json")))
      val f10 = f00.flatMap(_ => Http.get(mZ, List("xg" -> "colleagueOf.rel>employedBy.rel>mentorOf.rel>friendOf.rel>worksWith.rel>parentOf.rel","format" -> "json")))
      val f11 = f00.flatMap(_ => Http.get(mZ, List("xg" -> "colleagueOf.rel[doesNotExist:]>employedBy.rel>mentorOf.rel>friendOf.rel>worksWith.rel>parentOf.rel","format" -> "json")))
      val f12 = f00.flatMap(_ => Http.get(mZ, List("xg" -> "colleagueOf.rel[system.path:/not/real]>employedBy.rel>mentorOf.rel>friendOf.rel>worksWith.rel>parentOf.rel","format" -> "json")))
      val f13 = f00.flatMap(_ => Http.get(cKent, List("yg" -> s"<neighborOf.$$${ns.rel}>worksWith.$$${ns.rel}|<neighborOf.$$${ns.rel}<friendOf.$$${ns.rel}<mentorOf.$$${ns.rel}>knowsByReputation.$$${ns.rel}<collaboratesWith.$$${ns.rel}","format" -> "json")))
      val f14 = f00.flatMap(_ => Http.get(cKent, List("yg" -> "<$http://purl.org/vocab/relationship/neighborOf$>$http://purl.org/vocab/relationship/worksWith$|<$http://purl.org/vocab/relationship/neighborOf$<$http://purl.org/vocab/relationship/friendOf$<$http://purl.org/vocab/relationship/mentorOf$>$http://purl.org/vocab/relationship/knowsByReputation$<$http://purl.org/vocab/relationship/collaboratesWith$","format" -> "json")))
      val f15 = f00.flatMap(_ => Http.get(cKent, List("yg" -> "<neighborOf.rel>worksWith.rel|<neighborOf.rel<friendOf.rel<mentorOf.rel>knowsByReputation.rel<collaboratesWith.rel","format" -> "json")))
      val f16 = f00.flatMap(_ => Http.get(cKent, List("yg" -> "<neighborOf.rel[active.bold::true]>worksWith.rel[active.bold::false]|<neighborOf.rel[active.bold::true]<friendOf.rel[doesNotExist::SomeValue]<mentorOf.rel>knowsByReputation.rel<collaboratesWith.rel","format" -> "json")))
      val f17 = f00.flatMap(_ => Http.get(cKent, List("yg" -> "<neighborOf.rel[active.bold::true]>worksWith.rel[active.bold::false]|<neighborOf.rel[active.bold::true]<friendOf.rel[system.path:/not/real]<mentorOf.rel>knowsByReputation.rel<collaboratesWith.rel","format" -> "json")))

      it("should post N-Triple files") {
        Future.traverse(Seq("/relationships.nt","/relationships2.nt")) { file =>
          val fileNTriple = Source.fromURL(this.getClass.getResource(file)).mkString
          Http.post(_in, fileNTriple, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader).map(_ -> file)
        }.map { responses =>
          delayedTask(indexingDuration * 2)(ingestOfNtriplesFromFilesPromise.success(()))
          forAll(responses) { case (res, file) =>
            withClue(s"uploading [$file] failed") {
              Json.parse(res.payload) should be(jsonSuccess)
            }
          }
        }
      }

      it("should verify that xg's flatMap on Map bug is gone") {
        f01.map{res =>
          val j = Json.parse(res.payload)
          ((j \ "infotons").get: @unchecked) match {
            case JsArray(xs) => xs.size should be(3)
          }
        }
      }

      it("should expand 6 levels deep with explicit $ namespace") {
        val j = s"""{"type":"BagOfInfotons","infotons":[{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/G_H","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.net/Individuals/N-l_H"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/I_K","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"employedBy.rel":["http://example.net/Individuals/D_L"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/M_I","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"worksWith.rel":["http://example.net/Individuals/G_H"],"neighborOf.rel":["http://example.net/Individuals/Gilad_Zafran"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/N-l_H","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"childOf.rel":["http://example.net/Individuals/G_H"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/D_L","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"knowsByReputation.rel":["http://example.net/Individuals/M_O"],"mentorOf.rel":["http://example.net/Individuals/Y_B"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/Y_B","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.net/Individuals/T-S_B"],"friendOf.rel":["http://example.net/Individuals/M_I"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/M_Z","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"colleagueOf.rel":["http://example.net/Individuals/I_K"]}}]}"""
        val expected = Json.parse(j.getBytes("UTF-8")).transform(bagUuidDateEraserAndSorter).get
        f08.map { res =>
          lazy val clue = new String(res.payload, "UTF-8")
          withClue(res -> clue) {
            Try {
              Json
                .parse(res.payload)
                .transform(bagUuidDateEraserAndSorter)
                .get
            } match {
              case Success(j) => j shouldEqual expected
              case Failure(e) => fail(clue)
            }
          }
        }
      }

      it("should expand 6 levels deep using full NS URI") {
        val j = s"""{"type":"BagOfInfotons","infotons":[{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/G_H","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.net/Individuals/N-l_H"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/I_K","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"employedBy.rel":["http://example.net/Individuals/D_L"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/M_I","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"worksWith.rel":["http://example.net/Individuals/G_H"],"neighborOf.rel":["http://example.net/Individuals/Gilad_Zafran"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/N-l_H","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"childOf.rel":["http://example.net/Individuals/G_H"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/D_L","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"knowsByReputation.rel":["http://example.net/Individuals/M_O"],"mentorOf.rel":["http://example.net/Individuals/Y_B"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/Y_B","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.net/Individuals/T-S_B"],"friendOf.rel":["http://example.net/Individuals/M_I"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/M_Z","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"colleagueOf.rel":["http://example.net/Individuals/I_K"]}}]}"""
        val expected = Json.parse(j.getBytes("UTF-8")).transform(bagUuidDateEraserAndSorter).get
        f09.map { res =>
          lazy val clue = new String(res.payload, "UTF-8")
          withClue(res -> clue) {
            Try {
              Json
                .parse(res.payload)
                .transform(bagUuidDateEraserAndSorter)
                .get
            } match {
              case Success(j) => j shouldEqual expected
              case Failure(e) => fail(clue)
            }
          }
        }
      }

      it("should expand 6 levels deep with implicit namespace") {
        val j = s"""{"type":"BagOfInfotons","infotons":[{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/G_H","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.net/Individuals/N-l_H"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/I_K","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"employedBy.rel":["http://example.net/Individuals/D_L"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/M_I","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"worksWith.rel":["http://example.net/Individuals/G_H"],"neighborOf.rel":["http://example.net/Individuals/Gilad_Zafran"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/N-l_H","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"childOf.rel":["http://example.net/Individuals/G_H"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/D_L","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"knowsByReputation.rel":["http://example.net/Individuals/M_O"],"mentorOf.rel":["http://example.net/Individuals/Y_B"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/Y_B","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.net/Individuals/T-S_B"],"friendOf.rel":["http://example.net/Individuals/M_I"]}},{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/M_Z","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"colleagueOf.rel":["http://example.net/Individuals/I_K"]}}]}"""
        val expected = Json.parse(j.getBytes("UTF-8")).transform(bagUuidDateEraserAndSorter).get
        f10.map { res =>
          lazy val clue = new String(res.payload, "UTF-8")
          withClue(res -> clue) {
            Try {
              Json
                .parse(res.payload)
                .transform(bagUuidDateEraserAndSorter)
                .get
            } match {
              case Success(j) => j shouldEqual expected
              case Failure(e) => fail(clue)
            }
          }
        }
      }

      it("should NOT to expand 6 levels deep if guarded by a non existed filter") {
        val j = s"""{"type":"BagOfInfotons","infotons":[{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/M_Z","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"colleagueOf.rel":["http://example.net/Individuals/I_K"]}}]}"""
        val expected = Json.parse(j.getBytes("UTF-8")).transform(bagUuidDateEraserAndSorter).get
        f11.map { res =>
          val str = new String(res.payload, "UTF-8")
          val jsn = Json.parse(res.payload).transform(bagUuidDateEraserAndSorter)
          withClue(s"got: $str") {
            jsn.isSuccess should be(true)
            jsn.get shouldEqual expected
          }
        }
      }

      it("should NOT expand 6 levels deep if guarded by a filter") {
        val j = s"""{"type":"BagOfInfotons","infotons":[{"type":"ObjectInfoton","system":{"path":"/example.net/Individuals/M_Z","parent":"/example.net/Individuals","dataCenter":"$dcName"},"fields":{"colleagueOf.rel":["http://example.net/Individuals/I_K"]}}]}"""
        val expected = Json.parse(j.getBytes("UTF-8")).transform(bagUuidDateEraserAndSorter).get
        f12.map { res =>
          val str = new String(res.payload, "UTF-8")
          val jsn = Json.parse(res.payload).transform(bagUuidDateEraserAndSorter)
          withClue(s"got: $str") {
            jsn.isSuccess should be(true)
            jsn.get shouldEqual expected
          }
        }
      }

      it("should allow paths expansion with yg flag with explicit $ namespace") {
        val j = s"""{"type":"BagOfInfotons","infotons":[{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/JohnSmith","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.org/Individuals/SaraSmith"],"friendOf.rel":["http://example.org/Individuals/PeterParker"],"active.bold":["true"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/RonaldKhun","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"collaboratesWith.rel":["http://example.org/Individuals/MartinOdersky"],"category.bold":["deals","news"],"active.bold":["true"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/HarryMiller","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.org/Individuals/NatalieMiller"],"active.bold":["true"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/DonaldDuck","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"knowsByReputation.rel":["http://example.org/Individuals/MartinOdersky"],"active.bold":["true"],"mentorOf.rel":["http://example.org/Individuals/JohnSmith"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/ClarkKent","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"neighborOf.rel":["http://example.org/Individuals/PeterParker"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/PeterParker","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"active.bold":["true"],"worksWith.rel":["http://example.org/Individuals/HarryMiller"],"neighborOf.rel":["http://example.org/Individuals/ClarkKent"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/MartinOdersky","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"collaboratesWith.rel":["http://example.org/Individuals/RonaldKhun"],"active.bold":["true"]}}]}"""
        val expected = Json.parse(j.getBytes("UTF-8")).transform(bagUuidDateEraserAndSorter).get
        f13.map { res =>
          Json
            .parse(res.payload)
            .transform(bagUuidDateEraserAndSorter)
            .get shouldEqual expected
        }
      }

      it("should allow paths expansion with yg flag using full NS URI") {
        val j = s"""{"type":"BagOfInfotons","infotons":[{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/JohnSmith","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.org/Individuals/SaraSmith"],"friendOf.rel":["http://example.org/Individuals/PeterParker"],"active.bold":["true"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/RonaldKhun","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"collaboratesWith.rel":["http://example.org/Individuals/MartinOdersky"],"category.bold":["deals","news"],"active.bold":["true"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/HarryMiller","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.org/Individuals/NatalieMiller"],"active.bold":["true"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/DonaldDuck","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"knowsByReputation.rel":["http://example.org/Individuals/MartinOdersky"],"active.bold":["true"],"mentorOf.rel":["http://example.org/Individuals/JohnSmith"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/ClarkKent","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"neighborOf.rel":["http://example.org/Individuals/PeterParker"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/PeterParker","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"active.bold":["true"],"worksWith.rel":["http://example.org/Individuals/HarryMiller"],"neighborOf.rel":["http://example.org/Individuals/ClarkKent"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/MartinOdersky","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"collaboratesWith.rel":["http://example.org/Individuals/RonaldKhun"],"active.bold":["true"]}}]}"""
        val expected = Json.parse(j.getBytes("UTF-8")).transform(bagUuidDateEraserAndSorter).get
        f14.map { res =>
          Json
            .parse(res.payload)
            .transform(bagUuidDateEraserAndSorter)
            .get shouldEqual expected
        }
      }

      it("should allow paths expansion with yg flag with implicit namespace") {
        val j = s"""{"type":"BagOfInfotons","infotons":[{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/JohnSmith","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.org/Individuals/SaraSmith"],"friendOf.rel":["http://example.org/Individuals/PeterParker"],"active.bold":["true"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/RonaldKhun","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"collaboratesWith.rel":["http://example.org/Individuals/MartinOdersky"],"category.bold":["deals","news"],"active.bold":["true"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/HarryMiller","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"parentOf.rel":["http://example.org/Individuals/NatalieMiller"],"active.bold":["true"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/DonaldDuck","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"knowsByReputation.rel":["http://example.org/Individuals/MartinOdersky"],"active.bold":["true"],"mentorOf.rel":["http://example.org/Individuals/JohnSmith"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/ClarkKent","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"neighborOf.rel":["http://example.org/Individuals/PeterParker"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/PeterParker","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"active.bold":["true"],"worksWith.rel":["http://example.org/Individuals/HarryMiller"],"neighborOf.rel":["http://example.org/Individuals/ClarkKent"]}},{"type":"ObjectInfoton","system":{"path":"/example.org/Individuals/MartinOdersky","parent":"/example.org/Individuals","dataCenter":"$dcName"},"fields":{"collaboratesWith.rel":["http://example.org/Individuals/RonaldKhun"],"active.bold":["true"]}}]}"""
        val expected = Json.parse(j.getBytes("UTF-8")).transform(bagUuidDateEraserAndSorter).get
        f15.map { res =>
          Json
            .parse(res.payload)
            .transform(bagUuidDateEraserAndSorter)
            .get shouldEqual expected
        }
      }

      it("should NOT allow limited paths expansion if guarded by non existed filters") {
        val j = s"""{"type":"BagOfInfotons","infotons":[{"type":"ObjectInfoton","fields":{"neighborOf.rel":["http://example.org/Individuals/PeterParker"]},"system":{"path":"/example.org/Individuals/ClarkKent","dataCenter":"$dcName","parent":"/example.org/Individuals"}},{"type":"ObjectInfoton","fields":{"worksWith.rel":["http://example.org/Individuals/HarryMiller"],"neighborOf.rel":["http://example.org/Individuals/ClarkKent"],"active.bold":["true"]},"system":{"path":"/example.org/Individuals/PeterParker","dataCenter":"$dcName","parent":"/example.org/Individuals"}}]}"""
        val expected = Json.parse(j.getBytes("UTF-8")).transform(bagUuidDateEraserAndSorter).get
        f16.map { res =>
          val str = new String(res.payload, "UTF-8")
          val jsn = Json.parse(res.payload).transform(bagUuidDateEraserAndSorter)
          withClue(s"got: $str") {
            jsn.isSuccess should be(true)
            jsn.get shouldEqual expected
          }
        }
      }

      it("should allow limited paths expansion if guarded by filters") {
        val j = s"""{"type":"BagOfInfotons","infotons":[{"type":"ObjectInfoton","fields":{"neighborOf.rel":["http://example.org/Individuals/PeterParker"]},"system":{"path":"/example.org/Individuals/ClarkKent","dataCenter":"$dcName","parent":"/example.org/Individuals"}},{"type":"ObjectInfoton","fields":{"worksWith.rel":["http://example.org/Individuals/HarryMiller"],"neighborOf.rel":["http://example.org/Individuals/ClarkKent"],"active.bold":["true"]},"system":{"path":"/example.org/Individuals/PeterParker","dataCenter":"$dcName","parent":"/example.org/Individuals"}}]}"""
        val expected = Json.parse(j.getBytes("UTF-8")).transform(bagUuidDateEraserAndSorter).get
        f17.map { res =>
          val str = new String(res.payload, "UTF-8")
          val jsn = Json.parse(res.payload).transform(bagUuidDateEraserAndSorter)
          withClue(s"got: $str") {
            jsn.isSuccess should be(true)
            jsn.get shouldEqual expected
          }
        }
      }
    }

    it("should return 400 Bad Request for /ii/* with an invalid uuid format") {
      val path = cmw / "ii" / "not-a-valid-uuid"
      Http.get(path).map(_.status should be(400))
    }

    describe("should render non-english characters in various formats") {
      val path = cmt / "InfoObjHebrew"

      val f = {
        val infoton = Json.obj("Hello" -> "שלום", "World" -> "עולם").toString
        Http.post(path, infoton, Some("application/json;charset=UTF-8"), List("format" -> "json"), ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
      }

      it("should upload an Infoton with Hebrew values") {
        f.map(res => Json.parse(res.payload) should be(jsonSuccess))
      }

      it("should verify Hebrew is readable in each and every format") {
        f.flatMap(_ => scheduleFuture(indexingDuration) {
          Future.traverse(Seq("json", "jsonld", "n3", "ntriples", "turtle", "rdfxml", "yaml")) { format =>
            Http.get(path, List("format" -> format))
          }.map { responses =>
            forAll(responses) { res =>
              new String(res.payload,"UTF-8") should include("שלום")
            }
          }
        })
      }
    }

    describe("should render UTF-8 characters in subject") {

      it("should upload an Infoton with UTF-8 in subject using _in") {
        val triples = s"""<http://cmt/cm/test/Araújo> <http://ont.thomsonreuters.com/wiki#title> "Rock and Roll Over"^^<http://www.w3.org/2001/XMLSchema#string> ."""
        Http.post(_in, triples, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader).map { res =>
          withClue(res) {
            Json.parse(res.payload) should be(jsonSuccess)
          }
        }
      }

      val infotonUTF8 = Json.obj("title" -> "Rock and Roll Over")
      it("should upload an Infoton with escaped UTF-8 in subject using post") {
        Http.post(cmt / "Araújo3", infotonUTF8.toString(), Some("application/json;charset=UTF-8"), List("format" -> "json"), ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader).map { res =>
          withClue(res) {
            Json.parse(res.payload) should be(jsonSuccess)
          }
        }
      }

      it("should get an Infoton with UTF-8 in subject using get") {
        val expected = Json.parse(
          """
            |{
            |  "type": "ObjectInfoton",
            |  "system": {
            |    "path": "/cmt/cm/test/Araújo3",
            |    "dataCenter": "lh",
            |    "parent": "/cmt/cm/test"
            |  },
            |  "fields": {
            |    "title": [
            |      "Rock and Roll Over"
            |    ]
            |  }
            |}
          """.stripMargin)
        indexingDuration.fromNow.block
        Http.get(cmt / "Araújo3", List("format" -> "json")).map { res =>
          withClue(res) {
            Json.parse(res.payload).transform(uuidDateEraser).get should be(expected)
          }
        }
      }

      it("should get an Infoton with UTF-8 in subject using get without parameters") {
        import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
        Http.get(cmt / "Araújo3").map { res =>
          res.payload should include("Rock and Roll Over")
        }
      }

      it("should get an Infoton with UTF-8 in subject using _out") {
        import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
        Http.post(_out, "/cmt/cm/test/Araújo", Some("text/plain;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader).map { res =>
          res.payload should include("Rock and Roll Over")
        }
      }

      it("should delete an Infoton with UTF-8 in subject using delete") {
        Http.delete(cmt / "Araújo3", headers = tokenHeader).map(res => Json.parse(res.payload) should be(jsonSuccess))
      }

      it("should verify an Infoton with UTF-8 in subject actually was deleted") {
        import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
        scheduleFuture(indexingDuration) {
          Http.get(cmt / "Araújo3").map { res =>
            res.payload should include("Infoton was deleted")
          }
        }
      }

      it("should verify an Infoton with %2F in subject is rejected") {
        val errorJson = Json.parse("""{"success":false,"error":"%2F is illegal in the path part of the URI!"}""")
        val stringCmt: String = cmt
        Http.get(s"$stringCmt/moshe%2fdavid").map { res =>
          res.status should be(400)
          Json.parse(res.payload) should be(errorJson)
        }
      }
    }

    it("should ignore null updates") {
      val path = cmt / "InfotonForNullUpdatesTest"
      val jsonObjIn = Json.obj("Focus"->"OnYourBlueChips", "Be"->"HereNow")

      def postAndTest(msg: String) = {
        Http.post(path, Json.stringify(jsonObjIn), Some("application/json;charset=UTF-8"), Nil, ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader).map { res =>
          withClue(msg) {
            Json.parse(res.payload) should be(jsonSuccess)
          }
        }
      }

      val f = postAndTest("should put a new Infoton")
      val ff = f.flatMap { _ =>
        delayedTask(indexingDuration)(postAndTest("should put it again")).flatMap(identity)
      }

      ff.flatMap { _ =>
        Http.get(path, Seq("format"->"json", "with-history"->"true", "with-data"->"true")).map { res =>
          val versions = (Json.parse(res.payload) \ "versions": @unchecked)  match {
            case JsDefined(JsArray(seq)) => seq
          }
          withClue(s"should verify nothing happened: $versions"){
            versions.length should be(1)
          }
        }
      }
    }

    describe("purge") {
      implicit class JsValueExtensions(v: JsValue) {
        def getArr(prop: String): Seq[JsValue] = (v \ prop).get match { case JsArray(seq) => seq case _ => Seq() }
      }

      it("should add some historical data") {
        val data = Seq("DaisyDuck"->"Carrot", "DaisyDuck"->"Lemon", "PeterParker"->"Grapes", "PeterParker"->"Tomato", "DonaldDuck"->"Lemon",
          "RollMeBack"->"v1", "RollMeBack"->"v2", "RollMeBack"->"v3")

        data.foldLeft[Future[List[JsValue]]](Future.successful(Nil)) {
          case (f,(s,o)) => f.flatMap{ l =>
            Http.post(_in, s"<http://example.org/Individuals/$s> <http://purl.org/vocab/relationship/drinksJuiceOf> <http://example.org/Individuals/$o> .", None, Seq("format" -> "ntriples"), tokenHeader).flatMap{ res =>
              // need to wait between each POST, so versions won't collapse in IMP
              delayedTask(indexingDuration) {
                Json.parse(res.payload) :: l
              }
            }
          }
        }.map(all(_) should be(jsonSuccess))
      }

      it("should purge one historical version by uuid") {
        // get some uuid
        val path = cmw / "example.org" / "Individuals" / "DaisyDuck"
        Http.get(path, Seq("with-history" -> "", "format" -> "json")).flatMap { res =>

          val body = Json.parse(res.payload)

          val versions = (body getArr "versions")
            .sortBy(jsVal => Instant.parse((jsVal \ "system" \ "lastModified").as[String]).toEpochMilli)
          val midVersion = versions(1)
          val uuid = (midVersion \ "system" \ "uuid").as[String]

          // purge it
          val uuidPath = cmw / "ii" / uuid
          Http.get(uuidPath, Seq("op" -> "purge"), tokenHeader).flatMap { res =>

            val body1 = Json.parse(res.payload)
            body1 should be(jsonSimpleResponseSuccess)

            scheduleFuture(indexingDuration) {

              // verify it does not exist in direct access
              for {
                r1 <- Http.get(uuidPath)
                r2 <- Http.get(path, Seq("with-history" -> "", "format" -> "json"))
              } yield {
                r1.status should be(404)
                // verify it does not exist in search
                val body2 = Json.parse(r2.payload)
                val versions2 = body2 getArr "versions"
                versions2.length should be(2)
              }
            }
          }
        }
      }

      //FIXME: temporary not supported
//      ignore("should purge all historical version by path, but keep the last (current) one") {
//        val path = cmw / "example.org" / "Individuals" / "PeterParker"
//
//        // purge history
//        val f = Http.get(path, Seq("op"->"purge-history"), tokenHeader)
//          .map(res => Json.parse(res.payload))
//        val body = Await.result(f, requestTimeout)
//        body should be(jsonSimpleResponseSuccess)
//
//        indexingDuration.fromNow.block
//
//        // verify Infoton is still out there
//        val body1 = Await.result(Http.get(path).map(_.status), requestTimeout)
//        body1 should be(200)
//
//        // verify there's only 1 (last) version of it
//        val body2 = Await.result(Http.get(path, Seq("with-history" -> "", "format" -> "json"))
//          .map(res => Json.parse(res.payload)), requestTimeout)
//        val versions = body2 getArr "versions"
//        versions.length should be (1)
//      }

      it("should purge current and all historical versions by path") {
        val path = cmw / "example.org" / "Individuals" / "DonaldDuck"

        // purge all the versions!
        Http.get(path, Seq("op" -> "purge-all"), tokenHeader).flatMap { res =>
          val body = Json.parse(res.payload)
          body should be(jsonSimpleResponseSuccess)

          scheduleFuture(indexingDuration) {

            // verify Infoton does not exist
            Http.get(path).flatMap { r =>
              r.status should be(404)

              // verify there's no memory of it
              Http.get(path, Seq("with-history" -> "", "format" -> "json")).map { res =>
                val body2 = Json.parse(res.payload)
                val versions = body2 getArr "versions"
                versions.length should be(0)
              }
            }
          }
        }
      }

      //FIXME: temporary not supported
//      ignore("should rollback last version of an Infoton") {
//        val path = cmw / "example.org" / "Individuals" / "RollMeBack"
//
//        def rollback() = {
//          Await.result(Http.get(path, Seq("op" -> "purge-last"), tokenHeader).map(res => Json.parse(res.payload)), requestTimeout) should be(jsonSimpleResponseSuccess)
//          indexingDuration.fromNow.block
//        }
//
//        def assertVersions(amount: Int) = {
//          val versions = Await.result(Http.get(path, Seq("with-history" -> "", "format" -> "json")).map(res => Json.parse(res.payload)), requestTimeout) getArr "versions"
//          versions.length should be(amount)
//        }
//
//        // starting from 3 versions, rolling back last one, 3 times:
//        (0 to 2).reverse.foreach { i => rollback(); assertVersions(i) }
//
//        // verify Infoton does not exist:
//        Await.result(Http.get(path).map(_.status), requestTimeout) should be(404)
//      }
    }

    describe("fields masking") {
      val justWorksWith = "fields" -> "worksWith.rel"

      it("should mask fields, reading a single Infoton") {
        val fieldsMask = Seq("worksWith.rel","active.bold")
        val f = Http.get(cmw / "example.org" / "Individuals" / "PeterParker", Seq("fields"->fieldsMask.mkString(","), "format"->"json"))
        f.map { res =>
          (Json.parse(res.payload) \ "fields" \ "neighborOf.rel").asOpt[String] should be(None)
        }
      }

      it("should mask fields, for _out") {
        import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
        Http.post(_out, "/example.org/Individuals/PeterParker\n", Some("text/plain;charset=UTF-8"), List("format" -> "ntriples", justWorksWith), tokenHeader).map{ body =>
          body.status should be >=200
          body.status should be <400 //status should be OK
          body.payload should include("worksWith")
          body.payload should not include("active")
        }
      }

      ignore("should mask fields, for search") {
//        val req = requestOp("search", justWorksWith, "with-data"->"", "format"->"ntriples")
//        val res = Await.result(HWR(req OK as.String), requestTimeout)
//        res should include("worksWith")
//        res should not include("active")
        Future.successful(succeed)
      }

      ignore("should mask fields, for *stream") { // todo - this works for me but retunrs 500 on bingo. why?
//        Seq("","m","s","n").map(_+"stream").foreach { op =>
//          val req = requestOp(op, justWorksWith, "with-data"->"ntriples", "format"->"ntriples")
//          val res = Await.result(HWR(req OK as.String), requestTimeout)
//          res should include("worksWith")
//          res should not include("active")
//        }
        Future.successful(succeed)
      }
    }

    describe("peeling off bad fields when indexing") {
      val (goodDateValue,badDateValue) =  ("2005-08-11T04:00:00Z","2005-08-11 04:00:00")

      it("should upload an Infoton with a DateTime field") {
        val data = s"""<http://example.org/1-429589421> <http://ont.thomsonreuters.com/mdaas/ipoDate> "$goodDateValue"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."""
        Http.post(_in, data, Some("text/rdf+ntriples;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader).map { res =>
          Json.parse(res.payload) should be(jsonSuccess)
        }
      }

      ignore("should partially index an Infoton with bad field value") {
        val goodTriple = "<http://example.org/1--926126> <http://ont.thomsonreuters.com/mdaas/ipoName> \"Not 4295893421\" ."
        val data = s"""<http://example.org/1--926126> <http://ont.thomsonreuters.com/mdaas/ipoDate> "$badDateValue"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n$goodTriple"""

        Http.post(_in, data, Some("text/rdf+ntriples;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader).flatMap { res =>
          Json.parse(res.payload) should be(jsonSuccess)
          scheduleFuture(indexingDuration) {

            // asserting other field is readable
            val infotonReq = Http.get(cmw / "example.org" / "1--926126", Seq("format" -> "ntriples"))(cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler)

            // asserting indexing was successful (i.e. there's a cas version and a es version)
            Http.get(cmw / "example.org" / "1--926126", Seq("op" -> "x-verify")).map { res =>
              Json.parse(res.payload) should be(jsonSimpleResponseSuccess)
            }

            infotonReq.map { res =>
              val actualTripleIfExist = res.payload.lines.filter(_.contains("ipoName")).toSeq.headOption.getOrElse("")
              actualTripleIfExist should be(goodTriple)
            }
          }
        }
      }
    }

    it("should ingest large ObjectInfoton through Kafka via zStore") {
      import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
      val length = 55000
      val triples = (0 until length).map(i => s"""<http://example.org/1-90210> <http://ont.thomsonreuters.com/mdaas/largeField${i/(length/5)}> "$i" .""").sorted
      Http.post(_in, triples.mkString("\n"), Some("text/rdf+ntriples;charset=UTF-8"), List("format" -> "ntriples"), tokenHeader).flatMap { res =>
        Json.parse(res.payload) should be(jsonSuccess)
        scheduleFuture(indexingDuration){
          Http.get(cmw / "example.org" / "1-90210", List("format" -> "ntriples")).map { res =>
            val writtenData = res.payload.split('\n').filter(_.contains("largeField")).sorted
            withClue(s"Written data (${writtenData.length} triples) was not same NTriples as ingested data (${triples.length})\nFirst 5 written triples are: ${writtenData.take(5).mkString(" ")}") {
              (writtenData sameElements triples) should be(true)
            }
          }
        }
      }
    }

    it("should ingest large FileInfoton through Kafka via zStore") {
      import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
      val path = cmw / "example.org" / "LargeFile.txt"
      val length = 513*1024 // > 512KB
      val data = scala.util.Random.alphanumeric.take(length).mkString
      Http.post(path, data, Some("text/plain"), headers = tokenHeader :+ "X-CM-WELL-Type"->"File").flatMap { res =>
        Json.parse(res.payload) should be(jsonSuccess)
        scheduleFuture(indexingDuration){
          Http.get(path).map { res =>
            val writtenData = res.payload
            withClue(s"Written data (${writtenData.length}) was not same content as ingested data ($length)\nFirst 16 chars are: ${writtenData.take(16)}") {
              writtenData shouldBe data
            }
          }
        }
      }
    }
  }
}
