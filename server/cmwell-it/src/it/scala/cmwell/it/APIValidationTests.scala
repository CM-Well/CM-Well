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

import com.typesafe.config.ConfigFactory
import org.scalatest._
import cmwell.util.concurrent.delayedTask
import cmwell.util.http.SimpleResponse
import com.typesafe.scalalogging.LazyLogging
import org.apache.jena.graph.NodeFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.yaml.snakeyaml.Yaml
import play.api.libs.json.Json

import scala.xml._
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.language.{implicitConversions, reflectiveCalls}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.{Failure, Success}

class APIValidationTests extends FunSpec with Matchers with Inspectors with Helpers with LazyLogging {

  val wrongTokenHeader: List[(String, String)] = ("X-CM-WELL-TOKEN" -> "qXoFmO/mK9tKbSreT3xVRr3tKB1KEZr+9xTZ6We+oa0hvonnGjwF9QS7BTIQ57cr0k42cv9Qr9oMpoqHu0LdWLR8vdEPln3q/p66jDt8uZCITA/Epfopcj8dI0xDqL8X504ZRpuvzs3fAdiHmi2dKn5Xz13I5BuRXCYnq/VvDL1r2aKn/PJBEvb9315YKNSzeAWX7JM0ARyanBO2eVtfquTxGV9PUK79IxyzEG/9TXZlA/LNLd22yPhHxzgSgXCgQoWV/nEHjgNNnz70FqDVdPk2FcU0wTlIsjpveL8Prdqp3b/1ENNmA5s74TMo4XNf9/fjrVkcwmZD9WM4oZKLSg==") :: Nil

  val wsConfig = ConfigFactory.load("ws/application.conf")
  val root = "http://localhost:9000"
  val meta = s"$root/meta"

  def bodyAsString(resp: SimpleResponse[Array[Byte]]): String = new String(resp.body._2, "UTF-8")

  describe("CM-Well REST API") {
    it("should fail to put infoton without token with 403 error code") {
      val path = cmt / "PutInfotonWithoutToken"
      val req = Http.post(path, "Text content for File Infoton", textPlain, headers = List("X-CM-WELL-TYPE" -> "FILE"))
      Await.result(req, requestTimeout).status should be(403)
    }

    it("should fail to put infoton to a path different than token allows with 403 error code") {
      val path = cmt / "PutInfotonWithWrongToken"
      val req = Http.post(path, "Text content for File Infoton", textPlain, headers = List("X-CM-WELL-TYPE" -> "FILE") ++ wrongTokenHeader)
      Await.result(req, requestTimeout).status should be(403)
    }

    describe("put infoton with invalid type header") {
      val path = cmt / "FileWithWrongHeaderName"
      it("should fail with 400 error code when inserting it") {
        val req = Http.post(path, "Text content for File Infoton", textPlain, headers = ("X-CM-WELL-ZYPE" -> "FILE") :: tokenHeader)
        Await.result(req, requestTimeout).status should be(400)
      }

      it("should return 404 not found when trying to get it"){
        Await.result(Http.get(path), requestTimeout).status should be(404)
      }
    }

    describe("put file infoton without contents") {
      val path = cmt / "FileWithoutContent"
      it("should fail with 400 error code when inserting it") {
        val req = Http.post(path, "", textPlain, headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
        Await.result(req, requestTimeout).status should be(400)
      }

      it("should return 404 not found when trying to get it") {
        Await.result(Http.get(path), requestTimeout).status should be(404)
      }
    }

    describe("put small file infoton") {
      val path = cmt / "SmallFile"
      it("should succeed with 200 OK when inserting it") {
        val req = Http.post(path, "this is a small file", textPlain, headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
        Await.result(req, requestTimeout).status should be(200)
        2.seconds.fromNow.block
      }

      it("should get the data when trying to get it") {
        new String(Await.result(Http.get(path), requestTimeout).body._2, "UTF-8") should be("this is a small file")
      }
    }

    describe("put large file infoton") {
      val path = cmt / "LargeFile"
      val data: Array[Byte] = Array.fill(8*1000*1024)(1.toByte)
      val checksum = cmwell.util.string.Hash.sha1(data)
      it("should succeed with 200 OK when inserting it") {
        val req = Http.post(path, data, Some("application/octet-stream"), headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
        Await.result(req, requestTimeout).status should be(200)
        2.seconds.fromNow.block
      }

      it("should get the data when trying to get it") {
        cmwell.util.string.Hash.sha1(Await.result(Http.get(path), requestTimeout).body._2) should be(checksum)
      }
    }

    describe("put object infoton without content") {
      val path = cmt / "ObjectInfotonWithoutContent"
      it("should fail with 400 error code when inserting it") {
        val req = Http.post(path, "", textPlain, headers = ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
        Await.result(req, requestTimeout).status should be(400)
      }

      it("should return 404 not found when trying to get it") {
        Await.result(Http.get(path), requestTimeout).status should be(404)
      }
    }

    describe("put object infoton with invalid content") {
      val path = cmt / "ObjectInfotonWithInvalidJson"
      it("should fail with 400 error code when inserting it") {
        val req = Http.post(path, "invalid Json", Some("application/json;charset=UTF-8"), headers = ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
        Await.result(req, requestTimeout).status should be(400)
      }
      it("should return 404 not found when trying to get it") {
        Await.result(Http.get(path), requestTimeout).status should be(404)
      }
    }

    describe("put link infoton") {
//      TODO: un-ignore: test is currently failing.
      ignore("with relative target path") {
        val path = cmt / "LinkWithRelativeTargetPath"
        it("should fail with 400 error code when inserting it") {
          val req = Http.post(path, "sub/targetinfo", textPlain, headers = ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)
          Await.result(req, requestTimeout).status should be(400)
        }
        it("should return 404 not found when trying to get it") {
          Await.result(Http.get(path), requestTimeout).status should be(404)
        }
      }

//      TODO: un-ignore: test is currently failing.
      ignore("targeting non existing path") {
        val path = cmt / "LinkTargetingNonExitsPath"
        it("should fail with 400 error code when inserting it") {
          val req = Http.post(path, "/link/to/nowhere", textPlain, headers = ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)
          Await.result(req, requestTimeout).status should be(400)
        }

        it("should return 404 not found when trying to get it") {
          Await.result(Http.get(path), requestTimeout).status should be(404)
        }
      }

      describe("that is cyclic forward") {
        it("should succeed preparing 3 infotons for the cycle") {
          val req1 = Http.post(cmt / "file.txt", "Text content for File Infoton", textPlain, headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
          val req2 = Http.post(cmt / "fwc-link-1", "/file.txt", textPlain, headers = ("X-CM-WELL-LINK-TYPE" -> "2") :: ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)
          val req3 = Http.post(cmt / "fwc-link-2", "/fwc-link-1", textPlain, headers = ("X-CM-WELL-LINK-TYPE" -> "2") :: ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)
          Await.result(Future.sequence(Seq(req1, req2, req3)).map(_.forall(_.status == 200)), requestTimeout) should be(true)
        }

//        TODO: un-ignore: test is currently failing.
        ignore("should fail with 400 error code when inserting it") {
          val req = Http.post(cmt / "fwc-link-1", "/fwc-link-2", textPlain, headers = ("X-CM-WELL-LINK-TYPE" -> "2") :: ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)
          Await.result(req, requestTimeout).status should be(400)
        }
      }

      describe("with too deep forward link") {

        val p = Promise[Unit]()

        it("should succeed preparing the link chain") {
          val f = Http.post(
            uri = cmt / "fw-link-0",
            body =  "Text content for File Infoton",
            contentType = textPlain,
            headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
          val fs = (0 to 30).map(i =>
            Http.post(cmt / s"fw-link-${i+1}", s"/cmt/cm/test/fw-link-$i", textPlain, headers = ("X-CM-WELL-LINK-TYPE" -> "2") :: ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)
          )
          Future.sequence(fs :+ f).map { responses =>
            forAll(responses) { res =>
              res.status should be(200)
            }
          }.andThen {
            case _ => delayedTask(indexingDuration){
              p.success(())
            }
          }
        }

        it("should fail with 400 error code when getting it") {
          p.future.flatMap(_ => Http.get(cmt / "fw-link-31").map { res =>
            res.status should be(400)
          })
        }
      }
    }

    describe("Atom feed") {

      val jsonObj = Json.obj("name" -> "TestObject", "title" -> "title1")
      val fileStr = Source.fromURL(this.getClass.getResource("/article.txt")).getLines.reduceLeft(_ + _)

      describe("must validate against atom scheme") {
        val atom = root + "/atom"
        it("with succeessful preparation"){
          val path1 = atom + "/InfoObj"
          val path2 = atom + "/InfoFile"
          val req1 = Http.post(path1, Json.stringify(jsonObj), textPlain, headers = ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
          val req2 = Http.post(path2, fileStr, Some("application/json;charset=utf-8"), headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
          val p = Promise[Boolean]()
          Future.sequence(Seq(req1.map(_.status), req2.map(_.status))).onComplete{
            case Success(seq) if seq.forall(_ == 200) => {
              indexingDuration.fromNow expiring {
                p.success(true)
              }
            }
            case _ => p.success(false)
          }
          Await.result(p.future, requestTimeout) should be(true)
        }

        ignore("with retrieving latest updates"){
          val f = Http.get(atom, List("format" -> "atom"))
            //.map{_ beValidWith (this.getClass.getResource("/atomRev.rnc"))} TODO: what is "beValidWith" ?
          Await.result(f,requestTimeout)
        }
      }

      ignore("must match expected format and info") { //todo something wrong with formatting results?
        val expected = XML.loadString("""<feed xmlns:os="http://a9.com/-/spec/opensearch/1.1/" xmlns="http://www.w3.org/2005/Atom"><title>Infotons search results</title><id>http://localhost:9000/cmt/cm/test/atom1?format=atom&amp;length=4</id><os:totalResults>10</os:totalResults><link rel="self" href="http://localhost:9000/cmt/cm/test/atom1?format=atom&amp;length=4&amp;offset=0" type="application/atom+xml"/><link rel="first" href="http://localhost:9000/cmt/cm/test/atom1?format=atom&amp;length=4&amp;offset=0" type="application/atom+xml"/><link rel="next" href="http://localhost:9000/cmt/cm/test/atom1?format=atom&amp;length=4&amp;offset=4" type="application/atom+xml"/><link rel="last" href="http://localhost:9000/cmt/cm/test/atom1?format=atom&amp;length=4&amp;offset=8" type="application/atom+xml"/><author><name>CM-Well</name><uri>http://localhost:9000/cmt/sys/home.html</uri></author><entry><title>/cmt/cm/test/atom1/InfoObj9</title><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj9" rel="alternate" type="text/html"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj9?format=json" rel="alternate" type="application/json"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj9?format=n3" rel="alternate" type="text/rdf+n3"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj9?format=ntriple" rel="alternate" type="text/plain"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj9?format=rdfxml" rel="alternate" type="application/rdf+xml"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj9?format=yaml" rel="alternate" type="text/yaml"/><id>http://localhost:9000/cmt/cm/test/atom1/InfoObj9</id></entry><entry><title>/cmt/cm/test/atom1/InfoObj8</title><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj8" rel="alternate" type="text/html"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj8?format=json" rel="alternate" type="application/json"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj8?format=n3" rel="alternate" type="text/rdf+n3"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj8?format=ntriple" rel="alternate" type="text/plain"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj8?format=rdfxml" rel="alternate" type="application/rdf+xml"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj8?format=yaml" rel="alternate" type="text/yaml"/><id>http://localhost:9000/cmt/cm/test/atom1/InfoObj8</id></entry><entry><title>/cmt/cm/test/atom1/InfoObj7</title><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj7" rel="alternate" type="text/html"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj7?format=json" rel="alternate" type="application/json"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj7?format=n3" rel="alternate" type="text/rdf+n3"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj7?format=ntriple" rel="alternate" type="text/plain"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj7?format=rdfxml" rel="alternate" type="application/rdf+xml"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj7?format=yaml" rel="alternate" type="text/yaml"/><id>http://localhost:9000/cmt/cm/test/atom1/InfoObj7</id></entry><entry><title>/cmt/cm/test/atom1/InfoObj6</title><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj6" rel="alternate" type="text/html"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj6?format=json" rel="alternate" type="application/json"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj6?format=n3" rel="alternate" type="text/rdf+n3"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj6?format=ntriple" rel="alternate" type="text/plain"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj6?format=rdfxml" rel="alternate" type="application/rdf+xml"/><link href="http://localhost:9000/cmt/cm/test/atom1/InfoObj6?format=yaml" rel="alternate" type="text/yaml"/><id>http://localhost:9000/cmt/cm/test/atom1/InfoObj6</id></entry></feed>""")

        implicit def addGoodCopyToAttribute(attr: Attribute) = new {
          def attcopy(key: String = attr.key, value: Any = attr.value): Attribute =
            Attribute(attr.pre, key, Text(value.toString), attr.next)
        }

        implicit def iterableToMetaData(items: Iterable[MetaData]): MetaData = {
          items match {
            case Nil => Null
            case head :: tail => head.copy(next = iterableToMetaData(tail))
          }
        }

        val reg = """(to=|from=).*?\&""".r

        def filterAllDates(e: Elem): Elem = e.copy(child = e.child.filterNot(n => (n.label == "updated") || (n.label == "subtitle")).map {
          case e: Elem => filterAllDates(e)
          case x => x
        }, attributes = for (attr <- e.attributes) yield attr match {
          case attr@Attribute("href", _, _) => attr.attcopy(value = reg.replaceAllIn(attr.value.text, ""))
          case other => other
        })

        it("with posting 10 infotons as preparation") {
          val f = Future.sequence((0 until 10).map { i =>
            val path = cmt / "atom1" / s"InfoObj$i"
            Http.post(path, jsonObj.toString(), headers = ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader).map(_.status)
          }).map(_.forall(_ == 200))

          Await.result(f, requestTimeout) should be(true)
          indexingDuration.fromNow.block
        }

        it("with comparing expected to given results"){
          val p = Promise[Elem]()
          indexingDuration.fromNow.block
          val req = Http.get(cmt / "atom1", List("format" -> "atom", "length" -> "10", "op" -> "search"))
          req.map(bodyAsString).onComplete{
            case Failure(e) => p.failure(e)
            case Success(s) => {
              val xml = XML.loadString(s)
              val filtered = filterAllDates(xml)
              p.success(filtered)
            }
          }
          val actual = Await.result(p.future, requestTimeout)
          actual shouldEqual expected
        }
      }
    }

    describe("_in service") {
      describe("should block documents with value > 16K"){
        it("should fail with 400 error code when inserting it") {
          val triple = s"""<http://example.org/BigValue> <cmwell:meta/nn#bigValue> "${"BigValue" * math.pow(2,11).toInt + "$"}" ."""
          val req = Http.post(_in, triple, textPlain, List("format" -> "ntriples"), tokenHeader)
          Await.result(req, requestTimeout).status should be(400)
        }

        it("should return 404 not found when trying to get it") {
          val path = cmw / "example.org" / "BigValue"
          indexingDuration.fromNow expiring {
            Await.result(Http.get(path), requestTimeout).status should be(404)
          }
        }
      }

      describe("should fblock documents with illegal subject") {
        it("should fail with 400 error code when inserting it") {
          val triple = s"""<http://example.org/some$$illegal-path> <cmwell:meta/nn#messageOfTheDay> "Hello, World!" ."""
          val req = Http.post(_in, triple, textPlain, List("format" -> "ntriples"), tokenHeader)
          Await.result(req, requestTimeout).status should be(400)
        }

        it("should return 404 not found when trying to get it") {
          val path = cmw / "example.org" / "some$illegal-path"
          indexingDuration.fromNow expiring {
            Await.result(Http.get(path), requestTimeout).status should be(404)
          }
        }
      }
    }

    describe("uuid consistency") {
      val path = cmt / "YetAnotherObjectInfoton"
      val data = """{"foo":["bar1","bar2"],"consumer":"cmwell:a/fake/internal/path"}"""
      def req(format: String) = Http.get(path, List("format" -> format))

      var uuidFromJson = ""

      it("should be able to post a new Infoton") {
        val postReq = Http.post(path, data, Some("application/json;charset=utf-8"), headers = ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
        Await.result(postReq.map{ res => indexingDuration.fromNow.block; res }, requestTimeout).status should be(200)
        uuidFromJson = (Json.parse(Await.result(req("json").map(bodyAsString), requestTimeout)) \ "system" \ "uuid").asOpt[String].getOrElse("")
      }

      it("should be same uuid for json and yaml") {
        val yamlResp = Await.result(req("yaml").map(bodyAsString), requestTimeout)
        val uuidFromYaml = new Yaml().load(yamlResp) ⚡ "system" ⚡ "uuid"
        uuidFromYaml should be(uuidFromJson)
      }

      it("should be same uuid for json and N3") {
        val n3Resp = Await.result(req("n3").map(bodyAsString), requestTimeout)
        val model: Model = ModelFactory.createDefaultModel
        model.read(cmwell.util.string.stringToInputStream(n3Resp), null, "N3")
        val it = model.getGraph.find(NodeFactory.createURI(path), NodeFactory.createURI(s"$meta/sys#uuid"), null)
        val uuidFromN3 = it.next().getObject.getLiteral.getLexicalForm
        uuidFromN3 should be(uuidFromJson)
      }
    }

    ignore("iterator creation") {  //todo this should pass, but fails on Jenkins
      import scala.concurrent.duration._

      val defaultMax = wsConfig.getInt("webservice.max.search.contexts")
      val ttl = 60

      val req = Http.get(root, List("op" -> "create-iterator", "session-ttl" -> ttl.toString))
      val returnCodes = Future.traverse(1 to defaultMax)(_ => req.map(_.status))

      (ttl - 3).seconds.fromNow.block
      val returnCode = req.map(_.status)

      ignore(s"should allow $defaultMax iterators concurently") {
        val fi = returnCodes.map(_.count(_ == 200))
        Await.result(fi, requestTimeout) should be(defaultMax)
      }

      ignore(s"should reject the ${defaultMax+1}th request") {
        Await.result(returnCode, requestTimeout) should be(400)
      }
    }
  }
}
