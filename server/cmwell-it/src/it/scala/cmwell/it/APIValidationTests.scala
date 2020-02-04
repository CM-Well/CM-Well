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

import cmwell.util.concurrent.SimpleScheduler.scheduleFuture
import cmwell.util.concurrent.travector
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.jena.graph.NodeFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.scalatest._
import org.yaml.snakeyaml.Yaml
import play.api.libs.json.Json

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.Source
import scala.language.{implicitConversions, reflectiveCalls}
import scala.util.Try
import scala.xml._

class APIValidationTests extends AsyncFunSpec with Matchers with Inspectors with OptionValues with Helpers with LazyLogging {
  // scalastyle:off
  val wrongTokenHeader: List[(String, String)] = ("X-CM-WELL-TOKEN" -> "qXoFmO/mK9tKbSreT3xVRr3tKB1KEZr+9xTZ6We+oa0hvonnGjwF9QS7BTIQ57cr0k42cv9Qr9oMpoqHu0LdWLR8vdEPln3q/p66jDt8uZCITA/Epfopcj8dI0xDqL8X504ZRpuvzs3fAdiHmi2dKn5Xz13I5BuRXCYnq/VvDL1r2aKn/PJBEvb9315YKNSzeAWX7JM0ARyanBO2eVtfquTxGV9PUK79IxyzEG/9TXZlA/LNLd22yPhHxzgSgXCgQoWV/nEHjgNNnz70FqDVdPk2FcU0wTlIsjpveL8Prdqp3b/1ENNmA5s74TMo4XNf9/fjrVkcwmZD9WM4oZKLSg==") :: Nil
  // scalastyle:on
  val wsConfig = ConfigFactory.load("ws/application.conf")

  val metaSys = cmw / "meta" / "sys"

  describe("CM-Well REST API") {

    // HTTP calls

    val f0 = {
      val data =
        """
          |@prefix sys:   <http://cm-well.com/meta/sys#> .
          |<http://irrelevant.path.com> <cmwell://meta/sys#markDelete> [
          |    sys:path    "/" ;
          |    sys:type    "ObjectInfoton" ;
          |]
        """.
          stripMargin
      Http.post(_in, data, queryParams = List("format" -> "n3", "debug-log" -> ""), headers = tokenHeader)
    }
    val f1 = {
      val onlyPathIsBenign =
        """<http://test.permid.org/testsortby> <http://cm-well-uk-lab.int.thomsonreuters.com/meta/sys#path> "/test.permid.org/testsortby" ."""
      Http.post(_in, onlyPathIsBenign, textPlain, queryParams = List("format" -> "ntriples", "debug-log" -> ""), headers = tokenHeader)
    }
    val f2 = {
      val path = cmt / "PutInfotonWithoutToken"
      Http.post(path, "Text content for File Infoton", textPlain, headers = List("X-CM-WELL-TYPE" -> "FILE"))
    }
    val f3 = {
      val path = cmt / "PutInfotonWithWrongToken"
      Http.post(path, "Text content for File Infoton", textPlain, headers = List("X-CM-WELL-TYPE" -> "FILE") ++ wrongTokenHeader)
    }
    val (f4,f5) = {
      val path = cmt / "FileWithWrongHeaderName"
      val f = Http.post(path, "Text content for File Infoton", textPlain, headers = ("X-CM-WELL-ZYPE" -> "FILE") :: tokenHeader)
      f -> executeAfterCompletion(f)(scheduleFuture(indexingDuration)(spinCheck(100.millis, true)(Http.get(path))(_.status == 404)))
    }
    val (f6,f7) = {
      val path = cmt / "FileWithoutContent"
      val f = Http.post(path, "", textPlain, headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
      f -> executeAfterCompletion(f)(scheduleFuture(indexingDuration)(spinCheck(100.millis, true)(Http.get(path))(_.status == 404)))
    }
    val (f8,f9) = {
      import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
      val path = cmt / "SmallFile"
      val f = Http.post(path, "this is a small file", textPlain, headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
      f -> executeAfterCompletion(f)(spinCheck(100.millis,true)(Http.get(path))(_.payload == "this is a small file"))
    }
    val (f10,f11) = {
      val path = cmt / "LargeFile"
      val data: Array[Byte] = Array.fill(8*1000*1024)(1.toByte)
      val checksum = cmwell.util.string.Hash.sha1(data)
      val f = Http.post(path, data, Some("application/octet-stream"), headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
      f -> executeAfterCompletion(f)(spinCheck(100.millis,true)(Http.get(path)) { res =>
        cmwell.util.string.Hash.sha1(res.payload) == checksum
      }.map(_ -> checksum))
    }
    val (f12,f13) = {
      val path = cmt / "ObjectInfotonWithoutContent"
      val f = Http.post(path, "", textPlain, headers = ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
      f -> executeAfterCompletion(f)(scheduleFuture(indexingDuration)(spinCheck(100.millis)(Http.get(path))(_.status == 404)))
    }
    val (f14,f15) = {
      val path = cmt / "ObjectInfotonWithInvalidJson"
      val f = Http.post(path, "invalid Json", Some("application/json;charset=UTF-8"), headers = ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
      f -> executeAfterCompletion(f)(scheduleFuture(indexingDuration)(spinCheck(100.millis)(Http.get(path))(_.status == 404)))
    }
    val (f16,f17) = {
      val path = cmt / "LinkWithRelativeTargetPath"
      val f = Http.post(path, "sub/targetinfo", textPlain, headers = ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)
      f -> executeAfterCompletion(f)(scheduleFuture(indexingDuration)(spinCheck(100.millis)(Http.get(path))(_.status == 404)))
    }
    val (f18,f19) = {
      val path = cmt / "LinkTargetingNonExitsPath"
      val f = Http.post(path, "/link/to/nowhere", textPlain, headers = ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)
      f -> executeAfterCompletion(f)(scheduleFuture(indexingDuration)(spinCheck(100.millis, true)(Http.get(path))(_.status == 404)))
    }
    val (f20,f21) = {
      val req1 = Http.post(cmt / "file.txt", "Text content for File Infoton", textPlain, headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
      val req2 = Http.post(cmt / "fwc-link-1", "/file.txt", textPlain, headers = ("X-CM-WELL-LINK-TYPE" -> "2") :: ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)
      val req3 = Http.post(cmt / "fwc-link-2", "/fwc-link-1", textPlain, headers = ("X-CM-WELL-LINK-TYPE" -> "2") :: ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)

      def waitForIngest() = {
        val file1 = spinCheck(100.millis, true)(Http.get(cmt / "file.txt"))(_.status)
        val fw1 = spinCheck(100.millis, true)(Http.get(cmt / "fwc-link-1"))(_.status)
        val fw2 = spinCheck(100.millis, true)(Http.get(cmt / "fwc-link-2"))(_.status)

        file1.flatMap(_ => fw1.flatMap(_ => fw2))
      }

      val f = Future.sequence(Seq(req1,req2,req3))
      f -> executeAfterCompletion(f.flatMap(_ => waitForIngest)){
        Http.post(cmt / "fwc-link-1", "/fwc-link-2", textPlain, headers = ("X-CM-WELL-LINK-TYPE" -> "2") :: ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)
      }
    }
    val (f22,f23) = {
      val f = Http.post(
        uri = cmt / "fw-link-0",
        body =  "Text content for File Infoton",
        contentType = textPlain,
        headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
      val fs = travector(0 to 30){ i =>
        Http.post(
          cmt / s"fw-link-${i + 1}",
          s"/cmt/cm/test/fw-link-$i",
          textPlain,
          headers = ("X-CM-WELL-LINK-TYPE" -> "2") :: ("X-CM-WELL-TYPE" -> "LN") :: tokenHeader)
      }
      fs -> executeAfterCompletion(fs)(spinCheck(100.millis,true)(Http.get(cmt / "fw-link-31"))(_.status == 400))
    }
    val (f24,f25,f26,f27) = {
      val jsonObj = Json.obj("name" -> "TestObject", "title" -> "title1")
      val fileStr = Source.fromURL(this.getClass.getResource("/article.txt")).getLines.reduceLeft(_ + _)
      val atom = cmw / "atom"

      val ff = {
        val path1 = atom / "InfoObj"
        val path2 = atom / "InfoFile"
        val req1 = Http.post(path1, Json.stringify(jsonObj), textPlain, headers = ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
        val req2 = Http.post(path2, fileStr, Some("application/json;charset=utf-8"), headers = ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader)
        Future.sequence(Seq(req1, req2))
      }

      val g = executeAfterCompletion(ff)(spinCheck(100.millis)(Http.get(atom, List("format" -> "atom")))(_.status))

      val h = travector(0 until 10){ i =>
        val path = cmt / "atom1" / s"InfoObj$i"
        Http.post(path, jsonObj.toString(), headers = ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
      }

      // scalastyle:off
      val expected = XML.loadString(s"""<feed xmlns:os="http://a9.com/-/spec/opensearch/1.1/" xmlns="http://www.w3.org/2005/Atom"><title>Infotons search results</title><id>${cmw.url}/cmt/cm/test/atom1?format=atom&amp;length=4</id><os:totalResults>10</os:totalResults><link rel="self" href="${cmw.url}/cmt/cm/test/atom1?format=atom&amp;length=4&amp;offset=0" type="application/atom+xml"/><link rel="first" href="${cmw.url}/cmt/cm/test/atom1?format=atom&amp;length=4&amp;offset=0" type="application/atom+xml"/><link rel="next" href="${cmw.url}/cmt/cm/test/atom1?format=atom&amp;length=4&amp;offset=4" type="application/atom+xml"/><link rel="last" href="${cmw.url}/cmt/cm/test/atom1?format=atom&amp;length=4&amp;offset=8" type="application/atom+xml"/><author><name>CM-Well</name><uri>${cmw.url}/cmt/sys/home.html</uri></author><entry><title>/cmt/cm/test/atom1/InfoObj9</title><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj9" rel="alternate" type="text/html"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj9?format=json" rel="alternate" type="application/json"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj9?format=n3" rel="alternate" type="text/rdf+n3"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj9?format=ntriple" rel="alternate" type="text/plain"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj9?format=rdfxml" rel="alternate" type="application/rdf+xml"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj9?format=yaml" rel="alternate" type="text/yaml"/><id>${cmw.url}/cmt/cm/test/atom1/InfoObj9</id></entry><entry><title>/cmt/cm/test/atom1/InfoObj8</title><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj8" rel="alternate" type="text/html"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj8?format=json" rel="alternate" type="application/json"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj8?format=n3" rel="alternate" type="text/rdf+n3"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj8?format=ntriple" rel="alternate" type="text/plain"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj8?format=rdfxml" rel="alternate" type="application/rdf+xml"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj8?format=yaml" rel="alternate" type="text/yaml"/><id>${cmw.url}/cmt/cm/test/atom1/InfoObj8</id></entry><entry><title>/cmt/cm/test/atom1/InfoObj7</title><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj7" rel="alternate" type="text/html"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj7?format=json" rel="alternate" type="application/json"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj7?format=n3" rel="alternate" type="text/rdf+n3"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj7?format=ntriple" rel="alternate" type="text/plain"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj7?format=rdfxml" rel="alternate" type="application/rdf+xml"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj7?format=yaml" rel="alternate" type="text/yaml"/><id>${cmw.url}/cmt/cm/test/atom1/InfoObj7</id></entry><entry><title>/cmt/cm/test/atom1/InfoObj6</title><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj6" rel="alternate" type="text/html"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj6?format=json" rel="alternate" type="application/json"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj6?format=n3" rel="alternate" type="text/rdf+n3"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj6?format=ntriple" rel="alternate" type="text/plain"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj6?format=rdfxml" rel="alternate" type="application/rdf+xml"/><link href="${cmw.url}/cmt/cm/test/atom1/InfoObj6?format=yaml" rel="alternate" type="text/yaml"/><id>${cmw.url}/cmt/cm/test/atom1/InfoObj6</id></entry></feed>""")
      // scalastyle:on
      val i = {
        import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
        executeAfterCompletion(h)(spinCheck(100.millis, true)(Http.get(cmt / "atom1", List("format" -> "atom", "length" -> "10", "op" -> "search"))){
          r =>
            val xml = XML.loadString(r.payload)
            val filtered = filterAllDates(xml)
            filtered == expected
        })
      }.map { r =>
        val xml = XML.loadString(r.payload)
        val filtered = filterAllDates(xml)
        filtered shouldEqual expected
      }
      (ff,g,h,i)
    }
    val (f28,f29) = {
      val triple = s"""<http://example.org/BigValue> <cmwell:meta/nn#bigValue> "${"BigValue" * math.pow(2,11).toInt + "$"}" ."""
      val f = Http.post(_in, triple, textPlain, List("format" -> "ntriples"), tokenHeader)
      f -> executeAfterCompletion(f)(scheduleFuture(indexingDuration)(spinCheck(100.millis)(Http.get(cmw / "example.org" / "BigValue"))(_.status == 404)))
    }
    val f30 = {
      val triple = s"""<http://example.org/some-path> <cmwell:meta/nn#messageOfTheDay> "Hello, World!" ."""
      Http.post(_in, triple, textPlain, List("format" -> "ntriples", "dry-run" -> ""), tokenHeader)
    }
    val (f31,f32) = {
      val triple = s"""<http://example.org/some$$illegal-path> <cmwell:meta/nn#messageOfTheDay> "Hello, World!" ."""
      val f = Http.post(_in, triple, textPlain, List("format" -> "ntriples"), tokenHeader)
      f -> executeAfterCompletion(f)(scheduleFuture(indexingDuration)(spinCheck(100.millis, true)(Http.get(cmw / "example.org" / "some$illegal-path"))(
        _.status == 404)))
    }
    val (f33,f34) = {
      import cmwell.util.http.SimpleResponse.Implicits.InputStreamHandler
      val path = cmt / "YetAnotherObjectInfoton"
      val data = """{"foo":["bar1","bar2"],"consumer":"cmwell:a/fake/internal/path"}"""
      val f = Http.post(path, data, Some("application/json;charset=utf-8"), headers = ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
      val g = executeAfterCompletion(f)(spinCheck(100.millis,true)(Http.get(path, List("format" -> "json")))(_.status))
      val h = executeAfterCompletion(f)(spinCheck(100.millis,true)(Http.get(path, List("format" -> "yaml")))(_.status))
      val i = executeAfterCompletion(f)(spinCheck(100.millis,true)(Http.get(path, List("format" -> "n3")))(_.status))
      f -> g.zip(h.zip(i))
    }

    // Assertions

    it("should fail with 400 on markDelete of sys fields")(f0.map { res =>
      withClue(res) {
        res.status should be(400)
      }
    })

    it("should fail with 422 on well formed but benign _in updates")(f1.map { res =>
      withClue(res) {
        res.status should be(422)
      }
    })

    it("should fail to put infoton without token with 403 error code")(f2.map(_.status should be(403)))

    it("should fail to put infoton to a path different than token allows with 403 error code")(f3.map(_.status should be(403)))

    describe("put infoton with invalid type header") {
      it("should fail with 400 error code when inserting it")(f4.map(_.status should be(400)))
      it("should return 404 not found when trying to get it")(f5.map(_.status should be(404)))
    }

    describe("put file infoton without contents") {
      it("should fail with 400 error code when inserting it")(f6.map(_.status should be(400)))
      it("should return 404 not found when trying to get it")(f7.map(_.status should be(404)))
    }

    describe("put small file infoton") {
      it("should succeed with 200 OK when inserting it")(f8.map(_.status should be(200)))
      it("should get the data when trying to get it")(f9.map(_.payload should be("this is a small file")))
    }

    describe("put large file infoton") {
      it("should succeed with 200 OK when inserting it")(f10.map(_.status should be(200)))
      it("should get the data when trying to get it")(f11.map {
        case (res, checksum) => withClue(res) {
          cmwell.util.string.Hash.sha1(res.payload) should be(checksum)
        }
      })
    }

    describe("put object infoton without content") {
      it("should fail with 400 error code when inserting it")(f12.map(_.status should be(400)))
      it("should return 404 not found when trying to get it")(f13.map(_.status should be(404)))
    }

    describe("put object infoton with invalid content") {
      it("should fail with 400 error code when inserting it")(f14.map(_.status should be(400)))
      it("should return 404 not found when trying to get it")(f15.map(_.status should be(404)))
    }

    describe("put link infoton") {
      // TODO: un-ignore: test is currently failing.
      describe("with relative target path") {
        ignore("should fail with 400 error code when inserting it")(f16.map(_.status should be(400)))
        ignore("should return 404 not found when trying to get it")(f17.map(_.status should be(404)))
      }

      // TODO: un-ignore: test is currently failing.
      describe("targeting non existing path") {
        ignore("should fail with 400 error code when inserting it")(f18.map(_.status should be(400)))
        ignore("should return 404 not found when trying to get it")(f19.map(_.status should be(404)))
      }

      describe("that is cyclic forward") {
        it("should succeed preparing 3 infotons for the cycle")(f20.map( s => forAll(s)(_.status should be(200))))
        // TODO: un-ignore: test is currently failing.
        ignore("should fail with 400 error code when inserting it")(f21.map(_.status should be(400)))
      }

      describe("with too deep forward link") {
        it("should succeed preparing the link chain")(f22.map { responses =>
          forAll(responses) { res =>
            res.status should be(200)
          }
        })
        it("should fail with 400 error code when getting it")(f23.map(_.status should be(400)))
      }
    }

    describe("Atom feed") {


      describe("must validate against atom scheme") {
        it("with succeessful preparation")(f24.map(x => forAll(x)(_.status should be(200))))

        ignore("with retrieving latest updates")(f25.map(_.status should be(200)))
        //.map{_ beValidWith (this.getClass.getResource("/atomRev.rnc"))} TODO: what is "beValidWith" ?
      }

      describe("must match expected format and info") { //todo something wrong with formatting results?

        ignore("with posting 10 infotons as preparation")(f26.map(s => forAll(s)(_.status should be(200))))

        ignore("with comparing expected to given results")(f27)
      }
    }

    describe("_in service") {
      describe("should block documents with value > 16K"){
        it("should fail with 400 error code when inserting it")(f28.map(_.status should be(400)))
        it("should return 404 not found when trying to get it")(f29.map(_.status should be(404)))
      }

      it("should return successfully from a dry-run")(f30.map { res =>
        res.status should be(200)
        jsonSuccessPruner(Json.parse(res.payload)) should be(Json.parse("""{"success":true,"dry-run":true}"""))
      })

      describe("should block documents with illegal subject") {
        it("should fail with 400 error code when inserting it")(f31.map(_.status should be(400)))
        it("should return 404 not found when trying to get it")(f32.map(_.status should be(404)))
      }
    }

    describe("uuid consistency") {

      it("should be able to post a new Infoton")(f33.map { res =>
        withClue(res)(res.status should be(200))
      })

      it("should be same uuid for json, yaml, and n3")(f34.map {
        case (jr,(yr,nr)) =>
          val uuidFromJson = Try(Json.parse(jr.payload) \ "system" \ "uuid").toOption.flatMap(_.asOpt[String])
          val parsedYaml: java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, String]] = new Yaml().load(yr.payload)
          val uuidFromYaml = parsedYaml.get("system").get("uuid")
          val model: Model = ModelFactory.createDefaultModel
          model.read(nr.payload, null, "N3")
          val it = model.getGraph.find(NodeFactory.createURI(cmt / "YetAnotherObjectInfoton"), NodeFactory.createURI(metaSys ⋕ "uuid"), null)
          val uuidFromN3 = {
            if (it.hasNext) Some(it.next().getObject.getLiteral.getLexicalForm)
            else None
          }
          withClue(jr)(uuidFromJson should not be empty)
          withClue(yr)(uuidFromJson.value should be(uuidFromYaml))
          withClue(nr)(uuidFromJson.value should be(uuidFromN3.value))
      })
    }

    //FIXME: Implement limitation on max concurrent iterators
    ignore("iterator creation limitation") {
      import scala.concurrent.duration._

      val defaultMax = 613000000 // TODO Create config such as "webservice.max.search.contexts"
      val ttl = 60
      val queryParams = List("op" -> "create-iterator", "session-ttl" -> ttl.toString)

      val statusCodesFut = Future.traverse(1 to defaultMax)(_ => Http.get(cmw, queryParams).map(_.status))

      (ttl - 3).seconds.fromNow.block
      Http.get(cmw, queryParams).flatMap { anotherResp =>
        statusCodesFut.map { statusCodes =>
          all(statusCodes) should be(200)
          anotherResp.status should be(503)
        }
      }
    }
  }

  // Helpers

  val reg = """(to=|from=).*?\&""".r

  implicit class AddGoodCopyToAttribute(attr: Attribute)  {
    def attcopy(key: String = attr.key, value: Any = attr.value): Attribute =
      Attribute(attr.pre, key, Text(value.toString), attr.next)
  }

  implicit def iterableToMetaData(items: Iterable[MetaData]): MetaData = items match {
    case Nil => Null
    case head :: tail => head.copy(next = iterableToMetaData(tail))
  }

  def filterAllDates(e: Elem): Elem = e.copy(child = e.child.filterNot(n => (n.label == "updated") || (n.label == "subtitle")).map {
    case e: Elem => filterAllDates(e)
    case x => x
  }, attributes = for (attr <- e.attributes) yield attr match {
    case attr@Attribute("href", _, _) => attr.attcopy(value = reg.replaceAllIn(attr.value.text, ""))
    case other => other
  })
}
