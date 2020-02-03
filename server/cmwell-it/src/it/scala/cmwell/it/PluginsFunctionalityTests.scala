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

import java.util.concurrent.Executors

import cmwell.ctrl.utils.ProcUtil
import cmwell.util.http.SimpleResponse.Implicits
import cmwell.util.http.{SimpleResponse, StringPath}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FunSpec, Inspectors, Matchers}
import play.api.libs.json.{JsArray, JsValue, Json}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source

/**
  * Created by yaakov on 11/24/15.
  */
class PluginsFunctionalityTests extends FunSpec with Matchers with Helpers with BeforeAndAfterAll with Inspectors with LazyLogging {

  implicit val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(10))
  val _sp = cmw / "_sp"
  val sparqlIdentity = "CONSTRUCT { ?s ?p ?o . } WHERE { ?s ?p ?o }"

  override def beforeAll() = {
    val fileNTriple = Source.fromURL(this.getClass.getResource("/relationships3.nt")).mkString
    val resFut = Http.post(_in, fileNTriple, textPlain, List("format" -> "ntriples"),tokenHeader)
    Await.result(resFut, requestTimeout)

    waitForData(cmw / "example.org" / "Individuals2", 12)
  }

  def waitAndExtractBody(req: Future[SimpleResponse[Array[Byte]]]) = new String(Await.result(req, requestTimeout).payload, "UTF-8")

  // todo generalize and add to Helpers
  def waitForData(path: StringPath, expectedDataLength: Int, maxRetries: Int = 32, sleepTime: FiniteDuration = 1.second) = {

    var (length, retry) = (0, 0)
    do {
      val res = Await.result(Http.get(path, Seq("op"->"search", "length"->"1", "format"->"json"))(Implicits.UTF8StringHandler), requestTimeout).payload
      length = (Json.parse(res) \ "results" \ "total").asOpt[Int].getOrElse(0)
      retry+=1
      sleepTime.fromNow.block
    } while(length<expectedDataLength && retry<maxRetries)

    logger.info(s"was waiting for data of ${path.url} for ${sleepTime*retry} and found total of $length Infotons out of $expectedDataLength expected")

    if(length == 0)
      throw new RuntimeException(s"Data was not written after ${sleepTime*retry}")

  }

  describe("_sp") {

    def getInfo() = {
      val jpsData = ProcUtil.executeCommand("jps -l").toOption.getOrElse("<jps failed>")
      val contentData = Await.result(
        Http.get(
          s"${cmw.url}/example.org",
          Seq("op"->"search", "recursive"->"", "with-data"->"", "length"->"1000", "format"->"ntriples")
        )(Implicits.UTF8StringHandler), requestTimeout).payload
      s"jps data:\n$jpsData\n\nContent:\n$contentData"
    }

    def makeReqBody(paths: Seq[String], queryLang: String, query: String, imports: Seq[String] = Seq()) = {
      val importPart = imports match { case xs if xs.isEmpty => "" case xs => xs.mkString("IMPORT\n","\n","\n\n") }
      s"PATHS\n${paths.mkString("\n")}\n\n$importPart$queryLang\n$query"
    }
    def normalizeGremlinOutput(output: String) = output.replaceAll("\\[|\\]","")
    val paths = Seq("?op=search&length=1000&with-data",
      "/RonaldKhun",
      "/JohnSmith?xg=3").map("/example.org/Individuals2"+_)
    describe("should run SPARQL queries") {
      def testErr(something: String) = s"should run a SPARQL query with $something and get an error"
      def assertJsonFailure(req: Future[SimpleResponse[Array[Byte]]]) = {
        (Await.result(req.map(r => Json.parse(r.payload)), requestTimeout) \ "success").asOpt[Boolean] should be(Some(false))
      }

      it("should run a SPARQL query and render ascii format") {
        val sparql = "SELECT DISTINCT ?name ?active WHERE { ?name <http://www.tr-lbd.com/bold#active> ?active . } ORDER BY DESC(?active) ?name"
        val expectedResults = """-------------------------------------------------------------
                                || name                                            | active  |
                                |=============================================================
                                || <http://example.org/Individuals2/BruceWayne>    | "true"  |
                                || <http://example.org/Individuals2/DonaldDuck>    | "true"  |
                                || <http://example.org/Individuals2/HarryMiller>   | "true"  |
                                || <http://example.org/Individuals2/JohnSmith>     | "true"  |
                                || <http://example.org/Individuals2/MartinOdersky> | "true"  |
                                || <http://example.org/Individuals2/NatalieMiller> | "true"  |
                                || <http://example.org/Individuals2/PeterParker>   | "true"  |
                                || <http://example.org/Individuals2/RonaldKhun>    | "true"  |
                                || <http://example.org/Individuals2/SaraSmith>     | "true"  |
                                || <http://example.org/Individuals2/DaisyDuck>     | "false" |
                                || <http://example.org/Individuals2/RebbecaSmith>  | "false" |
                                |-------------------------------------------------------------""".stripMargin
        val req = Http.post(_sp, makeReqBody(paths, "SPARQL", sparql), textPlain, Seq("format"->"ascii"))
        val body = waitAndExtractBody(req).trim
        withClue(getInfo()) {
          body should be(expectedResults)
        }
      }
      it("should run a SPARQL query and render JSON format") {
        val sparql = "SELECT ?name WHERE { ?name <http://purl.org/vocab2/relationship/siblingOf> <http://example.org/Individuals2/SaraSmith> . }"
        val expectedResults = Json.obj(
          "head" -> Json.obj("vars" -> Json.arr("name")),
          "results" -> Json.obj(
            "bindings" -> Json.arr(
              Json.obj(
                "name" -> Json.obj(
                  "type" -> "uri",
                  "value" -> "http://example.org/Individuals2/RebbecaSmith")))))
        val req = Http.post(_sp, makeReqBody(paths, "SPARQL", sparql), textPlain, Seq("format"->"json"))
        val body = Json.parse(waitAndExtractBody(req))
        body should be(expectedResults)
      }
      describe("_sp Imports") {
        it("should upload a \"stored\" query") {
          val importedSpPath = cmw / "example.org" / "queries" / "foo.sparql"
          val importedSpBody =
            """CONSTRUCT { ?s <http://purl.org/vocab/relationship/siblingOf> "his sister" . }
              |WHERE { ?s <http://www.tr-lbd.com/bold#active> "true" }""".stripMargin
          Json.parse(
            waitAndExtractBody(
              Http.post(
                importedSpPath,
                importedSpBody,
                textPlain,
                headers = "x-cm-well-type" -> "file" :: tokenHeader))
          ) should be(jsonSuccess)
        }
        it("should run a SPARQL query with imported queries") {
          val sparql =
            """PREFIX rel: <http://purl.org/vocab/relationship/>
              |SELECT ?name WHERE { ?name rel:siblingOf "his sister" . } ORDER BY ?name""".stripMargin
          val expectedResults =
            """---------------------------------------------------
              || name                                            |
              |===================================================
              || <http://example.org/Individuals2/BruceWayne>    |
              || <http://example.org/Individuals2/DonaldDuck>    |
              || <http://example.org/Individuals2/HarryMiller>   |
              || <http://example.org/Individuals2/JohnSmith>     |
              || <http://example.org/Individuals2/MartinOdersky> |
              || <http://example.org/Individuals2/NatalieMiller> |
              || <http://example.org/Individuals2/PeterParker>   |
              || <http://example.org/Individuals2/RonaldKhun>    |
              || <http://example.org/Individuals2/SaraSmith>     |
              |---------------------------------------------------""".stripMargin

          validateResult(sparql, expectedResults, "/example.org/queries/foo.sparql")
        }
        it("should upload a \"stored\" JAR") {
          val jarPayload = Source.fromURL(this.getClass.getResource("/Add42.jar"), "ISO-8859-1").map(_.toByte).toArray
          Json.parse(
            waitAndExtractBody(
              Http.post(
                cmw / "meta" / "lib" / "Add42.jar",
                jarPayload,
                Some("application/java-archive"),
                headers = "x-cm-well-type" -> "file" :: tokenHeader))
          ) should be(jsonSuccess)
        }
        it("should run a SPARQL query with imported JARs") {
          val sparql =
            """PREFIX cmwellspi: <jar:cmwell.spi.>
              |SELECT DISTINCT ?name ?active ?res WHERE {
              |?name <http://www.tr-lbd.com/bold#active> ?active .
              |BIND( cmwellspi:Add42(?active) as ?res).
              |} ORDER BY DESC(?name)""".stripMargin
          val expectedResults =
            """--------------------------------------------------------------------------
              || name                                            | active  | res        |
              |==========================================================================
              || <http://example.org/Individuals2/SaraSmith>     | "true"  | "42_true"  |
              || <http://example.org/Individuals2/RonaldKhun>    | "true"  | "42_true"  |
              || <http://example.org/Individuals2/RebbecaSmith>  | "false" | "42_false" |
              || <http://example.org/Individuals2/PeterParker>   | "true"  | "42_true"  |
              || <http://example.org/Individuals2/NatalieMiller> | "true"  | "42_true"  |
              || <http://example.org/Individuals2/MartinOdersky> | "true"  | "42_true"  |
              || <http://example.org/Individuals2/JohnSmith>     | "true"  | "42_true"  |
              || <http://example.org/Individuals2/HarryMiller>   | "true"  | "42_true"  |
              || <http://example.org/Individuals2/DonaldDuck>    | "true"  | "42_true"  |
              || <http://example.org/Individuals2/DaisyDuck>     | "false" | "42_false" |
              || <http://example.org/Individuals2/BruceWayne>    | "true"  | "42_true"  |
              |--------------------------------------------------------------------------""".stripMargin

          validateResult(sparql, expectedResults, "Add42.jar")
        }

        def validateResult (sparql: String, expectedResults: String, path: String) : Future[scalatest.Assertion] = {
          spinCheck(100.millisecond,true)(
            Http.post(_sp, makeReqBody(paths, "SPARQL", sparql, Seq(path)), textPlain, Seq("format" -> "ascii")))(
            res => new String(res.payload, "UTF-8").trim == expectedResults).map{
            res => new String(res.payload, "UTF-8").trim should be (expectedResults)
          }
        }

        it("should upload Scala source") {
          val source = Source.fromURL(this.getClass.getResource("/Add42.scala")).map(_.toByte).toArray
          val importedSourcePath = cmw / "meta" / "lib" / "sources" / "scala" / "Add42.scala"
          Json.parse(
            waitAndExtractBody(
              Http.post(
                importedSourcePath,
                source,
                textPlain,
                headers = "x-cm-well-type" -> "file" :: tokenHeader))
          ) should be(jsonSuccess)
        }
        it("should run a SPARQL query with imported sources") {
          val sparql =
            """SELECT DISTINCT ?name ?active ?res WHERE {
              |?name <http://www.tr-lbd.com/bold#active> ?active .
              |BIND( <jar:Add42>(?active) as ?res).
              |} ORDER BY DESC(?name)""".stripMargin
          val expectedResults =
            """--------------------------------------------------------------------------
              || name                                            | active  | res        |
              |==========================================================================
              || <http://example.org/Individuals2/SaraSmith>     | "true"  | "42_true"  |
              || <http://example.org/Individuals2/RonaldKhun>    | "true"  | "42_true"  |
              || <http://example.org/Individuals2/RebbecaSmith>  | "false" | "42_false" |
              || <http://example.org/Individuals2/PeterParker>   | "true"  | "42_true"  |
              || <http://example.org/Individuals2/NatalieMiller> | "true"  | "42_true"  |
              || <http://example.org/Individuals2/MartinOdersky> | "true"  | "42_true"  |
              || <http://example.org/Individuals2/JohnSmith>     | "true"  | "42_true"  |
              || <http://example.org/Individuals2/HarryMiller>   | "true"  | "42_true"  |
              || <http://example.org/Individuals2/DonaldDuck>    | "true"  | "42_true"  |
              || <http://example.org/Individuals2/DaisyDuck>     | "false" | "42_false" |
              || <http://example.org/Individuals2/BruceWayne>    | "true"  | "42_true"  |
              |--------------------------------------------------------------------------""".stripMargin

          validateResult(sparql, expectedResults, "scala/Add42.scala")
        }

        it("should import recursively") {

          val firstFolder = "queries1"
          val secondFolder = "queries2"
          val firstPath = cmw / firstFolder
          val secondPath = cmw / secondFolder

          val leaves = for {
                   i <- 1 to 2
                     j <- 1 to 2
                     a <- Seq("a", "b")
          } yield s"$a$i$j"


          def prepareQueries() = {

            def makeQueryBody(resultObj: String, imports: Seq[String]) =
            s"""${if (imports.isEmpty) "" else "#cmwell-import"} ${imports.map(i => s"/$secondFolder/$i").mkString(",")}
                 |CONSTRUCT { ?s ?p "$resultObj" .} WHERE { ?s ?p ?o }
               """.stripMargin

            def addQuery(basePath: StringPath, name: String, imports: Seq[String] = Seq()) =
              Http.post(
                basePath / name,
                makeQueryBody(name, imports),
                textPlain,
                headers = "X-CM-WELL-TYPE" -> "File" :: tokenHeader).map { resp =>
                  Json.parse(new String(resp.payload, "UTF-8"))
                }
            Future.sequence(Seq(
              addQuery(firstPath, "a", Seq("a1", "a2")),
              addQuery(secondPath, "a1", Seq("a11", "a12")),
              addQuery(secondPath, "a2", Seq("a21", "a22")),
              addQuery(firstPath, "b", Seq("b1", "b2")),
              addQuery(secondPath, "b1", Seq("b11", "b12")),
              addQuery(secondPath, "b2", Seq("b21", "b22"))
            ) ++ leaves.map(addQuery(secondPath, _)))
          }

          prepareQueries().flatMap { r =>
            assert(r.forall(_ == jsonSuccess), "failed ingesting data")
            val spPostBody = makeReqBody(paths, "SPARQL", sparqlIdentity, Seq(s"/$firstFolder/_")) // using wildcards
            spinCheck(100.milliseconds,true)(
              Http.post(_sp, spPostBody, textPlain)){resp => val body = new String(resp.payload, "UTF-8").trim

              leaves.forall(l => body.contains(l))

            }.map {
              resp =>
                val body = new String(resp.payload, "UTF-8").trim
                withClue("could not find recursively-imported subgraph-expansions in result data") {
                  forAll(leaves) { l => body.contains(l) should be(true) }
                }
            }
          }
        }
      }
      it("should run a SPARQL query with 404 paths and get an empty response") {
        val expectedResults = Json.parse("""{"head":{"vars":["s","p","o"]},"results":{"bindings":[]}}""")
        val req = Http.post(_sp, makeReqBody(Seq("/no-such-path"), "SPARQL", "SELECT * WHERE { ?s ?p ?o . }"), textPlain, Seq("format"->"json"))
        val body = Json.parse(waitAndExtractBody(req))
        body should be(expectedResults)
      }
      it(testErr("Syntax Error")) {
        val sparql = "this is absolutely not a valid sparql string"
        val req = Http.post(_sp, makeReqBody(paths, "SPARQL", sparql), textPlain, Seq("format"->"ascii"))
        assertJsonFailure(req)
      }
      it(testErr("not allowed operators")) {
        val sparql = "SELECT * WHERE { ?s ?p ?o }"
        val req = Http.post(_sp, makeReqBody(Seq("/example.org/Individuals2?op=stream"), "SPARQL", sparql), textPlain, Seq("format"->"ascii"))
        assertJsonFailure(req)
      }
      it("should run a SPARQL query with secret parameter to use file") {
        val sparql = "SELECT ?name WHERE { ?name <http://purl.org/vocab2/relationship/siblingOf> <http://example.org/Individuals2/SaraSmith> . }"
        val expectedResults = Json.obj(
          "head" -> Json.obj("vars" -> Json.arr("name")),
          "results" -> Json.obj(
            "bindings" -> Json.arr(
              Json.obj(
                "name" -> Json.obj(
                  "type" -> "uri",
                  "value" -> "http://example.org/Individuals2/RebbecaSmith")))))

        val req = Http.post(_sp, makeReqBody(paths, "SPARQL", sparql), textPlain, Seq("format"->"json","x-write-file"->""))
        val body = Json.parse(waitAndExtractBody(req))
        body should be(expectedResults)
      }
      it("should run a SPARQL query with one or more bad PATHS and see headers/trailers with info") {
        val req = Http.post(_sp, makeReqBody(paths ++ Seq("/no/such/path1", "/no/such/path2"), "SPARQL", sparqlIdentity), textPlain, Seq("format"->"ascii"))
        val header = Await.result(req, requestTimeout).headers.find(_._1 == "X-CM-WELL-SG-RS")
        header shouldBe defined
        header.get._2 should be("404,404")
      }
    }
    describe("should run Gremlin queries") {
      it("should run a Gremlin query with Vertrices Iteration") {
        val gremlin = """g.V().order{it.a.id<=>it.b.id}"""
        val expectedResults = normalizeGremlinOutput("""v[http://example.org/Individuals2/BruceWayne]
v[http://example.org/Individuals2/ClarkKent]
v[http://example.org/Individuals2/DaisyDuck]
v[http://example.org/Individuals2/DonaldDuck]
v[http://example.org/Individuals2/HarryMiller]
v[http://example.org/Individuals2/JohnSmith]
v[http://example.org/Individuals2/MartinOdersky]
v[http://example.org/Individuals2/NatalieMiller]
v[http://example.org/Individuals2/PeterParker]
v[http://example.org/Individuals2/RebbecaSmith]
v[http://example.org/Individuals2/RonaldKhun]
v[http://example.org/Individuals2/SaraSmith]""")
        val req = Http.post(_sp, makeReqBody(paths, "Gremlin", gremlin), textPlain)
        val body = normalizeGremlinOutput(waitAndExtractBody(req).trim)
        body should be(expectedResults)
      }
      it("should run a Gremlin query with Literal filtering") {
        val gremlin = """g.v("http://example.org/Individuals2/MartinOdersky").out().filter{it["http://www.tr-lbd.com/bold#category"].contains("news")}"""
        val expectedResults = normalizeGremlinOutput("v[http://example.org/Individuals2/RonaldKhun]")
        val req = Http.post(_sp, makeReqBody(paths, "Gremlin", gremlin), textPlain)
        val body = normalizeGremlinOutput(waitAndExtractBody(req).trim)
        body should be(expectedResults)
      }
    }
  }

  ignore("SPARQL shim") {
    it("should get results from Integrated Shim") {
      val sprql = "SELECT * WHERE { <http://example.org/Individuals2/BruceWayne> ?p ?o }".replace(" ", "%20")
      val req = Http.get(cmw / "_shim", Seq("query"->sprql))
      val resp = Json.parse(Await.result(req, requestTimeout).payload)
      ((resp \ "results").get getArr "bindings").length should be(2)
    }
  }

  implicit class JsValueExtensions(v: JsValue) {
    def getArr(prop: String): collection.IndexedSeq[JsValue] = (v \ prop).get match { case JsArray(seq) => seq case _ => collection.IndexedSeq() }
  }
}
