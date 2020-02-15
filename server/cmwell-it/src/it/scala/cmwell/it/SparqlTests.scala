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

import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import play.api.libs.json.Json
import cmwell.util.http.{SimpleResponse, StringPath}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.io.Source

/**
  * Created by yaakov on 7/10/16.
  */
class SparqlTests extends FunSpec with Matchers with Helpers with BeforeAndAfterAll with LazyLogging {

  val _sparql = cmw / "_sparql"

  // todo uncomment once we have data file, also un-ignore ignored tests below
  override def beforeAll() = {
//    val fileNTriple = Source.fromURL(this.getClass.getResource("/data-for-sparql.nt")).mkString
//    val resFut = Http.post(_in, fileNTriple, textPlain, List("format" -> "ntriples"), tokenHeader)
//    Await.result(resFut, requestTimeout)
//
//    waitForData(cmw / "sparql.org" / "data.thomsonreuters.com", 316)
  }

  // todo move to Helpers
  def waitAndExtractBody(req: Future[SimpleResponse[Array[Byte]]]) = new String(Await.result(req, requestTimeout).payload, "UTF-8")

  // todo generalize and move to Helpers
  def waitForData(path: StringPath, expectedDataLength: Int, maxRetries: Int = 32, sleepTime: FiniteDuration = 1.second) = {
    implicit val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(10))

    var (length, retry) = (0, 0)
    do {
      val res = new String(Await.result(Http.get(path, Seq("op"->"search", "length"->"1", "format"->"json")), requestTimeout).payload, "UTF-8")
      length = (Json.parse(res) \ "results" \ "total").asOpt[Int].getOrElse(0)
      retry+=1
      sleepTime.fromNow.block
    } while(length<expectedDataLength && retry<maxRetries)
  }

  protected def postSparqlAndWaitForResults(query: String, queryParameters: Seq[(String,String)] = Seq()): String =
    waitAndExtractBody(postSparqlRequest(query, queryParameters)).trim

  protected def postSparqlRequest(query: String, queryParameters: Seq[(String,String)] = Seq()) =
    Http.post(_sparql, query, textPlain, queryParameters)

  protected def removeSysFieldsAndSort(ntriples: String) =
    ntriples.lines.toSeq.filterNot(_.contains("meta/sys#")).sorted.mkString("\n")


  describe("SPARQL Tests") {
    ignore("should run a CONSTRUCT sparql on whole graph and return one record") {
      val query = """""".stripMargin
      val expectedResults = ""
      postSparqlAndWaitForResults(query) should be(expectedResults)
    }

    ignore("should run a sparql query for a given subject without any other information (i.e. get infoton)") {
      val query = ""
      val expected = ""
      val actual = removeSysFieldsAndSort(postSparqlAndWaitForResults(query))
      actual should be(expected)
    }

    it("should explain to user that one does not simply stream all cm-well content using SPARQL") {
      // scalastyle:off
      postSparqlAndWaitForResults("SELECT * WHERE { ?s ?p ?o }") should be("[Error] Each triple-matching must have binding of a subject, a predicate or an object. If you'd like to download entire CM-Well's content, please use the Stream API\n\n-------------\n| s | p | o |\n=============\n-------------")
      // scalastyle:on
    }

    it("should fail for a non-existing namespace") {
      val notSuchPred = "<http://no.such.predicate/in/the/world>"
      val ns = "http://no.such.predicate/in/the/"
      postSparqlAndWaitForResults(s"CONSTRUCT { ?s $notSuchPred ?o . } WHERE { ?s $notSuchPred ?o }") should be(s"[Error] Namespace $ns does not exist")
    }

    ignore("should warn the user when some searches were exahusted") {
      val query = ""
      postSparqlAndWaitForResults(
        query,
        Seq("intermediate-limit"->"1")) should startWith(
        "[Warning] a query search was exhausted; results below may be partial! Please narrow your query to have complete results.")
    }

    it("should tell the user what is wrong with syntax of the query") {
      val query = "this string is not a valid sparql"
      postSparqlAndWaitForResults(query) should startWith("{\"success\":false,\"error\":\"Lexical error at line 1")
    }



    // TODO: PLEASE READ THIS CAREFULLY WHEN ADDING TESTS HERE. May the force be with you.
    //
    // SPARQL TESTS CONVENTIONS:
    // ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
    // Please only add data in NTriples to the resource file `data-for-sparql.nt`
    // Each test should use a different "subfolder" under /sparql.org
    // (i.e. do not add more ntriples which their subject starts with /sparql.org/data.thomsonreuters.com) (NOT MANDATORY)
    // Each time you add data, kindly add an invocation to `waitForData` with the path and the amount of Infotons -
    // at the end of `beforeAll` method (~ at line 30) (MANDATORY)


  }
}
