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

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Inspectors, Matchers}

import scala.concurrent.duration._

class HttpsTests extends AsyncFunSpec with Matchers with Helpers with Inspectors with LazyLogging {

  val path = cmw / "example.org"

  val bName = """<http://example.org/B> <https://www.tr-lbd.com/bold#name> "My name is B" ."""
  val aName = """<http://example.org/A> <https://www.tr-lbd.com/bold#name> "My name is A" ."""
  val AandB = List(bName, aName)
  val allASons = List(
    """<http://example.org/A1> <https://www.tr-lbd.com/bold#name> "My name is A1" .""",
    """<http://example.org/A2> <https://www.tr-lbd.com/bold#name> "My name is A2" .""",
    """<http://example.org/A3> <https://www.tr-lbd.com/bold#name> "My name is A3" ."""
  )

  val inject = {
    val data =
      """
          <https://example.org/A> <https://purl.org/vocab/relationship/predicate> <http://example.org/A1> .
          <https://example.org/A> <https://purl.org/vocab/relationship/predicate> <https://example.org/A2> .
          <https://example.org/A> <https://purl.org/vocab/relationship/predicate> <https://example.org/A3> .
          <https://example.org/A> <https://www.tr-lbd.com/bold#name> "My name is A" .
          <https://example.org/A1> <https://www.tr-lbd.com/bold#name> "My name is A1" .
          <https://example.org/A2> <https://www.tr-lbd.com/bold#name> "My name is A2" .
          <https://example.org/A3> <https://www.tr-lbd.com/bold#name> "My name is A3" .
          <https://example.org/B> <https://purl.org/vocab/relationship/predicate> <https://example.org/A> .
          <https://example.org/B> <https://www.tr-lbd.com/bold#name> "My name is B" .
      """.
        stripMargin
    Http.post(_in, data, queryParams = List("format" -> "ntriples"), headers = tokenHeader)
  }

  val verifyYgForB = inject.flatMap{ _ =>
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    spinCheck(100.millis, true)(Http.get(path / "B", List("yg" -> ">predicate.relationship", "format" -> "ntriples"))){
        res => {
          val resList = res.payload.lines.toList
          (res.status == 200) && AandB.forall(resList.contains)
        }
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          val resList = res.payload.lines.toList
          forAll(AandB) { l => resList.contains(l) should be(true) }
        }
      }
  }

  val verifyYgForA = inject.flatMap{ _ =>
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    spinCheck(100.millis, true)(Http.get(path / "A", List("yg" -> "<predicate.relationship", "format" -> "ntriples"))){
      res => {
        val resList = res.payload.lines.toList
        (res.status == 200) && AandB.forall(resList.contains)
      }
    }.map { res =>
      withClue(res) {
        res.status should be(200)
        val resList = res.payload.lines.toList
        forAll(AandB) { l => resList.contains(l) should be(true) }
      }
    }
  }

  val verifyXgForA = inject.flatMap{ _ =>
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    spinCheck(100.millis, true)(
      Http.get(cmw, List("op" -> "search", "qp" -> "name.bold::My name is A", "recursive" -> "", "with-data" -> "",
      "format" -> "ntriples", "xg" -> ""))){
      res => {
        val resList = res.payload.lines.toList
        (res.status == 200) && (aName :: allASons).forall(resList.contains)
      }
    }.map { res =>
      withClue(res) {
        res.status should be(200)
        val resList = res.payload.lines.toList
        forAll(aName :: allASons) { l => resList.contains(l) should be(true) }
      }
    }
  }

  val verifyGqpForPointingAtA = inject.flatMap{ _ =>
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    spinCheck(100.millis, true, 1.minute)(
      Http.get(cmw, List("op" -> "search", "qp" -> "name.bold::My name is A", "recursive" -> "", "with-data" -> "",
        "gqp" -> """<predicate.relationship[name.bold::My name is B]""", "format" -> "ntriples")))
    { res =>
      val resList = res.payload.lines.toList
      (res.status == 200) && resList.contains(aName)
    }.map { res =>
      withClue(res) {
        res.status should be(200)
        res.payload.lines.toList should contain(aName)
      }
    }
  }

  val verifyGqpForAPointingAtHttp = inject.flatMap{ _ =>
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    spinCheck(100.millis, true, 1.minute)(
      Http.get(cmw, List("op" -> "search", "qp" -> "name.bold::My name is A", "recursive" -> "", "with-data" -> "",
        "gqp" -> """>predicate.relationship[name.bold::My name is A1]""", "format" -> "ntriples")))
    { res =>
      val resList = res.payload.lines.toList
      (res.status == 200) && resList.contains(aName)
    }.map { res =>
      withClue(res) {
        res.status should be(200)
        res.payload.lines.toList should contain(aName)
      }
    }
  }

  val verifyGqpForAPointingAtHttps = inject.flatMap{ _ =>
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    spinCheck(100.millis, true, 1.minute)(
      Http.get(cmw, List("op" -> "search", "qp" -> "name.bold::My name is A", "recursive" -> "", "with-data" -> "",
        "gqp" -> """>predicate.relationship[name.bold::My name is A2]""", "format" -> "ntriples")))
    { res =>
      val resList = res.payload.lines.toList
      (res.status == 200) && resList.contains(aName)
    }.map { res =>
      withClue(res) {
        res.status should be(200)
        res.payload.lines.toList should contain(aName)
      }
    }
  }

  ignore("Inject data to cmwell")(inject.map(_.status should be(200)))
  ignore("Verify YG > for B")(verifyYgForB)
  ignore("Verify YG < for A")(verifyYgForA)
  ignore("Verify XG for A")(verifyXgForA)
  ignore("Verify GQP > for A with HTTP relation")(verifyGqpForAPointingAtHttp)
  ignore("Verify GQP > for A with HTTPS relation")(verifyGqpForAPointingAtHttps)
  ignore("Verify GQP < for A")(verifyGqpForPointingAtA)


}