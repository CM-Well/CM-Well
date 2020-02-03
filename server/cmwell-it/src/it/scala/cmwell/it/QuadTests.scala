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

import java.io.ByteArrayInputStream

import cmwell.it.fixture.NSHashesAndPrefixes
import com.typesafe.scalalogging.LazyLogging
import org.apache.jena.query.DatasetFactory
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.scalatest.{AsyncFunSpec, Matchers}
import play.api.libs.json.{Json, Reads, _}

import scala.concurrent.duration._
import scala.io.Source

class QuadTests extends AsyncFunSpec with Matchers with Helpers with NSHashesAndPrefixes with LazyLogging {

  val exampleOrg = cmw / "example.org"

  val spiderman = exampleOrg / "comics" / "characters" / "spiderman"
  val superman = exampleOrg / "comics" / "characters" / "superman"
  val batman =  exampleOrg / "comics" / "characters" / "batman"

  def arr(v: JsValue) = JsArray(Seq(v))

  def jsonlNoData(name: String) = {
    Json.obj(
      "type.sys" -> Json.arr(Json.obj("value" -> "ObjectInfoton")),
      "path.sys" -> Json.arr(Json.obj("value" -> s"/example.org/comics/characters/$name")),
      "lastModifiedBy.sys" -> Json.arr(Json.obj("value" -> "pUser")),
      "dataCenter.sys" -> Json.arr(Json.obj("value" -> dcName)),
      "parent.sys" -> Json.arr(Json.obj("value" -> "/example.org/comics/characters")))
  }

  val sEnemies = Json.obj("type" -> "ObjectInfoton",
    "system" -> Json.obj("lastModifiedBy" -> "pUser",
      "path" -> "/example.org/comics/characters/spiderman",
      "parent" -> "/example.org/comics/characters",
      "dataCenter" -> dcName),
    "fields" -> Json.obj("enemyOf.rel" -> Json.arr(
      "http://example.org/comics/characters/dr-octopus",
      "http://example.org/comics/characters/green-goblin",
      "http://example.org/comics/characters/venom")))

  def bEnemies(enemies: JsObject*) = Json.obj(
    "type.sys"             -> arr(Json.obj("value" -> "ObjectInfoton")),
    "path.sys"             -> arr(Json.obj("value" -> "/example.org/comics/characters/batman")),
    "parent.sys"           -> arr(Json.obj("value" -> "/example.org/comics/characters")),
    "lastModifiedBy.sys" -> Json.arr(Json.obj("value" -> "pUser")),
    "dataCenter.sys"       -> arr(Json.obj("value" -> dcName)),
    "enemyOf.rel" -> enemies.seq
  ).transform(jsonlSorter).get

  def supermanWithQuad(quad: String) = Json.obj(
    "type.sys" -> Json.arr(Json.obj("value" -> "ObjectInfoton")),
    "path.sys" -> Json.arr(Json.obj("value" -> "/example.org/comics/characters/superman")),
    "lastModifiedBy.sys" -> Json.arr(Json.obj("value" -> "pUser")),
    "protocol.sys" -> Json.arr(Json.obj("value" -> "http")),
    "dataCenter.sys" -> Json.arr(Json.obj("value" -> dcName)),
    "parent.sys" -> Json.arr(Json.obj("value" -> "/example.org/comics/characters")),
    "enemyOf.rel" -> Json.arr(
      Json.obj(
        "value" -> "http://example.org/comics/characters/general-zod",
        "quad" -> quad,
        "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
      Json.obj(
        "value" -> "http://example.org/comics/characters/lex-luthor",
        "quad" -> quad,
        "type" -> "http://www.w3.org/2001/XMLSchema#anyURI")
    ),
    "sameAs.owl" -> Json.arr(
      Json.obj(
        "value" -> "http://example.org/comics/characters/clark-kent",
        "type" -> "http://www.w3.org/2001/XMLSchema#anyURI")
    )
  ).transform(jsonlSorter andThen jsonlUuidDateIdEraser).get

  val batmanExpected = bEnemies(
    Json.obj(
      "value" -> "http://example.org/comics/characters/joker",
      "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
    Json.obj(
      "value" -> "http://example.org/comics/characters/riddler",
      "quad" -> "http://example.org/graphs/batman",
      "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
    Json.obj(
      "value" -> "http://example.org/comics/characters/joker",
      "quad" -> "http://example.org/graphs/batman",
      "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
    Json.obj(
      "value" -> "http://example.org/comics/characters/joker",
      "quad" -> "http://example.org/graphs/joker",
      "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"))


  describe("n-quads data") {

    //Assertions
    val ingestingNquads = {
      val spiderman = Source.fromURL(this.getClass.getResource("/spiderman.nq")).mkString
      val superman = Source.fromURL(this.getClass.getResource("/superman.nq")).mkString
      val batman = Source.fromURL(this.getClass.getResource("/batman.nq")).mkString
      val f1 = Http.post(_in, spiderman, Some("application/n-quads;charset=UTF-8"), List("format" -> "nquads"), tokenHeader)
      val f2 = Http.post(_in, superman, Some("application/n-quads;charset=UTF-8"), List("format" -> "nquads"), tokenHeader)
      val f3 = Http.post(_in, batman, Some("application/n-quads;charset=UTF-8"), List("format" -> "nquads"), tokenHeader)
      for {
        r1 <- f1
        r2 <- f2
        r3 <- f3
      } yield {
        Json.parse(r1.payload) should be(jsonSuccess)
        Json.parse(r2.payload) should be(jsonSuccess)
        Json.parse(r3.payload) should be(jsonSuccess)
      }
    }

    val failGlobalQuadReplace = ingestingNquads.flatMap { _ => {
      val data = """<> <cmwell://meta/sys#replaceGraph> <*> ."""
      Http.post(_in, data, None, List("format" -> "nquads"), tokenHeader).map { res =>
        withClue(res) {
          res.status should be(400)
        }
      }
    }
    }

    val failTooManyGraphReplaceStatements = ingestingNquads.flatMap { _ =>
      val stmtPrefix = "<> <cmwell://meta/sys#replaceGraph> <http://graph.number/"
      val stmtSuffix = "> .\n"
      val ntriplesRG = (1 to 21).mkString(stmtPrefix, stmtSuffix + stmtPrefix, stmtSuffix)
      Http.post(_in, ntriplesRG, None, List("format" -> "ntriples"), tokenHeader).map { res =>
        withClue(res) {
          res.status should be(400)
        }
      }
    }

    val fSpiderman1 = ingestingNquads.flatMap(_ =>
      spinCheck(100.millis, true)(Http.get(spiderman, List("format" -> "json")))(res =>
        Json.parse(res.payload).transform(fieldsSorter andThen uuidDateEraser).get == sEnemies
      )).map { res =>
      withClue(res) {
        Json
          .parse(res.payload)
          .transform(fieldsSorter andThen uuidDateEraser)
          .get shouldEqual sEnemies
      }
    }
    val fSpiderman2 = ingestingNquads.flatMap(_ =>
      spinCheck(100.millis, true)(Http.get(spiderman, List("format" -> "nquads"))) { result =>
        val res = result.payload
        val ds = DatasetFactory.createGeneral()
        RDFDataMgr.read(ds, new ByteArrayInputStream(res), Lang.NQUADS)
        !ds.getNamedModel("http://example.org/graphs/spiderman").isEmpty
      }).map { result =>
      val res = result.payload
      val ds = DatasetFactory.createGeneral()
      RDFDataMgr.read(ds, new ByteArrayInputStream(res), Lang.NQUADS)
      ds.getNamedModel("http://example.org/graphs/spiderman").isEmpty should be(false)
    }
    val fSpiderman3 = {
      val data = """<http://example.org/comics/characters/spiderman> <cmwell://meta/sys#markReplace> <*> <http://example.org/graphs/spiderman> ."""
      for {
        _ <- fSpiderman1
        _ <- fSpiderman2
        res <- Http.post(_in, data, None, List("format" -> "nquads"), tokenHeader)
      } yield withClue(res) {
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }
    val fSpiderman4 = fSpiderman3.flatMap(_ => spinCheck(100.millis, true)(Http.get(spiderman, List("format" -> "nquads")))(
      _.status==404).map { res =>
      withClue(res) {
        res.status should be(404)
      }
    })
    val fSuperman1 = ingestingNquads.flatMap(_ => { val expected =
      Json.obj(
        "type" -> "SearchResults",
        "total" -> 4,
        "offset" -> 0,
        "length" -> 4,
        "infotons" -> Json.arr(
          jsonlNoData("john-kent"),
          jsonlNoData("clark-kent"),
          jsonlNoData("martha-kent"),
          jsonlNoData("superman")
        )
      ).transform(jsonlSorter).get

      spinCheck(100.millis, true)(Http.get(
        exampleOrg,
        List(
          "op" -> "search",
          "qp" -> "system.quad::http://example.org/graphs/superman",
          "format" -> "jsonl",
          "recursive" -> ""))){ res =>
        Json
          .parse(res.payload)
          .transform((__ \ 'results).json.pick andThen
            (__ \ 'fromDate).json.prune andThen
            (__ \ 'toDate).json.prune andThen
            jsonlInfotonArraySorterAndUuidDateIdEraser)
          .get == expected
      }.map { res =>
        withClue(res) {
          Json
            .parse(res.payload)
            .transform((__ \ 'results).json.pick andThen
              (__ \ 'fromDate).json.prune andThen
              (__ \ 'toDate).json.prune andThen
              jsonlInfotonArraySorterAndUuidDateIdEraser)
            .get shouldEqual expected
        }
      }})
    val fSuperman2 = ingestingNquads.flatMap(_ => spinCheck(100.millis, true)(Http.get(
      exampleOrg,
      List("format" -> "jsonl", "op" -> "search", "qp" -> "system.quad::superman", "recursive" -> "")))(_.status == 422).map { res =>
      withClue(res) {
        res.status should be(422)
      }
    })
    val fSuperman3 = ingestingNquads.flatMap(_ => spinCheck(100.millis, true)(Http.get(superman, List("format" -> "jsonl", "pretty" -> ""))){
      res => val payload = res.payload
        if (payload.toString == "Infoton not found") false
        else
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get == supermanWithQuad("http://example.org/graphs/superman")
    }.map { res =>
      withClue(res) {
        Json
          .parse(res.payload)
          .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
          .get shouldEqual supermanWithQuad("http://example.org/graphs/superman")
      }
    })
    val fSuperman4 = for {
      _ <- fSuperman1
      _ <- fSuperman2
      _ <- fSuperman3
      data = """<> <cmwell://meta/sys#graphAlias> "superman" <http://example.org/graphs/superman> ."""
      res <- Http.post(_in, data, None, List("format" -> "nquads"), tokenHeader)
    } yield withClue(res) {
      jsonSuccessPruner(Json.parse(res.payload)) should be(jsonSuccess)
    }
    val fSuperman5 = fSuperman4.flatMap(_ => spinCheck(100.millis, true)(Http.get(superman, List("format" -> "jsonl", "pretty" -> ""))){
      res =>
        Json.parse(res.payload).transform(jsonlSorter andThen jsonlUuidDateIdEraser).get == supermanWithQuad("superman")
    }.map { res =>
      withClue(res) {
        Json
          .parse(res.payload)
          .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
          .get shouldEqual supermanWithQuad("superman")
      }
    })
    val fSuperman6 = fSuperman5.flatMap { _ =>
      val expected = Json.obj(
        "type" -> "SearchResults",
        "total" -> 4,
        "offset" -> 0,
        "length" -> 4,
        "infotons" -> Json.arr(
          jsonlNoData("john-kent"),
          jsonlNoData("clark-kent"),
          jsonlNoData("martha-kent"),
          jsonlNoData("superman"))).transform(jsonlSorter).get
      Http.get(exampleOrg, List("op" -> "search", "qp" -> "system.quad::superman", "format" -> "jsonl", "recursive" -> "")).map { res =>
        withClue(res) {
          Json
            .parse(res.payload)
            .transform((__ \ 'results).json.pick andThen
              (__ \ 'fromDate).json.prune andThen
              (__ \ 'toDate).json.prune andThen
              jsonlInfotonArraySorterAndUuidDateIdEraser)
            .get shouldEqual expected
        }
      }
    }
    val fSuperman7 = fSuperman6.flatMap{ _ =>
      val data = """<> <cmwell://meta/sys#replaceGraph> <http://example.org/graphs/superman> ."""
      Http.post(_in, data, None, List("format" -> "nquads"), tokenHeader).map { res =>
        withClue(res) {
          res.status should be(200)
        }
      }
    }
    val fSuperman8 = fSuperman7.flatMap{_ =>
      val expected = Json.obj(
        "type.sys" -> Json.arr(Json.obj("value" -> "ObjectInfoton")),
        "path.sys" -> Json.arr(Json.obj("value" -> "/example.org/comics/characters/superman")),
        "lastModifiedBy.sys" -> Json.arr(Json.obj("value" -> "pUser")),
        "dataCenter.sys" -> Json.arr(Json.obj("value" -> dcName)),
        "parent.sys" -> Json.arr(Json.obj("value" -> "/example.org/comics/characters")),
        "sameAs.owl" -> Json.arr(
          Json.obj(
            "value" -> "http://example.org/comics/characters/clark-kent",
            "type" -> "http://www.w3.org/2001/XMLSchema#anyURI")
        )
      ).transform(jsonlSorter andThen jsonlUuidDateIdEraser).get
      spinCheck(100.millis, true)(Http.get(superman, List("format" -> "jsonl"))){
        res =>
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get == expected

      }.map { res =>
        withClue(res) {
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get shouldEqual expected
        }
      }
    }
    val fBatman01 = ingestingNquads.flatMap(_ => spinCheck(100.millis, true)(Http.get(batman, List("format" -> "jsonl"))){
      res => val payload = res.payload
        if (payload.toString == "Infoton not found") false
        else
          Json.parse(res.payload).transform(jsonlSorter andThen jsonlUuidDateIdEraser).get == batmanExpected
    }.map { res =>
      withClue(res) {
        Json
          .parse(res.payload)
          .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
          .get shouldEqual batmanExpected
      }
    })
    val fBatman02 = {
      val quads =
        """
          |<http://example.org/comics/characters/batman> <cmwell://meta/sys#markDelete> _:batmanDeletes <http://example.org/graphs/joker> .
          |_:batmanDeletes <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/riddler> <http://example.org/graphs/batman> .
          |_:batmanDeletes <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/joker> .
        """.stripMargin
      fBatman01.flatMap(_ => Http.post(_in, quads, None, List("format" -> "nquads"), tokenHeader).map{res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      })
    }
    val fBatman03 = fBatman02.flatMap( _ => spinCheck(100.millis, true)(Http.get(batman, List("format" -> "jsonl"))){
      res => Json.parse(res.payload)
        .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
        .get == batmanExpected
    }.map { res =>
      withClue(res) {
        Json
          .parse(res.payload)
          .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
          .get shouldEqual batmanExpected
      }
    }
    )
    val fBatman04 = {
      val quads =
        """
          |<http://example.org/comics/characters/batman> <cmwell://meta/sys#markDelete> _:batmanDeletes <http://example.org/graphs/batman> .
          |_:batmanDeletes <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/riddler> <http://example.org/graphs/batman> .
          |_:batmanDeletes <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/joker> .
        """.stripMargin
      fBatman03.flatMap(_ => Http.post(_in, quads, None, List("format" -> "nquads"), tokenHeader).map { res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      })
    }
    val fBatman05 = fBatman04.flatMap(_ => {
      val expected = bEnemies(
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "quad" -> "http://example.org/graphs/batman",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "quad" -> "http://example.org/graphs/joker",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"))

      spinCheck(100.millis, true)(
        Http.get(batman, List("format" -> "jsonl"))) {
        res =>
          Json.parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get == expected
      }.map { res =>
        withClue(res) {
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get shouldEqual expected
        }
      }
    })
    val fBatman06 = {
      // scalastyle:off
      val quads =
        """
          |<http://example.org/comics/characters/batman> <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/scarecrow> <http://example.org/graphs/batman> .
          |<http://example.org/comics/characters/batman> <cmwell://meta/sys#markDelete> _:batmanDeletes <http://example.org/graphs/joker> .
          |_:batmanDeletes <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/joker> .
        """.stripMargin
      // scalastyle:on
      fBatman05.flatMap(_ => Http.post(_in, quads, None, List("format" -> "nquads"), tokenHeader).map{res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      })
    }
    val fBatman07 = fBatman06.flatMap(_ => {
      val expected = bEnemies(
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "quad" -> "http://example.org/graphs/batman",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "quad" -> "http://example.org/graphs/joker",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/scarecrow",
          "quad" -> "http://example.org/graphs/batman",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"))

      spinCheck(100.millis, true)(
        Http.get(batman, List("format" -> "jsonl"))){res => Json
        .parse(res.payload)
        .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
        .get == expected }.map { res =>
        withClue(res) {
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get shouldEqual expected
        }
      }
    })
    val fBatman08 = {
      val quads =
        """
          |<http://example.org/comics/characters/batman> <cmwell://meta/sys#markDelete> _:batmanDeletes <http://example.org/graphs/joker> .
          |_:batmanDeletes <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/joker> <http://example.org/graphs/joker> .
        """.stripMargin
      fBatman07.flatMap(_ => Http.post(_in, quads, None, List("format" -> "nquads"), tokenHeader).map { res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      })
    }
    val fBatman09 = fBatman08.flatMap(_ => {
      val expected = bEnemies(
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "quad" -> "http://example.org/graphs/batman",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/scarecrow",
          "quad" -> "http://example.org/graphs/batman",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"))

      spinCheck(100.millis, true)(Http.get(batman, List("format" -> "jsonl"))){ res =>
        Json
          .parse(res.payload)
          .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
          .get == expected
      }.map { res =>
        withClue(res) {
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get shouldEqual expected
        }
      }
    })
    val fBatman10 = {
      val quads =
        """
          |<http://example.org/comics/characters/batman> <cmwell://meta/sys#markDelete> _:batmanDeletes .
          |_:batmanDeletes <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/joker> .
        """.stripMargin
      fBatman09.flatMap(_ => Http.post(_in, quads, None, List("format" -> "ntriples"), tokenHeader).map { res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      })
    }
    val fBatman11 = fBatman10.flatMap(_ => {
      val expected = bEnemies(
        Json.obj(
          "value" -> "http://example.org/comics/characters/scarecrow",
          "quad" -> "http://example.org/graphs/batman",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"))

      spinCheck(100.millis, true)(Http.get(batman, List("format" -> "jsonl"))){res => Json
        .parse(res.payload)
        .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
        .get == expected
      }.map { res =>
        withClue(res){
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get shouldEqual expected
        }
      }
    })
    val fBatman12 = {
      // scalastyle:off
      val quads =
        """
          |<http://example.org/comics/characters/batman> <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/joker> <http://example.org/graphs/batman> .
          |<http://example.org/comics/characters/batman> <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/joker> .
          |<http://example.org/comics/characters/batman> <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/riddler> <http://example.org/graphs/batman> .
          |<http://example.org/comics/characters/batman> <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/joker> <http://example.org/graphs/joker> .
        """.stripMargin
      // scalastyle:on
      fBatman11.flatMap(_ => Http.post(_in, quads, None, List("format" -> "nquads", "replace-mode" -> "*"), tokenHeader).map{res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      })
    }
    val fBatman13 = fBatman12.flatMap(_ => {
      val expected = bEnemies(
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "quad" -> "http://example.org/graphs/joker",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "quad" -> "http://example.org/graphs/batman",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/riddler",
          "quad" -> "http://example.org/graphs/batman",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"))

      spinCheck(100.millis, true)(Http.get(batman, List("format" -> "jsonl"))){res =>
        Json.parse(res.payload)
          .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
          .get == expected
      }.map{res =>
        withClue(res){
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get shouldEqual expected
        }
      }
    })
    val fBatman14 = {
      // scalastyle:off
      val quads =
        """
          |<http://example.org/comics/characters/batman> <cmwell://meta/sys#markReplace> <http://purl.org/vocab/relationship/enemyOf> <http://example.org/graphs/batman> .
          |<http://example.org/comics/characters/batman> <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/enigma> <http://example.org/graphs/batman> .
        """.stripMargin
      // scalastyle:on
      fBatman13.flatMap(_ => Http.post(_in, quads, None, List("format" -> "nquads"), tokenHeader).map{res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      })
    }
    val fBatman15 = fBatman14.flatMap(_ => {
      val expected = bEnemies(
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "quad" -> "http://example.org/graphs/joker",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/enigma",
          "quad" -> "http://example.org/graphs/batman",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"))

      spinCheck(100.millis, true)(Http.get(batman, List("format" -> "jsonl"))){ res=>
        Json.parse(res.payload)
          .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
          .get == expected
      }.map { res =>
        withClue(res) {
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get shouldEqual expected
        }
      }
    })
    val fBatman16 = {
      val quads =
        """
          |<http://example.org/comics/characters/batman> <cmwell://meta/sys#markReplace> <http://purl.org/vocab/relationship/enemyOf> .
          |<http://example.org/comics/characters/batman> <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/scarecrow> .
        """.stripMargin
      fBatman15.flatMap(_ => Http.post(_in, quads, None, List("format" -> "nquads"), tokenHeader).map{res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      })
    }
    val fBatman17 = fBatman16.flatMap(_ => {
      val expected = bEnemies(
        Json.obj(
          "value" -> "http://example.org/comics/characters/scarecrow",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/joker",
          "quad" -> "http://example.org/graphs/joker",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
        Json.obj(
          "value" -> "http://example.org/comics/characters/enigma",
          "quad" -> "http://example.org/graphs/batman",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"))

      spinCheck(100.millis, true)(Http.get(batman, List("format" -> "jsonl"))){ res =>
        Json.parse(res.payload)
          .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
          .get == expected
      }.map { res =>
        withClue(res) {
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get shouldEqual expected
        }
      }
    })
    val fBatman18 = {
      // scalastyle:off
      val quads =
        """
          |<http://example.org/comics/characters/batman> <cmwell://meta/sys#markReplace> <http://purl.org/vocab/relationship/enemyOf> <*> .
          |<http://example.org/comics/characters/batman> <http://purl.org/vocab/relationship/enemyOf> <http://example.org/comics/characters/ivy> <http://example.org/graphs/batman> .
          |<http://example.org/comics/characters/batman> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/comics/characters/cat-woman> .
          |<http://example.org/comics/characters/batman> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/comics/characters/james-gordon> <http://example.org/graphs/batman> .
          |<http://example.org/comics/characters/cat-woman> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/comics/characters/batman> .
          |<http://example.org/comics/characters/james-gordon> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/comics/characters/batman> <http://example.org/graphs/batman> .
        """.stripMargin
      // scalastyle:on
      fBatman17.flatMap(_ => Http.post(_in, quads, None, List("format" -> "nquads"), tokenHeader).map { res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      })
    }
    val fBatman19 = fBatman18.flatMap(_ => {
      val addColls = __.json.update(
        Reads.JsObjectReads.map{
          case JsObject(xs) => JsObject(
            xs + ("collaboratesWith.rel" -> Json.arr(
              Json.obj(
                "value" -> "http://example.org/comics/characters/cat-woman",
                "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
              Json.obj(
                "value" -> "http://example.org/comics/characters/james-gordon",
                "type" -> "http://www.w3.org/2001/XMLSchema#anyURI",
                "quad" -> "http://example.org/graphs/batman")))
          )
        }
      )
      val expected = bEnemies(
        Json.obj(
          "value" -> "http://example.org/comics/characters/ivy",
          "quad" -> "http://example.org/graphs/batman",
          "type" -> "http://www.w3.org/2001/XMLSchema#anyURI")
      ).transform(addColls andThen jsonlSorter).get

      spinCheck(100.millis, true)(Http.get(batman, List("format" -> "jsonl"))){res =>
        Json
          .parse(res.payload)
          .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
          .get == expected
      }.map{res =>
        withClue(res) {
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get shouldEqual expected
        }
      }
    })
    val fBatman20 = {
      // scalastyle:off
      val quads =
        """
          |<http://example.org/comics/characters/batman> <cmwell://meta/sys#markReplace> <*> <http://example.org/graphs/batman> .
          |<http://example.org/comics/characters/batman> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/comics/characters/robin> <http://example.org/graphs/batman> .
          |<http://example.org/comics/characters/robin> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/comics/characters/batman> <http://example.org/graphs/batman> .
        """.stripMargin
      // scalastyle:on
      fBatman19.flatMap(_ => Http.post(_in, quads, None, List("format" -> "nquads"), tokenHeader).map { res =>
        withClue(res) {Json.parse(res.payload) should be(jsonSuccess)
        }
      })
    }
    val fBatman21 = fBatman20.flatMap(_ => {
      val expected = Json.obj(
        "type.sys"             -> arr(Json.obj("value" -> "ObjectInfoton")),
        "path.sys"             -> arr(Json.obj("value" -> "/example.org/comics/characters/batman")),
        "parent.sys"           -> arr(Json.obj("value" -> "/example.org/comics/characters")),
        "lastModifiedBy.sys" -> Json.arr(Json.obj("value" -> "pUser")),
        "dataCenter.sys"       -> arr(Json.obj("value" -> dcName)),
        "collaboratesWith.rel" -> Json.arr(
          Json.obj(
            "value" -> "http://example.org/comics/characters/cat-woman",
            "type" -> "http://www.w3.org/2001/XMLSchema#anyURI"),
          Json.obj(
            "value" -> "http://example.org/comics/characters/robin",
            "type" -> "http://www.w3.org/2001/XMLSchema#anyURI",
            "quad" -> "http://example.org/graphs/batman"))
      ).transform(jsonlSorter).get

      spinCheck(100.millis, true)(Http.get(batman, List("format" -> "jsonl"))){res =>
        Json
          .parse(res.payload)
          .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
          .get == expected
      }.map{res =>
        withClue(res){
          Json
            .parse(res.payload)
            .transform(jsonlSorter andThen jsonlUuidDateIdEraser)
            .get shouldEqual expected
        }
      }
    })
    // scalastyle:off
    it("should accept and process n-quads data")(ingestingNquads)
    it("should not succeed deleting all quads globally using <*>")(failGlobalQuadReplace)
    it("should fail to exceed the maximum allowed replaceGraph statements per request")(failTooManyGraphReplaceStatements)
    it("should be retrievable in json format")(fSpiderman1)
    it("should verify subgraph data is readable (nquads format)")(fSpiderman2)
    it("should succeed deleting spiderman's quads")(fSpiderman3)
    it("should not be able to get infoton after spiderman fields deletion")(fSpiderman4)
    it("should be searchable with system.quad as qp")(fSuperman1)
    it("should not be able to find superman's infotons by quad alias in search")(fSuperman2)
    it("should retrieve superman as pretty jsonl without quad aliasing")(fSuperman3)
    it("should register an alias to superman's graph")(fSuperman4)
    it("should retrieve superman as pretty jsonl with quad aliasing")(fSuperman5)
    it("should now (previously failed) be able to find superman's infotons by quad alias in search")(fSuperman6)
    it("should succeed deleting all superman's quads globally")(fSuperman7)
    it("should retrieve superman as jsonl without deleted quad asociated attributes")(fSuperman8)
    it("should retrieve batman as jsonl")(fBatman01)
    // MARK DELETE
    describe("should make sure wrong quads supplied to _in will not modify the data") {
      it("succeed posting the quads")(fBatman02)
      it("verifying the data")(fBatman03)
    }
    describe("should make sure to delete only meta operation related quads and not values that belong to other quads") {
      it("succeed posting the quads")(fBatman04)
      it("verifying the data")(fBatman05)
    }
    describe("should ignore deletes for wrong graph but insert new unrelated data"){
      it("succeed posting the quads")(fBatman06)
      it("verifying the data")(fBatman07)
    }
    describe("should delete joker from joker graph") {
      it("succeed posting the quads")(fBatman08)
      it("verifying the data")(fBatman09)
    }
    describe("should delete joker from all graphs when no graph is supplied") {
      it("succeed posting the quads")(fBatman10)
      it("verifying the data")(fBatman11)
    }
    //MARK REPLACE
    describe("should replace all with 'replace-mode' enabled") {
      it("succeed posting the quads")(fBatman12)
      it("verifying the data")(fBatman13)
    }
    describe("should replace all values in same quad when it is passed as a quad to 'markReplace' with the new values") {
      it("succeed posting the quads")(fBatman14)
      it("verifying the data")(fBatman15)
    }
    describe("should replace all default graph related when no quad is supplied") {
      it("succeed posting the quads")(fBatman16)
      it("verifying the data")(fBatman17)
    }
    describe("should respect the 'nuclear' option and replace all values for all quads when '*' is supplied as a quad") {
      it("succeed posting the quads")(fBatman18)
      it("verifying the data")(fBatman19)
    }
    describe("should respect the 'inverted nuclear' option and replace all fields associated with some quad when '*' is supplied as a predicate for markReplace") {
      it("succeed posting the quads")(fBatman20)
      it("verifying the data")(fBatman21)
    }
    // scalastyle:on
  }
}

