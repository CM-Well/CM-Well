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

import cmwell.util.concurrent.SimpleScheduler._
import cmwell.util.http.SimpleResponse
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Inspectors, Matchers}
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.duration._

class DeletionTests extends AsyncFunSpec with Matchers with Inspectors with Helpers with LazyLogging {
  describe("delete object infoton") {

    val iofd = cmt / "InfoObjForDelete"
    val ioffd = cmt / "InfoObjForFullDelete"
    val deletes500 = cmt / "bulkDeletes" / "d500"
    val deletes501 = cmt / "bulkDeletes" / "d501"
    val jsonObjForDelete: JsValue = Json.obj(
      "company" -> Seq("clearforest", "ibm", "microsoft"),
      "movie" -> Seq("the best"),
      "phrase" -> Seq("four eyes commit", "quality is a mindset", "ubuntu shmubuntu"))
    val j4d: JsValue = Json.obj("k" -> Seq("v"))

    //Assertions =
    val ingestOf2Infotons = {
      val f1 = Http.post(iofd, Json.stringify(jsonObjForDelete), None, Nil, ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
      val f2 = Http.post(ioffd, Json.stringify(jsonObjForDelete), None, Nil, ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
      f1.zip(f2).map {
        case (res1,res2) => {
          res1.status should be(200)
          res2.status should be(200)
          Json.parse(res1.payload) should be(jsonSuccess)
          Json.parse(res2.payload) should be(jsonSuccess)
        }
      }
    }

    val massiveIngestOf1001Infotons = {
      val d501f = Http.post(deletes501 / "i-501", Json.stringify(j4d), None, Nil, ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
      val futures: Seq[Future[SimpleResponse[Array[Byte]]]] = (1 to 500).foldLeft(List(d501f)) {
        case (fList, i) => {
          val f1 = Http.post(deletes500 / s"i-$i", Json.stringify(j4d), None, Nil, ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
          val f2 = Http.post(deletes501 / s"i-$i", Json.stringify(j4d), None, Nil, ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader)
          f1 :: f2 :: fList
        }
      }
      Future.sequence(futures).map { responses =>
        forAll(responses) { res =>
          res.status should be(200)
          Json.parse(res.payload) should be(jsonSuccess)
        }
      }
    }

    def waitForNumberOfResults[T](n: Int, waitDuration: FiniteDuration, tries: Int)
                                 (request: => Future[T])
                                 (computeNumOfResults: T => Int): Future[Int] = request.flatMap{ t =>
      val r = computeNumOfResults(t)
      if(r >= n || tries <= 0) Future.successful(r)
      else scheduleFuture(waitDuration)(waitForNumberOfResults(n,waitDuration,tries-1)(request)(computeNumOfResults))
    }

    val spinCheckUntilWritesAreIndexed = {
      //25ms*(40*60*5) = 25ms*12000 = 5min
      val f500 = waitForNumberOfResults(500,25.millis,12000)(Http.get(deletes500, List("op" -> "search", "length" -> "1", "format" -> "json"))){ res =>
        (Json.parse(res.payload) \ "results" \ "total").as[Int]
      }
      val f501 = waitForNumberOfResults(501,25.millis,12000)(Http.get(deletes501, List("op" -> "search", "length" -> "1", "format" -> "json"))){ res =>
        (Json.parse(res.payload) \ "results" \ "total").as[Int]
      }
      f500.zip(f501).map {
        case (r500, r501) => {
          r500 should be(500)
          r501 should be(501)
        }
      }
    }

    val verifyIngestOfInfotonInfoObjForDelete = ingestOf2Infotons.flatMap(_ => spinCheck(100.millis, true)(Http.get(iofd, List("format" -> "json"))){ res =>
      Json.parse(res.payload).transform(fieldsSorter andThen (__ \ 'fields).json.pick).get == jsonObjForDelete
    }.map { res =>
      withClue(res) {
        Json.parse(res.payload).transform(fieldsSorter andThen (__ \ 'fields).json.pick).get should be (jsonObjForDelete)
      }
    })

    val deleteKeyValuePair = verifyIngestOfInfotonInfoObjForDelete.flatMap(_ => {
      val data = """{"movie":"the best"}"""
      Http.delete(iofd,List("data" -> data),tokenHeader).map { res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      }
    })

    val verifyKeyValuePairDeleted = deleteKeyValuePair.flatMap{_ => val expected = jsonObjForDelete.transform((__ \ 'movie).json.prune).get
      spinCheck(100.millis,true)(Http.get(iofd, List("format" -> "json")))(
        res => res.status==200 && Json.parse(res.payload).transform(fieldsSorter andThen (__ \ 'fields).json.pick).get == expected ).map {
        res => withClue(res) {
          res.status should be(200)
          Json.parse(res.payload).transform(fieldsSorter andThen (__ \ 'fields).json.pick).get shouldEqual expected
        }
      }}

    val deleteFromArray = verifyKeyValuePairDeleted.flatMap(_ => {
      Http.delete(iofd,List("data" -> """{"phrase":"ubuntu shmubuntu"}"""),tokenHeader).map{ res =>
        withClue(res){
          Json.parse(res.payload) should be(jsonSuccess)
        }
      }
    })

    val getWithoutArrayValue = deleteFromArray.flatMap(_ => {
      val expected = Json.obj(
        "type"   -> "ObjectInfoton",
        "system" -> Json.obj(
          "lastModifiedBy"       -> "pUser",
          "path"       -> "/cmt/cm/test/InfoObjForDelete",
          "parent"     -> "/cmt/cm/test",
          "dataCenter" -> dcName),
        "fields" -> Json.obj(
          "company"    -> Json.arr("clearforest","ibm","microsoft"),
          "phrase"     -> Json.arr("four eyes commit","quality is a mindset")))

      spinCheck(100.millis,true)(Http.get(iofd, List("format" -> "json"))) { res =>
        res.status == 200 && {
          Json.parse(res.payload).transform(uuidDateEraser andThen fieldsSorter).get == expected
        }
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          Json.parse(res.payload).transform(uuidDateEraser andThen fieldsSorter).get shouldEqual expected
        }
      }
    })

    val deleteAllAttributes = getWithoutArrayValue.flatMap(_ => {
      Http.delete(
        iofd,
        List("data" -> """{"company":["clearforest","ibm","microsoft"],"phrase":["quality is a mindset","four eyes commit"]}"""),
        tokenHeader).map { res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      }
    })

    val verifyAllAttributesDeleted = deleteAllAttributes.flatMap(_ => {
      val expected = {
        Json.obj(
          "type"   -> "ObjectInfoton",
          "system" -> Json.obj(
            "lastModifiedBy"       -> "pUser",
            "path"       -> "/cmt/cm/test/InfoObjForDelete",
            "parent"     -> "/cmt/cm/test",
            "dataCenter" -> dcName))
      }
      spinCheck(100.millis,true)(Http.get(iofd, List("format" -> "json"))) { res =>
        res.status == 200 && Json.parse(res.payload).transform(uuidDateEraser).get == expected
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          Json.parse(res.payload).transform(uuidDateEraser).get shouldEqual expected
        }
      }
    })

    val verifyIngestOfInfotonInPathIoffd = ingestOf2Infotons.flatMap(_ => spinCheck(100.millis, true)(Http.get(ioffd, List("format" -> "json"))){ res =>
      Json.parse(res.payload).transform(fieldsSorter andThen (__ \ 'fields).json.pick).get == jsonObjForDelete
    }.map { res =>
      withClue(res) {
        Json.parse(res.payload).transform(fieldsSorter andThen (__ \ 'fields).json.pick).get should be (jsonObjForDelete)
      }
    })

    val deleteInfotonPath = verifyIngestOfInfotonInPathIoffd.flatMap(_ => {
      Http.delete(uri = ioffd,headers = tokenHeader).map { res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      }
    })

    val verify404 = deleteInfotonPath.flatMap(_ => scheduleFuture(indexingDuration){Http.get(ioffd, List("format" -> "json")).map { res =>
      withClue(res) {
        res.status should be(404)
      }
    }
    })

    val rewriteDeletedInfoton = verify404.flatMap(_ => {
      Http.post(ioffd, Json.stringify(jsonObjForDelete), None, Nil, ("X-CM-WELL-TYPE" -> "OBJ") :: tokenHeader).map { res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      }
    })

    val getRewrittenInfoton = rewriteDeletedInfoton.flatMap(_ => {
      spinCheck(100.millis,true)(Http.get(ioffd, List("format" -> "json"))){ res =>
        val payloadStr = (res.payload.map(_.toChar)).mkString
        if (payloadStr.startsWith("Infoton was deleted on")) false
        else
          Json.parse(res.payload).transform(fieldsSorter andThen (__ \ 'fields).json.pick).get == jsonObjForDelete
      }.map { res =>
        withClue(res) {
          Json.parse(res.payload).transform(fieldsSorter andThen (__ \ 'fields).json.pick).get  shouldEqual jsonObjForDelete
        }
      }
    })

    val failToDelete501InfotonsRecursively = spinCheckUntilWritesAreIndexed.flatMap(_ => Http.delete(uri = deletes501,headers = tokenHeader).map{ res =>
      withClue(res) {
        res.status should be(400)
      }
    })

    val failToDelete501InfotonsThroughUnderscoreIn = spinCheckUntilWritesAreIndexed.flatMap { _ =>
      val data = """<cmwell://cmt/cm/test/bulkDeletes/d501> <cmwell://meta/sys#fullDelete> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> ."""
      Http.post(_in, data, None, List("format" -> "ntriples"), tokenHeader).map { res =>
        withClue(res) {
          res.status should be(400)
        }
      }
    }

    val deleteSingleInfotonThroughUnderscoreIn = failToDelete501InfotonsRecursively.zip(failToDelete501InfotonsThroughUnderscoreIn).flatMap { _ =>
      val data = """<cmwell://cmt/cm/test/bulkDeletes/d501/i-501> <cmwell://meta/sys#fullDelete> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> ."""
      Http.post(_in, data, None, List("format" -> "ntriples"), tokenHeader).map { res =>
        res.status should be(200)
      }
    }

    val delete500Infotons = spinCheckUntilWritesAreIndexed.flatMap { _ =>
      Http.delete(uri = deletes500,headers = tokenHeader).map { res =>
        withClue(res) {
          Json.parse(res.payload) should be(jsonSuccess)
        }
      }
    }

    val verifyInfotonWasDeleted = deleteSingleInfotonThroughUnderscoreIn.flatMap(_ =>
      // gotta have format since the SPA returned isn't 404... just Pooh complaining...
      scheduleFuture(indexingDuration)(Http.get(deletes501 / "i-501", List("format"->"json"))).map { res =>
        withClue(res) {
          res.status should be(404)
        }
      }
    )


    val spinCheckDeletedInfotonIsConsistedInES = verifyInfotonWasDeleted.flatMap{ _ =>

      spinCheck(100.millis, true)(Http.get(deletes501, List("op" -> "search", "length" -> "1", "format" -> "json")))(
        res => (Json.parse(res.payload) \ "results" \ "total").as[Int] == 500
      ).map { res =>
        withClue(res) {
          (Json.parse(res.payload) \ "results" \ "total").as[Int] should be (500)
        }
      }
    }

    val delete500InfotonsThroughUnderscoreIn = spinCheckDeletedInfotonIsConsistedInES.flatMap(_ => {
      val data = """<cmwell://cmt/cm/test/bulkDeletes/d501> <cmwell://meta/sys#fullDelete> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> ."""
      Http.post(_in, data, None, List("format" -> "ntriples"), tokenHeader).map { res =>
        withClue(res) {
          res.status should be(200)
        }
      }
    })

    it("should post infotons to delete later")(ingestOf2Infotons)
    it("should upload 500 + 501 infotons to test deletes later")(massiveIngestOf1001Infotons)
    it("should spin-check until infoton is available")(spinCheckUntilWritesAreIndexed)
    it("should get the infoton 'InfoObjForDelete'")(verifyIngestOfInfotonInfoObjForDelete)
    it("delete only 1 pair of key:value attribute")(deleteKeyValuePair)
    it("should verify only one pair was deleted")(verifyKeyValuePairDeleted)
    it("should delete 1 value from array of values")(deleteFromArray)
    it("should get the json without deleted value")(getWithoutArrayValue)
    it("should delete all attributes")(deleteAllAttributes)
    it("should not get all attributes")(verifyAllAttributesDeleted)
    it("should get the infoton in Path ioffd")(verifyIngestOfInfotonInPathIoffd)
    it("should delete infoton path")(deleteInfotonPath)
    it("should not get a deleted infoton")(verify404)
    it("should re-write the deleted infoton")(rewriteDeletedInfoton)
    it("should get the infoton again")(getRewrittenInfoton)
    it("should fail to delete infoton containing 501 children with 400 error code")(failToDelete501InfotonsRecursively)
    it("should fail to delete infoton containing 501 children recursively via _in with 400 error code")(failToDelete501InfotonsThroughUnderscoreIn)
    it("should succeed to delete a single infoton via _in")(deleteSingleInfotonThroughUnderscoreIn)
    it("should succeed in deleting infoton containing 500 children")(delete500Infotons)
    it("should spin-check to get 404 for deleted infoton")(verifyInfotonWasDeleted)
    it("should spin-check to get <501 in total when searching")(spinCheckDeletedInfotonIsConsistedInES)
    it("should succeed to delete infoton containing 500 children recursively via _in")(delete500InfotonsThroughUnderscoreIn)
  }
}
