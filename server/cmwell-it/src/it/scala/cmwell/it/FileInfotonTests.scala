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

import cmwell.util.concurrent.SimpleScheduler._
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Matchers}
import play.api.libs.json._

import scala.io.Source

class FileInfotonTests extends AsyncFunSpec with Matchers with Helpers with LazyLogging {
  describe("file infoton") {
    val path = cmt / "InfoFile4"
    val fileStr = Source.fromURL(this.getClass.getResource("/article.txt")).mkString
    val j = Json.obj(("Offcourse" -> Seq("I can do it")),("I'm" -> Seq("a spellbinder")))

    val f0 = Http.post(path, fileStr, Some("text/plain;charset=UTF-8"), Nil, ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader).map { res =>
      withClue(res){
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }
    val f1 = f0.flatMap(_ => scheduleFuture(indexingDuration)(Http.get(path))).map { res =>
      withClue(res) {
        new String(res.payload, "UTF-8") should be(fileStr)
        res.contentType.takeWhile(_ != ';') should be("text/plain")
      }
    }
    val f2 = f1.flatMap(_ => Http.post(path, Json.stringify(j), None, Nil, ("X-CM-WELL-TYPE" -> "FILE_MD") :: tokenHeader)).map {res =>
      withClue(res) {
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }
    val f3 = f2.flatMap(_ => scheduleFuture(indexingDuration) {
      Http.get(path, List("format" -> "json")).map{ res =>
        withClue(res) {
          Json
            .parse(res.payload)
            .transform(fieldsSorter andThen (__ \ 'fields).json.pick)
            .get shouldEqual j
        }
      }
    })
    val f4 = f3.flatMap(_ => Http.delete(uri = path, headers = tokenHeader).map { res =>
       withClue(res) {
         Json.parse(res.payload) should be(jsonSuccess)
       }
    })

    it("should put File infoton")(f0)
    it("should get previously inserted file with text/plain mimetype")(f1)
    it("should put file infoton metadata")(f2)
    it("should get file infoton metadata")(f3)
    it("should delete file infoton")(f4)
  }
}
