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

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Matchers, TryValues}
import play.api.libs.json._

import scala.concurrent.duration.DurationInt
import scala.io.Source

class FileInfotonTests extends AsyncFunSpec with Matchers with TryValues with Helpers with LazyLogging {
  describe("file infoton") {
    val path = cmt / "InfoFile4"
    val fileStr = Source.fromURL(this.getClass.getResource("/article.txt")).mkString
    val j = Json.obj("Offcourse" -> Seq("I can do it"),"I'm" -> Seq("a spellbinder"))

    val f0 = Http.post(path, fileStr, Some("text/plain;charset=UTF-8"), Nil, ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader).map { res =>
      withClue(res){
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }
    val f1 = f0.flatMap {_ => spinCheck(100.millis, true)(Http.get(path)){res =>
      new String(res.payload, StandardCharsets.UTF_8) == fileStr && res.contentType.takeWhile(_ != ';') == "text/plain"}
      .map { res =>
        withClue(res) {
          new String(res.payload, StandardCharsets.UTF_8) should be(fileStr)
          res.contentType.takeWhile(_ != ';') should be("text/plain")
        }
      }}
    val f2 = f1.flatMap(_ => Http.post(path, Json.stringify(j), None, Nil, ("X-CM-WELL-TYPE" -> "FILE_MD") :: tokenHeader)).map {res =>
      withClue(res) {
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }
    val f3 = f2.flatMap(_ => spinCheck(100.millis, true)(Http.get(path, List("format" -> "json"))){
      res =>
        val jsonResult = Json.parse(res.payload).transform(fieldsSorter andThen (__ \ 'fields).json.pick)
        jsonResult match {
          case JsSuccess(value, _) => value == j
          case JsError(_) => false
        }
    }.map{ res =>
        withClue(res) {
          Json
            .parse(res.payload)
            .transform(fieldsSorter andThen (__ \ 'fields).json.pick)
            .get shouldEqual j
        }
      }
    )
    val f4 = f3.flatMap(_ => Http.delete(uri = path, headers = tokenHeader).map { res =>
       withClue(res) {
         Json.parse(res.payload) should be(jsonSuccess)
       }
    })
    val lenna = cmt / "lenna"
    val f5 = {
      val lennaInputStream = this.getClass.getResource("/Lenna.png").openStream()
      Http.post(lenna / "Lenna.png", () => lennaInputStream, Some("image/png"), Nil, ("X-CM-WELL-TYPE" -> "FILE") :: tokenHeader).transform { res =>
        // first, close the stream
        lennaInputStream.close()
        withClue(res)(res.map { r =>
          Json.parse(r.payload) should be(jsonSuccess)
        })
      }
    }
    val f6 = spinCheck(100.millis,true,1.minute)(Http.get(lenna,List("op" -> "search","qp" -> "content.mimeType:image/png", "format" -> "json"))){ res =>
        res.status match {
          case 503 => Recoverable
          case 200 => {
            val j = Json.parse(res.payload) \ "results"
            (j \ "total": @unchecked) match {
              case JsDefined(JsNumber(n)) => n.intValue() == 1
            }
          }
          case _ => UnRecoverable
        }
      }.map { res =>
      withClue(res) {
        val j = Json.parse(res.payload) \ "results"
        (j \ "infotons": @unchecked) match {
          case JsDefined(JsArray(arr)) => (arr.head \ "system" \ "path": @unchecked) match {
            case JsDefined(JsString(lennaPath)) =>
              lennaPath shouldEqual "/cmt/cm/test/lenna/Lenna.png"
          }
        }
      }
    }

    it("should put File infoton")(f0)
    it("should get previously inserted file with text/plain mimetype")(f1)
    it("should put file infoton metadata")(f2)
    it("should get file infoton metadata")(f3)
    it("should delete file infoton")(f4)
    it("should upload Lenna.png image")(f5)
    it("should search by content.mimeType")(f6)
  }
}
