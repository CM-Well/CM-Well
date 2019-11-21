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
import org.scalatest.{AsyncFunSpec, EitherValues, Inspectors, Matchers, OptionValues}
import scala.concurrent.duration.DurationInt

class ServicesRoutesTests extends AsyncFunSpec with Matchers with Helpers with LazyLogging {

  import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler

  describe("Services Routes") {
    val ingestServiceInfotonsAndRefreshCache = {
      val totalServices = 1
      val data =
        """@prefix nn:    <cmwell://meta/nn#> .
          |@prefix sys:   <cmwell://meta/sys#> .
          |@prefix o:     <cmwell://meta/services/> .
          |
          |o:PermID  a                 sys:Redirection ;
          |        nn:route            "/permid/" ;
          |        nn:sourcePattern    "/permid/(.*)" ;
          |        nn:replacement      "/graph.link/PID-$1" .""".stripMargin


      Http.post(_in, data, Some("text/rdf+turtle;charset=UTF-8"), List("format" -> "ttl"), tokenHeader).flatMap { _ =>
        spinCheck(100.millis, true)(Http.get(cmw / "meta" / "services", List("op" -> "stream")))(_.payload.trim().lines.size == totalServices)
      }.flatMap(_ => Http.get(cmw / "_services-cache", List("op" -> "refresh")))
    }

    it("Should get an Infoton via Service Route of Redirection type") {
      // testing redirection: GET /permid/XYZ should return content of /graph.link/PID-XYZ

      val payload = "Hello World 789"
      val actualPath = cmw / "graph.link" / "PID-789"

      val ingestTargetInfoton = Http.post(actualPath, payload, Some("text/plain"),
        headers = tokenHeader :+ "X-CM-Well-Type" -> "File").flatMap { _ =>
        spinCheck(100.millis, true)(Http.get(actualPath))(_.status == 200)
      }

      (ingestTargetInfoton zip ingestServiceInfotonsAndRefreshCache).flatMap { _ =>
        Http.get(cmw / "permid" / "789").map(_.payload should be(payload))
      }
    }
  }
}
