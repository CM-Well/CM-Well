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

import cmwell.util.http.SimpleResponse
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.binary.Base64
import org.scalatest.{FunSpec, Matchers}
import play.api.libs.json.{JsArray, JsValue, Json}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by yaakov on 2/1/15.
 */
class AuthTests extends FunSpec with Matchers with Helpers with LazyLogging {

  val _login = cmw / "_login"
  val exampleObj = Json.obj("header" -> "TestHeader", "title" -> "TestTitle").toString

  def waitAndExtractBody(req: Future[SimpleResponse[Array[Byte]]]) = waitAndExtractStatusAndBody(req)._2
  def waitAndExtractStatus(req: Future[SimpleResponse[Array[Byte]]]) = waitAndExtractStatusAndBody(req)._1

  def waitAndExtractStatusAndBody(req: Future[SimpleResponse[Array[Byte]]]) = {
    val res = Await.result(req, requestTimeout)
    res.status -> new String(res.payload, "UTF-8")
  }


  describe("CM-Well Auth") {

    var tokenForCustomUser = "" -> ""
    var tokenForOverwriter = "" -> ""
    var tokenForPriorityWriter = "" -> ""

    describe("Preparing data...") {
      def buildPathObj(path: String, recursive: Boolean, allow: Boolean, permissions: String) = {
        Json.toJson(
          Map(
            "id" -> Json.toJson(path),
            "recursive" -> Json.toJson(recursive),
            "sign" -> Json.toJson(if (allow) "+" else "-"),
            "permissions" -> Json.toJson(permissions)))
      }

      it("should add a custom user") {
        val path = cmw / "meta" / "auth" / "users" / "TestUser"
        val saltedPassword = "$2a$10$hrLLP9IUUJyUHP5skFtpC.LG.WFvcKHkbrcymcg0yPWXxg08z2mhW" // bcrypt("myPassword"+salt)
        val noSuchRole = "No-Such-Role" // testing WS is not failing with 500 in case of Broken Role Reference
        val userInfoton = Json.toJson(Map("digest" -> Json.toJson(saltedPassword), "paths" -> Json.toJson(Seq(
            buildPathObj("/tests", recursive = true, allow = true, "rw"),
            buildPathObj("/tests/some-sensitive-data", recursive = true, allow = false, "rw"),
            buildPathObj("/top-secret", recursive = true, allow = false, "rw"),
            buildPathObj("/employees", recursive = false, allow = false, "rw"),
            buildPathObj("/employees/himself", recursive = false, allow = true, "rw"),
            buildPathObj("/levels/empty", recursive = true, allow = true, ""),
            buildPathObj("/levels/r", recursive = true, allow = true, "r"),
            buildPathObj("/levels/w", recursive = true, allow = true, "w"),
            buildPathObj("/levels/rw", recursive = true, allow = true, "rw")
          )), "roles" -> Json.toJson(Seq(Json.toJson("QA-News-Reader"), Json.toJson("QA-WriteToDropBox"), Json.toJson(noSuchRole))),
          "rev"->Json.toJson(5))).toString

        val res = waitAndExtractBody(Http.post(path, userInfoton, Some("application/json"), headers = ("X-CM-Well-Type" -> "File") :: tokenHeader))
        Json.parse(res) should be(jsonSuccess)
      }

      it("should add a user with Overwrite permission") {
        val path = cmw / "meta" / "auth" / "users" / "Overwriter"
        val saltedPassword = "$2a$10$hrLLP9IUUJyUHP5skFtpC.LG.WFvcKHkbrcymcg0yPWXxg08z2mhW" // bcrypt("myPassword"+salt)
        val userInfoton = Json.toJson(Map("digest" -> Json.toJson(saltedPassword), "paths" -> Json.toJson(Seq(
            buildPathObj("/", recursive = true, allow = true, "rw")
          )), "operations" -> Json.toJson(Seq("Overwrite")))).toString
        val res = waitAndExtractBody(Http.post(path, userInfoton, Some("application/json"), headers = ("X-CM-Well-Type" -> "File") :: tokenHeader))
        Json.parse(res) should be(jsonSuccess)
      }

      it("should add a user with PriorityWrite permission") {
        val path = cmw / "meta" / "auth" / "users" / "PriorityWriter"
        val saltedPassword = "$2a$10$hrLLP9IUUJyUHP5skFtpC.LG.WFvcKHkbrcymcg0yPWXxg08z2mhW" // bcrypt("myPassword"+salt)
        val userInfoton = Json.toJson(Map("digest" -> Json.toJson(saltedPassword), "paths" -> Json.toJson(Seq(
            buildPathObj("/", recursive = true, allow = true, "rw")
          )), "operations" -> Json.toJson(Seq("PriorityWrite")))).toString
        val res = waitAndExtractBody(Http.post(path, userInfoton, Some("application/json"), headers = ("X-CM-Well-Type" -> "File") :: tokenHeader))
        Json.parse(res) should be(jsonSuccess)
      }

      it("should add a custom role 1") {
        val path = cmw / "meta" / "auth" / "roles" / "QA-News-Reader"
        val roleInfoton = Json.toJson(Map("paths" -> Json.toJson(Seq(
          buildPathObj("/qa", recursive = false, allow = true, "r"),
          buildPathObj("/tests/news", recursive = true, allow = true, "r")
        )))).toString
        val res = waitAndExtractBody(Http.post(path, roleInfoton, Some("application/json"), headers = ("X-CM-Well-Type" -> "File") :: tokenHeader))
        Json.parse(res) should be(jsonSuccess)
      }

      it("should add a custom role 2") {
        val path = cmw / "meta" / "auth" / "roles" / "QA-WriteToDropBox"
        val roleInfoton = Json.toJson(Map("paths" -> Json.toJson(Seq(
          buildPathObj("/qa/dropbox", recursive = true, allow = true, "w"),
          buildPathObj("/qa", recursive = true, allow = false, "r")
        )))).toString
        val res = waitAndExtractBody(Http.post(path, roleInfoton, Some("application/json"), headers = ("X-CM-Well-Type" -> "File") :: tokenHeader))
        Json.parse(res) should be(jsonSuccess)
      }

      it("should add an empty UserInfoton") {
        val path = cmw / "meta" / "auth" / "users" / "EmptyTestUser"
        val saltedPassword = "$2a$10$hrLLP9IUUJyUHP5skFtpC.LG.WFvcKHkbrcymcg0yPWXxg08z2mhW" // bcrypt("myPassword"+salt)
        val userInfoton = Json.toJson(
          Map(
            "digest" -> Json.toJson(saltedPassword),
            "paths" -> Json.toJson(Seq[String]()),
            "roles" -> Json.toJson(Seq[String]()))).toString
        val res = waitAndExtractBody(Http.post(path, userInfoton, Some("application/json"), headers = ("X-CM-Well-Type" -> "File") :: tokenHeader))
        Json.parse(res) should be(jsonSuccess)
      }

      val exampleData = {
        Set(
          cmw / "SafeZoneInfoton1",
          cmw / "tests" / "infoton1",
          cmw / "tests" / "some-sensitive-data" / "secret-infoton1",
          cmw / "top-secret" / "secret-infoton1",
          cmw / "top-secret" / "secret-infoton2",
          cmw / "employees" / "himself",
          cmw / "levels" / "r" / "infoton1",
          cmw / "levels" / "rw" / "infoton1",
          cmw / "tests" / "news" / "new-infoton-1"
        ) map { path =>
          waitAndExtractBody(Http.post(path, exampleObj, Some("application/json"), headers = ("X-CM-Well-Type" -> "File") :: tokenHeader))
        }
      }

      it("should add some example data") {
        all(exampleData.map(Json.parse)) should be(jsonSuccess)
      }

      it("should reset AuthCache once all user/role data was ingested") {
        spinCheck(100.millis, true)(Http.get(cmw / "_auth", List("op" -> "invalidate-cache"), tokenHeader))( res => Json.parse(res.payload) == jsonSuccess)
          .map{ res => Json.parse(res.payload) should be(jsonSuccess) }
      }

      it("should be able to login with the CustomUser and receive a token") {
        val res = waitAndExtractBody(Http.get(_login, headers = basicAuthHeader("TestUser", "myPassword")))
        val jwt = (Json.parse(res) \ "token").as[String]
        tokenForCustomUser = "X-CM-WELL-TOKEN" -> jwt
      }

      it("should be able to login with the Overwriter user and receive a token") {
        val res = waitAndExtractBody(Http.get(_login, headers = basicAuthHeader("Overwriter", "myPassword")))
        val jwt = (Json.parse(res) \ "token").as[String]
        tokenForOverwriter = "X-CM-WELL-TOKEN" -> jwt
      }

      it("should be able to login with the PriorityWriter user and receive a token") {
        val res = waitAndExtractBody(Http.get(_login, headers = basicAuthHeader("PriorityWriter", "myPassword")))
        val jwt = (Json.parse(res) \ "token").as[String]
        tokenForPriorityWriter = "X-CM-WELL-TOKEN" -> jwt
      }
    }

    describe("Anonymous User (AKA \"Safe Zone\")") {
      // note there is no token header present in these 3 tests:

      it("can read Infotons") {
        val path = cmw / "SafeZoneInfoton1"
        waitAndExtractStatus(Http.get(path)) should be(200)
      }

      it("can not write") {
        val path = cmw / "SafeZoneInfoton2"
        val res = waitAndExtractStatus(Http.post(path, exampleObj, Some("application/json"), headers = Seq("X-CM-WELL-TYPE" -> "OBJ")))
        res should be(403)
      }

      it("can not purge") {
        val path = cmw / "SafeZoneInfoton1"

        val res1 = waitAndExtractStatus(Http.get(path, Seq("op"->"purge-all")))
        res1 should be(403)

        // purge-history is temporary not supported
//        val res2 = waitAndExtractStatus(Http.get(path, Seq("op"->"purge-history")))
//        res2 should be(403)
      }

      it("cannot read /meta/auth") {
        val path = cmw / "meta" / "auth"
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json")))
        res should be(403)
      }
    }

    def basicAuthHeader(user: String, password: String) = Seq("Authorization" -> ("Basic " + new Base64(0).encodeToString(s"$user:$password".getBytes)))

    describe("Login: should not accept bad credentials") {
      it("should stay 401 for non-existing user") {
        val res = waitAndExtractStatus(Http.get(_login, headers = basicAuthHeader("Felix", "Amos")))
        res should be(401)
      }

      it("should stay 401 for existing user if password does not match") {
        val res = waitAndExtractStatus(Http.get(_login, headers = basicAuthHeader("TestUser", "qwerty"))) // his password actually is "myPassword", not "qwerty"
        res should be(401)
      }
    }

    describe("bad token") {
      // scalastyle:off
      Map("an expired token" -> "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJUZXN0VXNlciIsImV4cCI6MTQyMjgyODAwMDAwMCwicmV2IjoxfQ.0uKmaV6di_nHI1EJYW0xs5CyP4De2yU6C2KuPu7dd3E",
        "a token with bad rev num" -> "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJUZXN0VXNlciIsImV4cCI6NDYwNDk0MDAwMDAwMCwicmV2IjozfQ.MLMO8VRf0OXnI2o7wIcOKuaZD2deaxi3qO4fJgnQ8Lc",
        "a token with wrong signature" -> "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJUZXN0VXNlciIsImV4cCI6NDYwNDk0MDAwMDAwMCwicmV2IjoxfQ.VquZLMOHuTvHv495j0eacHpy2ikzwAKEaAe1U7nl5fg",
        "a token generated for a non-existing user" -> "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJOb1N1Y2hVc2VyIiwiZXhwIjo0NjA0OTQwMDAwMDAwLCJyZXYiOjF9.nZKBKtUATXrNW1d6sw4vPiD164m2GbE0wy02Irkntr8"
      ).foreach { case (testCase, token) =>
        it(s"should not accept $testCase") {
          waitAndExtractStatus(Http.get(cmw / "whatever", headers = Seq("X-CM-WELL-TOKEN" -> token))) should be(403)
        }
      }
      // scalastyle:on
    }

    describe("user with an empty permissions set") {
      var tokenForEmptyUser = "" -> ""

      it("should be able to login with the EmptyUser and receive a token") {
        val res = waitAndExtractBody(Http.get(_login, headers = basicAuthHeader("EmptyTestUser", "myPassword")))
        val jwt = (Json.parse(res) \ "token").as[String]
        tokenForEmptyUser = "X-CM-WELL-TOKEN" -> jwt
      }

      it("should not be able to read anything") {
        waitAndExtractStatus(Http.get(cmw / "whatever", headers = Seq(tokenForEmptyUser))) should be(403)
      }

      it("should not be able to write anything") {
        val path = cmw / "Infoton777"
        val res = waitAndExtractStatus(Http.post(path, exampleObj, Some("application/json"), Seq("X-CM-WELL-TYPE" -> "OBJ")))
        res should be(403)
      }
    }

    describe("/ii/*") {
      var readOnlyUuid = ""
      var allowedUuid = ""
      var notAllowedUuid = ""

      it("should fetch (as root) a uuid of an allowed path") {
        val path = cmw / "tests" / "infoton1"
        val res = waitAndExtractBody(Http.get(path, Seq("format"->"json"), tokenHeader))
        allowedUuid = (Json.parse(res) \ "system" \ "uuid").as[String]
        allowedUuid should not be ("")
      }

      it("should fetch (as root) a uuid of a not-allowed path") {
        val path = cmw / "top-secret" / "secret-infoton1"
        val res = waitAndExtractBody(Http.get(path, Seq("format"->"json"), tokenHeader))
        notAllowedUuid = (Json.parse(res) \ "system" \ "uuid").as[String]
        notAllowedUuid should not be ("")
      }

      it("should fetch (as a custom user) a uuid of a read-only path") {
        val path = cmw / "levels" / "r" / "infoton1"
        val res = waitAndExtractBody(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        readOnlyUuid = (Json.parse(res) \ "system" \ "uuid").as[String]
        readOnlyUuid should not be ("")
      }

      it("should allow reading an allowed Infoton Path") {
        val path = cmw / "ii" / allowedUuid
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        res should be(200)
      }

      it("should not allow purging a read-only-allowed Infoton Path") {
        val path = cmw / "ii" / readOnlyUuid
        val res = waitAndExtractStatus(Http.get(path, Seq("op"->"purge"), Seq(tokenForCustomUser)))
        res should be(403)
      }

      it("should not allow reading a not-allowed Infoton Path") {
        val path = cmw / "ii" / notAllowedUuid
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        res should be(403)
      }
    }

    describe("Permission Levels (Read vs. Write)") {
      val levels = cmw / "levels"
      val emptyPath = levels / "empty" / "whatever"
      val readOnlyPath = levels / "r" / "infoton1"
      val writeOnlyPath = levels / "w" / "infoton1"
      val readWritePath = levels / "rw" / "infoton1"

      it("should not be able to read nor write if there's no levels defined") {
        // weird case, but for the sake of soundness...
        val readRes = waitAndExtractStatus(
          Http.get(
            emptyPath,
            Seq("format"->"json"),
            Seq(tokenForCustomUser)))
        readRes should be(403)

        val writeRes = waitAndExtractStatus(
          Http.post(
            emptyPath,
            exampleObj, Some("application/json"),
            headers = Seq("X-CM-WELL-TYPE" -> "OBJ")))
        writeRes should be(403)
      }

      it("should be able to read if has Read Only permission") {
        val readRes = waitAndExtractStatus(
          Http.get(
            readOnlyPath,
            Seq("format"->"json"),
            Seq(tokenForCustomUser)))
        readRes should be(200)
      }

      it("should not be able to write if has Read Only permission") {
        val res = waitAndExtractStatus(
          Http.post(
            readOnlyPath,
            exampleObj,
            Some("application/json"),
            headers = Seq("X-CM-WELL-TYPE" -> "OBJ", tokenForCustomUser)))
        res should be(403)
      }

      it("should be able to write if has Write Only permission") {
        // weird case, but for the sake of soundness...
        val res = waitAndExtractStatus(
          Http.post(
            writeOnlyPath,
            exampleObj,
            Some("application/json"),
            headers = Seq("X-CM-WELL-TYPE" -> "OBJ", tokenForCustomUser)))
        res should be(200)
      }

      it("should not be able to read if has Write Only permission") {
        // weird case, but for the sake of soundness...
        val res = waitAndExtractStatus(
          Http.get(
            writeOnlyPath,
            Seq("format"->"json"),
            Seq(tokenForCustomUser)))
        res should be(403)
      }

      it("should be able to read and write if has all permissions") {
        val readRes = waitAndExtractStatus(
          Http.get(
            readWritePath,
            Seq("format"->"json"),
            Seq(tokenForCustomUser)))
        readRes should be(200)

        val writeRes = waitAndExtractStatus(
          Http.post(
            readWritePath,
            exampleObj,
            Some("application/json"),
            headers = Seq("X-CM-WELL-TYPE" -> "OBJ", tokenForCustomUser)))
        writeRes should be(200)
      }

      ignore("should be able to purge if has write permission") {
        indexingDuration.fromNow.block // to make sure there are no pending writes for this Infoton

        val purgeReq = waitAndExtractStatus(Http.get(readWritePath, Seq("op"->"purge-all"), Seq(tokenForCustomUser)))
        purgeReq should be(200)

        // verify it's gone
        val readRes = waitAndExtractStatus(Http.get(readWritePath, Seq("format"->"json"), Seq(tokenForCustomUser)))
        readRes should be(404)
      }
    }

    describe("Custom User Permissions") {
      // next two tests: testing allow with recursive=true
      it("should allow /tests") {
        val path = cmw / "tests"
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        res should be(200)
      }

      it("should allow /tests/subfolder") {
        val path = cmw / "tests" / "infoton1"
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        res should be(200)
      }

      // ensuring 'deny' is more powerful than 'allow'
      it("should not allow /tests/some-sensitive-data") {
        val path = cmw / "tests" / "some-sensitive-data" / "secret-infoton1"
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        res should be(403)
      }

      // next two tests: testing deny with recursive=true
      it("should not allow /top-secret") {
        val path = cmw / "top-secret"
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        res should be(403)
      }

      it("should not allow /top-secret/subfolder") {
        val path = cmw / "top-secret" / "whatever"
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        res should be(403)
      }

      // next two tests: testing deny with recursive=false
      it("should not allow /employees") {
        val path = cmw / "employees"
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        res should be(403)
      }

      it("should allow /employees/himself") {
        val path = cmw / "employees" / "himself"
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        res should be(200)
      }
    }

    describe("Custom Role Permissions") {
      // test case:
      //  role1:
      //    allow /tests/news
      //    allow /qa
      //  role2:
      //    deny  /qa
      //    allow /qa/dropbox

      it("should be able to read /tests/news") {
        val path = cmw / "tests" / "news" / "new-infoton-1"
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        res should be(200)
      }

      it("should not be able to read /qa") {
        val path = cmw / "qa"
        val res = waitAndExtractStatus(Http.get(path, Seq("format"->"json"), Seq(tokenForCustomUser)))
        res should be(403)
      }

      it("should be able to write to /qa/dropbox") {
        val path = cmw / "qa" / "dropbox" / "new-infoton"
        val res = waitAndExtractStatus(Http.post(path, exampleObj, Some("application/json"), headers = Seq("X-CM-WELL-TYPE" -> "OBJ", tokenForCustomUser)))
        res should be(200)
      }
    }

    describe("Filtering paths for _in and _out") {
      it("should filter not-allowed paths: /_out") {

        implicit class JsValueExtensions(v: JsValue) {
          def getArr(prop: String): Seq[JsValue] = ((v \ prop).get: @unchecked) match { case JsArray(seq) => seq }
        }

        val payload = Seq("/tests/infoton1", "/top-secret/secret-infoton1", "/top-secret/secret-infoton2").mkString("\n")
        val res = Json.parse(waitAndExtractBody(Http.post(_out, payload, Some("text/plain"), Seq("format"->"json"),  Seq(tokenForCustomUser))))
        res.getArr("infotons").length should be(1)
        res.getArr("irretrievablePaths").length should be(2)
      }

      val data =
        """{
          |  "@graph" : [
          |  {
          |    "@id" : "http://cm-well.clearforest.com/tests/infoton7",
          |    "Foo" : "Bar1"
          |  },
          |  {
          |    "@id" : "http://cm-well.clearforest.com/top-secret/secret-infoton3",
          |    "Foo" : [ "Bar2", "Bar3" ]
          |  }],
          |  "@context" : {
          |    "modifiedDate" : {
          |      "@id" : "http://cm-well.clearforest.com/meta/sys#modifiedDate",
          |      "@type" : "http://www.w3.org/2001/XMLSchema#dateTime"
          |    },
          |    "id" : "http://cm-well.clearforest.com/meta/nn#id",
          |    "Foo" : "http://cm-well.clearforest.com/meta/nn#Foo"
          |  }
          |}""".stripMargin

      it("should filter not-allowed paths: /_in") {
        val res = waitAndExtractStatus(Http.post(_in, data, None, Seq("format"->"jsonld"),  Seq(tokenForCustomUser)))
        res should be(403)
      }

      it("should not allow invalid token for _in") {
        val badSignedToken = tokenForCustomUser._1 -> (tokenForCustomUser._2.split('.').take(2).mkString(".") + ".invalid-signature")
        waitAndExtractStatus(Http.post(_in, data, None, Seq("format"->"jsonld"),  Seq(badSignedToken))) should be(403)
      }
    }

    describe("Providing lastModified to _in") {
      // scalastyle:off
      val daisyDuckWithLastModified = s"""<http://example.org/Individuals/DaisyDuck> <http://www.tr-lbd.com/bold#active> "false" .
                                        |<http://example.org/Individuals/DaisyDuck> <http://purl.org/vocab/relationship/colleagueOf> <http://example.org/Individuals/BruceWayne> .
                                        |<http://example.org/Individuals/DaisyDuck> <${cmw.url}/meta/sys#lastModified> "2015-11-21T19:09:25.508Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .""".stripMargin
      // scalastyle:on
      it("should allow with permission") {
        val resp = Json.parse(waitAndExtractBody(Http.post(_in, daisyDuckWithLastModified, None, Seq("format" -> "ntriples"), Seq(tokenForOverwriter))))
        resp should be(jsonSuccess)
      }
    }

    describe("priority write") {
      val priorityQueryParam = "priority" -> ""
      val priorityData = """<http://example.org/Individuals/DaisyDuck1> <http://www.tr-lbd.com/bold#active> "false" ."""

      it("should not allow priority write without a valid token supplied") {
        val (status, body) = waitAndExtractStatusAndBody(
          Http.post(
            _in,
            priorityData,
            None,
            Seq("format" -> "ntriples", priorityQueryParam),
            Seq(tokenForCustomUser)))
        status should be(403)
        Json.parse(body) should be(Json.parse("""{"success":false,"message":"User not authorized for priority write"}"""))
      }

      it("should allow priority write with a valid token supplied") {
        val resp = Json.parse(
          waitAndExtractBody(
            Http.post(
              _in,
              priorityData,
              None,
              Seq("format" -> "ntriples", priorityQueryParam),
              Seq(tokenForPriorityWriter))))
        resp should be(jsonSuccess)
      }
    }
  }
}
