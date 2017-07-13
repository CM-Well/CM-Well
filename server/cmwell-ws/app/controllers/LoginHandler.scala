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


package controllers

import cmwell.ws.Settings
import security.Token
import org.joda.time.DateTime
import play.api.libs.json.Json
import play.api.mvc._
import security.httpauth._
import security.{AuthCache, Authentication}
import javax.inject._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

@Singleton
class LoginHandler  @Inject()(authCache: AuthCache)(implicit ec: ExecutionContext) extends Controller with BasicHttpAuthentication with DigestHttpAuthentication {
  private val notAuthenticated = Unauthorized("Not authenticated.\n")

  def login: Action[AnyContent] = Action.async { implicit req =>
    val exp: Option[DateTime] = req.getQueryString("exp").map(parseShortFormatDuration)

    def whichAuthType: Option[HttpAuthType] = {
      req.headers.get("authorization").map { h => if(h.contains("Digest")) Digest else Basic }
    }

    def loginDigest = digestAuthenticate(authCache)(req).map(status => if (status.isAuthenticated) grantToken(status.username, exp) else notAuthenticated)

    def loginBasic = {
      decodeBasicAuth(req.headers("authorization")) match {
        case (username, pass) => {
          authCache.getUserInfoton(username) match {
            case Some(user) if Authentication.passwordMatches(user, pass) => grantToken(username, exp)
            case _ => notAuthenticated
          }
        }
        case _ => notAuthenticated
      }
    }

    // default (`case None` below) is Digest, s.t. client can be provided with the challenge.
    // However, Basic Auth is supported, if client initiates it (e.g. `curl -u user:pass` etc.)
    //
    // This provides two different options to login:
    //
    //  1.
    //       ------------[GET]------------>
    //       <-----[digest Challenge]------
    //       ------[digest Response]------>
    //       <---------[200/403]-----------
    //
    //  2.
    //       ---[GET + Basic user:pass]--->
    //       <---------[200/403]-----------

    val penalty = Settings.loginPenalty.seconds

    whichAuthType match {
      case Some(Basic) => cmwell.util.concurrent.delayedTask(penalty)(loginBasic)
      case Some(Digest) => cmwell.util.concurrent.delayedTask(penalty/2)(loginDigest).flatMap(identity)
      case None => cmwell.util.concurrent.delayedTask(penalty)(Unauthorized("Please provide your credentials.\n").withHeaders("WWW-Authenticate" -> initialDigestHeader.toString))
    }
  }

  // SAML2 POC
//  def sso: Action[AnyContent] = RequiresAuthentication("Saml2Client") { profile =>
//    Action { implicit request =>
//
//      ???
//      // this is only a POC.
//      // to make it Production-ready, please first implement configSSO method in Global.scala with proper a callback host (the IdP)
//
//      val username = profile.getAttribute("uid").asInstanceOf[util.ArrayList[String]].get(0)
//      grantTokenWithHtmlRedirectToSPA(username)
//    }
//  }

  //  private def grantToken(username: String) = Future(Ok(s"Token is hereby granted for $username.").withHeaders("X-CM-WELL-TOKEN" -> Authentication.generateToken(username)))
  private def grantToken(username: String, expiry: Option[DateTime]) = {
    Try(Token.generate(authCache, username, expiry)) match {
      case Success(token) => Ok(Json.obj("token" -> token))
      case Failure(err) => wsutil.exceptionToResponse(err)
    }
  }

  private def grantTokenWithHtmlRedirectToSPA(username: String) = Redirect(s"/?token=${Token.generate(authCache, username)}")

  private def parseShortFormatDuration(shortFormatDuration: String): DateTime = {
    val durs = Seq("d", "h", "m").map(part => part -> s"(\\d+)(?i)$part".r.findFirstMatchIn(shortFormatDuration).map(_.group(1).toInt).getOrElse(0)).toMap
    DateTime.now().
      plusDays(durs("d")).
      plusHours(durs("h")).
      plusMinutes(durs("m"))
  }
}