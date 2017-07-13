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


package security.httpauth

import k.grid.Grid
import play.api.mvc.Request
import security._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random
import scala.concurrent.duration._

/**
  * Created by yaakov on 12/16/15.
  */
trait DigestHttpAuthentication {

  import cmwell.util.string.Hash.md5

  implicit val timeout = akka.util.Timeout(10.seconds)

  val opaque = md5("any string will do. even 42.")
  val userInfotonPropName = "digest2"
  val ha2 = md5Concat("GET", "/_login")

  def initialDigestHeader = {
    val newNonce = md5(Random.alphanumeric.take(26).toString)
    Grid.serviceRef(classOf[NoncesManager].getName) ! AddNonce(newNonce)
    DigestServerHeader("cmwell", newNonce, opaque)
  }

  // This is a Server Side Implementation for HTTP Digest Authentication, according to RFC 2617
  // which is a "challenge-response" handshake, like this:
  //
  //  Request:  GET /_login (no headers)
  //  Response: 401 with initial Digest Header { realm, nonce, opaque } // that's the "challenge"
  //  Request:  GET /_login with Digest Header { username, nonce, opaque, response } // that's the "response"
  //  Response: 200 / 403 according to `response` value
  //
  // Where:
  //  realm is "cmwell"
  //  opaque is md5 of a const string
  //  nonce is md5 that should only be used once
  //  response is: md5(s"$HA1:$nonce:$HA2") where HA1 is md5(s"$username:$realm:$password")
  //                                          and HA2 is md5("GET:/_login")
  //
  // We keep HA1 per user inside its UserInfoton as "digest2"
  //
  def digestAuthenticate(authCache: AuthCache)(req: Request[_])(implicit ec: ExecutionContext): Future[DigestStatus] = {
    import akka.pattern.ask

    req.headers.get("Authorization") match {
      case None => Future.successful(DigestStatus(isAuthenticated = false, ""))
      case Some(authHeader) => {
        val header = DigestHeaderUtils.fromClientHeaderString(authHeader)
        (Grid.serviceRef(classOf[NoncesManager].getName) ? ConsumeNonce(header.nonce)).mapTo[NonceStatus].map {
          case NonceConsumed if header.opaque == opaque => authCache.getUserInfoton(header.username) match {
            case None => DigestStatus(isAuthenticated = false, "")
            case Some(user) =>
              val ha1 = (user \ userInfotonPropName).asOpt[String].getOrElse("")
              val calculatedResponse = md5Concat(ha1, header.nonce, ha2)
              DigestStatus(isAuthenticated = calculatedResponse == header.response, header.username)
          }
          case _ => DigestStatus(isAuthenticated = false, "")
        }
      }
    }
  }

  def md5Concat(values: String*) = md5(values.mkString(":"))
}

case class DigestStatus(isAuthenticated: Boolean, username: String)
