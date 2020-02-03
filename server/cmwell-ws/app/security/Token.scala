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
package security


import cmwell.ws.Settings
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import pdi.jwt.{Jwt, JwtAlgorithm, JwtClaim, JwtOptions}
import security.Token.{secret, secret2}

import scala.util.Try

class Token(jwt: String, authCache: EagerAuthCache) {
  private val requiredClaims = Set("sub", "exp")

  private val claimsSet = {
    val decodedJ = Jwt.decodeRaw(jwt, JwtOptions(signature = false))

    if (decodedJ.isFailure)
      throw new IllegalArgumentException("Given string was not in JWT format")

    val decoded = ujson.read(decodedJ.get)

    if (!(requiredClaims.map(c => Try(decoded(c))).forall(_.isSuccess)))
      throw new IllegalArgumentException("Mandatory claims are missing from token")

    decoded
  }

  val username = claimsSet("sub").str
  val expiry = new DateTime(claimsSet("exp").num.toLong)

  def isValid = {
    (Jwt.isValid(jwt, secret, Seq(JwtAlgorithm.HS256)) || Jwt.isValid(jwt, secret2, Seq(JwtAlgorithm.HS256))) &&
    expiry.isAfterNow &&
    (Try(claimsSet("rev").num.toInt).recover{case _ => 0}.get >=
      Token.getUserRevNum(username, authCache) || username == "root") // root has immunity to revision revoking
  }

  override def toString = s"Token(${claimsSet})"
}

object Token {
  def apply(jwt: String, authCache: EagerAuthCache) = Try(new Token(jwt, authCache)).toOption

  // not using ws.Settings, so it'd be available from `sbt ws/console`
  private lazy val secret = ConfigFactory.load().getString("play.http.secret.key")
  // not using ws.Settings, so it'd be available from `sbt ws/console`
  private lazy val secret2 = ConfigFactory.load().getString("cmwell.ws.additionalSecret.key")
  private val jwtHeader = JwtAlgorithm.HS256

  private def getUserRevNum(username: String, authCache: EagerAuthCache) =
    authCache.getUserInfoton(username).flatMap(u => (u \ "rev").asOpt[Int]).getOrElse(0)

  def generate(authCache: EagerAuthCache,
               username: String,
               expiry: Option[DateTime] = None,
               rev: Option[Int] = None,
               isAdmin: Boolean = false): String = {
    val maxDays = Settings.maxDaysToAllowGenerateTokenFor
    if (!isAdmin && expiry.isDefined && expiry.get.isAfter(DateTime.now.plusDays(maxDays))) {
      throw new IllegalArgumentException(s"Token expiry must be less than $maxDays days")
    }
    if (!isAdmin && rev.isDefined) {
      throw new IllegalArgumentException("rev should only be supplied in Admin mode (i.e. manually via console)")
    }

    val claimsJson =
        s"""{"sub":"$username", "exp":${expiry.getOrElse(DateTime.now.plusDays(1)).getMillis},""" +
           s""""rev": ${rev.getOrElse(getUserRevNum(username, authCache))}}"""

    Jwt.encode(JwtClaim(claimsJson), secret, jwtHeader)
  }
}
