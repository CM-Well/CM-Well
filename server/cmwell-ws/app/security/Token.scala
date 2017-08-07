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


package security

import authentikat.jwt.{JwtHeader, JwtClaimsSet, JwtClaimsSetJValue, JsonWebToken}
import cmwell.ws.Settings
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime

import scala.util.Try

class Token(jwt: String, authCache: AuthCache) {
  private val requiredClaims = Set("sub","exp")

  private val claimsSet = jwt match {
    case JsonWebToken(_, cs, _) if requiredClaims.map(cs.getClaimValue).forall(_.isDefined) => cs
    case _ => throw new IllegalArgumentException("Given string was not in JWT format, or there are mandatory claims missing")
  }

  val username = claimsSet.getClaimValue("sub").get
  val expiry = new DateTime(claimsSet.getClaimValue("exp").map(_.toLong).get)

  def isValid = {
    JsonWebToken.validate(jwt, Token.secret) &&
      expiry.isAfterNow &&
      (claimsSet.getClaimValue("rev").map(_.toInt).getOrElse(0) >= Token.getUserRevNum(username, authCache) || username=="root") // root has immunity to revision revoking
  }

  implicit class JwtClaimsSetJValueExtensions(cs: JwtClaimsSetJValue) {
    def getClaimValue(key: String): Option[String] = cs.asSimpleMap.toOption.getOrElse(Map()).get(key)
  }

  override def toString = s"Token(valid=$isValid,payload=${claimsSet.asJsonString})"
}

object Token {
  def apply(jwt: String, authCache: AuthCache) = Try(new Token(jwt, authCache)).toOption

  private lazy val secret = ConfigFactory.load().getString("play.crypto.secret") // not using ws.Settings, so it'd be available from `sbt ws/console`
  private val jwtHeader = JwtHeader("HS256")

  private def getUserRevNum(username: String, authCache: AuthCache) = authCache.getUserInfoton(username).flatMap(u => (u\"rev").asOpt[Int]).getOrElse(0)

  def generate(authCache: AuthCache, username: String, expiry: Option[DateTime] = None, rev: Option[Int] = None, isAdmin: Boolean = false): String = {
    val maxDays = Settings.maxDaysToAllowGenerateTokenFor
    if(!isAdmin && expiry.isDefined && expiry.get.isAfter(DateTime.now.plusDays(maxDays)))
      throw new IllegalArgumentException(s"Token expiry must be less than $maxDays days")
    if(!isAdmin && rev.isDefined)
      throw new IllegalArgumentException(s"rev should only be supplied in Admin mode (i.e. manually via console)")

    val claims = Map("sub" -> username, "exp" -> expiry.getOrElse(DateTime.now.plusDays(1)).getMillis, "rev" -> rev.getOrElse(getUserRevNum(username,authCache)))
    JsonWebToken(jwtHeader, JwtClaimsSet(claims), secret)
  }
}