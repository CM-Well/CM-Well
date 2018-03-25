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

import scala.util.parsing.combinator.RegexParsers
import scala.util.{Success, Try}

/**
  * Created by yaakov on 12/17/15.
  */
trait DigestHeader {
  val realm: String
  val nonce: String
  val opaque: String
}

case class DigestServerHeader(realm: String, nonce: String, opaque: String) extends DigestHeader {
  override def toString = {
    Seq("realm" -> realm, "nonce" -> nonce, "opaque" -> opaque).
      map { case (key, value) => s"""$key="$value"""" }.
      mkString("Digest ", ",", "")
  }
}

case class DigestClientHeader(realm: String, nonce: String, opaque: String, username: String, response: String) extends DigestHeader

object DigestClientHeader {
  private val mandatoryKeys = Set("realm", "nonce", "opaque", "username", "response")

  def fromMap(map: Map[String, String]) = {
    require(mandatoryKeys.forall(map.keySet), "Missing one or more mandatory keys")
    DigestClientHeader(map("realm"), map("nonce"), map("opaque"), map("username"), map("response"))
  }
}

object DigestHeaderUtils {
  def fromClientHeaderString(s: String) = {
    Try(DigestClientHeader.fromMap(DigestHeaderParser.parseHeader(s))) match {
      case Success(dch) => dch
      case _ =>
        throw new IllegalArgumentException(s"$s is not a valid Digest Client Header")
    }
  }
}

object DigestHeaderParser extends RegexParsers {
  private def keyParser: Parser[String] = "[a-zA-Z0-9\"?&/_-]+".r ^^ { _.toString.replace("\"","") }
  private def valueParser: Parser[String] = "[a-zA-Z0-9\"?=&/_-]+".r ^^ { _.toString.replace("\"","") }
  private def keyValueParser: Parser[(String,String)] = keyParser ~ "=" ~ valueParser ^^ { case k ~ _ ~ v => k -> v }
  private def digestHeaderParser: Parser[Map[String,String]] = "Digest " ~> repsep(keyValueParser, ",\\s?".r) ^^ { _.toMap }

  def parseHeader(headerValue: String): Map[String,String] = parse(digestHeaderParser, headerValue) match {
    case Success(map, _) => map
    case _ => Map.empty[String,String]
  }
}