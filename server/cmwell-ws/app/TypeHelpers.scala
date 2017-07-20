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



package cmwell.ws.util

import java.net.InetAddress
import cmwell.ws.Settings
import play.api.mvc.{RequestHeader, Request}
import play.api.http.HeaderNames

/**
 * Created with IntelliJ IDEA.
 * User: Israel
 * Date: 22/09/13
 * Time: 15:54
 * To change this template use File | Settings | File Templates.
 */
trait TypeHelpers {



  def asInt(number: String): Option[Int] = {
    //require(limit > 0, "must be positive!")
    try {
      val s = number.trim.dropWhile(_ == '0')
      require(s.forall(_.isDigit), s"Not a valid positive number format: $s")
      require(s.length < 10 || (s.length == 10 && s <= "2147483647"), s"the number $s seems to be too big") //don't try to parse something larger than MAX_INT
      Some(s.toInt)
    }
    catch {
      case _:NumberFormatException | _:NullPointerException => None
      case t:Throwable => throw t
    }
  }

  def asLong(number: String): Option[Long] = {
    //require(limit > 0, "must be positive!")
    try {
      val s = number.trim.dropWhile(_ == '0')
      require(s.forall(_.isDigit), s"Not a valid positive number format: $s")
      require(s.length < 19 || (s.length == 19 && s <= "9223372036854775807"), s"the number $s seems to be too big") //don't try to parse something larger than MAX_LONG
      Some(s.toLong)
    }
    catch {
      case _:NumberFormatException | _:NullPointerException => None
      case t:Throwable => throw t
    }
  }

  def asBoolean(s: String): Option[Boolean] = {
    if (null eq s) None
    else
      s.toLowerCase match {
        case "" | "t" | "true" | "yes" | "1" => Some(true)
        case "f" | "false" | "no" | "0" => Some(false)
        case _ => None
      }
  }

}

object TypeHelpers extends TypeHelpers


object RequestHelpers {

  def cmWellBase(implicit req:RequestHeader): String = {
    System.getProperty("cmwell.base", "http://" + req.headers.get(HeaderNames.HOST).getOrElse(InetAddress.getLocalHost().getHostName() + Option(System.getProperty("application.port")).map(":" + _).getOrElse("")))
  }
}
