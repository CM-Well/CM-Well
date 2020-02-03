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
package cmwell.common.formats

import cmwell.common.StatusTracking
import scala.util.{Try, Success => USuccess, Failure => UFailure}

import scala.util.parsing.combinator.JavaTokenParsers

object StatusTrackingFormat extends JavaTokenParsers {

  val restOfTheInputAsString = Parser { in =>
    val source = in.source
    val offset = in.offset
    val length = source.length()
    Success(source.subSequence(offset, length).toString, in.drop(length - offset))
  }

  val maxIntString = Int.MaxValue.toString
  val nonNegativeUnpaddedIntNumber: Parser[Int] =
    """\d+""".r ^? ({
      case s if s.length < maxIntString.length || s <= maxIntString => s.toInt
    }, { s =>
      s"'$s' is not a positive valid int (it is greater than the max int value [$maxIntString])"
    })

  val statusTracking = opt(nonNegativeUnpaddedIntNumber <~ ',') ~ restOfTheInputAsString ^^ {
    case Some(n) ~ t => StatusTracking(t, n)
    case None ~ t    => StatusTracking(t, 1)
  }

  def parseTrackingStatus(input: String): Try[StatusTracking] = parseAll(statusTracking, input) match {
    case Success(x, _)     => USuccess(x)
    case NoSuccess(msg, _) => UFailure(new ParsingException(msg))
  }
}

class ParsingException(msg: String) extends IllegalArgumentException(msg)
