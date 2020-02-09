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
package cmwell.util.string.test

import java.io.{ByteArrayInputStream, InputStream}

import org.scalatest.{FunSpec, Matchers}
import cmwell.util.string._

class StringTests extends FunSpec with Matchers {
  private def mkString(is: InputStream) = {
    val buffSrc = scala.io.Source.fromInputStream(is)
    val res = buffSrc.mkString
    buffSrc.close()
    res
  }

  describe("mapInputStreamLines should") {
    it("return empty for empty input") {
      val input = new ByteArrayInputStream(Array.emptyByteArray)
      val result = mapInputStreamLines(input)(identity)
      result.read() should be(-1)
      input.close()
      result.close()
    }
    it("provide the delimiter as well") {
      val delim = '\n'
      val s = "provide the\ndelimiter as well"
      val expectedAmount = s.count(delim.==)

      val input = stringToInputStream(s)
      val result = mapInputStreamLines(input)(_.toUpperCase)
      mkString(result).count(delim.==) should be(expectedAmount)
      input.close()
      result.close()
    }
    it("not end with the delimiter") {
      val input = stringToInputStream("not end with\nthe delimiter")
      val result = mapInputStreamLines(input)(_.toUpperCase)
      mkString(result).last should be('R')
      input.close()
      result.close()
    }
    it("handle a concat mapper") {
      val input = stringToInputStream("handle\na\nconcat\nmapper")
      val result = mapInputStreamLines(input)(_ + " not")
      mkString(result) should be("handle not\na not\nconcat not\nmapper not")
      input.close()
      result.close()
    }
  }

  describe("string sanitizer should") {
    it("interpolate the reference strings") {
      val (a, b, c) = ("a", "b", "c")

      val referenceStrings = Array(
        (san"1 $a 2 $b 3 $c", s"1 $a 2 $b 3 $c"), // starts with a string, ends with an arg
        (san"1 $a 2 $b 3", s"1 $a 2 $b 3"), // starts with a string, ends with a string
        (san"$a 2 $b 3 $c", s"$a 2 $b 3 $c"), // starts with an arg, ends with an arg
        (san"$a 2 $b 3 $c 4", s"${a} 2 ${b} 3 ${c} 4") // starts with an arg, ends with a string
      )

      referenceStrings.foreach {
        case ((interpolated, expected)) =>
          interpolated should be(expected)
      }
    }

    it("sanitize the reference strings") {
      val (z, b, t, p) = ('\u0000', '\b', '\t', "%")

      val referenceStrings = Array(
        (san"$z z $b b $t t $p p", "\\0 z \\b b \\t t \\% p"),
        (san"$z$b$t t ${p}", "\\0\\b\\t t \\%")
      )

      referenceStrings.foreach {
        case ((interpolated, expected)) =>
          interpolated should be(expected)
      }
    }
  }
}
