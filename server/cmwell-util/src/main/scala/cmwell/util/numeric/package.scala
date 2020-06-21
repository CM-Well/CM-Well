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
package cmwell.util

package object numeric {

  import scala.language.postfixOps

  trait BytesConverter[N] {
    def bytes(n: N): Array[Byte]
  }

  object BytesConverter {
    implicit val intBytes = new BytesConverter[Int] {
      def bytes(n: Int) = toIntegerBytes(n.toLong)
    }
    implicit val longBytes = new BytesConverter[Long] {
      def bytes(n: Long) = toLongBytes(n)
    }
  }

  /**
    * toIntegerBytes can be used for implementing, e.g:
    *   org.apache.commons.codec.binary.Base64.toIntegerBytes
    * specialized for long values (which we know only use 1st 4 bytes, since e.g. crc32 uses only 32 bytes)
    *
    * @param long
    * @return byte array representing the long value
    */
  def toIntegerBytes(long: Long): Array[Byte] = {
    var l = long
    val b3 = l.toByte
    l >>= 8
    val b2 = l.toByte
    l >>= 8
    val b1 = l.toByte
    l >>= 8
    val b0 = l.toByte
    Array(b0, b1, b2, b3)
  }

  def toLongBytes(long: Long): Array[Byte] = {
    var l = long
    val b7 = l.toByte
    l >>= 8
    val b6 = l.toByte
    l >>= 8
    val b5 = l.toByte
    l >>= 8
    val b4 = l.toByte
    l >>= 8
    val b3 = l.toByte
    l >>= 8
    val b2 = l.toByte
    l >>= 8
    val b1 = l.toByte
    l >>= 8
    val b0 = l.toByte
    Array(b0, b1, b2, b3, b4, b5, b6, b7)
  }

  object Radix64 {
    // format: off
    val encodeTable = Array(
      '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
      'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
      '_',
      'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
      'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    )
    // format: on

    trait SixBitCharHandler[N] {
      def numToArr(n: N): Array[Char]
    }

    implicit object LongSixBitsCharHandler extends SixBitCharHandler[Long] {
      override def numToArr(l: Long): Array[Char] = {
        var v = l
        val c0 = encodeTable(v & 0x3f toInt)
        if (v < 0x40) Array(c0)
        else {
          v >>= 6
          val c1 = encodeTable(v & 0x3f toInt)
          if (v < 0x40) Array(c1, c0)
          else {
            v >>= 6
            val c2 = encodeTable(v & 0x3f toInt)
            if (v < 0x40) Array(c2, c1, c0)
            else {
              v >>= 6
              val c3 = encodeTable(v & 0x3f toInt)
              if (v < 0x40) Array(c3, c2, c1, c0)
              else {
                v >>= 6
                val c4 = encodeTable(v & 0x3f toInt)
                if (v < 0x40) Array(c4, c3, c2, c1, c0)
                else {
                  v >>= 6
                  val c5 = encodeTable(v & 0x3f toInt)
                  if (v < 0x40) Array(c5, c4, c3, c2, c1, c0)
                  else {
                    v >>= 6
                    val c6 = encodeTable(v & 0x3f toInt)
                    if (v < 0x40) Array(c6, c5, c4, c3, c2, c1, c0)
                    else {
                      v >>= 6
                      val c7 = encodeTable(v & 0x3f toInt)
                      if (v < 0x40) Array(c7, c6, c5, c4, c3, c2, c1, c0)
                      else {
                        v >>= 6
                        val c8 = encodeTable(v & 0x3f toInt)
                        if (v < 0x40) Array(c8, c7, c6, c5, c4, c3, c2, c1, c0)
                        else {
                          v >>= 6
                          val c9 = encodeTable(v & 0x3f toInt)
                          if (v < 0x40) Array(c9, c8, c7, c6, c5, c4, c3, c2, c1, c0)
                          else {
                            v >>= 6
                            val c10 = encodeTable(v & 0x3f toInt)
                            Array(c10, c9, c8, c7, c6, c5, c4, c3, c2, c1, c0)
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    implicit object IntSixBitsCharHandler extends SixBitCharHandler[Int] {
      override def numToArr(i: Int): Array[Char] = {
        var v = i
        val c0 = encodeTable(v & 0x3f)
        if (v < 0x40) Array(c0)
        else {
          v >>= 6
          val c1 = encodeTable(v & 0x3f)
          if (v < 0x40) Array(c1, c0)
          else {
            v >>= 6
            val c2 = encodeTable(v & 0x3f)
            if (v < 0x40) Array(c2, c1, c0)
            else {
              v >>= 6
              val c3 = encodeTable(v & 0x3f)
              if (v < 0x40) Array(c3, c2, c1, c0)
              else {
                v >>= 6
                val c4 = encodeTable(v & 0x3f)
                if (v < 0x40) Array(c4, c3, c2, c1, c0)
                else {
                  v >>= 6
                  val c5 = encodeTable(v & 0x3f)
                  Array(c5, c4, c3, c2, c1, c0)
                }
              }
            }
          }
        }
      }
    }

    def encodeUnsigned[N: SixBitCharHandler](n: N, padToLength: Int = 1): String = {
      require(padToLength > 0, "must be padded with a positive value (1 means no padding)")
      val num = implicitly[SixBitCharHandler[N]].numToArr(n)
      if (num.length >= padToLength) new String(num)
      else new java.lang.StringBuilder(Array.fill(padToLength - num.length)('-')).append(num).toString()
    }
  }
}
