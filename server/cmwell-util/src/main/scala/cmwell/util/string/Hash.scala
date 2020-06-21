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
package cmwell.util.string

import java.nio.charset.StandardCharsets
import cmwell.util.numeric._
import scala.annotation.tailrec
import net.jpountz.xxhash._

object Hash {

  trait Radix {
    def getRadix: Int
    def digitMap: Map[Long, Char] = {
      val r = getRadix
      if (r > 62) throw new RuntimeException("unable to represent a radix that large. (" + r + ")")
      (0L to 61L).map { d: Long =>
        if (d < 10) d -> (d + 48).toChar
        else if (d < 36) d -> ('a' to 'z')(d.toInt - 10)
        else /*if(d <62)*/ d -> ('A' to 'Z')(d.toInt - 36)
      }.toMap
    }
  }

  object Binary extends Radix { def getRadix = 2 }
  object Octal extends Radix { def getRadix = 8 }
  object Decimal extends Radix { def getRadix = 10 }
  object Hexadecimal extends Radix { def getRadix = 16 }

  def crc32(s: String): String = crc32(s.getBytes(StandardCharsets.UTF_8))(Hexadecimal)

  def crc32long(s: String): Long = {
    val bytes = s.getBytes(StandardCharsets.UTF_8)
    val checksum = new java.util.zip.CRC32
    checksum.update(bytes, 0, bytes.length)
    checksum.getValue
  }

  def adler32long(s: String): Long = {
    val bytes = s.getBytes(StandardCharsets.UTF_8)
    adler32long(bytes)
  }

  def adler32long(a: Array[Byte]): Long = {
    val checksum = new java.util.zip.Adler32
    checksum.update(a, 0, a.length)
    checksum.getValue
  }

  def adler32int(a: Array[Byte]): Int = adler32long(a).toInt

  def crc32base64(s: String): String =
    hashToBase64(s)(crc32long)(toIntegerBytes)

  def hashToBase64(s: String)(hash: String => Long)(bytes: Long => Array[Byte]): String = {
    // following 2 lines are equivalent to:
    //   Base64.encodeIntegerString(checksum.getValue)
    // but more efficient, since it avoids the un-necessary wrapping of `BigInt(java.util.BigInteger))`
    // with implicits involved, and also, simplify the algorithm when passed `bytes` only takes 4 bytes,
    // since we know that exactly 4 bytes are in play for some hash functions (e.g. crc32,xxhash32)...
    val checksumAsBytes = bytes(hash(s))
    Base64.encodeBase64URLSafeString(checksumAsBytes)
  }

  def crc32(bytes: Array[Byte])(implicit radix: Radix = Hexadecimal): String = {
    val checksum = new java.util.zip.CRC32
    checksum.update(bytes, 0, bytes.length)

    radix match {
      case Binary      => checksum.getValue.toBinaryString
      case Octal       => checksum.getValue.toOctalString
      case Decimal     => checksum.getValue.toString
      case Hexadecimal => checksum.getValue.toHexString
      case r: Radix => {
        def toRadixString(n: Long): String = {
          @tailrec
          def recHelper(n: Long, acc: List[Char] = Nil): List[Char] = {
            def divRemainder(x: Long): (Long, Long) = {
              val remainder = x % r.getRadix
              (x / r.getRadix, remainder)
            }
            if (n < r.getRadix) r.digitMap(n) :: acc
            else {
              val (a, b) = divRemainder(n)
              recHelper(a, r.digitMap(b) :: acc)
            }
          }
          recHelper(n).mkString
        }
        if (r.getRadix < 1) throw new RuntimeException("radix = " + r.getRadix + " ???")
        else toRadixString(checksum.getValue)
      }
    }
  }

  def xxhash32base64(s: String): String =
    hashToBase64(s)(xxhash32)(toIntegerBytes)

  def xxhash64base64(s: String): String =
    hashToBase64(s)(xxhash64)(toLongBytes)

  def md5(s: String): String = md5(s.getBytes(StandardCharsets.UTF_8))
  def md5(d: Array[Byte]): String = md5Func(d)

  def sha1(s: String): String = sha1(s.getBytes(StandardCharsets.UTF_8))
  def sha1(d: Array[Byte]): String = sha1Func(d)

  def xxhash32(str: String): Long = xxhash32(str.getBytes(StandardCharsets.UTF_8))
  def xxhash32(arr: Array[Byte]): Long = {
    val i = xxhashFactory.hash32().hash(arr, 0, arr.length, 786)
    4294967295L & i // 4294967295L == -1L >>> 32, which will result in a long with 32 MSBits zeroed
  }

  def xxhash64(str: String): Long = xxhash64(str.getBytes(StandardCharsets.UTF_8))
  def xxhash64(arr: Array[Byte]): Long = xxhashFactory.hash64().hash(arr, 0, arr.length, 786)

  private lazy val xxhashFactory = XXHashFactory.fastestJavaInstance()
  private val md5Func = msgDigester("md5")
  private val sha1Func = msgDigester("sha1")

  private def msgDigester(alg: String): (Array[Byte]) => String =
    (d: Array[Byte]) => java.security.MessageDigest.getInstance(alg).digest(d).map("%02x".format(_)).mkString
}
