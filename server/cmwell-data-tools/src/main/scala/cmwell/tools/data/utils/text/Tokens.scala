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
package cmwell.tools.data.utils.text

import java.util.zip.Inflater

/**
  * Created by matan on 17/11/16.
  */
object Tokens {
  def decompress(token: String, encoding: String = "UTF-8"): String = {
    def adler32long(a: Array[Byte]): Long = {
      val checksum = new java.util.zip.Adler32
      checksum.update(a, 0, a.length)
      checksum.getValue
    }

    val input = java.util.Base64.getUrlDecoder.decode(token)
    val checksum = input.head
    val payload = input.tail

    require(adler32long(payload).toByte == checksum, "compressed data contains invalid checksum byte")
    val sizeHeader = payload.take(2)
    val compressed = payload.drop(2)
    val size = sizeHeader.foldLeft(0) { case (i, b) => (i << 8) | (255 & b) }
    if (size == 0) new String(compressed, encoding)
    else {
      val decompresser = new Inflater()
      decompresser.setInput(compressed, 0, compressed.length)
      val output = new Array[Byte](size)
      val uncompressedDataLength = decompresser.inflate(output)
      decompresser.end()
      new String(output, 0, uncompressedDataLength, encoding)
    }
  }

  /**
    * Extract indexTime field from a given token
    * @param token input token
    * @return indexTime field encoded in given token
    */
  def getFromIndexTime(token: String): Long = {
    val arr = decompress(token).split('|')

    val indexTime = arr(0)

    indexTime.split(",") match {
      case Array(from, to) => from.toLong
      case Array(from)     => from.toLong
    }
  }
}
