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


package cmwell.util

import org.apache.commons.codec.binary.{Base64 => ApacheBase64}
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}

import scala.util.Try
import scala.util.matching.Regex
import scala.collection.immutable.StringOps
import com.typesafe.scalalogging.LazyLogging
import cmwell.util.os.Props._

/**
 * Created with IntelliJ IDEA.
 * User: gilad
 * Date: 7/23/13
 * Time: 9:43 AM
 * To change this template use File | Settings | File Templates.
 */
package object string extends LazyLogging  {

  /**
   * proxy for Base64 static methods
   */
  object Base64 {

    def isBase64(octet: Byte): Boolean = ApacheBase64.isBase64(octet)

    def isBase64(base64: String): Boolean = ApacheBase64.isBase64(base64)

    def isBase64(arrayOctet: Array[Byte]): Boolean = ApacheBase64.isBase64(arrayOctet)

    def encodeBase64(binaryData: Array[Byte]): Array[Byte] = ApacheBase64.encodeBase64(binaryData)

    def encodeBase64(stringData: String): Array[Byte] = ApacheBase64.encodeBase64(stringData.getBytes("UTF-8"))

    def encodeBase64String(binaryData: Array[Byte]): String = ApacheBase64.encodeBase64String(binaryData)

    def encodeBase64String(stringData: String): String = ApacheBase64.encodeBase64String(stringData.getBytes("UTF-8"))

    def encodeBase64URLSafe(binaryData: Array[Byte]): Array[Byte] = ApacheBase64.encodeBase64URLSafe(binaryData)

    def encodeBase64URLSafe(stringData: String): Array[Byte] = ApacheBase64.encodeBase64URLSafe(stringData.getBytes("UTF-8"))

    def encodeBase64URLSafeString(binaryData: Array[Byte]): String = ApacheBase64.encodeBase64URLSafeString(binaryData)

    def encodeBase64URLSafeString(stringData: String): String = ApacheBase64.encodeBase64URLSafeString(stringData.getBytes("UTF-8"))

    def encodeBase64Chunked(binaryData: Array[Byte]): Array[Byte] = ApacheBase64.encodeBase64Chunked(binaryData)

    def encodeBase64Chunked(stringData: String): Array[Byte] = ApacheBase64.encodeBase64Chunked(stringData.getBytes("UTF-8"))

    def encodeBase64(binaryData: Array[Byte], isChunked: Boolean): Array[Byte] = ApacheBase64.encodeBase64(binaryData,isChunked)

    def encodeBase64(stringData: String, isChunked: Boolean): Array[Byte] = ApacheBase64.encodeBase64(stringData.getBytes("UTF-8"),isChunked)

    def encodeBase64(binaryData: Array[Byte], isChunked: Boolean, urlSafe: Boolean): Array[Byte] = ApacheBase64.encodeBase64(binaryData,isChunked,urlSafe)

    def encodeBase64(stringData: String, isChunked: Boolean, urlSafe: Boolean): Array[Byte] = ApacheBase64.encodeBase64(stringData.getBytes("UTF-8"),isChunked,urlSafe)

    def encodeBase64(binaryData: Array[Byte], isChunked: Boolean, urlSafe: Boolean, maxResultSize: Int): Array[Byte] = ApacheBase64.encodeBase64(binaryData,isChunked,urlSafe,maxResultSize)

    def encodeBase64(stringData: String, isChunked: Boolean, urlSafe: Boolean, maxResultSize: Int): Array[Byte] = ApacheBase64.encodeBase64(stringData.getBytes("UTF-8"),isChunked,urlSafe,maxResultSize)

    def decodeBase64(base64String: String): Array[Byte] = ApacheBase64.decodeBase64(base64String)

    def decodeBase64(base64Data: Array[Byte]): Array[Byte] = ApacheBase64.decodeBase64(base64Data)

    def decodeBase64String(base64String: String, charset: String): String = new String(ApacheBase64.decodeBase64(base64String),charset)

    def decodeBase64String(base64Data: Array[Byte], charset: String): String =  new String(ApacheBase64.decodeBase64(base64Data),charset)

    def decodeInteger(pArray: Array[Byte]): BigInt = ApacheBase64.decodeInteger(pArray)

    def encodeInteger(bigInt: BigInt): Array[Byte] = ApacheBase64.encodeInteger(bigInt.bigInteger)

    def encodeIntegerString(bigInt: BigInt): String = org.apache.commons.codec.binary.StringUtils.newStringUtf8(ApacheBase64.encodeInteger(bigInt.bigInteger))
  }

  object Zip {
    import java.util.zip._

    private[this] val sizeLimit = 1 << 16

    /**
     * compress the input string, and store original length & checksum byte as first 3 bytes
     *
     * input = "#########################"
     * as bytes = [100011, ... ,100011]
     * bytes length = 25
     * size header [a,b] = [00000000,00011001]
     * compressed payload = [11000001,0,11001,1111000,10011100,1010011,1010110,11000110,1,0,101100,10001000,11,1101100]
     * checksum byte [c] = adler32( ab + input ) lsb = 11000001
     * output = c+a+b++payload = [11000001,00000000,00011001,11000001,0,11001,1111000,10011100,1010011,1010110,11000110,1,0,101100,10001000,11,1101100]
     *
     * @param input string to compress
     * @param encoding the string encoding
     * @param level delegate to java.util.zip.Deflater
     * @param strategy delegate to java.util.zip.Deflater
     * @return compressed output prepended with checksum byte & size bytes
     * @see java.util.zip.Deflater
     */
    def compress(input: String, encoding: String = "UTF-8", level: Int = Deflater.DEFAULT_COMPRESSION, strategy: Int = Deflater.DEFAULT_STRATEGY): Array[Byte] = {
      val bytes = input.getBytes(encoding)
      val l = bytes.length
      require(l < sizeLimit,"compress is allowed only for small input (max 64KB)")
      val compressor = new Deflater(level)
      compressor.setStrategy(strategy)
      compressor.setInput(bytes)
      compressor.finish()
      val output = new Array[Byte](l*2)
      val compressedDataLength = compressor.deflate(output)
      compressor.end()
      if(compressedDataLength >= l) {
        val sizeHeader = Array(0.toByte,0.toByte)
        val payload = sizeHeader ++: bytes
        val checksum = Hash.adler32long(payload)
        checksum.toByte +: payload
      }
      else {
        val sizeHeader = Array((l >> 8).toByte, l.toByte)
        val payload = sizeHeader ++: output.take(compressedDataLength)
        val checksum = Hash.adler32long(payload)
        checksum.toByte +: payload
      }
    }

    /**
     * decompresses data that were compressed using [[compress]]
     * @param input compressed byte array
     * @param encoding the uncompressed string encoding
     * @return original uncompressed string
     */
    def decompress(input: Array[Byte], encoding: String = "UTF-8"): String = {
      val checksum = input.head
      val payload = input.tail
      require(Hash.adler32long(payload).toByte == checksum,"compressed data contains invalid checksum byte")
      val sizeHeader = payload.take(2)
      val compressed = payload.drop(2)
      val size = sizeHeader.foldLeft(0){case (i,b) => (i << 8) | (255 & b)}
      if(size == 0) new String(compressed, encoding)
      else {
        val decompresser = new Inflater()
        decompresser.setInput(compressed, 0, compressed.length)
        val output = new Array[Byte](size)
        val uncompressedDataLength = decompresser.inflate(output)
        decompresser.end()
        new String(output, 0, uncompressedDataLength, encoding)
      }
    }
  }

  def unixify(path: String): String = {
    val (thisOs, thatOs) = if (isWin) ('/', '\\') else ('\\', '/')
    val rv = if (isWin) path.replaceAllLiterally(thisOs.toString, thatOs.toString).replaceAllLiterally("%20", " ")
    else path.replaceAllLiterally(thisOs.toString, thatOs.toString)

    if (isWin && rv.length > 0 && rv(0) == thisOs) {
      new StringOps(rv).dropWhile(_ == thisOs)
    } else {
      rv
    }
  }

  val dateStringify = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).print(_ : DateTime)

  def parseDate(dateStr: String): Option[DateTime] = {
    val fullDateFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC)
    val longDateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val shortDateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val funcs: Seq[String => Option[DateTime]] =
      Seq(fullDateFormatter.parseDateTime(_),longDateFormatter.parseDateTime(_),shortDateFormatter.parseDateTime(_)).map{
        f ⇒ (s: String) ⇒ Try(f(s)).toOption
      }
    (Option.empty[DateTime] /: funcs) {
      case (None,f) => f(dateStr)
      case (opt,_) => opt
    }
  }

  def dropRightWhile(s: String)(p: Char => Boolean): String = {
    val it = s.reverseIterator
    var count = 0
    while(it.hasNext && p(it.next)) {
      count += 1
    }
    if(count == 0) s
    else s.dropRight(count)
  }

  def stringToInputStream(s: String) = new java.io.ByteArrayInputStream(s.getBytes("UTF-8"));

  def lines(str: String): List[String] = str.split(endln).toList

  /**
   * like splitAt, but without the separating char
   * @param str
   * @param sep
   * @return
   */
  def splitAtNoSep(str: String, sep: Char): (String, String) = {
    if(!str.contains(sep)) (str,"")
    else {
      val tuple = str.span(sep.!=)
      (tuple._1,tuple._2.tail)
    }
  }

  def classpathResourceToString(resource:String) = {
    val is = Thread.currentThread.getContextClassLoader.getResourceAsStream(resource)
    val rv = scala.io.Source.fromInputStream(is,"UTF-8").getLines().mkString(endln)
    is.close()
    rv
  }

  def resourceTemplateToGeneratedString(resource: String, valueMap: Map[String, String]): String = {
    val is = Thread.currentThread.getContextClassLoader.getResourceAsStream(resource)
    val st = scala.io.Source.fromInputStream(is,"UTF-8").getLines().mkString(endln)
    is.close()
    replaceTemplates(st, valueMap)
  }

  def replaceTemplates(text: String, templates: Map[String, String]): String =
    """\{\{([^{}]*)\}\}""".r replaceSomeIn ( text,  { case Regex.Groups(name) => templates get name } )

  /**
    * Like splitAt, but discarding the separator. e.g. splitAtNoSep("Hello World", ' ') == "Hello" -> "World"
    */
  def splitAtNoSep(s: String, idx: Int): (String,String) = s.take(idx) -> s.drop(idx+1)

  def spanNoSepBy(s: String, p: Char) = spanNoSep(s, _ != p) // this is just to spoil the user for the most common case

  /**
    * Like span, but discarding the separator. e.g. spanNoSep("Hello World", ' '.!=) == "Hello" -> "World"
    */
  def spanNoSep(s: String, p: Char => Boolean) = splitAtNoSep(s, s.prefixLength(p))
}