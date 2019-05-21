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
package cmwell.domain

import collection.{breakOut, mutable}
import scala.Predef._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.apache.commons.codec.binary._
import com.typesafe.scalalogging.LazyLogging
import cmwell.syntaxutils._
import scala.util.{Success, Try}
import scala.collection.mutable.{Map => MMap}

/**
  * Created with IntelliJ IDEA.
  * User: markz
  * Date: 1/13/13
  * Time: 12:06 PM
  *
  */
object InfotonSerializer extends LazyLogging {

  //////////////
  // NEW API: //
  //////////////

  type CUuid = String
  type CQuad = String
  type CField = String
  type CData = Array[Byte]
  type CValue = (String, CData)
  type SerializedInfoton = Seq[(CQuad, Seq[(CField, Seq[CValue])])]

  // TODO: make this configurable
  val chunkSize = 65536
  val sysQuad: CQuad = "cmwell://meta/sys"
  val default: CQuad = "default"
  val fmt: DateTimeFormatter = ISODateTimeFormat.dateTime

  val fieldValuesToQuadSet = breakOut[Set[FieldValue], CQuad, Set[CQuad]]
  val valuesToQuadSet = breakOut[Iterable[Set[FieldValue]], CQuad, Set[CQuad]]
  val tuplesToFields =
    breakOut[Map[CField, Iterable[(CQuad, CField, CValue)]], (CField, Vector[CValue]), Vector[(CField, Vector[CValue])]]
  val tuplesToValues = breakOut[Iterable[(CQuad, CField, CValue)], CValue, Vector[CValue]]

  /**
    * not thread safe
    */
  class MutableInfotonBuilder(uuidHint: String) {

    private[this] var infoton: Infoton = _
    private[this] var kind: String = _
    private[this] var path: String = _
    private[this] var lastModified: DateTime = _
    private[this] var dc: String = _
    private[this] var indexTime: Option[Long] = None
    private[this] var indexName: String = ""
    private[this] var protocol: Option[String] = None
    private[this] var lastModifiedBy: String = _
    private[this] var fields: Option[MMap[String, Set[FieldValue]]] = None
    private[this] var fileContentBuildPosition = 0
    private[this] var fileContentBuilder = null.asInstanceOf[Array[Byte]]
    private[this] var fileContentCountVerifier: Int = 0
    private[this] var fileContentLength: Int = 0
    private[this] var mimeType: String = _
    private[this] var dataPointer: String = _
    private[this] var linkTo: String = _
    private[this] var linkType: Int = -1

    def setKind(kind: String): Unit = {
      if (this.kind eq null) this.kind = kind
      else throw new IllegalStateException(s"kind was already set for uuid [$uuidHint] [${this.kind},$kind]")
    }

    def setPath(path: String): Unit = {
      if (this.path eq null) this.path = path
      else throw new IllegalStateException(s"path was already set for uuid [$uuidHint] [${this.path},$path]")
    }

    def setLastModified(lastModified: String): Unit = {
      if (this.lastModified eq null) this.lastModified = fmt.parseDateTime(lastModified)
      else {
        val newLastModified = fmt.parseDateTime(lastModified)
        if (this.lastModified.compareTo(newLastModified) == 0)
          logger.warn(
            s"Been called twice on lastModified with same (parsed) value [${this.lastModified.toString(fmt)},$lastModified]"
          )
        else
          throw new IllegalStateException(
            s"lastModified was already set for uuid [$uuidHint] [${this.lastModified.toString(fmt)},$lastModified]"
          )
      }
    }

    def setDc(dc: String): Unit = {
      if (this.dc eq null) this.dc = dc
      else throw new IllegalStateException(s"dc was already set for uuid [$uuidHint] [${this.dc},$dc]")
    }

    def setIndexTime(indexTime: Long): Unit = this.indexTime match {
      case None    => this.indexTime = Some(indexTime)
      case Some(t) => throw new IllegalStateException(s"indexTime was already set for uuid [$uuidHint] [$t,$indexTime]")
    }

    def setIndexName(indexName: String): Unit = this.indexName match {
      case "" => this.indexName = indexName
      case currentIndexName =>
        throw new IllegalStateException(
          s"indexName was already set for uuid [$uuidHint] [$currentIndexName,$indexName]"
        )
    }

    def setProtocol(protocol: String): Unit = this.protocol match {
      case None => this.protocol = Some(protocol)
      case currentProtocol =>
        throw new IllegalStateException(
          s"protocol was already set for uuid [$uuidHint] [$currentProtocol,$protocol]"
        )
    }

    def addFieldValue(field: String, value: FieldValue): Unit = fields match {
      case None => this.fields = Some(MMap(field -> Set(value)))
      case Some(m) => {
        val newSet = Set(value)
        m.getOrElseUpdate(field, newSet) match {
          case `newSet`   => //Do Nothing
          case exisingSet => m.update(field, exisingSet + value)
        }
      }
    }

    def setContentLength(contentLength: Int): Unit = {
      this.fileContentLength = contentLength
    }

    def setMimeType(mimeType: String): Unit = {
      if (this.mimeType eq null) this.mimeType = mimeType
      else
        throw new IllegalStateException(s"mimeType was already set for uuid [$uuidHint] [${this.mimeType},$mimeType]")
    }

    def setFileContent(fileContent: Array[Byte], contentIndex: Int): Unit = {
      require(
        contentIndex == fileContentCountVerifier,
        s"file content chunk must arrive in order! got chunk #$contentIndex but expected #$fileContentCountVerifier for uuid [$uuidHint]"
      )

      if (fileContentBuilder eq null)
        fileContentBuilder = new Array[Byte](this.fileContentLength)

      var j = 0
      val srcUntil = fileContentBuildPosition + fileContent.length
      while (fileContentBuildPosition < srcUntil) {
        fileContentBuilder(fileContentBuildPosition) = fileContent(j)
        fileContentBuildPosition += 1
        j += 1
      }
      fileContentCountVerifier += 1
    }

    def setFileContentPointer(dataPointer: String): Unit = {
      this.dataPointer = dataPointer
    }

    def setLinkTo(linkTo: String): Unit = {
      if (this.linkTo eq null) this.linkTo = linkTo
      else throw new IllegalStateException(s"linkTo was already set for uuid [$uuidHint] [${this.linkTo},$linkTo]")
    }

    def setLinkType(linkType: Int): Unit = {
      if (this.linkType == -1) this.linkType = linkType
      else
        throw new IllegalStateException(s"linkType was already set for uuid [$uuidHint] [${this.linkType},$linkType]")
    }

    def result(): Infoton = {
      if (this.infoton ne null) this.infoton
      else {
        require(kind ne null, s"must have kind initialized [$uuidHint]")
        require(path ne null, s"must have path initialized [$uuidHint]")
        require(lastModified ne null, s"must have lastModified initialized [$uuidHint]")
        require(dc ne null, s"must have dc initialized [$uuidHint]")

        infoton = kind match {
          case "o" => new ObjectInfoton(path, dc, indexTime, lastModified, fields.map(_.toMap), indexName, protocol)
          case "f" => {
            val fileContent = {
              if (mimeType eq null) None
              else {
                val dataPointerOpt =
                  if (fileContentCountVerifier == 0) Option(dataPointer)
                  else {
                    if (fileContentBuildPosition != fileContentLength) {
                      val l = s"$fileContentBuildPosition/${fileContentBuilder.length}"
                      logger.warn(s"content has different length than expected. expected: [$fileContentLength], actual: [$l] for uuid [$uuidHint]")
                    }
                    if (dataPointer ne null) {
                      val l = fileContentBuilder.length
                      logger.warn(s"both dataPointer and content are present! dataPointer: [$dataPointer], content length: [$l] for uuid [$uuidHint]")
                    }
                    None
                  }
                Some(FileContent(Option(fileContentBuilder), mimeType, fileContentLength, dataPointerOpt))
              }
            }
            new FileInfoton(path, dc, indexTime, lastModified, fields.map(_.toMap), fileContent, indexName, protocol)
          }
          case "l" => {
            if ((linkTo eq null) || (linkType == -1))
              throw new IllegalStateException(
                s"cannot create a LinkInfoton for uuid [$uuidHint] if linkTo [$linkTo] or linkType [$linkType] is not initialized."
              )
            else new LinkInfoton(path, dc, indexTime, lastModified, fields.map(_.toMap), linkTo, linkType, indexName, protocol)
          }
          case "d" => new DeletedInfoton(path, dc, indexTime, lastModified, indexName)
          case _   => throw new IllegalStateException(s"unrecognized type was inserted [$kind] for uuid [$uuidHint]")
        }

        //be nice to gc :-)
        kind = null
        path = null
        lastModified = null
        dc = null
        indexTime = null
        indexName = null
        fields = null
        fileContentBuilder = null
        mimeType = null
        linkTo = null

        infoton
      }
    }
  }

  def deserialize2(uuidHint: String, serializedInfoton: Iterator[(CQuad, CField, CValue)]): Infoton = {

    val infotonBuilder = new MutableInfotonBuilder(uuidHint)

    def applySystemChanges(field: CField, value: CValue): Unit = field match {
      case "type"           => infotonBuilder.setKind(value._1)
      case "path"           => infotonBuilder.setPath(value._1)
      case "lastModified"   => infotonBuilder.setLastModified(value._1)
      case "dc"             => infotonBuilder.setDc(value._1)
      case "indexTime"      => infotonBuilder.setIndexTime(value._1.toLong)
      case "indexName"      => infotonBuilder.setIndexName(value._1)
      case "protocol"       => infotonBuilder.setProtocol(value._1)
      case "mimeType"       => infotonBuilder.setMimeType(value._1)
      case "contentLength"  => infotonBuilder.setContentLength(value._1.toInt)
      case "data"           => infotonBuilder.setFileContent(value._2, value._1.toInt)
      case "contentPointer" => infotonBuilder.setFileContentPointer(value._1)
      case "linkTo"         => infotonBuilder.setLinkTo(value._1)
      case "linkType"       => infotonBuilder.setLinkType(value._1.toInt)
      case f                => logger.error(s"got a weird system field [$f] from cassandra for uuid [$uuidHint]")
    }

    def appendValues(field: CField, value: String, quad: Option[String]): Unit = field.head match {
      case 's' =>
        field.indexOf('$') match {
          case 1 => infotonBuilder.addFieldValue(field.drop(2), FString(value, None, quad))
          case i => infotonBuilder.addFieldValue(field.drop(i + 1), FString(value, Some(field.substring(1, i)), quad))
        }
      case 'r' => infotonBuilder.addFieldValue(field.drop(2), FReference(value, quad))
      case 'd' => infotonBuilder.addFieldValue(field.drop(2), FDate(value, quad))
      case 'b' => infotonBuilder.addFieldValue(field.drop(2), FBoolean(value.toBoolean, quad))
      case 'i' => infotonBuilder.addFieldValue(field.drop(2), FInt(value.toInt, quad))
      case 'l' => infotonBuilder.addFieldValue(field.drop(2), FLong(value.toLong, quad))
      case 'k' => infotonBuilder.addFieldValue(field.drop(2), FBigInt(BigInt(value).underlying(), quad))
      case 'f' => infotonBuilder.addFieldValue(field.drop(2), FFloat(value.toFloat, quad))
      case 'g' => infotonBuilder.addFieldValue(field.drop(2), FDouble(value.toDouble, quad))
      case 'h' => infotonBuilder.addFieldValue(field.drop(2), FBigDecimal(BigDecimal(value).underlying(), quad))
      case 'x' =>
        field.indexOf('$') match {
          case i => infotonBuilder.addFieldValue(field.drop(i + 1), FExternal(value, field.substring(1, i), quad))
        }
    }

    for {
      (quad, field, vd @ (value, _)) <- serializedInfoton
    } quad match {
      case `sysQuad` => applySystemChanges(field, vd)
      case `default` => appendValues(field, value, None)
      case otherQuad => appendValues(field, value, Some(otherQuad))
    }

    infotonBuilder.result()
  }

  def serialize2(infoton: Infoton): (CUuid, SerializedInfoton) = {

    def systemAttributes(i: Infoton): Vector[(CField, Vector[CValue])] = {

      import scala.language.implicitConversions
      implicit def stringToCValue(s: String): CValue = s -> null.asInstanceOf[CData]

      val builder: mutable.Builder[(CField, Vector[CValue]), Vector[(CField, Vector[CValue])]] = {
        val b = Vector.newBuilder[(CField, Vector[CValue])]
        b += "type" -> Vector(i.kind.take(1).toLowerCase)
        b += "path" -> Vector(i.path)
        b += "lastModified" -> Vector(i.lastModified.toString(fmt))
        b += "dc" -> Vector(i.dc)
        b += "indexName" -> Vector(i.indexName)
        i.indexTime.fold(b) { indexTime => b += "indexTime" -> Vector(indexTime.toString) }
        i.protocol.fold(b) { protocol => b += "protocol" -> Vector(protocol) }
      }

      i match {
        case FileInfoton(_, _, _, _, _, Some(FileContent(data, mime, dl, dp)), _, _) => {
          builder += "mimeType" -> Vector(mime)
          data.foreach { d =>
            def padWithZeros(num: String, padToLength: Int): String = {
              val len = num.length
              require(len <= padToLength, s"numeric String [$num] longer than padded length [$padToLength]")
              if (len == padToLength) num
              else s"${"0" * (padToLength - len)}$num" // String.join(Array.fill(padToLength-len)('0'),num)
            }

            val length = d.length
            val indexLength = {
              val numOfChunks = length / chunkSize
              if (length % chunkSize == 0) numOfChunks.toString.length
              else (numOfChunks + 1).toString.length
            }

            var count = 0
            for (arr <- d.grouped(chunkSize)) {
              builder += "data" -> Vector(padWithZeros(count.toString, indexLength) -> arr)
              count += 1
            }
            builder += "contentLength" -> Vector(length.toString)
          }
          dp.foreach { dataPointer =>
            builder += "contentPointer" -> Vector(dataPointer)
            builder += "contentLength" -> Vector(dl.toString)
          }
        }
        case LinkInfoton(_, _, _, _, _, linkTo, linkType, _, _) => {
          builder += "linkTo" -> Vector(linkTo.toString)
          builder += "linkType" -> Vector(linkType.toString)
        }
        case _ => //Do Nothing
      }

      builder.result()
    }

    def aggregateFieldsByQuad(fields: Map[String, Set[FieldValue]]): Iterable[(CQuad, CField, CValue)] =
      for {
        (field, values) <- fields
        value <- values
      } yield
        value match {
          // format: off
      case FString(v, l, q)     => (q.getOrElse(default), s"s${l.getOrElse("")}$$$field", v -> null.asInstanceOf[CData])
      case FReference(v, q)     => (q.getOrElse(default), s"r$$$field", v -> null.asInstanceOf[CData])
      case FDate(v, q)          => (q.getOrElse(default), s"d$$$field", v -> null.asInstanceOf[CData])
      case FBoolean(v, q)       => (q.getOrElse(default), s"b$$$field", v.toString -> null.asInstanceOf[CData])
      case FInt(v, q)           => (q.getOrElse(default), s"i$$$field", v.toString -> null.asInstanceOf[CData])
      case FLong(v, q)          => (q.getOrElse(default), s"l$$$field", v.toString -> null.asInstanceOf[CData])
      case FBigInt(v, q)        => (q.getOrElse(default), s"k$$$field", v.toString -> null.asInstanceOf[CData])
      case FFloat(v, q)         => (q.getOrElse(default), s"f$$$field", v.toString -> null.asInstanceOf[CData])
      case FDouble(v, q)        => (q.getOrElse(default), s"g$$$field", v.toString -> null.asInstanceOf[CData])
      case FBigDecimal(v, q)    => (q.getOrElse(default), s"h$$$field", v.toString -> null.asInstanceOf[CData])
      case FExternal(v, uri, q) => (q.getOrElse(default), s"x$uri$$$field", v -> null.asInstanceOf[CData])
      case _:FNull => !!! // FNull is just a marker for IMP, should not index it anywhere...
      case _:FExtra[_] => !!! // FExtra is just a marker for outputting special properties, should not index it anywhere...
      // format: on
        }

    val vecBuilder = Vector.newBuilder[(CQuad, Vector[(CField, Vector[CValue])])] += sysQuad -> systemAttributes(
      infoton
    )
    infoton.uuid -> infoton.fields.fold(vecBuilder.result()) { fields =>
      aggregateFieldsByQuad(fields).groupBy(_._1).foreach {
        case (quad, tuplesByQuad) => {
          vecBuilder += (quad -> tuplesByQuad
            .groupBy(_._2)
            .map {
              case (field, tuplesByField) => field -> tuplesByField.map(_._3)(tuplesToValues)
            }(tuplesToFields))
        }
      }
      vecBuilder.result()
    }
  }

  //////////////
  // OLD API: //
  //////////////

  import java.util.zip.CRC32
  private val digest = new CRC32

  private def hash(value: String): Long = {
    digest.reset()
    digest.update(value.getBytes("UTF-8"), 0, value.length)
    val v = digest.getValue
    v
  }

  private def getType(value: FieldValue): String = value match {
    // format: off
    case FString(_,l,q) =>     s"s${l.getOrElse("")}$$$$" + q.getOrElse("") // "s$xsd#string"
    case FReference(_,q) =>     "r$$"                     + q.getOrElse("") // "r$xsd#anyURI"
    case FDate(_,q) =>          "d$$"                     + q.getOrElse("") // "d$xsd#date"
    case FBoolean(_,q) =>       "b$$"                     + q.getOrElse("") // "b$xsd#boolean"
    case FInt(_,q) =>           "i$$"                     + q.getOrElse("") // "i$xsd#int"
    case FLong(_,q) =>          "j$$"                     + q.getOrElse("") // "j$xsd#long"
    case FBigInt(_,q) =>        "k$$"                     + q.getOrElse("") // "k$xsd#integer"
    case FFloat(_,q) =>         "f$$"                     + q.getOrElse("") // "f$xsd#float"
    case FDouble(_,q) =>        "g$$"                     + q.getOrElse("") // "g$xsd#double"
    case FBigDecimal(_,q) =>    "h$$"                     + q.getOrElse("") // "h$xsd#decimal"
    case FExternal(_,uri,q) => s"x$$$uri$$"               + q.getOrElse("")
    case _:FNull => !!! //this is just a marker for IMP, should not index it anywhere...
    case _:FExtra[_] => !!! // FExtra is just a marker for outputting special properties, should not index it anywhere...
    // format: on
  }

  def serialize(infoton: Infoton): mutable.Buffer[(String, Array[Byte])] = {
    val data: mutable.Buffer[(String, Array[Byte])] = new mutable.ListBuffer[(String, Array[Byte])]()
    val uuid = infoton.uuid
    val path = infoton.path
    val dc = infoton.dc

    val l: DateTime = infoton.lastModified
    //val l : Long = infoton.lastModified.get
    // add system attributes 2x (( one for collection += and one for tuple
    data += (("$path", path.getBytes))
    data += (("$uuid", uuid.getBytes))
    data += (("$lastModified", l.toString(fmt).getBytes))
    data += (("$dc", dc.getBytes))
    infoton.indexTime.foreach { it =>
      data += ("$indexTime" -> it.toString.getBytes)
    }

    //data += ( ("$lastModified" , l.toString.getBytes() ) )
    // add data attributes
    var i = 0
    infoton.fields.foreach { d =>
      for ((k, values) <- d) {
        for (value <- values) {
          data += ((k + "$" + i + "$" + getType(value), value.toString.getBytes("UTF-8")))
          i += 1
        }
      }
    }

    // lets save specific data for each infoton
    infoton match {
      case ObjectInfoton(_, _, _, _, _, _, _) =>
        data += (("$type", "o".getBytes("UTF-8")))

      case FileInfoton(_, _, _, _, _, c, _, _) =>
        data += (("$type", "f".getBytes("UTF-8")))
        c match {
          case Some(FileContent(d, mimeType, dl, dp)) =>
            if (d.isDefined) {
              val x = new Base64(0).encodeToString(d.get)
              // now lets split the content into 32K size chunks
              val chunks = x.grouped(32 * 1024)
              chunks.zipWithIndex.foreach {
                case (chunk, i) =>
                  data += (("$content_%d".format(i), chunk.getBytes))
                  if (!chunks.hasNext)
                    data += (("$content_count", i.toString.getBytes("UTF-8")))
              }
            }
            dp.foreach { dataPointer =>
              data += (("$content_pointer", dataPointer.getBytes("UTF-8")))
              data += (("$content_length", dl.toString.getBytes("UTF-8")))
            }
            data += (("$mimeType", mimeType.getBytes("UTF-8")))

          case None =>
        }

      case LinkInfoton(_, _, _, _, _, t, lt, _, _) =>
        data += (("$type", "l".getBytes("UTF-8")))
        data += (("$to", t.getBytes("UTF-8")))
        data += (("$lt", lt.toString.getBytes("UTF-8")))

      case DeletedInfoton(_, _, _, _, _) =>
        data += (("$type", "d".getBytes("UTF-8")))
//
//      case EmptyInfoton(_) =>
//        data += ( ("$type" , "o".getBytes("utf-8") ) )
      case _ =>
        ???

    }
    data
  }

  private def clean_key(key: String): String = {
    val k: String = key.substring(0, key.indexOf("$"))
    k
  }

  private def getFieldValue(keyStr: String, valStr: String): FieldValue = {
    val arr: Array[String] = keyStr.split('$')
    val quad: Option[String] = Try(arr(4)) match {
      case Success(s) if s.isEmpty => None
      case x                       => x.toOption
    }
    arr(2) match {
      case s if s.head == 's' => {
        val lang =
          if (s == "s") None
          else Some(s.tail)
        FString(valStr, lang, quad)
      }
      case "r" => FReference(valStr, quad)
      case "d" => FDate(valStr, quad)
      case "b" => FBoolean(valStr.toBoolean, quad)
      case "i" => FInt(valStr.toInt, quad)
      case "j" => FLong(valStr.toLong, quad)
      case "k" => FBigInt(BigInt(valStr).underlying(), quad)
      case "f" => FFloat(valStr.toFloat, quad)
      case "g" => FDouble(valStr.toDouble, quad)
      case "h" => FBigDecimal(BigDecimal(valStr).underlying(), quad)
      case "x" => FExternal(valStr, arr(3), quad)
      case unknown =>
        throw new IllegalStateException(
          s"""deserialization failed: got "$unknown" instead of a regular one-letter type string. (original keyStr = "$keyStr")"""
        )
    }
  }

  def deserialize(data: mutable.Buffer[(String, Array[Byte])]): Infoton = {
    // partition into
    val (system_buffer, data_buffer) = data.partition(v => v._1.startsWith("$"))

    var v = new mutable.HashMap[String, Array[Byte]]
    for (item <- system_buffer) {
      v += item
    }
    val path = v("$path")
    val uuid = v("$uuid")
    val dc = v.getOrElse("$dc", "na".getBytes("utf-8"))
    val infoType = new String(v("$type"), "UTF-8")
    val lastModified: DateTime = fmt.parseDateTime(new String(v("$lastModified"), "UTF-8"))
    val indexTime = v.get("$indexTime").map { v =>
      new String(v).toLong
    }
    //val lastModified : Long =  new String(v("$lastModified")).toLong
//    val sysInfo = SysInfo(new lang.String(path, "utf-8"),lastModified)
//    val sysFields = SystemFields(new lang.String(path, "utf-8"), lastModified, new lang.String(uuid, "utf-8"))
    val pathString = new String(path, "UTF-8")
    val uuidString = new String(uuid, "UTF-8")
    val dcString = new String(dc, "UTF-8")

    // Very Very bad code
    val l = data_buffer.toSet
    val x = l.map { kv =>
      (clean_key(kv._1), getFieldValue(kv._1, new String(kv._2, "UTF-8")))
    }
    val g = x.groupBy(_._1) //{ kv => kv._1 }
    val t = g.map { k =>
      k._1 -> (for (v <- k._2) yield v._2)
    }

    val fields = if (t.nonEmpty) Some(t) else None
    // get type
    val reply = infoType match {
      case "o" =>
        val i = ObjectInfoton(pathString, dcString, indexTime, lastModified, fields, "", None)
        i

      case "f" =>
        val mimeType = v("$mimeType")
        if (v.contains("$content_pointer")) {
          val dataPointer = new String(v("$content_pointer"), "UTF-8")
          val dataLength = new String(v("$content_length"), "UTF-8").toInt
          val f = FileInfoton(pathString,
                              dcString,
                              indexTime,
                              lastModified,
                              fields,
                              Some(FileContent(None, new String(mimeType), dataLength, Option(dataPointer))), "", None)
          f
        } else if (v.contains("$content_count") == true) {
          val chunks_count = new String(v("$content_count"), "UTF-8").toInt
          val c: Array[Byte] = (0 to chunks_count)
            .map { f =>
              v("$content_%d".format(f))
            }
            .flatten
            .toArray
          //val c = v("$content")
          //val content = new sun.misc.BASE64Decoder().decodeBuffer(new String(c,"utf-8"))
          val content = new Base64().decode(c)
          //val content = new String(new sun.misc.BASE64Decoder().decodeBuffer(new String(c,"utf-8")))
          //val content = new String(c)
          //        val f = FileInfoton(sysInfo, fields, FileContent(content ,new String(mimeType)))
          val f = FileInfoton(pathString,
                              dcString,
                              indexTime,
                              lastModified,
                              fields,
                              Some(FileContent(content, new String(mimeType))), "", None)
          f
        } else {
          val fc = FileContent(new String(mimeType), 0)
          val f = FileInfoton(pathString, dcString, indexTime, lastModified, fields, Some(fc), "", None)
          f
        }

      case "l" =>
        val to = v("$to")
        val lt = v("$lt")
//        val i = LinkInfoton(sysInfo,fields ,new String(to) , new String(lt).toByte )
        val i = LinkInfoton(pathString,
                            dcString,
                            indexTime,
                            lastModified,
                            fields,
                            new String(to, "UTF-8"),
                            new String(lt, "UTF-8").toByte, "", None)
        i
      case "d" =>
//        val d = DeletedInfoton(sysInfo)
        val d = DeletedInfoton(pathString, dcString, indexTime, lastModified)
        d
    }

    if (!reply.uuid.equals(uuidString))
      logger.error(
        s"deserialize error for infoton path: $pathString original uuid: $uuidString new uuid: ${reply.uuid}"
      )
    reply
  }
}
