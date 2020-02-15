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
package cmwell.domain
import java.util.TimeZone
import collection.mutable
import scala.Predef._
import org.joda.time.{DateTime, DateTimeZone}
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

  /**
    * not thread safe
    */
  class MutableInfotonBuilder(uuidHint: String) {

    private[this] var infoton: Infoton = _
    private[this] var kind: String = _
    private[this] var path: String = _
    private[this] var lastModified: DateTime = _
    private[this] var lastModifiedBy: String = _
    private[this] var dc: String = _
    private[this] var indexTime: Option[Long] = None
    private[this] var indexName: String = ""
    private[this] var protocol: String = ""
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
      if (this.lastModified eq null) this.lastModified = fmt.withZone(DateTimeZone.UTC).parseDateTime(lastModified)
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

    def setLastModifiedBy(newLastModifiedBy: String): Unit = {
      if (this.lastModifiedBy eq null) this.lastModifiedBy = newLastModifiedBy
      else {
        if (this.lastModifiedBy == newLastModifiedBy)
          logger.warn(
            s"Been called twice on lastModifiedBy with same value [${this.lastModifiedBy},$newLastModifiedBy]"
          )
        else
          throw new IllegalStateException(
            s"lastModifiedBy was already set for uuid [$uuidHint] [${this.lastModifiedBy},$newLastModifiedBy]"
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
      case "" => this.protocol = protocol
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
        require(lastModifiedBy ne null, s"must have lastModifiedBy initialized [$uuidHint]")
        require(dc ne null, s"must have dc initialized [$uuidHint]")

        infoton = kind match {
          case "o" => new ObjectInfoton(SystemFields(path, lastModified, lastModifiedBy, dc, indexTime, indexName, protocol), fields.map(_.toMap))
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
            new FileInfoton(SystemFields(path, lastModified, lastModifiedBy, dc, indexTime, indexName, protocol), fields.map(_.toMap), fileContent)
          }
          case "l" => {
            if ((linkTo eq null) || (linkType == -1))
              throw new IllegalStateException(
                s"cannot create a LinkInfoton for uuid [$uuidHint] if linkTo [$linkTo] or linkType [$linkType] is not initialized."
              )
            else new LinkInfoton(SystemFields(path, lastModified, lastModifiedBy, dc, indexTime, indexName, protocol), fields.map(_.toMap), linkTo, linkType)
          }
          case "d" => new DeletedInfoton(SystemFields(path, lastModified, lastModifiedBy, dc, indexTime, indexName, protocol))
          case _   => throw new IllegalStateException(s"unrecognized type was inserted [$kind] for uuid [$uuidHint]")
        }

        //be nice to gc :-)
        kind = null
        path = null
        lastModified = null
        lastModifiedBy = null
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
      case "lastModifiedBy"   => infotonBuilder.setLastModifiedBy(value._1)
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
        b += "path" -> Vector(i.systemFields.path)
        b += "lastModified" -> Vector(i.systemFields.lastModified.toString(fmt))
        b += "dc" -> Vector(i.systemFields.dc)
        b += "indexName" -> Vector(i.systemFields.indexName)
        i.systemFields.indexTime.fold(b) { indexTime => b += "indexTime" -> Vector(indexTime.toString) }
        b += "protocol" -> Vector(i.systemFields.protocol)
        b += "lastModifiedBy" -> Vector(i.systemFields.lastModifiedBy)
      }

      i match {
        case FileInfoton(_, _, Some(FileContent(data, mime, dl, dp))) => {
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
        case LinkInfoton(_, _, linkTo, linkType) => {
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
            .view
            .map {
              case (field, tuplesByField) => field -> tuplesByField.view.map(_._3).to(Vector)
            }.to(Vector))
        }
      }
      vecBuilder.result()
    }
  }

}
