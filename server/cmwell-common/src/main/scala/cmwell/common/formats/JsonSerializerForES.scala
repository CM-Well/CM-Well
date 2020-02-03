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

import java.io._

import cmwell.domain._
import cmwell.syntaxutils._
import cmwell.common.file.MimeTypeIdentifier
import com.fasterxml.jackson.core._
import com.typesafe.scalalogging.LazyLogging

object
JsonSerializerForES extends AbstractJsonSerializer with NsSplitter with LazyLogging {

  val typeChars = List(/*'s',*/ 'i', 'l', 'w', 'b', 'd', 'f')

  def encodeInfoton(infoton: Infoton, current: Boolean = true): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val jsonGenerator = jsonFactory.createGenerator(baos).enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
    encodeInfotonWithGenerator(infoton, jsonGenerator, current)
    jsonGenerator.close()
    baos.toByteArray
  }

  private def encodeFieldsWithGenerator(fields: Map[String, Set[FieldValue]],
                                        jsonGenerator: JsonGenerator,
                                        name: String = "fields") = {

    def encodeESFieldValue(fv: FieldValue, jp: JsonGenerator): Unit = fv match {
      case FBoolean(bool, _)          => jp.writeBoolean(bool)
      case FString(str, _, _)         => jp.writeString(str)
      case FReference(fr, _)          => jp.writeString(fr)
      case FDate(dateTime, _)         => jp.writeString(dateTime)
      case FExternal(value, _, _)     => jp.writeString(value)
      case FInt(int, _)               => jp.writeNumber(int)
      case FLong(long, _)             => jp.writeNumber(long)
      case FBigInt(bigInt, _)         => jp.writeNumber(bigInt.intValue)
      case FFloat(float, _)           => jp.writeNumber(float)
      case FDouble(double, _)         => jp.writeNumber(double)
      case FBigDecimal(bigDecimal, _) => jp.writeNumber(bigDecimal.doubleValue)
      case _: FNull                   => !!! //this is just a marker for IMP, should not index it anywhere...
      case _: FExtra[_] =>
        !!! // FExtra is just a marker for outputting special properties, should not index it anywhere...
    }

    def prefixByType(fValue: FieldValue): String = FieldValue.prefixByType(fValue) match {
      case 's' => ""
      case chr => s"$chr$$"
    }

    val namespaceToTypedFieldsMap = fields.foldLeft(Map.empty[String, Map[String, Set[FieldValue]]]) {
      case (m, (key, values)) => {
        val (namespaceIdentifier, fieldName) = splitNamespaceField(key)
        val innerMap = m.getOrElse(namespaceIdentifier, Map.empty)
        val innerMapAddition = {
          //TODO: can `vs` be empty? how? when? if not, delete this line...
          if (values.isEmpty)
            ((fieldName -> values) :: typeChars.map(typeChar => (typeChar + fieldName) -> values)).toMap
          else
            values.groupBy { v =>
              s"${prefixByType(v)}$fieldName"
            }
        }
        m.updated(namespaceIdentifier, innerMap ++ innerMapAddition)
      }
    }

    jsonGenerator.writeObjectFieldStart(name)

    namespaceToTypedFieldsMap.foreach {
      case (ns, fieldsByType) => {

        jsonGenerator.writeObjectFieldStart(ns)
        fieldsByType.foreach {
          case (k, vs) => {
            jsonGenerator.writeArrayFieldStart(k)
            vs.foreach(encodeESFieldValue(_, jsonGenerator))
            jsonGenerator.writeEndArray()
          }
        }
        jsonGenerator.writeEndObject()
      }
    }

    jsonGenerator.writeEndObject()
  }

  private def encodeInfotonWithGenerator(infoton: Infoton,
                                         jsonGenerator: JsonGenerator,
                                         current: Boolean = true): Unit = {
    jsonGenerator.writeStartObject()
    jsonGenerator.writeObjectFieldStart("system") // start system object field
    jsonGenerator.writeStringField("kind", infoton.kind)
    jsonGenerator.writeStringField("path", infoton.systemFields.path)
    jsonGenerator.writeStringField("lastModified", dateFormatter.print(infoton.systemFields.lastModified))
    if (infoton.systemFields.lastModifiedBy == null)
      jsonGenerator.writeStringField("lastModifiedBy", "anonymous")
    else
      jsonGenerator.writeStringField("lastModifiedBy", infoton.systemFields.lastModifiedBy)
    jsonGenerator.writeStringField("uuid", infoton.uuid)
    jsonGenerator.writeStringField("parent", infoton.parent)
    jsonGenerator.writeStringField("dc", infoton.systemFields.dc)

    if (infoton.systemFields.indexTime.nonEmpty && infoton.systemFields.dc == SettingsHelper.dataCenter) {
      logger.debug(
        s"should not happen when writing a new infoton! indexTime should only be created while indexing, and not before. uuid = ${infoton.uuid}"
      )
    }

    val idxT = {
      if (infoton.systemFields.indexTime.isEmpty) {
        logger.error(
          s"indexing an infoton with no indexTime defined! setting a value of 613 as default. uuid = [${infoton.uuid}]"
        )
        // an indexTime of something in the future is problematic.
        // i.e: when dc sync reaches "event horizon",
        // it will be synced, and new infotons indexed after it,
        // with current time will not be seen by remote dc.
        // default value MUST be set in the past,
        // though it really should'nt happen.
        613L
      } else infoton.systemFields.indexTime.get
    }

    jsonGenerator.writeNumberField("indexTime", idxT)

    val quadsOpt = infoton.fields.map(
      _.values
        .flatMap(_.collect {
          case fv if fv.quad.isDefined =>
            fv.quad.get
        })
        .toSet
    )

    //will add quad under system containing all the quads available in the fields
    quadsOpt match {
      case None => //DO NOTHING!
      case Some(quadsSet) => {
        jsonGenerator.writeFieldName("quad")
        jsonGenerator.writeStartArray(quadsSet.size)
        quadsSet.foreach(jsonGenerator.writeString)
        jsonGenerator.writeEndArray()
      }
    }

    if (current) jsonGenerator.writeBooleanField("current", true)
    else jsonGenerator.writeBooleanField("current", false)

    jsonGenerator.writeStringField("protocol", infoton.systemFields.protocol)

    jsonGenerator.writeEndObject() // end system object field
    // write field object, if not empty
    infoton.fields.foreach { fields =>
      encodeFieldsWithGenerator(fields, jsonGenerator)
    }
    infoton match {
      case FileInfoton(_, _, Some(FileContent(dataOpt, mimeType, dl, dp))) =>
        jsonGenerator.writeObjectFieldStart("content")
        jsonGenerator.writeStringField("mimeType", mimeType)
        dataOpt.foreach { data =>
          if (MimeTypeIdentifier.isTextual(mimeType)) {
            val charset = mimeType.lastIndexOf("charset=") match {
              case i if i != -1 => mimeType.substring(i + 8).trim
              case _            => "utf-8"
            }
            jsonGenerator.writeStringField("data", new String(data, charset))
          }
        }
        dp.foreach { dataPointer =>
          jsonGenerator.writeStringField("data-pointer", dataPointer)
        }
        jsonGenerator.writeNumberField("length", dataOpt.fold(dl)(_.length))
        jsonGenerator.writeEndObject()
      case LinkInfoton(_, _, linkTo, linkType) =>
        jsonGenerator.writeObjectFieldStart("link")
        jsonGenerator.writeStringField("to", linkTo)
        jsonGenerator.writeNumberField("kind", linkType)
        jsonGenerator.writeEndObject()
      case _ =>
    }
    jsonGenerator.writeEndObject() // end Infoton object
  }
}
