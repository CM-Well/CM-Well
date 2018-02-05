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


package cmwell.common.formats

import java.io._

import cmwell.domain._
import cmwell.syntaxutils._
import cmwell.common.file.MimeTypeIdentifier
import cmwell.common.{BulkCommand, MergedInfotonCommand, WriteCommand, _}
import com.fasterxml.jackson.core._
import com.typesafe.scalalogging.LazyLogging

/**
 * Created with IntelliJ IDEA.
 * User: israel
 * Date: 12/19/13
 * Time: 3:48 PM
 * To change this template use File | Settings | File Templates.
 */
object JsonSerializer extends AbstractJsonSerializer with LazyLogging {

  val typeChars = List(/*'s',*/'i','l','w','b','d','f')

  def encodeCommand(command:Command):Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val jsonGenerator = jsonFactory.createGenerator(baos).enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
    encodeCommandWithGenerator(command, jsonGenerator)
    jsonGenerator.close()
    baos.toByteArray
  }

  def decodeCommand(in:Array[Byte]):Command = {
    val bais = new ByteArrayInputStream(in)
    val jsonParser = jsonFactory.createParser(bais).enable(JsonParser.Feature.AUTO_CLOSE_SOURCE)
    val command = decodeCommandWithParser(jsonParser)
    jsonParser.close()
    command
  }

  //TODO: when TLog serialization is replaced (to binary serialization), ditch the toEs boolean (since it will always be to ES...)
  def encodeInfoton(infoton:Infoton, omitBinaryData:Boolean = false, toEs: Boolean = false, newBG:Boolean = false, current:Boolean = true):Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val jsonGenerator = jsonFactory.createGenerator(baos).enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
    encodeInfotonWithGenerator(infoton, jsonGenerator, omitBinaryData, toEs, newBG, current)
    jsonGenerator.close()
    baos.toByteArray
  }

  def decodeTrackingIDWithParser(jsonParser: JsonParser): Option[Either[Vector[StatusTracking],String]] = {
    if (jsonParser.nextToken() == JsonToken.VALUE_STRING) Some(Right(jsonParser.getText()))
    else {
      assume(jsonParser.currentToken == JsonToken.START_ARRAY, s"expected value of tid is either a string or an array")
      val b = Vector.newBuilder[StatusTracking]
      while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
        assume(jsonParser.currentToken == JsonToken.VALUE_STRING, s"expected value for tid field\n${jsonParser.getCurrentLocation.toString}")
        val t = StatusTrackingFormat.parseTrackingStatus(jsonParser.getText)
        assume(t.isSuccess, s"expected success for tid field\n$t")
        b += t.get
      }
      Some(Left(b.result()))
    }
  }

  def decodePrevUUIDWithParser(jsonParser: JsonParser): Option[String] = {
    assume(jsonParser.nextToken() == JsonToken.VALUE_STRING, s"expected uuid string for 'prevUUID' field\n${jsonParser.getCurrentLocation.toString}")
    val rv = Some(jsonParser.getText())
    //consume "type" field name
    assume(jsonParser.nextToken()==JsonToken.FIELD_NAME && "type".equals(jsonParser.getCurrentName()), s"expected 'type' field name\n${jsonParser.getCurrentLocation.toString}")
    rv
  }

  def decodeCommandWithParser(jsonParser: JsonParser, assumeStartObject:Boolean = true):Command = {
      // If requested, expect start of command object
    if(assumeStartObject) {
      assume(jsonParser.nextToken()== JsonToken.START_OBJECT, s"expected start of command object\n${jsonParser.getCurrentLocation.toString}")
    }

    assume(jsonParser.nextToken()==JsonToken.FIELD_NAME, s"expected field")
    jsonParser.getCurrentName() match {
      case "type" => JsonSerializer0.decodeCommandWithParser(jsonParser)
      case "version" => {
        val jt = jsonParser.nextToken()
        val ver = jsonParser.getText

        assume(jsonParser.nextToken()==JsonToken.FIELD_NAME, s"expected 'type','tid',or 'prevUUID' field name\n${jsonParser.getCurrentLocation.toString}")

        val (tidOpt,prevUUIDOpt) = jsonParser.getCurrentName() match {
          case "type" => None -> None
          case "prevUUID" => None -> decodePrevUUIDWithParser(jsonParser)
          case "tid" => decodeTrackingIDWithParser(jsonParser) -> {
            assume(jsonParser.nextToken()==JsonToken.FIELD_NAME, s"expected 'type',or 'prevUUID' field name\n${jsonParser.getCurrentLocation.toString}")
            jsonParser.getCurrentName() match {
              case "type" => None
              case "prevUUID" => decodePrevUUIDWithParser(jsonParser)
            }
          }
        }

        ver match {
          case "1" => JsonSerializer1.decodeCommandWithParser(jsonParser)
          case "2" => JsonSerializer2.decodeCommandWithParser(jsonParser)
          case "3" => JsonSerializer3.decodeCommandWithParser(jsonParser)
          case "4" => JsonSerializer4.decodeCommandWithParser(jsonParser)
          case "5" => JsonSerializer5.decodeCommandWithParser(jsonParser,tidOpt,prevUUIDOpt)
          case "6" => JsonSerializer6.decodeCommandWithParser(jsonParser, tidOpt,prevUUIDOpt)
          case x => logger.error(s"got: $x"); ???
        }
      }
    }
  }

  private def encodeCommandWithGenerator(command: Command, jsonGenerator: JsonGenerator):Unit = {
    jsonGenerator.writeStartObject()
    jsonGenerator.writeStringField("version", cmwell.util.build.BuildInfo.encodingVersion)
    command match {
      case sc: SingleCommand  => {
        sc.trackingID.foreach(jsonGenerator.writeStringField("tid",_))
        sc.prevUUID.foreach(jsonGenerator.writeStringField("prevUUID",_))
      }
      case ic: IndexCommand if ic.trackingIDs.nonEmpty =>
        jsonGenerator.writeArrayFieldStart("tid")
        ic.trackingIDs.foreach{
          case StatusTracking(tid,1) => jsonGenerator.writeString(tid)
          case StatusTracking(t,n) => jsonGenerator.writeString(s"$n,$t")
        }
        jsonGenerator.writeEndArray()
      case _ => //Do Nothing!
    }
    jsonGenerator.writeStringField("type", command.getClass.getSimpleName)
    command match {
      case WriteCommand(infoton, trackingID, prevUUID) =>
        jsonGenerator.writeFieldName("infoton")
        encodeInfotonWithGenerator(infoton, jsonGenerator)
      case IndexNewInfotonCommand(uuid, isCurrent, path, infotonOpt, indexName, trackingIDs) =>
        jsonGenerator.writeStringField("uuid", uuid)
        jsonGenerator.writeBooleanField("isCurrent", isCurrent)
        jsonGenerator.writeStringField("path", path)
        jsonGenerator.writeStringField("indexName", indexName)
        infotonOpt.foreach{ infoton =>
          jsonGenerator.writeFieldName("infoton")
          encodeInfotonWithGenerator(infoton, jsonGenerator)
        }
      case IndexExistingInfotonCommand(uuid, weight, path, indexName, trackingIDs) =>
        jsonGenerator.writeStringField("uuid", uuid)
        jsonGenerator.writeNumberField("weight", weight)
        jsonGenerator.writeStringField("path", path)
        jsonGenerator.writeStringField("indexName", indexName)
      case BulkCommand(commands) =>
        jsonGenerator.writeArrayFieldStart("commands")
        commands.foreach{ encodeCommandWithGenerator(_, jsonGenerator)}
        jsonGenerator.writeEndArray()
      case MergedInfotonCommand(previous, current, _) =>
        previous match {
          case Some(data) =>
            jsonGenerator.writeStringField("previousInfoton", data._1)
            jsonGenerator.writeNumberField("previousInfotonSize" , data._2)
          case None =>
        }
        jsonGenerator.writeStringField("currentInfoton", current._1)
        jsonGenerator.writeNumberField("currentInfotonSize", current._2)
      case OverwrittenInfotonsCommand(previous, current, historic) =>
        previous match {
          case Some(data) =>
            jsonGenerator.writeStringField("previousInfoton", data._1)
            jsonGenerator.writeNumberField("previousInfotonSize" , data._2)
          case None =>
        }
        current match {
          case Some(data) =>
            jsonGenerator.writeStringField("currentInfoton", data._1)
            jsonGenerator.writeNumberField("currentInfotonSize" , data._2)
            jsonGenerator.writeNumberField("currentInfotonIndexTime" , data._3)
          case None =>
        }
        if(historic.nonEmpty) {
          jsonGenerator.writeArrayFieldStart("historicInfotons")
          historic.foreach {
            case (uuid, weight, indexTime) =>
              jsonGenerator.writeStartArray()
              jsonGenerator.writeString(uuid)
              jsonGenerator.writeNumber(weight)
              jsonGenerator.writeNumber(indexTime)
              jsonGenerator.writeEndArray()
          }
          jsonGenerator.writeEndArray()
        }
      case DeleteAttributesCommand(path, fields, lastModified, trackingID, prevUUID) =>
        jsonGenerator.writeStringField("path", path)
        encodeFieldsWithGenerator(fields, jsonGenerator)
        jsonGenerator.writeStringField("lastModified", dateFormatter.print(lastModified))
      case DeletePathCommand(path, lastModified, trackingID, prevUUID) =>
        jsonGenerator.writeStringField("path", path)
        jsonGenerator.writeStringField("lastModified", dateFormatter.print(lastModified))
      case UpdatePathCommand(path, deleteFields , updateFields, lastModified, trackingID, prevUUID) =>
        jsonGenerator.writeStringField("path", path)
        encodeUpdateFieldsWithGenerator(deleteFields,updateFields, jsonGenerator)
        jsonGenerator.writeStringField("lastModified", dateFormatter.print(lastModified))
      case OverwriteCommand(infoton, trackingID) =>
        jsonGenerator.writeFieldName("infoton")
        encodeInfotonWithGenerator(infoton, jsonGenerator)
      case CommandRef(ref) =>
        jsonGenerator.writeStringField("ref", ref)
      case HeartbitCommand => ???
    }
    jsonGenerator.writeEndObject()
  }

  private def encodeUpdateFieldsWithGenerator(deleteFields:Map[String,  Set[FieldValue]],updateFields:Map[String, Set[FieldValue]], jsonGenerator:JsonGenerator) = {
    encodeFieldsWithGenerator(deleteFields,jsonGenerator, "deleteFields")
    encodeFieldsWithGenerator(updateFields,jsonGenerator, "updateFields")
  }

  //TODO: when TLog serialization is replaced (to binary serialization), ditch the toEs boolean (since it will always be to ES...)
  private def encodeFieldsWithGenerator(fields:Map[String, Set[FieldValue]], jsonGenerator:JsonGenerator, name : String = "fields", toEs: Boolean =false) = {

    def encodeESFieldValue(fv: FieldValue, jp: JsonGenerator): Unit = fv match {
      case FBoolean(bool,_) => jp.writeBoolean(bool)
      case FString(str,_,_) => jp.writeString(str)
      case FReference(fr,_) => jp.writeString(fr)
      case FDate(dateTime,_) => jp.writeString(dateTime)
      case FExternal(value,_,_) => jp.writeString(value)
      case FInt(int,_) => jp.writeNumber(int)
      case FLong(long,_) => jp.writeNumber(long)
      case FBigInt(bigInt,_) => jp.writeNumber(bigInt)
      case FFloat(float,_) => jp.writeNumber(float)
      case FDouble(double,_) => jp.writeNumber(double)
      case FBigDecimal(bigDecimal,_) => jp.writeNumber(bigDecimal)
      case _:FNull => !!! //this is just a marker for IMP, should not index it anywhere...
      case _:FExtra[_] => !!! // FExtra is just a marker for outputting special properties, should not index it anywhere...
    }

    def encodeTLogFieldValue(fv: FieldValue, jp: JsonGenerator): Unit = fv match {
      case FString(str,l,q) => jp.writeString(s"s${l.getOrElse("")}\n${q.getOrElse("")}\n$str")
      case FBoolean(bool,q) => jp.writeString(s"b${q.getOrElse("")}\n${bool.toString.head}")
      case FReference(fr,q) => jp.writeString(s"r${q.getOrElse("")}\n$fr")
      case FDate(dateTime,q) => jp.writeString(s"d${q.getOrElse("")}\n$dateTime")
      case FInt(int,q) => jp.writeString(s"i${q.getOrElse("")}\n$int")
      case FLong(long,q) => jp.writeString(s"j${q.getOrElse("")}\n$long")
      case FBigInt(bigInt,q) => jp.writeString(s"k${q.getOrElse("")}\n$bigInt")
      case FFloat(float,q) => jp.writeString(s"f${q.getOrElse("")}\n$float")
      case FDouble(double,q) => jp.writeString(s"g${q.getOrElse("")}\n$double")
      case FBigDecimal(bigDecimal,q) => jp.writeString(s"h${q.getOrElse("")}\n$bigDecimal")
      case FExternal(value,uri,q) => jp.writeString(s"x$uri\n${q.getOrElse("")}\n$value") //require !uri.exists(_ == '\n')
      case FNull(q) => jp.writeString(s"n${q.getOrElse("")}")
      case _:FExtra[_] => !!! // FExtra is just a marker for outputting special properties, should not ingest it anywhere...
    }

    def escapeString(s: String): String = if(!s.isEmpty && s.head == '#' && !toEs) s"#$s" else s
    jsonGenerator.writeObjectFieldStart(name)


    def prefixByType(fValue:FieldValue):String = FieldValue.prefixByType(fValue) match { //.fold("")(`type` => s"${`type`}$$")
      case 's' => ""
      case chr => s"$chr$$"
    }
//      fValue match {
//      case _:FString | _:FBigInt | _:FReference | _:FExternal => "s$"
//      case _:FInt => "i$"
//      case _:FLong => "l$"
//      case _:FBigDecimal | _:FDouble => "w$"
//      case _:FBoolean => "b$"
//      case _:FDate => "d$"
//      case _:FFloat => "f$"
//    }

    val fieldsByType = fields.flatMap {
      case (k,vs) if toEs && vs.isEmpty => ((k -> vs) :: typeChars.map(typeChar => (typeChar + k) -> vs)).toMap
      case (k,vs) if toEs => vs.groupBy { v =>  s"${prefixByType(v)}$k"}
      case (k,vs) => Map(k -> vs)
    }

    fieldsByType.foreach{ case (key, values) =>
      jsonGenerator.writeArrayFieldStart(key)

      if(toEs) values.foreach(encodeESFieldValue(_,jsonGenerator))
      else values.foreach(encodeTLogFieldValue(_,jsonGenerator))

      jsonGenerator.writeEndArray()
    }
    jsonGenerator.writeEndObject()
  }

  //TODO: when TLog serialization is replaced (to binary serialization), ditch the toEs boolean (since it will always be to ES...)
  //TODO: aren't `toEs` and `omitBinaryData` always the same? we only omit binary data if we're indexing ES...
  private def encodeInfotonWithGenerator(infoton: Infoton, jsonGenerator: JsonGenerator, omitBinaryData: Boolean = false,
                                         toEs: Boolean = false, newBG: Boolean = false, current: Boolean = true): Unit = {
    jsonGenerator.writeStartObject()
    if(!newBG || !toEs) {
      jsonGenerator.writeStringField("type", infoton.kind)
    }
    jsonGenerator.writeObjectFieldStart("system") // start system object field
    if(newBG && toEs) {
      jsonGenerator.writeStringField("kind", infoton.kind)
    }
    jsonGenerator.writeStringField("path", infoton.path)
    jsonGenerator.writeStringField("lastModified", dateFormatter.print(infoton.lastModified))
    jsonGenerator.writeStringField("uuid", infoton.uuid)
    jsonGenerator.writeStringField("parent", infoton.parent)
    jsonGenerator.writeStringField("dc" , infoton.dc)

    //will add quad under system containing all the quads available in the fields
    if(toEs) {
      if(infoton.indexTime.nonEmpty && infoton.dc == SettingsHelper.dataCenter) {
        logger.debug(s"should not happen when writing a new infoton! indexTime should only be created while indexing, and not before. uuid = ${infoton.uuid}")
      }

      val idxT = {
        if (infoton.indexTime.isEmpty) {
          logger.error(s"indexing an infoton with no indexTime defined! setting a value of 613 as default. uuid = [${infoton.uuid}]")
          // an indexTime of something in the future is problematic.
          // i.e: when dc sync reaches "event horizon",
          // it will be synced, and new infotons indexed after it,
          // with current time will not be seen by remote dc.
          // default value MUST be set in the past,
          // though it really should'nt happen.
          613L
        }
        else infoton.indexTime.get
      }
      jsonGenerator.writeNumberField("indexTime", idxT)
      val quadsOpt = infoton.fields.map(
        _.values.flatMap(
          _.collect {
            case fv if fv.quad.isDefined =>
              fv.quad.get
          }).toSet)

      quadsOpt match {
        case None => //DO NOTHING!
        case Some(quadsSet) => {
          jsonGenerator.writeFieldName("quad")
          jsonGenerator.writeStartArray(quadsSet.size)
          quadsSet.foreach(jsonGenerator.writeString)
          jsonGenerator.writeEndArray()
        }
      }
      if(newBG){
        if(current)
          jsonGenerator.writeBooleanField("current", true)
        else
          jsonGenerator.writeBooleanField("current", false)
      }
    } else if (infoton.indexTime.isDefined) { //this means it's an overwrite command to tlog 1
      if(infoton.dc == SettingsHelper.dataCenter){
        logger.debug("if should not exist (I think...)")
      }
      val idxT = infoton.indexTime.get
//      p.success(None)//(Some(idxT))
      jsonGenerator.writeNumberField("indexTime", idxT)
    }

    jsonGenerator.writeEndObject() // end system object field
    // write field object, if not empty
    infoton.fields.foreach { fields =>
      encodeFieldsWithGenerator(fields, jsonGenerator, toEs = toEs)
    }
    infoton match {
      case FileInfoton(_,_, _,_, _, Some(FileContent(dataOpt, mimeType, dl, dp)),_) =>
        jsonGenerator.writeObjectFieldStart("content")
        jsonGenerator.writeStringField("mimeType", mimeType)
        dataOpt.foreach{ data =>
          if(MimeTypeIdentifier.isTextual(mimeType)) {
            val charset = mimeType.lastIndexOf("charset=") match {
              case i if i != -1 => mimeType.substring(i + 8).trim
              case _ => "utf-8"
            }
            jsonGenerator.writeStringField("data", new String(data, charset))
          } else if(!omitBinaryData) {
            jsonGenerator.writeBinaryField("base64-data", data)
          }
        }
        dp.foreach { dataPointer =>
          jsonGenerator.writeStringField("data-pointer", dataPointer)
        }
        jsonGenerator.writeNumberField("length", dataOpt.fold(dl)(_.length))
        jsonGenerator.writeEndObject()
      case LinkInfoton(_, _,_,_, _,linkTo, linkType,_) =>
        jsonGenerator.writeStringField("linkTo", linkTo)
        jsonGenerator.writeNumberField("linkType", linkType)
      case _ =>
    }
    jsonGenerator.writeEndObject() // end Infoton object
  }
}
