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

import cmwell.common._
import cmwell.domain._
import com.fasterxml.jackson.core._
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

/**
  * Created by gilad on 2/26/15.
  */
object JsonSerializer6 extends AbstractJsonSerializer with LazyLogging {

  private def decodeFieldsWithParser(
    parser: JsonParser
  ): Map[String, Set[FieldValue]] = {
    assume(
      parser.nextToken() == JsonToken.START_OBJECT,
      s"expected start of 'fields' object\n${parser.getCurrentLocation.toString}"
    )
    val fields =
      collection.mutable.Map[String, collection.mutable.Set[FieldValue]]()
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      val fieldName = parser.getCurrentName
      assume(
        parser.nextToken() == JsonToken.START_ARRAY,
        s"expected START_ARARY token\n${parser.getCurrentLocation.toString}"
      )
      val values = collection.mutable.Set[FieldValue]()

      var jType = parser.nextToken()
      while (jType != JsonToken.END_ARRAY) {
        jType match {
          case JsonToken.VALUE_NUMBER_INT | JsonToken.VALUE_NUMBER_FLOAT =>
            values.add(getFieldValue(parser))
          case JsonToken.VALUE_STRING => {
            val v: String = parser.getText
            require(!v.isEmpty)
            def getQuadValue(str: String): (Option[String], String) = {
              val q = str.takeWhile(_ != '\n')
              val v = str.drop(q.length + 1)
              val quad =
                if (q.isEmpty) None
                else Some(q)
              quad -> v
            }
            val fv: FieldValue = v.head match {
              // format: off
              case 'b' => {val (q,s) = getQuadValue(v.tail); FBoolean(s == "t",q)}
              case 'd' => {val (q,s) = getQuadValue(v.tail); FDate(s,q)}
              case 'f' => {val (q,s) = getQuadValue(v.tail); FFloat(s.toFloat,q)}
              case 'g' => {val (q,s) = getQuadValue(v.tail); FDouble(s.toDouble,q)}
              case 'h' => {val (q,s) = getQuadValue(v.tail); FBigDecimal(BigDecimal(s).underlying(),q)}
              case 'i' => {val (q,s) = getQuadValue(v.tail); FInt(s.toInt,q)}
              case 'j' => {val (q,s) = getQuadValue(v.tail); FLong(s.toLong,q)}
              case 'k' => {val (q,s) = getQuadValue(v.tail); FBigInt(BigInt(s).underlying(),q)}
              case 'r' => {val (q,s) = getQuadValue(v.tail); FReference(s,q)}
              // format: on
              case 's' => {
                val arr = v.tail.split("\n", -1) //http://stackoverflow.com/questions/14602062/java-string-split-removed-empty-values
                val lang = {
                  val l = arr(0)
                  if (l.isEmpty) None
                  else Some(l)
                }
                val quad = {
                  val q = arr(1)
                  if (q.isEmpty) None
                  else Some(q)
                }
                FString(arr.drop(2).mkString("\n"), lang, quad)
              }
              case 'x' => {
                val arr = v.tail.split("\n", -1)
                val (uri, q, value) =
                  (arr(0), arr(1), arr.drop(2).mkString("\n"))
                val quad =
                  if (q.isEmpty) None
                  else Some(q)
                FExternal(value, uri, quad)
              }
              case 'n' =>
                v.tail match {
                  case s if s.isEmpty => FNull(None).asInstanceOf[FieldValue]
                  case s              => FNull(Some(s)).asInstanceOf[FieldValue]
                }
              case _ => ???
            }
            values.add(fv)
          }
          case JsonToken.VALUE_TRUE  => values.add(FBoolean(true))
          case JsonToken.VALUE_FALSE => values.add(FBoolean(false))
          case _ =>
            ??? // added this to suppress build warning. (added by Michael)
        }
        jType = parser.nextToken()
      }
      fields.put(fieldName, values)
    }
    fields.map { case (k, v) => (k, v.toSet) }.toMap
  }

  private def getFieldValue(jp: JsonParser): FieldValue =
    jp.getNumberType match {
      case JsonParser.NumberType.INT         => FInt(jp.getIntValue)
      case JsonParser.NumberType.LONG        => FLong(jp.getLongValue)
      case JsonParser.NumberType.BIG_INTEGER => FBigInt(jp.getBigIntegerValue)
      case JsonParser.NumberType.FLOAT       => FFloat(jp.getFloatValue)
      case JsonParser.NumberType.DOUBLE      => FDouble(jp.getDoubleValue)
      case JsonParser.NumberType.BIG_DECIMAL => FBigDecimal(jp.getDecimalValue)
    }

  /**
    *
    * @param jsonParser
    * @return
    */
  private[cmwell] def decodeInfotonWithParser(
    jsonParser: JsonParser
  ): Infoton = {

    // expecting start of json object
    assume(
      jsonParser.nextToken() == JsonToken.START_OBJECT,
      s"expected start of infoton object\n${jsonParser.getCurrentLocation.toString}"
    )

    assume(
      jsonParser.nextToken() == JsonToken.FIELD_NAME && "type".equals(
        jsonParser.getCurrentName()
      ),
      s"expected 'type' field name\n${jsonParser.getCurrentLocation.toString}"
    )
    assume(
      jsonParser.nextToken() == JsonToken.VALUE_STRING,
      s"expected value for 'type' field\n${jsonParser.getCurrentLocation.toString}"
    )
    val infotonType = jsonParser.getText()
    // Start of system object
    assume(
      jsonParser.nextToken() == JsonToken.FIELD_NAME && "system".equals(
        jsonParser.getCurrentName()
      ),
      s"expected 'system' field name\n${jsonParser.getCurrentLocation.toString}"
    )
    assume(
      jsonParser.nextToken() == JsonToken.START_OBJECT,
      s"expected start of 'system' object\n${jsonParser.getCurrentLocation.toString}"
    )

    assume(
      jsonParser.nextToken() == JsonToken.FIELD_NAME && "path".equals(
        jsonParser.getCurrentName()
      ),
      s"expected 'path' field name\n${jsonParser.getCurrentLocation.toString}"
    )
    assume(
      jsonParser.nextToken() == JsonToken.VALUE_STRING,
      s"expected value for 'path' field\n${jsonParser.getCurrentLocation.toString}"
    )
    val path = jsonParser.getText()

    assume(
      jsonParser.nextToken() == JsonToken.FIELD_NAME && "lastModified".equals(
        jsonParser.getCurrentName()
      ),
      s"expected 'lastModified' field name\n${jsonParser.getCurrentLocation.toString}"
    )
    assume(
      jsonParser.nextToken() == JsonToken.VALUE_STRING,
      s"expected value for 'lastModified' field\n${jsonParser.getCurrentLocation.toString}"
    )
    val lastModified = dateFormatter.parseDateTime(jsonParser.getText())

    assume(
      jsonParser.nextToken() == JsonToken.FIELD_NAME && "uuid".equals(
        jsonParser.getCurrentName()
      ),
      s"expected 'uuid' field name\n${jsonParser.getCurrentLocation.toString}"
    )
    assume(
      jsonParser.nextToken() == JsonToken.VALUE_STRING,
      s"expected value for 'uuid' field\n${jsonParser.getCurrentLocation.toString}"
    )
    val uuid = jsonParser.getText()

    assume(
      jsonParser.nextToken() == JsonToken.FIELD_NAME && "parent".equals(
        jsonParser.getCurrentName()
      ),
      s"expected 'parent' field name\n${jsonParser.getCurrentLocation.toString}"
    )
    assume(
      jsonParser.nextToken() == JsonToken.VALUE_STRING,
      s"expected value for 'parent' field\n${jsonParser.getCurrentLocation.toString}"
    )
    val parent = jsonParser.getText()

    assume(
      jsonParser.nextToken() == JsonToken.FIELD_NAME && "dc".equals(
        jsonParser.getCurrentName()
      ),
      s"expected 'dc' field name\n${jsonParser.getCurrentLocation.toString}"
    )
    assume(
      jsonParser.nextToken() == JsonToken.VALUE_STRING,
      s"expected value for 'dc' field\n${jsonParser.getCurrentLocation.toString}"
    )
    val dataCenter = jsonParser.getText()

    val nt = jsonParser.nextToken()
    val indexTime =
      if (nt == JsonToken.FIELD_NAME && "indexTime"
            .equals(jsonParser.getCurrentName())) {
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_NUMBER_INT,
          s"expected value for 'indexTime' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val idxTime = Try(jsonParser.getLongValue).toOption
        // End of system object
        assume(
          jsonParser.nextToken() == JsonToken.END_OBJECT,
          s"expected end of 'system' object\n${jsonParser.getCurrentLocation.toString}"
        )
        idxTime
      } else {
        assume(
          nt == JsonToken.END_OBJECT,
          s"expected end of 'system' object\n${jsonParser.getCurrentLocation.toString}"
        )
        None
      }

    var fields: Option[Map[String, Set[FieldValue]]] = None

    val nextToken = jsonParser.nextToken().id() match {
      case JsonTokenId.ID_FIELD_NAME =>
        jsonParser.getText() match {
          case "fields" =>
            fields = Some(decodeFieldsWithParser(jsonParser))
            jsonParser.nextToken()
          case _ => jsonParser.getCurrentToken
        }
      case _ => jsonParser.getCurrentToken
    }

    // the rest is up to the infoton's type
    infotonType match {
      case "ObjectInfoton" =>
        //expecting end of json object
        assume(
          nextToken == JsonToken.END_OBJECT,
          s"expected end of json object\n${jsonParser.getCurrentLocation.toString}"
        )
        ObjectInfoton(path, dataCenter, indexTime, lastModified, fields)
      case "LinkInfoton" =>
        assume(
          nextToken == JsonToken.FIELD_NAME && "linkTo".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'linkTo' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'linkTo' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val linkTo = jsonParser.getText()

        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "linkType".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'linkType' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_NUMBER_INT,
          s"expected value for 'linkType' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val linkType = jsonParser.getIntValue

        //expecting end of json object
        assume(
          jsonParser.nextToken() == JsonToken.END_OBJECT,
          s"expected end of json object\n${jsonParser.getCurrentLocation.toString}"
        )
        LinkInfoton(
          path,
          dataCenter,
          indexTime,
          lastModified,
          fields,
          linkTo,
          linkType
        )
      case "FileInfoton" =>
        var fileContent: Option[FileContent] = None
        if (nextToken.id == JsonTokenId.ID_FIELD_NAME) {
          assume(
            nextToken == JsonToken.FIELD_NAME && "content".equals(
              jsonParser.getCurrentName()
            ),
            s"expected 'content' field name\n${jsonParser.getCurrentLocation.toString}"
          )
          // expecting start of 'content' object
          assume(
            jsonParser.nextToken() == JsonToken.START_OBJECT,
            s"expected start of 'content' object\n${jsonParser.getCurrentLocation.toString}"
          )
          // expecting 'mimetype' fields
          assume(
            jsonParser.nextToken() == JsonToken.FIELD_NAME && "mimeType".equals(
              jsonParser.getCurrentName()
            ),
            s"expected 'mimeType' field name\n${jsonParser.getCurrentLocation.toString}"
          )
          assume(
            jsonParser.nextToken() == JsonToken.VALUE_STRING,
            s"expected value for 'mimeType' field\n${jsonParser.getCurrentLocation.toString}"
          )
          val mimeType = jsonParser.getText()

          //expecting either 'data|base64-data' field or skipping to length
          assume(
            jsonParser.nextToken() == JsonToken.FIELD_NAME,
            s"expected field name token, either 'data|base64' or 'content'\n${jsonParser.getCurrentLocation.toString}"
          )
          var data: Option[Array[Byte]] = None
          var dp: Option[String] = None
          jsonParser.getCurrentName match {
            case "data" =>
              assume(
                jsonParser.nextToken() == JsonToken.VALUE_STRING,
                s"expected value for 'data' field\n${jsonParser.getCurrentLocation.toString}"
              )
              val charset = mimeType.lastIndexOf("charset=") match {
                case i if (i != -1) => mimeType.substring(i + 8).trim
                case _              => "UTF-8"
              }
              data = Some(jsonParser.getText.getBytes(charset))
              // skipping to next token
              assume(
                jsonParser.nextToken() == JsonToken.FIELD_NAME,
                s"expected 'length' field name token\n${jsonParser.getCurrentLocation.toString}"
              )
            case "base64-data" =>
              assume(
                jsonParser.nextToken() == JsonToken.VALUE_STRING,
                s"expected value for 'base64-data' field\n${jsonParser.getCurrentLocation.toString}"
              )
              data = Some(jsonParser.getBinaryValue)
              // skipping to next token
              assume(
                jsonParser.nextToken() == JsonToken.FIELD_NAME,
                s"expected 'length' field name token\n${jsonParser.getCurrentLocation.toString}"
              )
            case "data-pointer" =>
              assume(
                jsonParser.nextToken() == JsonToken.VALUE_STRING,
                s"expected value for 'data-pointer' field\n${jsonParser.getCurrentLocation.toString}"
              )
              dp = Some(jsonParser.getText)
              // skipping to next token
              assume(
                jsonParser.nextToken() == JsonToken.FIELD_NAME,
                s"expected 'length' field name token\n${jsonParser.getCurrentLocation.toString}"
              )
            case _ =>
          }
          assume(
            "length".equals(jsonParser.getCurrentName),
            s"expected 'length' field name token\n${jsonParser.getCurrentLocation.toString}"
          )
          // expecting 'length' field
          assume(
            jsonParser.nextToken() == JsonToken.VALUE_NUMBER_INT,
            s"expected value for 'length' field\n${jsonParser.getCurrentLocation.toString}"
          )
          val dl = jsonParser.getIntValue()

          //expecting end of content object
          assume(
            jsonParser.nextToken() == JsonToken.END_OBJECT,
            s"expected end of json object\n${jsonParser.getCurrentLocation.toString}"
          )

          //expecting end of infoton object
          assume(
            jsonParser.nextToken() == JsonToken.END_OBJECT,
            s"expected end of json object\n${jsonParser.getCurrentLocation.toString}"
          )

          fileContent = Some(FileContent(data, mimeType, dl, dp))
        } else {
          //expecting end of infoton object
          assume(
            nextToken == JsonToken.END_OBJECT,
            s"expected end of json object\n${jsonParser.getCurrentLocation.toString}"
          )
        }

        FileInfoton(
          path,
          dataCenter,
          indexTime,
          lastModified,
          fields,
          fileContent
        )
      case "DeletedInfoton" =>
        assume(
          nextToken == JsonToken.END_OBJECT,
          s"expected end of json object\n${jsonParser.getCurrentLocation.toString}"
        )
        DeletedInfoton(path, dataCenter, indexTime, lastModified)
    }
  }

  def decodeCommandWithParser(
    jsonParser: JsonParser,
    tidOpt: Option[Either[Vector[StatusTracking], String]],
    prevUUIDOpt: Option[String]
  ): Command = {
    assume(
      jsonParser.nextToken() == JsonToken.VALUE_STRING,
      s"expected value for 'type' field: ${jsonParser.getText}\n${jsonParser.getCurrentLocation.toString}"
    )
    val commandType = jsonParser.getText()
    var command: Command = null
    commandType match {
      case "WriteCommand" =>
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "infoton".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'infoton' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        val infoton = decodeInfotonWithParser(jsonParser)
        command =
          WriteCommand(infoton, tidOpt.flatMap(_.right.toOption), prevUUIDOpt)
        //expecting end of command object
        assume(
          jsonParser.nextToken() == JsonToken.END_OBJECT,
          s"expected end of command object\n${jsonParser.getCurrentLocation.toString}"
        )

      case "IndexNewInfotonCommand" =>
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "uuid".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'uuid' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'uuid' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val uuid = jsonParser.getText
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "isCurrent".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'isCurrent' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken().isBoolean,
          s"expected value for 'isCurrent' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val isCurrent = jsonParser.getBooleanValue
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "path".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'path' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'path' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val path = jsonParser.getText
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "indexName".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'indexName' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'indexName' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val indexName = jsonParser.getText
        var infotonOpt: Option[Infoton] = None
        val nextToken = jsonParser.nextToken()
        if ("infoton".equals(jsonParser.getCurrentName)) {
          val infoton = decodeInfotonWithParser(jsonParser)
          infotonOpt = Some(infoton)
          assume(
            jsonParser.nextToken() == JsonToken.END_OBJECT,
            s"expected end of command object\n${jsonParser.getCurrentLocation.toString}"
          )
        } else {
          assume(
            nextToken == JsonToken.END_OBJECT,
            s"expected end of command object\n${jsonParser.getCurrentLocation.toString}"
          )
        }
        command = IndexNewInfotonCommand(
          uuid,
          isCurrent,
          path,
          infotonOpt,
          indexName,
          tidOpt.fold(Seq.empty[StatusTracking])(_.left.get)
        )

      case "IndexExistingInfotonCommand" =>
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "uuid".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'uuid' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'uuid' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val uuid = jsonParser.getText
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "weight".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'weight' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_NUMBER_INT,
          s"expected value for 'weight' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val weight = jsonParser.getLongValue
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "path".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'path' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'path' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val path = jsonParser.getText
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "indexName".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'indexName' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'indexName' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val indexName = jsonParser.getText
        command = IndexExistingInfotonCommand(
          uuid,
          weight,
          path,
          indexName,
          tidOpt.fold(Seq.empty[StatusTracking])(_.left.get)
        )
        assume(
          jsonParser.nextToken() == JsonToken.END_OBJECT,
          s"expected end of command object\n${jsonParser.getCurrentLocation.toString}"
        )

      case "OverwriteCommand" | "OverWriteCommand" =>
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "infoton".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'infoton' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        val infoton = decodeInfotonWithParser(jsonParser)
        command = OverwriteCommand(infoton, tidOpt.flatMap(_.right.toOption))
        //expecting end of command object
        assume(
          jsonParser.nextToken() == JsonToken.END_OBJECT,
          s"expected end of command object\n${jsonParser.getCurrentLocation.toString}"
        )

      case "DeleteAttributesCommand" =>
        // expecting 'path' field
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "path".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'path' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'path' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val path = jsonParser.getText
        // expecting 'fields' object
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "fields".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'fields' field name token\n${jsonParser.getCurrentLocation.toString}"
        )
        val fields = decodeFieldsWithParser(jsonParser)
        // expecting 'lastModified' field
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "lastModified"
            .equals(jsonParser.getCurrentName()),
          s"expected 'lastModified' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'lastModified' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val lastModified = dateFormatter.parseDateTime(jsonParser.getText())
        command = DeleteAttributesCommand(
          path,
          fields,
          lastModified,
          tidOpt.flatMap(_.right.toOption),
          prevUUIDOpt
        )
        //expecting end of command object
        assume(
          jsonParser.nextToken() == JsonToken.END_OBJECT,
          s"expected end of command object\n${jsonParser.getCurrentLocation.toString}"
        )

      case "DeletePathCommand" =>
        // expecting 'path' field
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "path".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'path' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'path' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val path = jsonParser.getText
        // expecting 'lastModified' field
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "lastModified"
            .equals(jsonParser.getCurrentName()),
          s"expected 'lastModified' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'lastModified' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val lastModified = dateFormatter.parseDateTime(jsonParser.getText())
        command = DeletePathCommand(
          path,
          lastModified,
          tidOpt.flatMap(_.right.toOption),
          prevUUIDOpt
        )
        //expecting end of command object
        assume(
          jsonParser.nextToken() == JsonToken.END_OBJECT,
          s"expected end of command object\n${jsonParser.getCurrentLocation.toString}"
        )

      case "UpdatePathCommand" =>
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "path".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'path' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'path' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val path = jsonParser.getText
        // expecting 'deleteFields' object
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "deleteFields"
            .equals(jsonParser.getCurrentName()),
          s"expected 'deleteFields' field name token\n${jsonParser.getCurrentLocation.toString}"
        )
        val deleteFields = decodeFieldsWithParser(jsonParser)
        // expecting 'updateFields' object
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "updateFields"
            .equals(jsonParser.getCurrentName()),
          s"expected 'updateFields' field name token\n${jsonParser.getCurrentLocation.toString}"
        )
        val updateFields = decodeFieldsWithParser(jsonParser)
        // expecting 'lastModified' field
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "lastModified"
            .equals(jsonParser.getCurrentName()),
          s"expected 'lastModified' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'lastModified' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val lastModified = dateFormatter.parseDateTime(jsonParser.getText())
        command = UpdatePathCommand(
          path,
          deleteFields,
          updateFields,
          lastModified,
          tidOpt.flatMap(_.right.toOption),
          prevUUIDOpt
        )
        //expecting end of command object
        assume(
          jsonParser.nextToken() == JsonToken.END_OBJECT,
          s"expected end of command object\n${jsonParser.getCurrentLocation.toString}"
        )

      case "CommandRef" =>
        assume(
          jsonParser.nextToken() == JsonToken.FIELD_NAME && "ref".equals(
            jsonParser.getCurrentName()
          ),
          s"expected 'ref' field name\n${jsonParser.getCurrentLocation.toString}"
        )
        assume(
          jsonParser.nextToken() == JsonToken.VALUE_STRING,
          s"expected value for 'ref' field\n${jsonParser.getCurrentLocation.toString}"
        )
        val ref = jsonParser.getText
        command = CommandRef(ref)
        assume(
          jsonParser.nextToken() == JsonToken.END_OBJECT,
          s"expected end of command object\n${jsonParser.getCurrentLocation.toString}"
        )
    }
    command
  }
}
