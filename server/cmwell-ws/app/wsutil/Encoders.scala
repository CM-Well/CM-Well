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
package cmwell.util.formats

import cmwell.common.file.MimeTypeIdentifier
import cmwell.domain._
import cmwell.syntaxutils._
import cmwell.ws.Settings
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.binary.Base64
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone}
import play.api.libs.json._
import cmwell.domain.{SystemFields => SF}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * Created with IntelliJ IDEA.
  * User: Michael
  * Date: 3/20/14
  * Time: 10:38 AM
  * To change this template use File | Settings | File Templates.
  */
object Encoders extends LazyLogging{
  private val withDotdateParser = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC)
  private val withoutDotdateParser = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(DateTimeZone.UTC)

  //TODO: instead of using a "can parse" + "parse" methods, why not just using one that returns a Try[DateTime] or Option[DateTime] ???
  private def canParseDate(str: String): Boolean = FDate.isDate(str)

  private def parseDate(str: String) = {
    if (str.contains(".")) withDotdateParser.parseDateTime(str) else withoutDotdateParser.parseDateTime(str)
  }

  class FieldAggregator() {
    val l = new ListBuffer[(String, JsValue)]

    def add(value: (String, JsValue)) = {
      l += value
    }

    def +=(value: (String, JsValue)) = {
      add(value)
    }

    def get(): JsObject = {
      JsObject(l)
    }
  }

  case class InfotonParsingException(msg: String = "") extends Exception {
    override def getMessage: String = msg
  }

  implicit object FieldValueEncoder extends Format[FieldValue] {
    override def writes(o: FieldValue): JsValue = o match {
      case FString(str, _, _)         => JsString(str)
      case FReference(ur, _)          => JsString(ur)
      case FDate(date, _)             => JsString(date)
      case FBoolean(bool, _)          => JsBoolean(bool)
      case FExternal(v, _, _)         => JsString(v)
      case FInt(int, _)               => JsNumber(BigDecimal(int))
      case FLong(long, _)             => JsNumber(BigDecimal(long))
      case FBigInt(bigInt, _)         => JsNumber(BigDecimal(bigInt))
      case FFloat(float, _)           => JsNumber(BigDecimal.decimal(float))
      case FDouble(double, _)         => JsNumber(BigDecimal(double))
      case FBigDecimal(bigDecimal, _) => JsNumber(BigDecimal(bigDecimal))
      case FNull(_)                   => !!! //this is just a marker for IMP, should not index it anywhere...
      case _: FExtra[_] =>
        !!! // FExtra is just a marker for outputting special properties, should not index it anywhere...
    }

    override def reads(json: JsValue): JsResult[FieldValue] = json match {
      case JsString(str) if FReference.isUriRef(str) => JsSuccess(FReference(str))
      case JsString(str) if FDate.isDate(str)        => JsSuccess(FDate(str))
      case JsString(str)                             => JsSuccess(FString(str))
      case JsNumber(num)                             => JsSuccess(numToFieldValue(num))
      case JsBoolean(bool)                           => JsSuccess(FBoolean(bool))
    }
  }

  //TODO: is there a better, more elegant way to write this ugly method?
  def numToFieldValue(num: BigDecimal): FieldValue = {
    Try(num.toIntExact) match {
      case Success(int) => FInt(int)
      case Failure(_) =>
        Try(num.toLongExact) match {
          case Success(long) => FLong(long)
          case Failure(_) =>
            num.toBigIntExact match {
              case Some(bigInt) => FBigInt(bigInt.underlying())
              case None =>
                num.isDecimalFloat match {
                  case true => FFloat(num.floatValue)
                  case false =>
                    num.isDecimalDouble match {
                      case true  => FDouble(num.doubleValue)
                      case false => FBigDecimal(num.underlying())
                    }
                }
            }
        }
    }
  }

  implicit object InfotonEncoder extends Format[Infoton] {

    /**
      * extract the path field from the given json. The default date will be used and uuid and parent fields are
      * calculated fields anyway
      * @param node
      * @return
      */
    private def processSystem(node: JsValue): (String, DateTime, String, String, String) = {
      val pathOpt = (node \ "path").asOpt[String]
      val lmOpt = (node \ "lastModified")
        .asOpt[String]
        .orElse((node \ "modifiedDate").asOpt[String])
        .flatMap(cmwell.util.string.parseDate)
      val modifier = (node \ "lastModifiedBy").asOpt[String]
      val dataCenter = (node \ "dataCenter").asOpt[String].getOrElse(Settings.dataCenter)
      val protocol = (node \ "protocol").asOpt[String].getOrElse("http")
      pathOpt match {
        case None    => throw InfotonParsingException("No path specified in system")
        case Some(p) => (p, lmOpt.getOrElse(new DateTime), modifier.getOrElse(""), dataCenter, protocol)
      }
    }

    private def processFields(node: JsValue): Map[String, Set[FieldValue]] = {
      val mapOpt = node.asOpt[Map[String, Set[FieldValue]]]

      mapOpt match {
        case None      => throw InfotonParsingException("Failed to decode fields")
        case Some(map) => map
      }
    }

    private def processFileContent(node: JsValue): FileContent = {
      val mimeTypeOpt = (node \ "mimeType").asOpt[String]
      val lengthOpt = (node \ "length").asOpt[Long]
      val dataOpt = (node \ "data").asOpt[String]
      val base64dataOpt = (node \ "base64-data").asOpt[String]

      (mimeTypeOpt, lengthOpt, dataOpt, base64dataOpt) match {
        case (Some(_), None, None, None) =>
          throw InfotonParsingException("Not enough parameters in the file content section")
        case (None, _, _, _) => throw InfotonParsingException("No mimetype specified in the file content section")
        case (_, _, Some(d), Some(d64)) =>
          throw InfotonParsingException("Both data and base64-data are present in file content section")
        case (Some(mt), Some(l), None, None) =>
          val mimeType = mt
          val length = l
          FileContent(mimeType, length.toLong)
        case (Some(mt), _, Some(d), None) =>
          val mimeType = mt
          val textData = d

          val charset = mimeType.lastIndexOf("charset=") match {
            case i if (i != -1) => mimeType.substring(i + 8).trim
            case _              => "utf-8"
          }
          FileContent(textData.getBytes(charset), mimeType)
        case (Some(mt), _, None, Some(d64)) =>
          val mimeType = mt
          val binaryData = d64
          FileContent(new Base64().decode(binaryData), mimeType)
      }

    }

    private def processObjectInfoton(node: JsValue): ObjectInfoton = {
      val systemFieldOpt = (node \ "system").asOpt[JsValue]
      val fieldsOpt = (node \ "fields").asOpt[JsValue]

      (systemFieldOpt, fieldsOpt) match {
        case (None, _) => throw InfotonParsingException("System object is missing")
        case (Some(sf), Some(f)) =>
          val (path, md, modifier, dc, protocol) = processSystem(sf)
          val fields = processFields(f)
          ObjectInfoton(SF(path, md, modifier, dc, None, "", protocol), fields)
        case (Some(sf), None) =>
          val (path, md, modifier, dc, protocol) = processSystem(sf)
          ObjectInfoton(SF(path, md, modifier, dc, None, "", protocol), None)
      }
    }

    private def processCompoundInfoton(node: JsValue): ObjectInfoton = processObjectInfoton(node)

    private def processFileInfoton(node: JsValue): FileInfoton = {
      val systemFieldOpt = (node \ "system").asOpt[JsValue]
      val fieldsOpt = (node \ "fields").asOpt[JsValue]
      val fileContentsOpt = (node \ "content").asOpt[JsValue]

      (systemFieldOpt, fieldsOpt, fileContentsOpt) match {
        case (Some(sf), None, None) =>
          val (path, md, modifier, dc, p) = processSystem(sf)
          FileInfoton(SF(path, md, modifier, dc, None, "", p), None, None)
        case (None, _, _) => throw InfotonParsingException("System field is not present")
        case (Some(sf), Some(f), Some(fc)) =>
          val (path, md, modifier, dc, p) = processSystem(sf)
          val fields = processFields(f)
          val fileContent = processFileContent(fc)
          FileInfoton(SF(path, md, modifier, dc, None, "", p), fields, fileContent)
        case (Some(sf), None, Some(fc)) =>
          val (path, md, modifier, dc, p) = processSystem(sf)
          val fields = None
          val fileContent = processFileContent(fc)
          FileInfoton(SF(path, md, modifier, dc, None, "", p), None, Some(fileContent))
        case (Some(sf), Some(f), None) =>
          val (path, md, modifier, dc, p) = processSystem(sf)
          val fields = processFields(f)
          FileInfoton(SF(path, md, modifier, dc, None, "", p), Some(fields), None)
      }
    }

    private def processLinkInfoton(node: JsValue): LinkInfoton = {
      val systemFieldOpt = (node \ "system").asOpt[JsValue]
      val fieldsOpt = (node \ "fields").asOpt[JsValue]
      val linkTo = (node \ "linkTo").asOpt[String]
      val linkType = (node \ "linkType").asOpt[Int]
      val modifier = (node \ "lastModifiedBy").asOpt[String]

      (systemFieldOpt, fieldsOpt, linkTo, linkType) match {
        case (None, _, _, _) => throw InfotonParsingException("System field is not present")
        case (_, _, _, None) => throw InfotonParsingException("LinkTo field is not present")
        case (_, _, None, _) => throw InfotonParsingException("LinkType field is not present")
        case (Some(s), Some(f), Some(lTo), Some(lType)) =>
          val (path, md, modBy, dc, p) = processSystem(s)
          val fields = processFields(f)
          LinkInfoton(SF(path, md, modBy, dc, None, "", p), fields, lTo, lType)
        case (Some(s), None, Some(lTo), Some(lType)) =>
          val (path, md, modBy, dc, p) = processSystem(s)
          LinkInfoton(SF(path, md, modBy, dc, None, "", p), linkTo = lTo, linkType = lType)
      }
    }

    private def getSystem(infoton: Infoton): JsValue = {
      val structure = new FieldAggregator

      structure += "path" -> Json.toJson(infoton.systemFields.path)
      structure += "lastModified" -> Json.toJson(
        infoton.systemFields.lastModified.toString(ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC))
      )
      structure += "parent" -> Json.toJson(infoton.parent)
      structure += "uuid" -> Json.toJson(infoton.uuid)
      structure.get()
    }

    private def getContent(fc: FileContent): JsValue = {
      val structure = new FieldAggregator

      structure += "mimeType" -> Json.toJson(fc.mimeType)
      try {
        structure += "length" -> Json.toJson(fc.length)
      } catch {
        case _: Throwable =>
      }

      (fc.data, MimeTypeIdentifier.isTextual(fc.mimeType)) match {
        case (None, _) =>
        case (Some(d), true) =>
          val charset = fc.mimeType.lastIndexOf("charset=") match {
            case i if (i != -1) => fc.mimeType.substring(i + 8).trim
            case _              => "utf-8"
          }
          structure += "data" -> Json.toJson(new String(d, charset))
        case (Some(d), false) =>
          structure += "base64-data" -> Json.toJson(new Base64(0).encodeToString(d))
      }

      structure.get()
    }

    def reads(json: JsValue): JsResult[Infoton] = {
      val typeFieldOpt = (json \ "type").asOpt[String]
      typeFieldOpt match {
        case Some(typeField) =>
          typeField match {
            case "ObjectInfoton"   => JsSuccess(processObjectInfoton(json))
            case "CompoundInfoton" => JsSuccess(processCompoundInfoton(json))
            case "FileInfoton"     => JsSuccess(processFileInfoton(json))
            case "LinkInfoton"     => JsSuccess(processLinkInfoton(json))
            case _                 => JsError()
          }
        case None => JsError()
      }
    }

    def writes(infoton: Infoton): JsValue = {
      val structure = new FieldAggregator

      infoton match {
        case oi: ObjectInfoton =>
          structure += "type" -> Json.toJson("ObjectInfoton")
          structure += "system" -> getSystem(oi)

          val fields = oi.fields
          fields.collect { case f => structure += "fields" -> Json.toJson(f) }
        case ci: CompoundInfoton =>
          structure += "type" -> Json.toJson("CompoundInfoton")
          structure += "system" -> getSystem(ci)

          val fields = ci.fields
          fields.collect { case f => structure += "fields" -> Json.toJson(f) }

          structure += "children" -> Json.toJson(ci.children)

          structure += "offset" -> Json.toJson(ci.offset)
          structure += "length" -> Json.toJson(ci.length)
          structure += "total" -> Json.toJson(ci.total)

        case fi: FileInfoton =>
          structure += "type" -> Json.toJson("FileInfoton")
          structure += "system" -> getSystem(fi)

          val fields = fi.fields
          fields.collect { case f => structure += "fields" -> Json.toJson(f) }

          val content = fi.content
          content.collect { case c => structure += "content" -> getContent(c) }

        case li: LinkInfoton =>
          structure += "type" -> Json.toJson("LinkInfoton")
          structure += "system" -> getSystem(li)

          val fields = li.fields
          fields.collect { case f => structure += "fields" -> Json.toJson(f) }

          structure += "linkTo" -> Json.toJson(li.linkTo)
          structure += "linkType" -> Json.toJson(li.linkType)
        case di: DeletedInfoton =>
          structure += "type" -> Json.toJson("DeletedInfoton")
          structure += "system" -> getSystem(di)
        case x: GhostInfoton => logger.error(s"Unexpected infoton. Received: $x"); ???
      }
      structure.get()
    }
  }

  implicit object BagOfInfotonsEncoder extends Format[BagOfInfotons] {
    def reads(node: JsValue): JsResult[BagOfInfotons] = {
      val itOpt = (node \ "infotons").asOpt[ListBuffer[JsValue]]
      itOpt match {
        case None => JsError()
        case Some(it) =>
          try {
            JsSuccess(BagOfInfotons(it.toList.map(i => i.as[Infoton])))
          } catch {
            case t: JsResultException => JsError()
          }
      }
    }

    def writes(o: BagOfInfotons): JsValue = {
      val structure = new FieldAggregator
      structure += "type" -> Json.toJson("BagOfInfotons")
      structure += "infotons" -> Json.toJson(o.infotons)

      structure.get()
    }
  }

  implicit object IterationResultsEncoder extends Format[IterationResults] {
    def reads(json: JsValue): JsResult[IterationResults] = ???

    def writes(o: IterationResults): JsValue = {
      val structure = new FieldAggregator
      structure += "type" -> Json.toJson("IterationResults")
      structure += "iterator-id" -> Json.toJson(o.iteratorId)
      structure += "totalHits" -> Json.toJson(o.totalHits)
      o.infotons.collect { case is => structure += "infotons" -> Json.toJson(is) }

      structure.get()
    }
  }

  implicit object PaginationInfoEncoder extends Format[PaginationInfo] {
    def reads(json: JsValue): JsResult[PaginationInfo] = ???

    def writes(o: PaginationInfo): JsValue = {
      val structure = new FieldAggregator
      structure += "type" -> Json.toJson("PaginationInfo")
      structure += "first" -> Json.toJson(o.first)
      structure += "self" -> Json.toJson(o.self)
      if (o.previous.isDefined) structure += "previous" -> Json.toJson(o.previous.get)
      if (o.next.isDefined) structure += "next" -> Json.toJson(o.next.get)
      structure += "last" -> Json.toJson(o.last)
      structure.get()
    }
  }

  implicit object BucketEncoder extends Format[Bucket] {
    override def reads(json: JsValue): JsResult[Bucket] = ???

    override def writes(b: Bucket): JsValue = {
      val structure = new FieldAggregator
      structure += "key" -> Json.toJson(b.key)
      structure += "objects" -> Json.toJson(b.docCount)
      b match {
        case SignificantTermsBucket(_, _, score, bgCount, _) =>
          structure += "score" -> Json.toJson(score)
          structure += "bgCount" -> Json.toJson(bgCount)
        case _ =>
      }

      b.subAggregations.foreach {
        structure += "AggregationsResponse" -> aggregationsResponseToJson(_)
      }
      structure.get
    }
  }

  implicit object AggregationFilterEncoder extends Format[AggregationFilter] {
    def writes(af: AggregationFilter): JsValue = {
      val structure = new FieldAggregator
      af match {
        case taf: TermAggregationFilter =>
          structure += "field" -> JsString(taf.field.value)
          structure += "size" -> JsNumber(taf.size)
        case saf: StatsAggregationFilter =>
          structure += "field" -> JsString(saf.field.value)
        case haf: HistogramAggregationFilter =>
          structure += "field" -> JsString(haf.field.value)
          structure += "interval" -> JsNumber(haf.interval)
          structure += "minimum doc count" -> JsNumber(haf.minDocCount)
          haf.extMin.foreach { structure += "extended minimum" -> JsNumber(_) }
          haf.extMax.foreach { structure += "extended maximum" -> JsNumber(_) }
        case SignificantTermsAggregationFilter(_, field, backGroundTerm, minDocCount, size, _) =>
          structure += "field" -> JsString(field.value)
          backGroundTerm.foreach { kv =>
            structure += "background term" -> JsObject(Seq("key" -> JsString(kv._1), "value" -> JsString(kv._2)))
          }
          structure += "minimum doc count" -> JsNumber(minDocCount)
          structure += "size" -> JsNumber(size)
        case CardinalityAggregationFilter(_, field, precisionThresholdOpt) =>
          structure += "field" -> JsString(field.value)
          precisionThresholdOpt.foreach { pt =>
            structure += "precision threshold" -> JsNumber(pt)
          }
      }

      structure.get()
    }

    override def reads(json: JsValue): JsResult[AggregationFilter] = ???
  }

  def aggregationsResponseToJson(asr: AggregationsResponse): JsArray = {
    JsArray(
      asr.responses.map { ar =>
        val intStructure = new FieldAggregator
        intStructure += "name" -> JsString(ar.name)
        intStructure += "type" -> JsString(ar.`type`)
        intStructure += "filter" -> Json.toJson(ar.filter)

        ar match {
          case sar: StatsAggregationResponse =>
            intStructure += "results" -> JsObject(
              Seq(
                "count" -> JsNumber(sar.count),
                "min" -> JsNumber(sar.min),
                "max" -> JsNumber(sar.max),
                "avg" -> JsNumber(sar.avg),
                "sum" -> JsNumber(sar.sum)
              )
            )
          case star: SignificantTermsAggregationResponse =>
            intStructure += "objects" -> JsNumber(star.docCount)
          case CardinalityAggregationResponse(_, count) =>
            intStructure += "count" -> JsNumber(count)
          case _ =>
        }

        if (ar.isInstanceOf[BucketsAggregationResponse]) {
          intStructure += "buckets" -> Json.toJson(ar.asInstanceOf[BucketsAggregationResponse].buckets)
        }

        intStructure.get()
      }
    )

  }

  implicit object AggregationsResponseEncoder extends Format[AggregationsResponse] {

    def reads(json: JsValue): JsResult[AggregationsResponse] = ???

    def writes(asr: AggregationsResponse): JsValue = {
      val structure = new FieldAggregator

      structure += "AggregationsResponse" -> aggregationsResponseToJson(asr)

      asr.debugInfo.foreach { debugInfoStr =>
        structure += "debugInfo" -> JsString(debugInfoStr)
      }

      structure.get
    }
  }

  implicit object InfotonPathsEncoder extends Format[InfotonPaths] {
    def reads(json: JsValue): JsResult[InfotonPaths] = {
      val pathsOpt = (json \ "paths").asOpt[List[String]]

      pathsOpt match {
        case Some(paths) => JsSuccess(InfotonPaths(paths))
        case None        => throw InfotonParsingException("Bad paths list")
      }
    }

    def writes(o: InfotonPaths): JsValue = {
      val structure = new FieldAggregator
      structure += "paths" -> JsArray(o.paths.map { JsString(_) })
      structure.get()
    }
  }
}

case class SystemFields(path: String, lastModified: Option[DateTime], uuid: Option[String], parent: Option[String])
