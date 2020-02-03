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
package cmwell.formats

import java.util

import cmwell.domain._
import cmwell.util.collections._
import cmwell.util.string.dateStringify
import cmwell.common.file.MimeTypeIdentifier._
import org.apache.commons.csv.CSVPrinter

import scala.math.max
import org.apache.commons.csv._
import org.apache.commons.codec.binary.Base64.encodeBase64String

/**
  * Created by gilad on 11/15/15.
  */
object CSVFormatter {
  def apply(fieldNameModifier: String => String) =
    new CSVFormatter(fieldNameModifier)
}

class CSVFormatter(fieldNameModifier: String => String) extends Formatter {

  override def format: FormatType = CsvType

  override def render(formattable: Formattable): String = formattable match {
    case aggr: AggregationsResponse => renderAggregationsResponse(aggr)
    case generic                    => renderGeneric(generic)
  }

  private[this] def renderGeneric(formattable: Formattable): String = {
    val infotons: Seq[Infoton] = extractInfotonsFromFormattable(formattable)
    val internalFields = infotons
      .map(_.fields)
      .collect {
        case Some(m) => m.keySet
      }
      .flatten
      .distinct
      .toVector
      .sortBy { s =>
        s.lastIndexOf('.') match {
          case -1 => s"${Char.MaxValue}" * 3
          case n  => s.splitAt(n)._2.tail
        }
      }
    val modifiedFieldsMap = internalFields.map(f => f -> fieldNameModifier(f)).toMap
    //                      0       1               2                  3       4       5         6            7
    val regularSystem = Seq("path", "lastModified", "lastModifiedBy", "type", "uuid", "parent", "dataCenter", "indexTime")
    val specialSystem = distinctBy(infotons)(_.kind).collect {
      case _: CompoundInfoton => Seq("children", "offset", "length", "total")
      case _: LinkInfoton     => Seq("linkTo", "linkType")
      case _: FileInfoton =>
        Seq("mimeType", "length") ++ {
          val mimes = infotons.collect {
            case FileInfoton(_, _, Some(FileContent(_, mimeType, _, _))) => mimeType
          }
          if (mimes.forall(isTextual)) Seq("data")
          else if (mimes.exists(isTextual)) Seq("data", "base64-data")
          else Seq("base64-data")
        }
    }.flatten

    val systemHeader = regularSystem ++ specialSystem
    val (internalFieldsMap, totalLength) = {
      val intMap = infotons.foldLeft(Map.empty[String, Int].withDefaultValue(0)) { (m, i) =>
        {
          i.fields match {
            case None => m
            case Some(fields) =>
              fields.foldLeft(m) {
                case (mm, (field, valueSet)) => mm.updated(field, max(mm(field), valueSet.size))
              }
          }
        }
      }
      var index = systemHeader.size
      internalFields.map { field =>
        val tmp = index
        index += intMap(field)
        field -> tmp
      }.toMap -> index
    }

    val ssys = specialSystem.zipWithIndex.toMap.mapValues(8.+)

    val columnMapper: Infoton => Map[Int, String] = { i =>
      def flattenFields(fields: Map[String, Set[FieldValue]]): Seq[(Int, String)] = {
        fields.toSeq.flatMap {
          case (k, vs) => {
            val idx = internalFieldsMap(k)
            vs.toSeq.zipWithIndex.map {
              case (v, i) => (i + idx) -> v.toString
            }
          }
        }
      }

      def prependToFields(pre: Seq[(Int, String)], fields: Option[Map[String, Set[FieldValue]]]) = fields match {
        case Some(flds) => pre ++ flattenFields(flds)
        case None       => pre
      }

      val shared = Seq(
        0 -> i.systemFields.path,
        1 -> dateStringify(i.systemFields.lastModified),
        2 -> i.systemFields.lastModifiedBy,
        3 -> i.kind,
        4 -> i.uuid,
        5 -> i.parent,
        6 -> i.systemFields.dc
      ) ++ i.systemFields.indexTime.map(7 -> _.toString)

      ((i: @unchecked) match {
        case CompoundInfoton(_, fields, children, offset, length, total) => {
          val sys = shared ++ Seq(
            ssys("children") -> children.map(_.systemFields.path).mkString("[", ",", "]"),
            ssys("offset") -> offset.toString,
            ssys("length") -> length.toString,
            ssys("total") -> total.toString
          )
          prependToFields(sys, fields)
        }
        case ObjectInfoton(_, fields) => prependToFields(shared, fields)
        case LinkInfoton(_, fields, linkTo, linkType) => {
          val sys = shared ++ Seq(
            ssys("linkTo") -> linkTo,
            ssys("linkType") -> linkType.toString
          )
          prependToFields(sys, fields)
        }
        case FileInfoton(_, fields, content) => {
          val sys = content match {
            case None    => shared
            case Some(c) => shared ++ super.fileContent[Int, String](c, ssys, identity, _.toString, encodeBase64String)
          }
          prependToFields(sys, fields)
        }
        case _: DeletedInfoton => shared
      }).toMap
    }

    val format = {
      val xs = internalFieldsMap.toSeq.sortBy(_._2) :+ ("DUMMY" -> totalLength)
      val ys = {
        if (xs.size < 2) Nil //xs.map(fieldNameModifier compose {_._1})
        else
          xs.sliding(2)
            .map { zs =>
              val (f, i) = zs.head
              val (_, j) = zs.tail.head
              for {
                k <- i until j
                suffix = if (1 + i == j) "" else s" #${k - i + 1}"
              } yield fieldNameModifier(f) + suffix
            }
            .toSeq
            .flatten
      }
      CSVFormat.RFC4180.withHeader(systemHeader ++ ys: _*)
    }
    val sb = new StringBuffer
    val printer = new CSVPrinter(sb, format)

    infotons.foreach { i =>
      val m = columnMapper(i)
      (0 until totalLength).foreach { j =>
        printer.print(m.getOrElse(j, ""))
      }
      printer.println()
    }

    printer.flush()
    printer.close()

    sb.toString
  }

  def extractInfotonsFromFormattable(formattable: Formattable): Seq[Infoton] = formattable match {
    case i: Infoton                  => Seq(i)
    case bag: BagOfInfotons          => bag.infotons
    case ihv: InfotonHistoryVersions => ihv.versions
    case rp: RetrievablePaths        => rp.infotons //TODO: output unreachable paths?
    case sr: SearchResults           => sr.infotons
    case sr: SearchResponse          => sr.results.infotons
    case ir: IterationResults        => ir.infotons.getOrElse(Seq.empty[Infoton])
    //    case pi: PaginationInfo => pi
    //    case sr: SimpleResponse => sr
    case _ =>
      throw new NotImplementedError(
        s"extractInfotonsFromFormattable implementation not found for ${formattable.getClass.toString}"
      )
  }

  private[this] def renderAggregationsResponse(aggr: AggregationsResponse): String = {

    def depth(ag: AggregationsResponse): Int = {
      val allBuckets = ag.responses.collect { case b: BucketsAggregationResponse => b }.flatMap(_.buckets)
      //WHY for histogram there is subAggr that is empty, it should be None !
      val allDepths = allBuckets.map(b => b.subAggregations.fold(0)(sa => if (sa.responses.nonEmpty) depth(sa) else 0))
      1 + allDepths.fold(0)(max)
    }

    def validAggregationsResponse(aggrsResponse: AggregationsResponse): AggregationsResponse = {
      def validAggregationResponse(ar: AggregationResponse): Boolean = ar match {
        case br: BucketsAggregationResponse if br.buckets.nonEmpty       => true
        case StatsAggregationResponse(_, count, _, _, _, _) if count > 0 => true
        case CardinalityAggregationResponse(_, count) if count > 0       => true
        case _                                                           => false
      }
      AggregationsResponse(aggrsResponse.responses.filter(validAggregationResponse), aggrsResponse.debugInfo)
    }

    val aggRespDepth = depth(aggr)
    val validAggResponse = validAggregationsResponse(aggr)

    require(aggRespDepth <= 2, "csv format isn't supported for aggregations of depth greater than 2")
    require(validAggResponse.responses.nonEmpty, "Empty aggregation response! Did you use an existing field?!")
    require(validAggResponse.responses.size < 2, "csv format isn't supported for more than one aggregation response")

    def getSortedKeys(ar: AggregationResponse) = ar match {
      case StatsAggregationResponse(filter, count, min, max, avg, sum) =>
        ("Field-Name", Seq("count", "min", "max", "avg", "sum"))
      case CardinalityAggregationResponse(filter, count) => ("Field-Name", Seq("count"))
      case bar: BucketsAggregationResponse => {
        val keys = aggRespDepth match {
          case 1 => Seq("value")
          case 2 =>
            bar.buckets.flatMap(
              _.subAggregations.fold(Nil: Seq[Any])(
                sa => sa.responses.map(_.asInstanceOf[BucketsAggregationResponse]).flatMap(_.buckets.map(_.key.value))
              )
            )
          case _ => ???
        }
        (s"""${fieldNameModifier(bar.filter.field.value) + bar.buckets.head.subAggregations
          .flatMap(_.responses.headOption)
          .fold("")(ar => " \\ " + fieldNameModifier(ar.filter.field.value))}""", keys.distinct)
      }
    }

    val agr = validAggResponse.responses.head
    val (upperLeftCorner, sortedKeys) = getSortedKeys(agr)
    val format = CSVFormat.RFC4180.withHeader(upperLeftCorner +: sortedKeys.map(_.toString): _*)
    val sb = new java.lang.StringBuilder
    val printer = new CSVPrinter(sb, format)
    agr match {
      case StatsAggregationResponse(filter, count, min, max, avg, sum) =>
        printer.printRecord(fieldNameModifier(filter.field.value),
                            count.toString,
                            min.toString,
                            max.toString,
                            avg.toString,
                            sum.toString)
      case CardinalityAggregationResponse(filter, count) =>
        printer.printRecord(fieldNameModifier(filter.field.value), count.toString)
      case bar: BucketsAggregationResponse =>
        aggRespDepth match {
          case 1 => {
            bar.buckets.foreach(b => printer.printRecord(b.key.value.toString, b.docCount.toString))
          }
          case 2 =>
            bar.buckets.foreach(b => {
              val bucketKeysMap = b.subAggregations.fold(Map[Any, Any]())(
                _.responses.head
                  .asInstanceOf[BucketsAggregationResponse]
                  .buckets
                  .map(b => (b.key.value, b.docCount))
                  .toMap
                  .withDefaultValue("")
              )
              printer.print(b.key.value.toString)
              printer.printRecord(sortedKeys.map(bucketKeysMap).map(_.toString): _*)
            })
          case _ => ???
        }
    }
    printer.flush()
    printer.close()
    sb.toString
  }
}
