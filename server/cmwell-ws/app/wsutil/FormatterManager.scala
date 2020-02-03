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
package wsutil

import javax.inject._
import cmwell.domain.{Formattable, Infoton}
import cmwell.formats._
import cmwell.fts.{FieldFilter, FieldOperator}
import cmwell.web.ld.cmw.CMWellRDFHelper
import com.typesafe.scalalogging.LazyLogging
import markdown.PrettyCsvFormatter

object FormatterManager {
  val prettyMangledField: String => String = {
    case s if s.length > 1 && s(1) == '$' =>
      s.head match {
        case 'i' => s.drop(2) + ": Int"
        case 'l' => s.drop(2) + ": Long/BigInt"
        case 'w' => s.drop(2) + ": Double/BigDecimal"
        case 'b' => s.drop(2) + ": Boolean"
        case 'd' => s.drop(2) + ": Date"
        case 'f' => s.drop(2) + ": Float"
      }
    case s => s
  }
  def multiFormattableToSeq(formattable: Formattable, formatter: Formatter): String = {

    val infotons: Seq[Infoton] = formattable match {
      case _ => ???
    }

    formatFormattableSeq(infotons, formatter)
  }

  def formatFormattableSeq[T <: Formattable](infotons: Seq[T], formatter: Formatter): String = {

    val sb = new StringBuilder()

    infotons.foreach { i =>
      val formatted = formatter.render(i) + "\n"
      sb.append(formatted)
    }

    sb.mkString
  }

  private def getKeyForRdfFormatterMap(rdfFlavor: RdfFlavor,
                                       host: String,
                                       withoutMeta: Boolean,
                                       filterOutBlanks: Boolean,
                                       forcrUniquness: Boolean,
                                       pretty: Boolean,
                                       raw: Boolean,
                                       callback: Option[String]): String = {

    def bool2string(b: Boolean): String = if (b) "T" else "F"

    if (Set[RdfFlavor](JsonLDFlavor, JsonLDQFlavor)(rdfFlavor)) {
      s"${rdfFlavor.key}\t$host\t${bool2string(withoutMeta)}\t${bool2string(filterOutBlanks)}\t${bool2string(
        forcrUniquness
      )}\t${bool2string(pretty)}\t${bool2string(raw)}\t${callback.getOrElse("")}"
    } else {
      s"${rdfFlavor.key}\t$host\t${bool2string(withoutMeta)}\t${bool2string(filterOutBlanks)}\t${bool2string(raw)}\t${bool2string(forcrUniquness)}\t\t"
    }
  }
}

@Singleton
class FormatterManager @Inject()(C: CMWellRDFHelper) extends LazyLogging {

  import FormatterManager._

  //var is OK as not volatile, cache, frequent reads + rare writes of immutable object pattern (Gilad + Dudi)
  private[this] var rdfFormatterMap = Map[String, RDFFormatter]()

  def innerToSimpleFieldName(fieldName: String): String = {
    fieldName.lastIndexOf('.') match {
      case -1 => fieldName
      case i => {
        val (first, dotLast) = fieldName.splitAt(i)
        val last = dotLast.tail
        C.hashToUrlAndPrefix(last, None) match {
          case None              => fieldName
          case Some((_, prefix)) => s"$first.$prefix"
        }
      }
    }
  }

  lazy val jsonFormatter = new JsonFormatter(innerToSimpleFieldName)
  lazy val prettyJsonFormatter = new PrettyJsonFormatter(innerToSimpleFieldName)
  lazy val yamlFormatter = new YamlFormatter(innerToSimpleFieldName)
  lazy val csvFormatter = CSVFormatter(prettyMangledField.compose(innerToSimpleFieldName))
  lazy val prettyCsvFormatter = new PrettyCsvFormatter(innerToSimpleFieldName)

  val fieldTranslatorForRichRDF: Option[Long] => String => Option[(String, Option[String])] = (t: Option[Long]) =>
    (s: String) => C.hashToUrlAndPrefix(s, t).map { case (url, prefix) => url -> Option(prefix) }
  val fieldTranslatorForPrefixlessRDF: Option[Long] => String => Option[(String, Option[String])] = (t: Option[Long]) =>
    (s: String) => C.hashToUrl(s, t).map { case url => url -> None }

  def getFormatter(
    format: FormatType,
    timeContext: Option[Long],
    host: String = "http://cm-well",
    uri: String = "http://cm-well",
    pretty: Boolean = false,
    raw: Boolean = false,
    callback: Option[String] = None,
    fieldFilters: Option[FieldFilter] = None,
    offset: Option[Long] = None,
    length: Option[Long] = None,
    withData: Option[String] = None,
    withoutMeta: Boolean = false,
    forceUniqueness: Boolean = false, //if you want histories to not collide, e.g. searching with-history and output RDF (RDF only flag)
    filterOutBlanks: Boolean = false
  ): Formatter = {
    format match {
      case TextType                                 => PathFormatter
      case TsvType                                  => TsvFormatter
      case CsvType if pretty                        => prettyCsvFormatter
      case CsvType                                  => csvFormatter
      case JsonType if pretty && callback.isDefined => new PrettyJsonFormatter(innerToSimpleFieldName, callback)
      case JsonType if pretty                       => prettyJsonFormatter
      case JsonType if callback.isDefined           => new JsonFormatter(innerToSimpleFieldName, callback)
      case JsonType                                 => jsonFormatter
      case JsonlType if pretty =>
        new PrettyJsonlFormatter(C.hashToUrlAndPrefix(_, timeContext), { quadUrl =>
          C.getAliasForQuadUrl(quadUrl) match {
            case opt @ Some(alias) => opt
            case None              => Some(quadUrl)
          }
        }, callback)
      case JsonlType => new JsonlFormatter(C.hashToUrlAndPrefix(_, timeContext), Some.apply, callback)
      case YamlType  => yamlFormatter
      case RdfType(rdfFlavor) => {
        val key =
          getKeyForRdfFormatterMap(rdfFlavor, host, withoutMeta, filterOutBlanks, forceUniqueness, pretty, raw, callback)
        if (rdfFormatterMap.contains(key)) rdfFormatterMap(key)
        else {
          val newFormatter = rdfFlavor match {
            case RdfXmlFlavor =>
              new RDFXmlFormatter(host,
                                  fieldTranslatorForRichRDF(timeContext),
                                  withoutMeta,
                                  filterOutBlanks,
                                  raw,
                                  forceUniqueness)
            case TurtleFlavor =>
              new TurtleFormatter(host,
                                  fieldTranslatorForRichRDF(timeContext),
                                  withoutMeta,
                                  filterOutBlanks,
                                  raw,
                                  forceUniqueness)
            case N3Flavor =>
              new N3Formatter(host,
                              fieldTranslatorForRichRDF(timeContext),
                              withoutMeta,
                              filterOutBlanks,
                              raw,
                              forceUniqueness)
            case NTriplesFlavor =>
              new NTriplesFormatter(host,
                                    fieldTranslatorForPrefixlessRDF(timeContext),
                                    withoutMeta,
                                    filterOutBlanks,
                                    raw,
                                    forceUniqueness)
            case JsonLDFlavor =>
              JsonLDFormatter(host,
                              fieldTranslatorForRichRDF(timeContext),
                              withoutMeta,
                              filterOutBlanks,
                              forceUniqueness,
                              pretty,
                              raw,
                              callback)
            case NquadsFlavor =>
              new NQuadsFormatter(host,
                                  fieldTranslatorForPrefixlessRDF(timeContext),
                                  withoutMeta,
                                  filterOutBlanks,
                                  raw,
                                  forceUniqueness)
            case TriGFlavor =>
              new TriGFormatter(host,
                                fieldTranslatorForRichRDF(timeContext),
                                C.getAliasForQuadUrl,
                                withoutMeta,
                                filterOutBlanks,
                                raw,
                                forceUniqueness)
            case TriXFlavor =>
              new TriXFormatter(host,
                                fieldTranslatorForRichRDF(timeContext),
                                C.getAliasForQuadUrl,
                                withoutMeta,
                                filterOutBlanks,
                                raw,
                                forceUniqueness)
            case JsonLDQFlavor =>
              JsonLDQFormatter(host,
                               fieldTranslatorForRichRDF(timeContext),
                               C.getAliasForQuadUrl,
                               withoutMeta,
                               filterOutBlanks,
                               forceUniqueness,
                               pretty,
                               raw,
                               callback)
          }
          rdfFormatterMap = rdfFormatterMap.updated(key, newFormatter)
          newFormatter
        }
      }
      case AtomType => {

        val innerFormatterOpt = withData.map(ft => FormatExtractor.withDefault(ft, RdfType(TriGFlavor))).map { ft =>
          if (ft eq AtomType) throw new IllegalArgumentException("you can't have atom format with inline atom data!")
          else getFormatter(ft, timeContext, host, uri, pretty, raw, callback, fieldFilters, offset, length, None)
        }
        (offset, length) match {
          case (Some(o), Some(l)) => AtomFormatter(host, uri, fieldFilters, o, l, innerFormatterOpt)
          case (None, None)       => AtomFormatter(host, uri, innerFormatterOpt)
          case _ => {
            logger.warn(s"Atom formatter: case that was un-thought of reached with: $fieldFilters , $offset , $length")
            AtomFormatter(host, uri, innerFormatterOpt)
          }
        }
      }
    }
  }
}
