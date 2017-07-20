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
    case s if s.length > 1 && s(1) == '$' => s.head match {
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

    formatFormattableSeq(infotons,formatter)
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
                                             callback: Option[String]): String = {

    def bool2string(b: Boolean): String = if(b) "T" else "F"

    if(Set[RdfFlavor](JsonLDFlavor,JsonLDQFlavor)(rdfFlavor)){
      s"${rdfFlavor.key}\t$host\t${bool2string(withoutMeta)}\t${bool2string(filterOutBlanks)}\t${bool2string(forcrUniquness)}\t${bool2string(pretty)}\t${callback.getOrElse("")}"
    } else {
      s"${rdfFlavor.key}\t$host\t${bool2string(withoutMeta)}\t${bool2string(filterOutBlanks)}\t${bool2string(forcrUniquness)}\t\t"
    }
  }
}

@Singleton
class FormatterManager @Inject()(C: CMWellRDFHelper) extends LazyLogging {

  import FormatterManager._

  //var is OK as not volatile, cache, frequent reads + rare writes of immutable object pattern (Gilad + Dudi)
  private[this] var rdfFormatterMap = Map[String, RDFFormatter]()

  def innerToSimpleFieldName(nbg: Boolean)(fieldName: String): String = {
    fieldName.lastIndexOf('.') match {
      case -1 => fieldName
      case i => {
        val (first, dotLast) = fieldName.splitAt(i)
        val last = dotLast.tail
        C.hashToUrlAndPrefix(last, nbg) match {
          case None => fieldName
          case Some((_, prefix)) => s"$first.$prefix"
        }
      }
    }
  }


  private lazy val nJsonFormatter = new JsonFormatter(innerToSimpleFieldName(true))
  private lazy val oJsonFormatter = new JsonFormatter(innerToSimpleFieldName(false))

  def jsonFormatter(nbg: Boolean): JsonFormatter = {
    if (nbg) nJsonFormatter
    else oJsonFormatter
  }

  private lazy val nPrettyJsonFormatter = new PrettyJsonFormatter(innerToSimpleFieldName(true))
  private lazy val oPrettyJsonFormatter = new PrettyJsonFormatter(innerToSimpleFieldName(false))

  def prettyJsonFormatter(nbg: Boolean): PrettyJsonFormatter = {
    if (nbg) nPrettyJsonFormatter
    else oPrettyJsonFormatter
  }

  private lazy val nYamlFormatter = new YamlFormatter(innerToSimpleFieldName(true))
  private lazy val oYamlFormatter = new YamlFormatter(innerToSimpleFieldName(false))

  def yamlFormatter(nbg: Boolean): YamlFormatter = {
    if (nbg) nYamlFormatter
    else oYamlFormatter
  }

  private lazy val nCsvFormatter = CSVFormatter(prettyMangledField compose innerToSimpleFieldName(true))
  private lazy val oCsvFormatter = CSVFormatter(prettyMangledField compose innerToSimpleFieldName(false))
  def csvFormatter(nbg: Boolean): CSVFormatter = {
    if (nbg) nCsvFormatter
    else oCsvFormatter
  }


  private lazy val nPrettyCsvFormatter = new PrettyCsvFormatter(innerToSimpleFieldName(true))
  private lazy val oPrettyCsvFormatter = new PrettyCsvFormatter(innerToSimpleFieldName(false))
  def prettyCsvFormatter(nbg: Boolean): PrettyCsvFormatter = {
    if (nbg) nPrettyCsvFormatter
    else oPrettyCsvFormatter
  }

  val fieldTranslatorForRichRDF: Boolean => String => Option[(String,Option[String])] = nbg => (s: String) => C.hashToUrlAndPrefix(s,nbg).map{ case (url,prefix) => url -> Option(prefix)}
  val fieldTranslatorForPrefixlessRDF: Boolean => String => Option[(String,Option[String])] = nbg => (s: String) => C.hashToUrl(s,nbg).map{ case url => url -> None}

  def getFormatter(format: FormatType,
                   host: String = "http://cm-well",
                   uri: String = "http://cm-well",
                   pretty: Boolean = false,
                   callback: Option[String] = None,
                   fieldFilters: Option[FieldFilter] = None,
                   offset: Option[Long] = None,
                   length: Option[Long] = None,
                   withData: Option[String] = None,
                   withoutMeta: Boolean = false,
                   forceUniqueness: Boolean = false, //if you want histories to not collide, e.g. searching with-history and output RDF (RDF only flag)
                   filterOutBlanks: Boolean = false,
                   nbg: Boolean): Formatter = {
    format match {
      case TextType => PathFormatter
      case TsvType => TsvFormatter
      case CsvType if pretty => prettyCsvFormatter(nbg)
      case CsvType => csvFormatter(nbg)
      case JsonType if pretty && callback.isDefined => new PrettyJsonFormatter(innerToSimpleFieldName(nbg), callback)
      case JsonType if pretty => prettyJsonFormatter(nbg)
      case JsonType if callback.isDefined => new JsonFormatter(innerToSimpleFieldName(nbg), callback)
      case JsonType => jsonFormatter(nbg)
      case JsonlType if pretty => new PrettyJsonlFormatter(C.hashToUrlAndPrefix(_,nbg), { quadUrl =>
        C.getAliasForQuadUrl(quadUrl,nbg) match {
          case opt@Some(alias) => opt
          case None => Some(quadUrl)
        }
      }, callback)
      case JsonlType => new JsonlFormatter(C.hashToUrlAndPrefix(_,nbg), Some.apply, callback)
      case YamlType => yamlFormatter(nbg)
      case RdfType(rdfFlavor) => {
        val key = getKeyForRdfFormatterMap(rdfFlavor, host, withoutMeta, filterOutBlanks, forceUniqueness, pretty, callback)
        if (rdfFormatterMap.contains(key)) rdfFormatterMap(key)
        else {
          val newFormatter = rdfFlavor match {
            case RdfXmlFlavor => new RDFXmlFormatter(host, fieldTranslatorForRichRDF(nbg), withoutMeta, filterOutBlanks, forceUniqueness)
            case TurtleFlavor => new TurtleFormatter(host, fieldTranslatorForRichRDF(nbg), withoutMeta, filterOutBlanks, forceUniqueness)
            case N3Flavor => new N3Formatter(host, fieldTranslatorForRichRDF(nbg), withoutMeta, filterOutBlanks, forceUniqueness)
            case NTriplesFlavor => new NTriplesFormatter(host, fieldTranslatorForPrefixlessRDF(nbg), withoutMeta, filterOutBlanks, forceUniqueness)
            case JsonLDFlavor => JsonLDFormatter(host, fieldTranslatorForRichRDF(nbg), withoutMeta, filterOutBlanks, forceUniqueness, pretty, callback)
            case NquadsFlavor => new NQuadsFormatter(host, fieldTranslatorForPrefixlessRDF(nbg), withoutMeta, filterOutBlanks, forceUniqueness)
            case TriGFlavor => new TriGFormatter(host, fieldTranslatorForRichRDF(nbg), C.getAliasForQuadUrl(_,nbg), withoutMeta, filterOutBlanks, forceUniqueness)
            case TriXFlavor => new TriXFormatter(host, fieldTranslatorForRichRDF(nbg), C.getAliasForQuadUrl(_,nbg), withoutMeta, filterOutBlanks, forceUniqueness)
            case JsonLDQFlavor => JsonLDQFormatter(host, fieldTranslatorForRichRDF(nbg), C.getAliasForQuadUrl(_,nbg), withoutMeta, filterOutBlanks, forceUniqueness, pretty, callback)
          }
          rdfFormatterMap = rdfFormatterMap.updated(key, newFormatter)
          newFormatter
        }
      }
      case AtomType => {

        val innerFormatterOpt = withData.map(ft => FormatExtractor.withDefault(ft, RdfType(TriGFlavor))).map { ft =>
          if (ft eq AtomType) throw new IllegalArgumentException("you can't have atom format with inline atom data!")
          else getFormatter(ft, host, uri, pretty, callback, fieldFilters, offset, length, None, nbg = nbg)
        }
        (offset, length) match {
          case (Some(o), Some(l)) => AtomFormatter(host, uri, fieldFilters, o, l, innerFormatterOpt)
          case (None, None) => AtomFormatter(host, uri, innerFormatterOpt)
          case _ => {
            logger.warn(s"Atom formatter: case that was un-thought of reached with: $fieldFilters , $offset , $length")
            AtomFormatter(host, uri, innerFormatterOpt)
          }
        }
      }
    }
  }
}



