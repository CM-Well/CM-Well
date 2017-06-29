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

import cmwell.domain.{Formattable, Infoton}
import cmwell.formats._
import cmwell.fts.{FieldFilter, FieldOperator}
import cmwell.web.ld.cmw.{CMWellRDFHelper => C}
import com.typesafe.scalalogging.LazyLogging
import markdown.PrettyCsvFormatter

/**
 * Created by gilad on 1/13/15.
 */
object FormatterManager extends LazyLogging {
  //var is OK as not volatile, cache, frequent reads + rare writes of immutable object pattern (Gilad + Dudi)
  private[this] var rdfFormatterMap = Map[String, RDFFormatter]()

def innerToSimpleFieldName(fieldName: String): String = {
    fieldName.lastIndexOf('.') match {
      case -1 => fieldName
      case i => {
        val (first,dotLast) = fieldName.splitAt(i)
        val last = dotLast.tail
        C.hashToUrlAndPrefix(last) match {
          case None => fieldName
          case Some((_,prefix)) => s"$first.$prefix"
        }
      }
    }
  }

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

  JsonFormatter.init(innerToSimpleFieldName)
  PrettyJsonFormatter.init(innerToSimpleFieldName)
  YamlFormatter.init(innerToSimpleFieldName)
  val csvFormatter = CSVFormatter(prettyMangledField compose innerToSimpleFieldName)

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
                   filterOutBlanks: Boolean = false): Formatter = {
    format match {
      case TextType => PathFormatter
      case TsvType => TsvFormatter
      case CsvType if pretty => PrettyCsvFormatter
      case CsvType => csvFormatter
      case JsonType if pretty && callback.isDefined => new PrettyJsonFormatter(innerToSimpleFieldName,callback)
      case JsonType if pretty => PrettyJsonFormatter()
      case JsonType if callback.isDefined => new JsonFormatter(innerToSimpleFieldName,callback)
      case JsonType => JsonFormatter()
      case JsonlType if pretty => new PrettyJsonlFormatter(C.hashToUrlAndPrefix, { quadUrl =>
        C.getAliasForQuadUrl(quadUrl) match {
          case opt@Some(alias) => opt
          case None => Some(quadUrl)
        }
      }, callback)
      case JsonlType => new JsonlFormatter(C.hashToUrlAndPrefix, Some.apply, callback)
      case YamlType => YamlFormatter
      case RdfType(rdfFlavor) => {
        val key = getKeyForRdfFormatterMap(rdfFlavor, host, withoutMeta, filterOutBlanks, forceUniqueness, pretty,callback)
        if (rdfFormatterMap.contains(key)) rdfFormatterMap(key)
        else {
          val newFormatter = rdfFlavor match {
            case RdfXmlFlavor => new RDFXmlFormatter(host, C.hashToUrlAndPrefix, withoutMeta, filterOutBlanks, forceUniqueness)
            case TurtleFlavor => new TurtleFormatter(host, C.hashToUrlAndPrefix, withoutMeta, filterOutBlanks, forceUniqueness)
            case N3Flavor => new N3Formatter(host, C.hashToUrlAndPrefix, withoutMeta, filterOutBlanks, forceUniqueness)
            case NTriplesFlavor => new NTriplesFormatter(host, C.hashToUrlAndPrefix, withoutMeta, filterOutBlanks, forceUniqueness)
            case JsonLDFlavor => JsonLDFormatter(host, C.hashToUrlAndPrefix, withoutMeta, filterOutBlanks, forceUniqueness, pretty, callback)
            case NquadsFlavor => new NQuadsFormatter(host, C.hashToUrlAndPrefix, withoutMeta, filterOutBlanks, forceUniqueness)
            case TriGFlavor => new TriGFormatter(host, C.hashToUrlAndPrefix, C.getAliasForQuadUrl, withoutMeta, filterOutBlanks, forceUniqueness)
            case TriXFlavor => new TriXFormatter(host, C.hashToUrlAndPrefix, C.getAliasForQuadUrl, withoutMeta, filterOutBlanks, forceUniqueness)
            case JsonLDQFlavor => JsonLDQFormatter(host, C.hashToUrlAndPrefix, C.getAliasForQuadUrl, withoutMeta, filterOutBlanks, forceUniqueness, pretty, callback)
          }
          rdfFormatterMap = rdfFormatterMap.updated(key, newFormatter)
          newFormatter
        }
      }
      case AtomType => {

        val innerFormatterOpt = withData.map(ft => FormatExtractor.withDefault(ft, RdfType(TriGFlavor))).map { ft =>
          if (ft eq AtomType) throw new IllegalArgumentException("you can't have atom format with inline atom data!")
          else getFormatter(ft, host, uri, pretty, callback, fieldFilters, offset, length, None)
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

  private[this] def getKeyForRdfFormatterMap(rdfFlavor: RdfFlavor,
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



