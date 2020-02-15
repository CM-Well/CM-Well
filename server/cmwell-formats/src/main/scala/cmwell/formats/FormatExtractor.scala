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

/**
  * Created by markz on 12/7/14.
  */
// format: off
sealed abstract class FormatType         {def mimetype: String ; def isThin: Boolean = false}
case object TextType  extends FormatType {def mimetype = "text/plain;charset=UTF8"; override def isThin = true}
case object TsvType   extends FormatType {def mimetype = "text/tab-separated-values;charset=UTF8"; override def isThin = true}
case object CsvType   extends FormatType {def mimetype = "text/csv;charset=UTF8"}
case object HtmlType  extends FormatType {def mimetype = "text/html;charset=UTF8"}
case object JsonType  extends FormatType {def mimetype = "application/json;charset=UTF8"}
case object JsonpType extends FormatType {def mimetype = "application/javascript;charset=UTF8"}
case object JsonlType extends FormatType {def mimetype = "application/json;charset=UTF8"}
case object YamlType  extends FormatType {def mimetype = "text/yaml;charset=UTF8"}
case object AtomType  extends FormatType {def mimetype = "application/atom+xml;charset=UTF8"}
case class RdfType(flavor: RdfFlavor) extends FormatType {def mimetype = flavor.mimetype}

sealed abstract class RdfFlavor              {def mimetype: String; def key: String}
case object RdfXmlFlavor   extends RdfFlavor {def mimetype = "application/rdf+xml;charset=UTF8";      def key = "xml"}
case object TurtleFlavor   extends RdfFlavor {def mimetype = "text/turtle;charset=UTF8";              def key = "ttl"}
case object NTriplesFlavor extends RdfFlavor {def mimetype = "text/plain;charset=UTF8";               def key = "nt"}
case object N3Flavor       extends RdfFlavor {def mimetype = "text/rdf+n3;charset=UTF8";              def key = "n3"}
case object JsonLDFlavor   extends RdfFlavor {def mimetype = "application/ld+json;charset=UTF8";      def key = "jld"}
case object NquadsFlavor   extends RdfFlavor {def mimetype = "application/n-quads;charset=UTF8";      def key = "nq"}
case object TriGFlavor     extends RdfFlavor {def mimetype = "application/trig;charset=UTF8";         def key = "trig"}
case object TriXFlavor     extends RdfFlavor {def mimetype = "application/trix;ext=xml;charset=UTF8"; def key = "trix"}
case object JsonLDQFlavor  extends RdfFlavor {def mimetype = "application/ld+json;charset=UTF8";      def key = "jldq"}
// format: on
object FormatExtractor {

  def withDefault(format: String, ft: FormatType): FormatType = unapply(format).getOrElse(ft)

  // format: off
  def unapply(format: String): Option[FormatType] = format.toLowerCase.trim match {
    case "text"   | "path"               => Some(TextType)
    case "tsv"    | "tab"                => Some(TsvType)
    case "csv"                           => Some(CsvType)
    case "json"                          => Some(JsonType)
    case "jsonl"                         => Some(JsonlType)
    case "atom"                          => Some(AtomType)
    case "yaml"   | "yml"                => Some(YamlType)
    case "nt"     | "ntriples"           => Some(RdfType(NTriplesFlavor))
    case "turtle" | "ttl"                => Some(RdfType(TurtleFlavor))
    case "jsonld" | "json-ld"            => Some(RdfType(JsonLDFlavor))
    case "jsonldq"| "json-ldq"           => Some(RdfType(JsonLDQFlavor))
    case "rdfxml" | "xmlrdf" | "rdf-xml" => Some(RdfType(RdfXmlFlavor))
    case "n3"                            => Some(RdfType(N3Flavor))
    case "nq"     | "nquads"             => Some(RdfType(NquadsFlavor))
    case "trig"                          => Some(RdfType(TriGFlavor))
    case "trix"                          => Some(RdfType(TriXFlavor))
    case _ => None
  }
  // format: on
}
