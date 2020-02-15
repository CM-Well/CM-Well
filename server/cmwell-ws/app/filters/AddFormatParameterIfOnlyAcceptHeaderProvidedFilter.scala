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
package filters

import akka.stream.Materializer
import com.typesafe.scalalogging.LazyLogging
import javax.inject._
import play.api.http.MediaType
import play.api.mvc.{Filter, RequestHeader, Result}

import scala.concurrent.Future

class AddFormatParameterIfOnlyAcceptHeaderProvidedFilter @Inject()(implicit val mat: Materializer) extends Filter {
  //MediaType2Format
  private[this] val mt2f: PartialFunction[String, PartialFunction[String, String]] = (mt: String) =>
    mt match {
      case "text" => {
        case "yaml"       => "yaml"
        case "json"       => "json"
        case "rdf+n3"     => "n3"
        case "n3"         => "n3"
        case "plain"      => "ntriples"
        case "ntriples"   => "ntriples"
        case "turtle"     => "ttl"
        case "ttl"        => "ttl"
        case "rdf+turtle" => "ttl"
        case "rdf+ttl"    => "ttl"
        case "n-quads"    => "nquads"
      }
      case "application" => {
        case "json"     => "jsonl"
        case "ld+json"  => "jsonld"
        case "rdf+xml"  => "rdfxml"
        case "x-nquads" => "nquads"
      }
      //    case "xml" => {
      //      case "rdf" => "rdfxml"
      //    }
  }

  private def formatToValidType(mt: MediaType): String = mt2f(mt.mediaType)(mt.mediaSubType)

  private def isCMWellAccepted(mt: MediaType): Boolean =
    mt2f.isDefinedAt(mt.mediaType) && mt2f(mt.mediaType).isDefinedAt(mt.mediaSubType)

  override def apply(next: (RequestHeader) => Future[Result])(request: RequestHeader): Future[Result] = {
    val withFormat =
      if ((request.getQueryString("format").isDefined || request.acceptedTypes.isEmpty) && Set("post", "get")(
            request.method.toLowerCase
          )) request
      else
        request.acceptedTypes.find(isCMWellAccepted(_)) match {
          case Some(mt) =>
            val newTarget = request.target.withQueryString(request.target.queryMap + ("format" -> Seq(formatToValidType(mt))))
            request.withTarget(newTarget)
          case None     => request
        }
    next(withFormat)
  }
}
