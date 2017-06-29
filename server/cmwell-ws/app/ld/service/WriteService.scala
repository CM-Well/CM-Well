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


package cmwell.web.ld.service

import java.io.InputStream

import cmwell.web.ld.util.LDFormatParser
import cmwell.web.ld.util.LDFormatParser.ParsingResponse
import security.Token

import scala.concurrent.Future

/**
 * this application is registered via Global
 */
object WriteService {

  val invalidFormatMessage = "Your request should contains either format param (rdfxml, n3, ntriples, turtle,jsonld, nquads,trig ) or content-type header" +
    " (application/rdf+xml, text/n3, text/plain, " +
    "text/turtle, application/ld+json,application/json, application/n-quads,text/x-nquads, application/trig)."

  def handleFormatByFormatParameter(body : InputStream, formats : Option[List[String]], contentType : Option[String], token: Option[Token], skipValidation: Boolean, isOverwrite: Boolean): Future[ParsingResponse] = {
    formats match {
      case Some(list : List[String]) =>
        list(0).toLowerCase match {
          case "rdfxml" => LDFormatParser.rdfToInfotonsMap(body, "RDF/XML", token, skipValidation, isOverwrite)
          case "n3" => LDFormatParser.rdfToInfotonsMap(body, "N3", token, skipValidation, isOverwrite)
          case "ntriples" => LDFormatParser.rdfToInfotonsMap(body, "N-TRIPLE", token, skipValidation, isOverwrite)
          case "turtle" | "ttl" => LDFormatParser.rdfToInfotonsMap(body, "TURTLE", token, skipValidation, isOverwrite)
          case "jsonld" => LDFormatParser.rdfToInfotonsMap(body, "JSON-LD", token, skipValidation, isOverwrite)
          case q @ ("nq" | "nquads") => LDFormatParser.rdfToInfotonsMap(body, q.toUpperCase, token, skipValidation, isOverwrite)
          case "trig" => LDFormatParser.rdfToInfotonsMap(body, "TRIG", token, skipValidation, isOverwrite)
          case _ => handleFormatByContentType(body, contentType, token, skipValidation, isOverwrite) //try by mimetype
        }
      case None => handleFormatByContentType(body, contentType, token, skipValidation, isOverwrite) //try by mimetype
    }
  }

  def handleFormatByContentType(body : InputStream, contentType : Option[String], token: Option[Token], skipValidation: Boolean, isOverwrite: Boolean): Future[ParsingResponse] = {
    contentType match {
      case Some("application/rdf+xml") => LDFormatParser.rdfToInfotonsMap(body, "RDF/XML", token, skipValidation, isOverwrite)
      case Some("text/n3") => LDFormatParser.rdfToInfotonsMap(body, "N3", token, skipValidation, isOverwrite)
      case Some("text/plain") => LDFormatParser.rdfToInfotonsMap(body, "N-TRIPLE", token, skipValidation, isOverwrite)
      case Some("text/turtle") => LDFormatParser.rdfToInfotonsMap(body, "TURTLE", token, skipValidation, isOverwrite)
      case Some("application/ld+json") | Some("application/json") => LDFormatParser.rdfToInfotonsMap(body, "JSON-LD", token, skipValidation, isOverwrite)
      case Some("application/n-quads") | Some("text/x-nquads") => LDFormatParser.rdfToInfotonsMap(body, "NQUADS", token, skipValidation, isOverwrite)
      case Some("application/trig") => LDFormatParser.rdfToInfotonsMap(body, "TRIG", token, skipValidation, isOverwrite)
      case Some(ctype : String) => throw new RuntimeException(invalidFormatMessage)
      case None => throw new RuntimeException(invalidFormatMessage)
    }
  }
}
