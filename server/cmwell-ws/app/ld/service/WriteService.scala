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
package cmwell.web.ld.service

import java.io.InputStream

import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.util.LDFormatParser
import cmwell.web.ld.util.LDFormatParser.ParsingResponse
import logic.CRUDServiceFS
import security.{AuthUtils, Token}

import scala.concurrent.Future

/**
  * this application is registered via Global
  */
object WriteService {

  val invalidFormatMessage = "Your request should contains either format param (rdfxml, n3, ntriples, turtle, jsonld, nquads, trig, trix ) " +
    "or content-type header (application/rdf+xml, text/n3, text/plain, " +
    "text/turtle, application/ld+json,application/json, application/n-quads,text/x-nquads, application/trig, application/trix)."

  def handleFormatByFormatParameter(cmwellRDFHelper: CMWellRDFHelper,
                                    crudServiceFS: CRUDServiceFS,
                                    authUtils: AuthUtils,
                                    body: InputStream,
                                    formats: Option[List[String]],
                                    contentType: Option[String],
                                    token: Option[Token],
                                    skipValidation: Boolean,
                                    isOverwrite: Boolean,
                                    timeContext: Option[Long]): Future[ParsingResponse] = {
    formats match {
      case Some(list: List[String]) =>
        list(0).toLowerCase match {
          case "rdfxml" =>
            LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                            crudServiceFS,
                                            authUtils,
                                            body,
                                            "RDF/XML",
                                            token,
                                            skipValidation,
                                            isOverwrite,
                                            timeContext)
          case "n3" =>
            LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                            crudServiceFS,
                                            authUtils,
                                            body,
                                            "N3",
                                            token,
                                            skipValidation,
                                            isOverwrite,
                                            timeContext)
          case "ntriples" =>
            LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                            crudServiceFS,
                                            authUtils,
                                            body,
                                            "N-TRIPLE",
                                            token,
                                            skipValidation,
                                            isOverwrite,
                                            timeContext)
          case "turtle" | "ttl" =>
            LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                            crudServiceFS,
                                            authUtils,
                                            body,
                                            "TURTLE",
                                            token,
                                            skipValidation,
                                            isOverwrite,
                                            timeContext)
          case "jsonld" =>
            LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                            crudServiceFS,
                                            authUtils,
                                            body,
                                            "JSON-LD",
                                            token,
                                            skipValidation,
                                            isOverwrite,
                                            timeContext)
          case q @ ("nq" | "nquads") =>
            LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                            crudServiceFS,
                                            authUtils,
                                            body,
                                            q.toUpperCase,
                                            token,
                                            skipValidation,
                                            isOverwrite,
                                            timeContext)
          case "trig" =>
            LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                            crudServiceFS,
                                            authUtils,
                                            body,
                                            "TRIG",
                                            token,
                                            skipValidation,
                                            isOverwrite,
                                            timeContext)
//TODO: We cannot support Data Ingest in TriX format, due to https://issues.apache.org/jira/browse/JENA-1567
//TODO: A workaround is available but dangerous: System.setProperty(classOf[XMLInputFactory].getName, "com.sun.xml.internal.stream.XMLInputFactoryImpl" )
//          case "trix" =>
//            LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
//                                            crudServiceFS,
//                                            authUtils,
//                                            body,
//                                            "TRIX",
//                                            token,
//                                            skipValidation,
//                                            isOverwrite,
//                                            timeContext)
          case _ =>
            handleFormatByContentType(cmwellRDFHelper,
                                      crudServiceFS,
                                      authUtils,
                                      body,
                                      contentType,
                                      token,
                                      skipValidation,
                                      isOverwrite,
                                      timeContext) //try by mimetype
        }
      case None =>
        handleFormatByContentType(cmwellRDFHelper,
                                  crudServiceFS,
                                  authUtils,
                                  body,
                                  contentType,
                                  token,
                                  skipValidation,
                                  isOverwrite,
                                  timeContext) //try by mimetype
    }
  }

  def handleFormatByContentType(cmwellRDFHelper: CMWellRDFHelper,
                                crudServiceFS: CRUDServiceFS,
                                authUtils: AuthUtils,
                                body: InputStream,
                                contentType: Option[String],
                                token: Option[Token],
                                skipValidation: Boolean,
                                isOverwrite: Boolean,
                                timeContext: Option[Long]): Future[ParsingResponse] = {
    contentType match {
      case Some("application/rdf+xml") =>
        LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                        crudServiceFS,
                                        authUtils,
                                        body,
                                        "RDF/XML",
                                        token,
                                        skipValidation,
                                        isOverwrite,
                                        timeContext)
      case Some("text/n3") =>
        LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                        crudServiceFS,
                                        authUtils,
                                        body,
                                        "N3",
                                        token,
                                        skipValidation,
                                        isOverwrite,
                                        timeContext)
      case Some("text/plain") =>
        LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                        crudServiceFS,
                                        authUtils,
                                        body,
                                        "N-TRIPLE",
                                        token,
                                        skipValidation,
                                        isOverwrite,
                                        timeContext)
      case Some("text/turtle") =>
        LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                        crudServiceFS,
                                        authUtils,
                                        body,
                                        "TURTLE",
                                        token,
                                        skipValidation,
                                        isOverwrite,
                                        timeContext)
      case Some("application/ld+json") | Some("application/json") =>
        LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                        crudServiceFS,
                                        authUtils,
                                        body,
                                        "JSON-LD",
                                        token,
                                        skipValidation,
                                        isOverwrite,
                                        timeContext)
      case Some("application/n-quads") | Some("text/x-nquads") =>
        LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                        crudServiceFS,
                                        authUtils,
                                        body,
                                        "NQUADS",
                                        token,
                                        skipValidation,
                                        isOverwrite,
                                        timeContext)
      case Some("application/trig") =>
        LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
                                        crudServiceFS,
                                        authUtils,
                                        body,
                                        "TRIG",
                                        token,
                                        skipValidation,
                                        isOverwrite,
                                        timeContext)
//      case Some("application/trix") =>
//        LDFormatParser.rdfToInfotonsMap(cmwellRDFHelper,
//                                        crudServiceFS,
//                                        authUtils,
//                                        body,
//                                        "TRIX",
//                                        token,
//                                        skipValidation,
//                                        isOverwrite,
//                                        timeContext)
      case Some(ctype: String) => throw new RuntimeException(invalidFormatMessage)
      case None                => throw new RuntimeException(invalidFormatMessage)
    }
  }
}
