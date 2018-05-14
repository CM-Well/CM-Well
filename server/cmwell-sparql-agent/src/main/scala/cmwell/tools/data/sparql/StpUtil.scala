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
package cmwell.tools.data.sparql

import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import io.circe._, io.circe.parser._
import scala.concurrent.ExecutionContext

object StpUtil {
  def headerString(header: (String, String)): String = header._1 + ":" + header._2

  def headersString(headers: Seq[(String, String)]): String = headers.map(headerString).mkString("[", ",", "]")

  def extractLastPart(path: String) = {
    val p =
      if (path.endsWith("/")) path.init
      else path
    val (_, name) = p.splitAt(p.lastIndexOf("/"))
    name.tail
  }

  def readPreviousTokens(baseUrl: String, path: String, format: String)(implicit context: ExecutionContext) = {

    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler

    cmwell.util.http.SimpleHttpClient
      .get(s"http://$baseUrl$path/tokens?op=stream&recursive&format=json")
      .map{
        case response if response.status == 200 || response.status == 404 =>
          Left(response.payload.lines
            .map({
              row =>
                parse(row) match {
                  case Left(parseFailure @ ParsingFailure(_, _)) => throw parseFailure
                  case Right(json) => {

                    val token = (for {
                      vec <- json.hcursor.downField("fields").downField("token").values
                      jsn <- vec.headOption
                      str <- jsn.asString
                    } yield str).getOrElse("")

                    val receivedInfotons: Option[DownloadStats] =
                      json.hcursor.downField("fields").downField("receivedInfotons").downArray.as[Long].toOption.map {
                        value =>
                          DownloadStats(receivedInfotons = value)
                      }

                    val sensor = extractLastPart(json.hcursor.downField("system").get[String]("path").toOption.get)
                    sensor -> (token, receivedInfotons)
                  }
                }
            })
            .foldLeft(Map.newBuilder[String, TokenAndStatistics])(_.+=(_))
            .result())
        case _ => Right(s"Could not read token infoton from cm-well for agent: ${path}")
      }
  }

}
