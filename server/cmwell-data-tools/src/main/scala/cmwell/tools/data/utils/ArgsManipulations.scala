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
package cmwell.tools.data.utils

/**
  * Created by matan on 18/5/16.
  */
object ArgsManipulations {
  def formatHost(host: String) = {
    if (host.startsWith("http")) host
    else "http://" + host
  }

  def formatPath(path: String) = {
    if (!path.startsWith("/"))
      "/" + path
    else
      path
  }

  def extractBaseUrl(baseUrl: String): HttpAddress = {
    // TODO: every untrivial regex should either be explained (LDFormatter style) or constructed using
    // TODO: e.g: verbal expressions (https://github.com/VerbalExpressions)
    val pattern = """(http[s]?:\/\/)?([^\?\:\/#]+)(\:([0-9]+))?(\/[^\?\#]*)?(\?([^#]*))?(#.*)?""".r

    baseUrl match {
      case pattern(protocol, host, _, port, uriPrefix, _, _*) =>
        HttpAddress(
          protocol =
            if (protocol != null && protocol.startsWith("https")) "https"
            else "http",
          host = host,
          port = Option(port).getOrElse("80").toInt,
          uriPrefix = Option(uriPrefix).fold("")(identity)
        )
    }
  }

  case class HttpAddress(protocol: String = "http",
                         host: String,
                         port: Int = 80,
                         uriPrefix: String = "")
}
