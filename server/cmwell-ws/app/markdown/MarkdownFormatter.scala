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
package markdown

import cmwell.domain.FileInfoton

import scala.util.Try

/**
  * Created by gilad on 10/6/14.
  */
object MarkdownFormatter {
  def asHtmlString(i: FileInfoton): String = {
    val title = Try(i.systemFields.path.drop(i.systemFields.path.lastIndexOf('/') + 1).replace(
      ".md", "")).toOption.getOrElse(i.systemFields.path)
    val content = xml.Utility.escape(i.content.get.asString)
    asHtmlString(title, content)
  }

  def asHtmlString(title: String, content: String): String = {
    s"""|<span style="display:none">[comment]: <> (
        |<html>
        |  <head>
        |    <meta charset="utf-8">
        |    <script src="/meta/app/sys/js/jquery-1.6.2.min.js" type="text/javascript"></script>
        |    <script type="text/javascript" src="/meta/app/sys/js/Markdown.Converter.js"></script>
        |    <script type="text/javascript" src="/meta/app/sys/js/Markdown.Sanitizer.js"></script>
        |    <script type="text/javascript" src="/meta/app/sys/js/Markdown.Extra.js"></script>
        |    <script src="/meta/app/sys/js/highlight/highlight.pack.js" type="text/javascript"></script>
        |    <script src="/meta/app/sys/js/cmwmdviewer.js" type="text/javascript"></script>
        |    <title>$title</title>
        |    <link rel="stylesheet" href="/meta/app/sys/js/highlight/styles/aiaas.css"/>
        |    <link rel="stylesheet" href="/meta/app/sys/css/cmwmdviewer.css"/>
        |  </head>
        |)</span>""".stripMargin.replace("\n", "") + s"\n\n$content"
  }
}
