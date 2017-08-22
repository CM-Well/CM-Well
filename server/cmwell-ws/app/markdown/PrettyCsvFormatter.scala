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


package markdown

import cmwell.domain.Formattable
import wsutil.FormatterManager._
import cmwell.formats.{CSVFormatter, FormatType, HtmlType}

class PrettyCsvFormatter(innerToSimpleFieldName: String => String) extends CSVFormatter(prettyMangledField compose innerToSimpleFieldName) {
  override def format: FormatType = HtmlType

  override def render(formattable: Formattable): String = {
    val csv = super.render(formattable).replaceAll("\r?\n", "\\\\n")
    val firstCommaPos = csv.indexOf(',')
    val modifiedCsv = csv.substring(0, firstCommaPos).replace("\\", "\\\\") + csv.drop(firstCommaPos)
    val len = modifiedCsv.reverseIterator.takeWhile('\n'.==).size
    views.html.csvPretty(s"${formattable.getClass.getSimpleName} Table", modifiedCsv.dropRight(len)).body
  }
}
