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
package cmwell.ws.qp

import cmwell.fts._

object Encoder {
  def encodeFieldName(name: String) = name.lastIndexOf(".") match {
    case i if name.startsWith("system.") || i < 0 => name
    case i => {
      val (firstDot, last) = name.splitAt(i + 1)
      firstDot + "$" + last
    }
  }

  def encodeFieldOperator(fieldOperator: FieldOperator): String = fieldOperator match {
    case Must    => ""
    case Should  => "*"
    case MustNot => "-"
  }

  def encodeValueOperator(valueOperator: ValueOperator): String = valueOperator match {
    case Contains            => ":"
    case Equals              => "::"
    case GreaterThan         => ">"
    case GreaterThanOrEquals => ">>"
    case LessThan            => "<"
    case LessThanOrEquals    => "<<"
    case Like                => "~"
  }

  def encodeFieldFilter(fieldFilter: FieldFilter): String = {

    def formatSingleFieldFilter(singleFieldFilter: SingleFieldFilter) =
      encodeFieldOperator(singleFieldFilter.fieldOperator) +
        encodeFieldName(singleFieldFilter.name) +
        encodeValueOperator(singleFieldFilter.valueOperator) +
        singleFieldFilter.value.getOrElse("")

    fieldFilter match {
      case sff: SingleFieldFilter => formatSingleFieldFilter(sff)
      case mff @ MultiFieldFilter(fieldsOperator, filters) =>
        s"${encodeFieldOperator(fieldsOperator)}[${filters.map(encodeFieldFilter).mkString(",")}]"
    }

  }
}
