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
package actions

/**
  * Created by michael on 7/6/15.
  */
case class MarkdownTuple(fields: String*) {
  def add(v: String) = {
    MarkdownTuple((fields :+ v): _*)
  }

  def add(seq: Seq[String]) = {

    val newSeq = fields ++ seq

    MarkdownTuple(newSeq: _*)
  }
}
case class MarkdownTable(header: MarkdownTuple, body: Seq[MarkdownTuple], footer: Option[MarkdownTuple] = None) {

  private def pad(str: String, pattern: String, amount: Int = 30): String = {
    str.padTo(amount, pattern).mkString
  }

  private def genHeader = {
    header.fields.map(f => pad(s"**$f**", " ")).mkString("|", "|", "|\n") +
      header.fields.map(f => pad(s"", "-")).mkString("|", "|", "|\n")
  }

  private def genTuple(tuple: MarkdownTuple): String = {
    tuple.fields.map(f => pad(f, " ")).mkString("|", "|", "|\n")
  }

  private def genBody = {
    body.map(genTuple).mkString
  }

  def get = genHeader + genBody + footer.map(genTuple).getOrElse("")
}
