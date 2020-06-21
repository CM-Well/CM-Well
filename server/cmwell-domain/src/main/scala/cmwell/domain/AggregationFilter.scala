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
package cmwell.domain

/**
  * User: israel
  * Date: 11/12/14
  * Time: 22:07
  */
trait AggregationFilter extends Formattable {
  def name: String
  def field: Field
  def `type`: String = this.getClass.getSimpleName.dropRight(6) // 6 == "Filter".length
  def copyWithSuffix(suffix: String): AggregationFilter
}

trait BucketAggregationFilter extends AggregationFilter with Formattable {
  def subFilters: Seq[AggregationFilter]
}

sealed trait FieldValeOperator
case object AnalyzedField extends FieldValeOperator
case object NonAnalyzedField extends FieldValeOperator
case class Field(operator: FieldValeOperator, value: String)

case class StatsAggregationFilter(name: String = "Statistics Aggregation", override val field: Field)
    extends AggregationFilter {
  override def copyWithSuffix(suffix: String): AggregationFilter = this.copy(name = name + suffix)
}

case class TermAggregationFilter(name: String = "Term Aggregation",
                                 override val field: Field,
                                 size: Int = 10,
                                 subFilters: Seq[AggregationFilter] = Seq.empty)
    extends BucketAggregationFilter {
  override def copyWithSuffix(suffix: String): AggregationFilter = {
    var counter = 0
    this.copy(name = name + suffix, subFilters = subFilters.map { counter += 1; _.copyWithSuffix(suffix + counter) })
  }
}

case class HistogramAggregationFilter(name:String = "Histogram Aggregation", override val field:Field, interval:Int, minDocCount:Int,
                                      extMin:Option[Double], extMax:Option[Double],
                                      subFilters:Seq[AggregationFilter] = Seq.empty) extends BucketAggregationFilter {
  override def copyWithSuffix(suffix: String): AggregationFilter = {
    var counter = 0
    this.copy(name = name + suffix, subFilters = subFilters.map { counter += 1; _.copyWithSuffix(suffix + counter) })
  }
}

case class SignificantTermsAggregationFilter(name: String = "Signigicant Terms Aggregation",
                                             override val field: Field,
                                             backgroundTerm: Option[(String, String)],
                                             minDocCount: Int,
                                             size: Int,
                                             subFilters: Seq[AggregationFilter] = Seq.empty)
    extends BucketAggregationFilter {
  override def copyWithSuffix(suffix: String): AggregationFilter = {
    var counter = 0
    this.copy(name = name + suffix, subFilters = subFilters.map { counter += 1; _.copyWithSuffix(suffix + counter) })
  }
}

case class CardinalityAggregationFilter(name: String, override val field: Field, precisionThreshold: Option[Long])
    extends AggregationFilter {
  override def copyWithSuffix(suffix: String): AggregationFilter = this.copy(name = name + suffix)
}
