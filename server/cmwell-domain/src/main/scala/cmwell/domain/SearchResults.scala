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

import org.joda.time.DateTime

import scala.collection.immutable

/**
  * Created by:
  * User: Israel
  * Date: Nov 10, 2010
  * Time: 3:05:32 PM
  */
case class SearchThinResult(path: String,
                            uuid: String,
                            lastModified: String,
                            lastModifiedBy: String,
                            indexTime: Long,
                            score: Option[Float] = None)
    extends Formattable

case class SearchThinResults(total: Long,
                             offset: Long,
                             length: Long,
                             thinResults: immutable.Seq[SearchThinResult],
                             debugInfo: Option[String] = None)

case class SearchResults(fromDate: Option[DateTime],
                         toDate: Option[DateTime],
                         total: Long,
                         offset: Long,
                         length: Long,
                         infotons: Seq[Infoton],
                         debugInfo: Option[String] = None)
    extends Jsonable
    with Formattable {
  def masked(fieldsMask: Set[String]): SearchResults = copy(infotons = infotons.map(_.masked(fieldsMask)))
}

case class PaginationInfo(first: String, previous: Option[String], self: String, next: Option[String], last: String)
    extends Jsonable
    with Formattable

case class SearchResponse(pagination: PaginationInfo, results: SearchResults) extends Jsonable with Formattable

case class IterationResults(iteratorId: String,
                            totalHits: Long,
                            infotons: Option[Seq[Infoton]] = None,
                            nodeId: Option[String] = None,
                            debugInfo: Option[String] = None)
    extends Jsonable
    with Formattable {
  def masked(fieldsMask: Set[String]): IterationResults = copy(infotons = infotons.map(_.map(_.masked(fieldsMask))))
}

case class SimpleResponse(success: Boolean, msg: Option[String]) extends Formattable

trait AggregationResponse extends Formattable {
  def name = filter.name
  def `type` = this.getClass.getSimpleName
  def filter: AggregationFilter
}

trait BucketsAggregationResponse extends AggregationResponse {
  def buckets: Seq[Bucket]
}

sealed trait Bucket extends Formattable {
  def key: FieldValue
  def docCount: Long
  def subAggregations: Option[AggregationsResponse]
}
object Bucket {
  def apply(k: FieldValue, dc: Long, subAgg: Option[AggregationsResponse]) = new Bucket {
    def key = k; def docCount = dc; def subAggregations = subAgg
  }
  def unapply(b: Bucket): Option[(FieldValue, Long, Option[AggregationsResponse])] =
    Some((b.key, b.docCount, b.subAggregations))
}

case class SignificantTermsBucket(override val key: FieldValue,
                                  override val docCount: Long,
                                  score: Double,
                                  bgCount: Long,
                                  override val subAggregations: Option[AggregationsResponse])
    extends Bucket

case class AggregationsResponse(responses: Seq[AggregationResponse], debugInfo: Option[String] = None)
    extends Formattable

case class TermsAggregationResponse(filter: TermAggregationFilter, buckets: Seq[Bucket])
    extends BucketsAggregationResponse
case class StatsAggregationResponse(filter: StatsAggregationFilter,
                                    count: Long,
                                    min: Double,
                                    max: Double,
                                    avg: Double,
                                    sum: Double)
    extends AggregationResponse
case class HistogramAggregationResponse(filter: HistogramAggregationFilter, buckets: Seq[Bucket])
    extends BucketsAggregationResponse
case class SignificantTermsAggregationResponse(filter: SignificantTermsAggregationFilter,
                                               docCount: Long,
                                               buckets: Seq[SignificantTermsBucket])
    extends BucketsAggregationResponse
case class CardinalityAggregationResponse(filter: CardinalityAggregationFilter, count: Long) extends AggregationResponse
