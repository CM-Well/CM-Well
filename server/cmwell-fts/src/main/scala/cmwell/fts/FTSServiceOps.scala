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
package cmwell.fts

import akka.NotUsed
import akka.stream.scaladsl.Source
import cmwell.domain.{AggregationFilter, AggregationsResponse, Infoton, InfotonSerializer}
import com.typesafe.scalalogging.Logger
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetResponse
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

trait EsSourceExtractor[T] {
  def extract(hit: GetResponse): T
}

object EsSourceExtractor {
  implicit val esStringSourceExtractor = new EsSourceExtractor[String] {
    override def extract(hit: GetResponse): String = hit.getSourceAsString
  }

  object Implicits {

    implicit val esMapSourceExtractor = new EsSourceExtractor[java.util.Map[String, AnyRef]] {
      override def extract(hit: GetResponse): java.util.Map[String, AnyRef] = hit.getSourceAsMap
    }
  }
}
