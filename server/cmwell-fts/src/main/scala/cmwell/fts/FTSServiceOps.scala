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
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetResponse

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

    implicit val esMapSourceExtractor = new EsSourceExtractor[java.util.Map[String,AnyRef]] {
      override def extract(hit: GetResponse): java.util.Map[String,AnyRef] = hit.getSourceAsMap
    }
  }
}

trait FTSServiceOps {
  // TODO: make sysQuad configurable & take from configurations
  val sysQuad: Option[String] = Some(InfotonSerializer.sysQuad)
  val defaultPartition: String
  val defaultScrollTTL: Long

  def close(): Unit

  def latestIndexNameAndCount(prefix:String): Option[(String, Long)]

  def index(infoton:Infoton, previousInfoton:Option[Infoton], partition: String = defaultPartition)
           (implicit executionContext:ExecutionContext): Future[Unit]

  def extractSource[T : EsSourceExtractor](uuid: String, index: String)
                   (implicit executionContext:ExecutionContext) : Future[(T,Long)]

  def executeBulkActionRequests(actionRequests:Iterable[ActionRequest[_ <: ActionRequest[_ <: AnyRef]]])
                               (implicit executionContext:ExecutionContext): Future[BulkResponse]

  def executeBulkIndexRequests(indexRequests:Iterable[ESIndexRequest], numOfRetries: Int = 15,
                               waitBetweenRetries:Long = 3000)
                              (implicit executionContext:ExecutionContext) : Future[SuccessfulBulkIndexResult]

  def getMappings(withHistory: Boolean, partition: String = defaultPartition)
                 (implicit executionContext:ExecutionContext): Future[Set[String]]

  def bulkIndex(currentInfotons: Seq[Infoton], previousInfotons: Seq[Infoton], partition: String = defaultPartition)
               (implicit executionContext:ExecutionContext): Future[BulkResponse]

  def delete(deletedInfoton: Infoton, previousInfoton: Infoton, partition: String = defaultPartition)
            (implicit executionContext:ExecutionContext): Future[Boolean]

  def purge(uuid: String, partition: String = defaultPartition)
           (implicit executionContext:ExecutionContext): Future[Boolean]

  def purgeByUuids(historyUuids: Seq[String], currentUuid: Option[String], partition: String = defaultPartition)
                  (implicit executionContext:ExecutionContext) : Future[BulkResponse]

  def purgeByUuidsAndIndexes(uuidsAtIndexes: Vector[(String,String)], partition: String = defaultPartition)
                            (implicit executionContext:ExecutionContext) : Future[BulkResponse]

  def purgeByUuidsFromAllIndexes(uuids: Vector[String], partition: String = defaultPartition)
                                (implicit executionContext:ExecutionContext) : Future[BulkResponse]

  def purgeAll(path: String, isRecursive: Boolean, partition:String = defaultPartition)
              (implicit executionContext:ExecutionContext) : Future[Boolean]

  def purgeHistory(path: String, isRecursive: Boolean, partition:String = defaultPartition)
                  (implicit executionContext:ExecutionContext) : Future[Boolean]

  def getIndicesNamesByType(suffix: String, partition: String = defaultPartition): Seq[String]

  def listChildren(path: String, offset: Int, length: Int, descendants: Boolean = false,
                   partition: String = defaultPartition)
                  (implicit executionContext:ExecutionContext) : Future[FTSSearchResponse]

  def aggregate(pathFilter: Option[PathFilter], fieldFilter: Option[FieldFilter],
                datesFilter: Option[DatesFilter] = None, paginationParams: PaginationParams,
                aggregationFilters: Seq[AggregationFilter], withHistory: Boolean = false,
                partition: String = defaultPartition, debugInfo: Boolean = false)
               (implicit executionContext:ExecutionContext) : Future[AggregationsResponse]

  def search(pathFilter: Option[PathFilter], fieldsFilter: Option[FieldFilter], datesFilter: Option[DatesFilter],
             paginationParams: PaginationParams, sortParams: SortParam = SortParam.empty, withHistory: Boolean = false,
             withDeleted: Boolean = false, partition: String = defaultPartition, debugInfo: Boolean = false,
             timeout : Option[Duration] = None) (implicit executionContext:ExecutionContext): Future[FTSSearchResponse]

  def thinSearch(pathFilter: Option[PathFilter], fieldsFilter: Option[FieldFilter], datesFilter: Option[DatesFilter],
                 paginationParams: PaginationParams, sortParams: SortParam = SortParam.empty,
                 withHistory: Boolean = false, withDeleted: Boolean = false, partition: String = defaultPartition,
                 debugInfo: Boolean = false, timeout : Option[Duration] = None)
                (implicit executionContext:ExecutionContext): Future[FTSThinSearchResponse]

  def getLastIndexTimeFor(dc: String, partition: String = defaultPartition)
                         (implicit executionContext:ExecutionContext): Future[Option[Long]]

  def startSuperScroll(pathFilter: Option[PathFilter], fieldsFilter: Option[FieldFilter], datesFilter: Option[DatesFilter],
                       paginationParams: PaginationParams, scrollTTL: Long = defaultScrollTTL,
                       withHistory: Boolean = false, withDeleted: Boolean = false)
                      (implicit executionContext:ExecutionContext): Seq[Future[FTSStartScrollResponse]]

  def startScroll(pathFilter: Option[PathFilter], fieldsFilter: Option[FieldFilter],
                  datesFilter: Option[DatesFilter], paginationParams: PaginationParams,
                  scrollTTL: Long = defaultScrollTTL, withHistory: Boolean = false, withDeleted: Boolean = false,
                  indexNames:Seq[String] = Seq.empty, onlyNode:Option[String] = None,
                  partition: String = defaultPartition, debugInfo: Boolean = false)
                 (implicit executionContext:ExecutionContext) : Future[FTSStartScrollResponse]

  def startSuperMultiScroll(pathFilter: Option[PathFilter], fieldsFilter: Option[FieldFilter],
                            datesFilter: Option[DatesFilter], paginationParams: PaginationParams,
                            scrollTTL: Long = defaultScrollTTL, withHistory: Boolean = false,
                            withDeleted:Boolean = false, partition: String = defaultPartition)
                           (implicit executionContext:ExecutionContext) : Seq[Future[FTSStartScrollResponse]]

  def startMultiScroll(pathFilter: Option[PathFilter], fieldsFilter: Option[FieldFilter], datesFilter: Option[DatesFilter],
                       paginationParams: PaginationParams, scrollTTL: Long = defaultScrollTTL,
                       withHistory: Boolean = false, withDeleted: Boolean = false, partition: String = defaultPartition)
                      (implicit executionContext:ExecutionContext): Seq[Future[FTSStartScrollResponse]]

  def scroll(scrollId: String, scrollTTL: Long = defaultScrollTTL, nodeId: Option[String] = None)
            (implicit executionContext:ExecutionContext): Future[FTSScrollResponse]

  def info(path: String, paginationParams: PaginationParams, withHistory: Boolean,
           partition: String = defaultPartition)
          (implicit executionContext:ExecutionContext): Future[Vector[(String,String)]]

  def rInfo(path: String, scrollTTL: Long = defaultScrollTTL, paginationParams: PaginationParams = DefaultPaginationParams,
            withHistory: Boolean = false, partition: String = defaultPartition)
           (implicit executionContext:ExecutionContext): Future[Source[Vector[(Long, String,String)],NotUsed]]

  def countSearchOpenContexts(): Array[(String,Long)]
}

trait ESMBean {
  def getTotalRequestedToIndex():Long
  def getTotalIndexed():Long
  def getTotalFailedToIndex():Long
}

trait FTSServiceESMBean extends ESMBean

trait FTSServiceNewMBean extends ESMBean