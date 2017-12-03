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
import cmwell.domain.{AggregationFilter, Infoton}
import com.typesafe.scalalogging.Logger
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.bulk.BulkResponse

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

object FTSServiceES {
  def getOne(classPathConfigFile: String, waitForGreen: Boolean = true) =
    new FTSServiceES(classPathConfigFile, waitForGreen)
}

class FTSServiceES private(classPathConfigFile: String, waitForGreen: Boolean)
  extends FTSServiceOps {
  val defaultPartition: String = ???
  val defaultScrollTTL: Long = ???

  override def aggregate(pathFilter: Option[PathFilter]
                         , fieldFilter: Option[FieldFilter]
                         , datesFilter: Option[DatesFilter]
                         , paginationParams: PaginationParams
                         , aggregationFilters: Seq[AggregationFilter]
                         , withHistory: Boolean, partition: String
                         , debugInfo: Boolean)(implicit executionContext: ExecutionContext) = ???

  override def search(pathFilter: Option[PathFilter]
                      , fieldsFilter: Option[FieldFilter]
                      , datesFilter: Option[DatesFilter]
                      , paginationParams: PaginationParams
                      , sortParams: SortParam, withHistory: Boolean
                      , withDeleted: Boolean, partition: String
                      , debugInfo: Boolean
                      , timeout: Option[Duration])(implicit executionContext: ExecutionContext, logger: Logger) = ???

  override def thinSearch(pathFilter: Option[PathFilter]
                          , fieldsFilter: Option[FieldFilter]
                          , datesFilter: Option[DatesFilter]
                          , paginationParams: PaginationParams
                          , sortParams: SortParam, withHistory: Boolean
                          , withDeleted: Boolean, partition: String
                          , debugInfo: Boolean
                          , timeout: Option[Duration])(implicit executionContext: ExecutionContext, logger: Logger) = ???

  override def getLastIndexTimeFor(dc: String
                                   , withHistory: Boolean
                                   , partition: String
                                   , fieldFilters: Option[FieldFilter])(implicit executionContext: ExecutionContext) = ???

  override def startSuperScroll(pathFilter: Option[PathFilter]
                                , fieldsFilter: Option[FieldFilter]
                                , datesFilter: Option[DatesFilter]
                                , paginationParams: PaginationParams
                                , scrollTTL: Long
                                , withHistory: Boolean
                                , withDeleted: Boolean)(implicit executionContext: ExecutionContext) = ???

  override def startScroll(pathFilter: Option[PathFilter]
                           , fieldsFilter: Option[FieldFilter]
                           , datesFilter: Option[DatesFilter]
                           , paginationParams: PaginationParams
                           , scrollTTL: Long
                           , withHistory: Boolean
                           , withDeleted: Boolean
                           , indexNames: Seq[String]
                           , onlyNode: Option[String]
                           , partition: String
                           , debugInfo: Boolean)(implicit executionContext: ExecutionContext, logger: Logger) = ???

  override def startSuperMultiScroll(pathFilter: Option[PathFilter]
                                     , fieldsFilter: Option[FieldFilter]
                                     , datesFilter: Option[DatesFilter]
                                     , paginationParams: PaginationParams
                                     , scrollTTL: Long
                                     , withHistory: Boolean
                                     , withDeleted: Boolean
                                     , partition: String)(implicit executionContext: ExecutionContext, logger: Logger) = ???

  override def startMultiScroll(pathFilter: Option[PathFilter]
                                , fieldsFilter: Option[FieldFilter]
                                , datesFilter: Option[DatesFilter]
                                , paginationParams: PaginationParams
                                , scrollTTL: Long
                                , withHistory: Boolean
                                , withDeleted: Boolean
                                , partition: String)(implicit executionContext: ExecutionContext, logger: Logger) = ???

  override def info(path: String
                    , paginationParams: PaginationParams
                    , withHistory: Boolean
                    , partition: String)(implicit executionContext: ExecutionContext) = ???

  override def rInfo(path: String
                     , scrollTTL: Long
                     , paginationParams: PaginationParams
                     , withHistory: Boolean
                     , partition: String)(implicit executionContext: ExecutionContext) = ???

  override def close(): Unit = ???

  override def latestIndexNameAndCount(prefix: String): Option[(String, Long)] = ???

  override def index(infoton: Infoton, previousInfoton: Option[Infoton], partition: String)(implicit executionContext: ExecutionContext): Future[Unit] = ???

  override def extractSource[T: EsSourceExtractor](uuid: String, index: String)(implicit executionContext: ExecutionContext): Future[(T, Long)] = ???

  override def executeBulkActionRequests(actionRequests: Iterable[ActionRequest])
                                        (implicit executionContext: ExecutionContext, logger: Logger): Future[BulkResponse] = ???

  override def executeBulkIndexRequests(indexRequests: Iterable[ESIndexRequest]
                                        , numOfRetries: Int
                                        , waitBetweenRetries: Long)
                                       (implicit executionContext: ExecutionContext, logger: Logger): Future[SuccessfulBulkIndexResult] = ???

  override def getMappings(withHistory: Boolean, partition: String)(implicit executionContext: ExecutionContext): Future[Set[String]] = ???

  override def bulkIndex(currentInfotons: Seq[Infoton]
                         , previousInfotons: Seq[Infoton]
                         , partition: String)(implicit executionContext: ExecutionContext): Future[BulkResponse] = ???

  override def delete(deletedInfoton: Infoton, previousInfoton: Infoton, partition: String)(implicit executionContext: ExecutionContext): Future[Boolean] = ???

  override def purge(uuid: String, partition: String)(implicit executionContext: ExecutionContext): Future[Boolean] = ???

  override def purgeByUuids(historyUuids: Seq[String]
                            , currentUuid: Option[String]
                            , partition: String)(implicit executionContext: ExecutionContext): Future[BulkResponse] = ???

  override def purgeByUuidsAndIndexes(uuidsAtIndexes: Vector[(String, String)]
                                      , partition: String)(implicit executionContext: ExecutionContext): Future[BulkResponse] = ???

  override def purgeByUuidsFromAllIndexes(uuids: Vector[String], partition: String)(implicit executionContext: ExecutionContext): Future[BulkResponse] = ???

  override def purgeAll(path: String, isRecursive: Boolean, partition: String)(implicit executionContext: ExecutionContext): Future[Boolean] = ???

  override def purgeHistory(path: String, isRecursive: Boolean, partition: String)(implicit executionContext: ExecutionContext): Future[Boolean] = ???

  override def getIndicesNamesByType(suffix: String, partition: String): Seq[String] = ???

  override def listChildren(path: String
                            , offset: Int
                            , length: Int
                            , descendants: Boolean
                            , partition: String)(implicit executionContext: ExecutionContext): Future[FTSSearchResponse] = ???

  override def scroll(scrollId: String
                      , scrollTTL: Long
                      , nodeId: Option[String])(implicit executionContext: ExecutionContext, logger: Logger): Future[FTSScrollResponse] = ???

  override def uinfo(uuid: String
                     , partition: String)(implicit executionContext: ExecutionContext): Future[Vector[(String, Long, String)]] = ???

  override def countSearchOpenContexts(): Array[(String, Long)] = ???

  override def get(uuid: String
                   , indexName: String
                   , partition: String)(implicit executionContext: ExecutionContext): Future[Option[(FTSThinInfoton, Boolean)]] = ???
}
