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
package cmwell.stortill

import cmwell.common.formats.{JsonSerializer, SettingsHelper}
import cmwell.domain.Infoton
import cmwell.fts._
import cmwell.irw._
import cmwell.util.{BoxedFailure, FullBox}
import cmwell.common.formats.JsonSerializer
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.action.{ActionRequest, DocWriteRequest}
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.Requests
import org.elasticsearch.index.VersionType

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by markz on 10/30/14.
  */
object Strotill {

  type CasInfo = Vector[(String, Option[Infoton])]
  type EsInfo = Vector[(String, String)] //uuid,indexName
  type EsExtendedInfo = Vector[(String, String, Long, String)] //uuid,indexName,version,esSource
  type ZStoreInfo = Vector[String]

  def apply(irw: IRWService, ftsService: FTSService): Strotill = new Strotill(irw, ftsService)
}

class Strotill(irw: IRWService, ftsService: FTSService) extends LazyLogging {
  import Strotill._

  def irwProxy: IRWService = irw
  def ftsProxy: FTSService = ftsService

  def extractHistoryCas(path: String, limit: Int): Future[CasInfo] = {
    irw.historyAsync(path, limit).flatMap { casHistory =>
      Future.traverse(casHistory.map(_._2)) { uuid =>
        irw.readUUIDAsync(uuid, ConsistencyLevel.QUORUM).map {
          case BoxedFailure(e) =>
            logger.error(s"readUUIDAsync failed for [$uuid] of path [$path]", e)
            uuid -> None
          case box => uuid -> box.toOption
        }
      }
    }
  }

  def extractLastCas(path: String): Future[Infoton] = {
    irw.readPathAsync(path).collect { case FullBox(infoton) => infoton }
  }

  def extractHistoryEs(path: String, limit: Int = 100): Future[EsInfo] =
    ftsService.info(path, PaginationParams(0, limit), withHistory = true)

  def fixEs(v: Vector[Infoton]): Future[SuccessfulBulkIndexResult] = {
    val esActions = createActionsToFixEs(v)
    ftsService.executeBulkIndexRequests(esActions)
  }

  @deprecated("wrong usage in irw2, and seems unused anyway...", "1.5.x")
  def fixCasAndFetchInfotons(path: String, limit: Int): Future[Seq[Infoton]] = {
    val historyFromEs = ftsService
      .search(None,
              Some(FieldFilter(Must, Equals, "system.path", path)),
              None,
              DefaultPaginationParams,
              withHistory = true)
      .map { searchResp =>
        searchResp.infotons.map(i => i.systemFields.lastModified -> i.uuid).sortBy(_._1.getMillis)
      }

    historyFromEs.flatMap {
      case history if history.isEmpty =>
        val uuids = irw.history(path, limit).map(_._2)
        irw
          .readUUIDSAsync(uuids)
          .map(_.collect {
            case FullBox(i) => i
          })

      case history =>
        val last = history.last
        irw.fixPath(path, last, history)
    }
  }

  def createActionsToFixEs(v: Vector[Infoton]): Seq[ESIndexRequest] = {

    def modifyInfoton(i: Infoton) = {
      /*
       * to handle badly fixed infotons that wrongly had new indexTime generated for historic versions,
       * if indexTime > lastModified by more than 24 hours (86400000 millis), we set indexTime to be
       * the last modified time instead.
       */
      val newIndexTime = {
        val lm = i.systemFields.lastModified.getMillis
        i.systemFields.indexTime match {
          case None                                       => lm
          case Some(it) if it > lm && it - lm > 86400000L => lm
          case Some(it)                                   => it
        }
      }

      val newDc =
        if (i.systemFields.dc != "na") i.systemFields.dc
        else SettingsHelper.dataCenter

      cmwell.domain.addDcAndIndexTimeForced(i, newDc, newIndexTime)
    }

    val actions: ArrayBuffer[DocWriteRequest[_]] =
      new ArrayBuffer[DocWriteRequest[_]](v.size)
    val cur = v.lastOption
    val history = v.init
    cur.foreach { i =>
      actions.append(
        Requests
          .indexRequest("cmwell_current_latest")
          .id(i.uuid)
          .create(true)
          .source(JsonSerializer.encodeInfoton(modifyInfoton(i), true, true))
      )
    }

    history.foreach { i =>
      actions.append(
        Requests
          .indexRequest("cmwell_history_latest")
          .id(i.uuid)
          .create(true)
          .source(JsonSerializer.encodeInfoton(modifyInfoton(i), true, true))
      )
    }
    actions.view.map(ESIndexRequest(_, None)).to(Seq)
  }
}
