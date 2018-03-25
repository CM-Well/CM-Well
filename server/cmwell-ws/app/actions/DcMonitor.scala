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


package actions

import akka.util.Timeout
import cmwell.ctrl.checkers._
import cmwell.ctrl.client.CtrlClient
import cmwell.domain._
import cmwell.fts.{FieldFilter, FieldOperator, PaginationParams, DatesFilter}
import k.grid.{WhoIAm, WhoAreYou, Grid}
import logic.CRUDServiceFS
import org.joda.time.DateTime

import scala.concurrent.Future
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

object DcMonitor {
  private implicit val timeout = Timeout(5.seconds)
  lazy val dc = Grid.serviceRef("DataCenterSyncManager")

  private def getDcAddress : Future[String] = {
    (dc ? WhoAreYou).mapTo[WhoIAm].map(_.address).recover{case err : Throwable => "NA"}
  }
  
  def dcDistribution(path : String, dc : String, crudServiceFS: CRUDServiceFS) : Future[Option[VirtualInfoton]] = {

    val fDcAddress = getDcAddress

    val fAggRes = crudServiceFS.aggregate(None,
                            None,
                            None,
                            PaginationParams(0, 20),
                            true,
                            List(TermAggregationFilter("TermAggregation",Field(AnalyzedField,"system.dc"))),
                            false)

    for {
      dcAddress <- fDcAddress
      aggRes <- fAggRes
    } yield {
      val tuples = aggRes.responses.flatMap {
        case TermsAggregationResponse(filter, buckets) => {
          buckets.map { bucket =>
            MarkdownTuple(bucket.key.value.asInstanceOf[String], bucket.docCount.toString)
          }
        }
      }
      val markdownTable = MarkdownTable(MarkdownTuple("dc", "count"), tuples)
      val body =
        s"""
          |***Data center distribution*** <br>
          |**Current Data center host: $dcAddress** <br>
          |""".stripMargin + markdownTable.get
      Some(VirtualInfoton(FileInfoton(path, dc, None, content = Some(FileContent(body.getBytes, "text/x-markdown")))))
    }
  }
  
  
  def dcHealthInfotonMD(path : String, dc : String) : Future[Option[VirtualInfoton]] = {
    val fDcAddress = getDcAddress
    val fM = CtrlClient.getDataCenterStatus

    for {
      dcAddress <- fDcAddress
      m <- fM
    } yield {
      val header = MarkdownTuple("Dc", "Local index", "Remote index", "Sync status")

      val tuples = m.values.map {
        case DcSyncing(dcId, dcDiff, ch, genTime) =>
          MarkdownTuple(dcId, cmwell.util.string.dateStringify(new DateTime(dcDiff.indextimeDiff.me)), cmwell.util.string.dateStringify(new DateTime(dcDiff.indextimeDiff.remote)), "Syncing")
        case DcNotSyncing(dcId, dcDiff, notSyncingCounter, ch, genTime) =>
          val msg = if(notSyncingCounter > 3)
            "Not syncing"
          else if(notSyncingCounter > 20)
            "Lost connection"
          else "Syncing"
          MarkdownTuple(dcId, cmwell.util.string.dateStringify(new DateTime(dcDiff.indextimeDiff.me)), cmwell.util.string.dateStringify(new DateTime(dcDiff.indextimeDiff.remote)), msg)
        case DcCouldNotGetDcStatus(dcId, dcDiff , errCounter , ch, genTime) =>
          MarkdownTuple(dcId, cmwell.util.string.dateStringify(new DateTime(dcDiff.indextimeDiff.me)), "NA", s"Couldn't get remote host info.")
        case ReportTimeout(genTime) =>
          MarkdownTuple("NA", "NA", "NA", s"Didn't receive DC stats for long time.")
      }

      val table = MarkdownTable(header, tuples.toSeq)

      val body =
        s"""
          |***Data center monitor*** <br>
          |**Current Data center host: $dcAddress** <br>
          |""".stripMargin + table.get


      Some(VirtualInfoton(FileInfoton(path, dc, None, content = Some(FileContent(body.getBytes, "text/x-markdown")))))
    }
  }

//  def dcHealthInfoton(path : String, dc : String) : Future[Option[VirtualInfoton]] = {
//    CtrlClient.getDataCenterStatus.map {
//      m =>
//        //Map[String,Set[FieldValue]](
//        m.map{
//          record =>
//            record._2 match {
//              case DcSyncStatus(dcId, dcDiff, notSyncingCounter, genTime) =>
//                Map(s"$dcId")
//            }
//            s"${record._1}.${record._2}"
//        }
//    }
//  }
}
