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

import com.typesafe.scalalogging.LazyLogging
import k.grid.{Grid, WhoAreYou, WhoIAm}
import akka.pattern._
import akka.util.Timeout
import cmwell.ctrl.checkers.StpChecker.{RequestStats, ResponseStats, Row, Table}
import cmwell.domain.{FileContent, FileInfoton, SystemFields, VirtualInfoton}
import cmwell.ws.Settings
import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by matan on 14/5/17.
  */
object SparqlTriggeredProcessorMonitor extends LazyLogging {
  implicit val timeout = Timeout(30.seconds)

  def getAddress =
    (stpManager ? WhoAreYou)
      .mapTo[WhoIAm]
      .map(_.address)
      .recover { case err: Throwable => "NA" }

  def stpManager = Grid.serviceRef("sparql-triggered-processor-manager")

  def jobsDataToTuple(lines: Iterable[Row]) = for (line <- lines) yield MarkdownTuple(line.toSeq: _*)

  def generateTables(path: String, dc: String, isAdmin: Boolean): Future[Option[VirtualInfoton]] = {
    val jobsDataFuture = (stpManager ? RequestStats(isAdmin))
      .mapTo[ResponseStats]
      .map { case ResponseStats(tables) => tables }

    val future = for {
      address <- getAddress
      tables <- jobsDataFuture

    } yield {
      val title =
        s"""
          |# Sparql Triggered Processor Monitor<br>
          |## Current host: $address  <br>
        """.stripMargin

      val tablesFormattedData = tables.map { table =>

        val mdTable = MarkdownTable(
          header = MarkdownTuple(table.header.toSeq: _*),
          body = jobsDataToTuple(table.body).toSeq
        )

        s"""
         |${table.title.mkString("### ", "<br>\n### ", "<br>")}
         |${mdTable.get} <hr>""".stripMargin
      }

      title + "\n\n" + tablesFormattedData.mkString("\n\n")
    }
    future.map { content =>
      Some(
        VirtualInfoton(
          FileInfoton(SystemFields(path, new DateTime(DateTimeZone.UTC), "VirtualInfoton", dc, None, "", "http"),
            None, content = Some(FileContent(content.getBytes("utf-8"), "text/x-markdown")))
        )
      )
    }
  }
}
