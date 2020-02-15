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

import akka.util.Timeout
import cmwell.domain.{FileContent, FileInfoton, SystemFields, VirtualInfoton}
import cmwell.ws.util.DateParser._
import k.grid._
import k.grid.monitoring._
import org.joda.time.{DateTime, DateTimeZone, Period}
import org.joda.time.format.PeriodFormatterBuilder

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success
import akka.pattern.ask

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 7/6/15.
  */
object GridMonitoring {
  implicit val timeout = Timeout(30.seconds)

  /*  // todo: maybe should be moved to util... might be useful..
  val daysHoursMinutesFormat = new PeriodFormatterBuilder()
    .appendYears()
    .appendSuffix(" year", " years")
    .appendSeparator(" and ")
    .appendMonths()
    .appendSuffix(" month", " months")
    .appendSeparator(" and ")
    .appendDays()
    .appendSuffix(" day", " days")
    .appendSeparator(" and ")
    .appendMinutes()
    .appendSuffix(" minute", " minutes")
    .appendSeparator(" and ")
    .appendSeconds()
    .appendSuffix(" second", " seconds")
    .toFormatter()*/

  def durationToWords(duration: Long): String = {
    val sec = 1L
    val min = 60 * sec
    val hr = 60 * min
    val day = 24 * hr

    val days = duration / day
    val hrs = (duration % day) / hr
    val mins = ((duration % day) % hr) / min
    val secs = (((duration % day) % hr) % min) / sec

    s"$days " + "%02d".format(hrs) + ":" + "%02d".format(mins) + ":" + "%02d".format(secs)
  }
  //

  private def genMemInfo(memSet: Set[MemoryInfo]): String = {
    memSet
      .map { mem =>
        s"${mem.name} -> ${mem.usedPercent}%"
      }
      .mkString("<br>")
  }

  private def genGcInfo(gcSet: Set[GcInfo]): String = {
    gcSet
      .map { gc =>
        s"${gc.name} -> t:${gc.gcTime},c:${gc.gcCount}"
      }
      .mkString("<br>")
  }

  private def getRoleSymbol(jvmInfo: JvmInfo): String = {
    jvmInfo.role match {
      case k.grid.GridMember   => "△"
      case k.grid.Controller   => "▲"
      case k.grid.ClientMember => "◯"
    }
  }

  trait RetrievalMethod
  case object All extends RetrievalMethod
  case object Active extends RetrievalMethod

  trait TableFormat
  case object Markdown extends TableFormat
  case object CsvPretty extends TableFormat

  def members(path: String,
              dc: String,
              method: RetrievalMethod,
              isRoot: Boolean,
              format: TableFormat = Markdown,
              contentTranformator: String => String = identity): Future[Option[VirtualInfoton]] = {
    val members = {
      method match {
        case All    => Grid.getAllJvmsInfo
        case Active => Grid.getRunningJvmsInfo
      }
    }
    members.map { m =>
      val tupls = m
        .map { tpl =>
          val t = Seq(
            getRoleSymbol(tpl._2),
            tpl._1.identity.map(_.name).getOrElse("NA"),
            tpl._2.pid.toString,
            tpl._1.hostname,
            durationToWords(tpl._2.uptime / 1000),
            genMemInfo(tpl._2.memInfo),
            genGcInfo(tpl._2.gcInfo),
            tpl._2.logLevel,
            tpl._2.status.toString,
            fdf(new DateTime(tpl._2.sampleTime)),
            tpl._2.extraData
          )
          if (isRoot)
            t :+ s"<a href='/_hcn?host=${tpl._1.host}&jvm=${tpl._1.identity.map(_.name).getOrElse("")}'>restart</a>"
          else t
        }
        .toVector
        .sortBy(_(3))

      val headers =
        Seq("Role", "Name", "Pid", "Address", "Uptime", "Mem", "Gc", "Level", "Status", "ST", "Extra", "Restart")
          .filterNot(_ == "Restart" && !isRoot)

      val (payload, mimeType) = format match {
        case Markdown =>
          MarkdownTable(MarkdownTuple(headers: _*), tupls.map(MarkdownTuple(_: _*))).get -> "text/x-markdown"
        case CsvPretty =>
          s"${headers.mkString(",")}\\n${tupls.map(_.map(_.replace(",", " ")).mkString(",")).mkString("\\n")}" -> "text/html"
      }
      Some(
        VirtualInfoton(
          FileInfoton(SystemFields(path, new DateTime(DateTimeZone.UTC), "VirtualInfoton", dc, None, "", "http"),
            content = Some(FileContent(contentTranformator(payload).getBytes, mimeType)))
        )
      )
    }
  }

  def singletons(path: String, dc: String): Future[Option[VirtualInfoton]] = {
    val singletons = Grid.getSingletonsInfo
    val body = singletons.map { t =>
      MarkdownTuple(t.name, t.role, t.location)
    }
    val md = MarkdownTable(MarkdownTuple("Singleton", "Role", "Location"), body.toSeq).get
    Future.successful(
      Some(VirtualInfoton(FileInfoton(SystemFields(path, new DateTime(DateTimeZone.UTC), "VirtualInfoton", dc, None, "", "http"),
        content = Some(FileContent(md.getBytes, "text/x-markdown")))))
    )
  }

  def actors(path: String, dc: String): Future[Option[VirtualInfoton]] = {
    val allActorsF = Grid.allActors

    allActorsF.map { allActors =>
      val actors = allActors.map(m => m.actors.map(a => (m.host, a.name, a.latency.toString)))
      val actorsFlatten = actors.flatten
      val body = actorsFlatten.toSeq.sortBy(_._1).map(t => MarkdownTuple(t._1, t._2, t._3))
      val md = MarkdownTable(MarkdownTuple("Member", "Actor", "Latency (ms)"), body).get
      Some(VirtualInfoton(FileInfoton(SystemFields(path, new DateTime(DateTimeZone.UTC), "VirtualInfoton", dc, None, "", "http"),
        content = Some(FileContent(md.getBytes, "text/x-markdown")))))
    }
  }

  def actorsDiff(path: String, dc: String): Future[Option[VirtualInfoton]] = {
    val membersWithActorsF = Grid.allActors
    membersWithActorsF.map { membersWithActors =>
      val actorsMap = membersWithActors.map(mwa => mwa.host -> mwa.actors.map(_.name)).toMap

      val actors = membersWithActors.flatMap { memberWithActors =>
        memberWithActors.actors.map(_.name)
      }
      val bodyData = actors.map { actor =>
        actorsMap.keySet.toSeq.sorted.map { host =>
          if (actorsMap(host).contains(actor)) s"<span style='color:green'>$actor</span>"
          else s"<span style='color:red'>$actor</span>"
        }
      }
      val body = bodyData.map { bodyDatum =>
        MarkdownTuple(bodyDatum: _*)
      }
      val md = MarkdownTable(MarkdownTuple(actorsMap.keySet.toSeq.sorted: _*), body.toSeq).get
      Some(VirtualInfoton(FileInfoton(SystemFields(path, new DateTime(DateTimeZone.UTC), "VirtualInfoton", dc, None, "", "http"),
        content = Some(FileContent(md.getBytes, "text/x-markdown")))))
    }
  }
}
