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


package cmwell.web.ld

import java.nio.charset.Charset

import cmwell.domain._
import cmwell.common.file.MimeTypeIdentifier.identify
import cmwell.ws.Settings
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.ISODateTimeFormat

import scala.util.Try

/**
 * Created with IntelliJ IDEA.
 * User: gilad
 * Date: 7/21/13
 * Time: 1:39 PM
 * To change this template use File | Settings | File Templates.
 */
package object util {

  val uriSeparator: Set[Char] = Set('#','/',';') //TODO: does these possibilities cover every use case?
  val zeroTime = new DateTime(0L)
  lazy val dtf = ISODateTimeFormat.dateTime()

  def dt(date: String) = dtf.parseDateTime(date)

  def prependSlash(s: String): String = if(s.head == '/') s else s"/$s"

  def removeCmwHostAndPrependSlash(cmwHosts: Set[String], s: String) = {
    val p = s.dropWhile(_ == '/')
    if(cmwHosts.contains(p.takeWhile(_ != '/'))) p.dropWhile(_ != '/')
    else "/" + p
  }

  def inferCharsetFromMimetipe(mimeType: String): String = mimeType.lastIndexOf("charset=") match {
    case i if (i != -1) => mimeType.substring(i + 8).trim
    case _ => "utf-8"
  }

  private[this] def makeMetaWithZero(path: String,fields: Option[Map[String, Set[FieldValue]]]): ObjectInfoton = {
    if(path.startsWith("/meta/")) {
      ObjectInfoton(path, Settings.dataCenter,None,zeroTime,fields)
    } else {
      ObjectInfoton(path=path, fields=fields,dc = Settings.dataCenter)
    }
  }

  def infotonFromMaps(cmwHostsSet: Set[String], ipath: String, fields: Option[Map[String, Set[FieldValue]]], metaData: Option[MetaData]): Infoton = {
    val path = removeCmwHostAndPrependSlash(cmwHostsSet, ipath)
    metaData match {
      case Some(MetaData(mdt, date, data, text, ctype, linktype, linkto, dataCenter, indexTime)) => {
        lazy val (_date,dc) =
          if (path.startsWith("/meta/")) DateTime.now(DateTimeZone.UTC) -> Settings.dataCenter
          else date.getOrElse(DateTime.now(DateTimeZone.UTC)) -> dataCenter.getOrElse(Settings.dataCenter)
        mdt match {
          case Some(ObjectMetaData) if path.startsWith("/meta/") => makeMetaWithZero(path, fields)
          case Some(ObjectMetaData) => {
            ObjectInfoton(path,dc,indexTime,_date,fields)
          }
          case Some(FileMetaData) => {
            val contentTypeFromByteArray = ctype match {
              case Some(ct) => (x: Array[Byte]) => ct
              case None => (x: Array[Byte]) => identify(x, path).getOrElse("text/plain")
            }

            (data, text) match {
              case (Some(ba), None) => FileInfoton(path = path, lastModified = _date, fields = fields, content = Some(FileContent(ba, contentTypeFromByteArray(ba))), dc = dc, indexTime = indexTime)
              case (None, Some(txt)) => FileInfoton(path = path, lastModified = _date, fields = fields, content = Some(FileContent(txt.getBytes(Charset.forName("UTF-8")), "text/plain; utf-8")), dc = dc, indexTime = indexTime)
              case _ => ??? //TODO: case is untreated yet
            }
          }
          case Some(LinkMetaData) => (linktype, linkto) match {
            //??? //TODO: case is untreated yet
            case (Some(ltype), Some(lto)) =>
              LinkInfoton(
                path = path,
                fields = fields,
                lastModified = _date,
                linkTo = lto,
                linkType = ltype,
                dc = dc,
                indexTime = indexTime
              )
            case _ => ??? //TODO: case is untreated yet
          }
          case Some(DeletedMetaData) => DeletedInfoton(path,dc,indexTime,_date)
          case None => (data, text, ctype) match {
            case (None, None, None) => ObjectInfoton(path = path, lastModified = _date, fields = fields, dc = dc, indexTime = indexTime)
            case _ => infotonFromMaps(cmwHostsSet, path, fields, Some(metaData.get.copy(mdType = Some(FileMetaData)))) //TODO: better inference of types. needs to be refactored when link infotons will be used.
          }
        }
      }
      case None => makeMetaWithZero(path=path, fields=fields)
    }
  }

  //TODO: very very naive! please refactor
  def detectIfUrlBelongsToCmwellAndGetLength(subjectUrl: String): Int = {
    val domain = subjectUrl.dropWhile(_ != ':').drop(3).takeWhile(_ != '/')
    if(domain.contains("cm-well") || domain.startsWith("connext")) domain.length
    else 0
  }
}

package util {

  sealed trait TypeMetaData
  case object FileMetaData extends TypeMetaData
  case object LinkMetaData extends TypeMetaData
  case object ObjectMetaData extends TypeMetaData
  case object DeletedMetaData extends TypeMetaData

  case class MetaData(
    mdType: Option[TypeMetaData],
    date: Option[DateTime],
    data: Option[Array[Byte]],
    text: Option[String],
    mimeType: Option[String],
    linkType: Option[Int],
    linkTo: Option[String],
    dataCenter: Option[String],
    indexTime: Option [Long]
  ) {
    def merge(that: MetaData): MetaData = {
      val mdType = Try(this.mdType.getOrElse(that.mdType.get)).toOption
      val date = Try(this.date.getOrElse(that.date.get)).toOption
      val data = Try(this.data.getOrElse(that.data.get)).toOption
      val text = Try(this.text.getOrElse(that.text.get)).toOption
      val mimeType = Try(this.mimeType.getOrElse(that.mimeType.get)).toOption
      val linkType = Try(this.linkType.getOrElse(that.linkType.get)).toOption
      val linkTo = Try(this.linkTo.getOrElse(that.linkTo.get)).toOption
      val dataCenter = Try(this.dataCenter.getOrElse(that.dataCenter.get)).toOption
      val indexTime = Try(this.indexTime.getOrElse(that.indexTime.get)).toOption
      MetaData(mdType, date, data, text, mimeType, linkType, linkTo, dataCenter, indexTime)
    }
  }
  object MetaData {
    val empty = MetaData(None, None, None, None, None, None, None , None, None)
  }
}
