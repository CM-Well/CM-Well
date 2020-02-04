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
package cmwell.common

import cmwell.common.formats.{JsonSerializer, Offset}
import cmwell.domain.{FieldValue, Infoton}
import org.joda.time.DateTime

/**
  * Created with IntelliJ IDEA.
  * User: markz
  * Date: 12/17/12
  * Time: 5:58 PM
  *
  */
sealed abstract class Command

case object HeartbitCommand extends Command

sealed abstract class SingleCommand extends Command {
  def path: String

  def trackingID: Option[String]

  def prevUUID: Option[String]

  def lastModified: DateTime

  def lastModifiedBy: String
}

object SingleCommand{
  def unapply(c: SingleCommand): Option[(String, Option[String], Option[String], DateTime, String)] =
    Option(c) map { c =>
      (c.path, c.trackingID, c.prevUUID, c.lastModified, c.lastModifiedBy)
    }
}

case class CommandRef(ref: String) extends Command

case class WriteCommand(infoton: Infoton, trackingID: Option[String] = None, prevUUID: Option[String] = None)
  extends SingleCommand {
  override def path = infoton.systemFields.path

  override def lastModified: DateTime = infoton.systemFields.lastModified

  override def lastModifiedBy: String = infoton.systemFields.lastModifiedBy
}

case class DeleteAttributesCommand(path: String,
                                   fields: Map[String, Set[FieldValue]],
                                   lastModified: DateTime,
                                   lastModifiedBy: String,
                                   trackingID: Option[String] = None,
                                   prevUUID: Option[String] = None)
  extends SingleCommand

case class DeletePathCommand(path: String,
                             lastModified: DateTime = new DateTime(),
                             lastModifiedBy: String,
                             trackingID: Option[String] = None,
                             prevUUID: Option[String] = None)
  extends SingleCommand

// ( Option[Set[FieldValue]] , Set[FieldValue] )
// 1 .when getting None delete all values of specific field
// 2. when getting a empty Set we just add the new fields without delete
case class UpdatePathCommand(path: String,
                             deleteFields: Map[String, Set[FieldValue]],
                             updateFields: Map[String, Set[FieldValue]],
                             lastModified: DateTime,
                             lastModifiedBy: String,
                             trackingID: Option[String] = None,
                             prevUUID: Option[String] = None,
                             protocol: String)
  extends SingleCommand

case class OverwriteCommand(infoton: Infoton, trackingID: Option[String] = None) extends SingleCommand {
  //  require(infoton.indexTime.isDefined && infoton.dc != SettingsHelper.dataCenter,
  //    s"OverwriteCommands must be used only for infotons from other data centers [${infoton.indexTime}] &&
  // ${infoton.dc} != ${SettingsHelper.dataCenter}.\ninfoton: ${new String(JsonSerializer.encodeInfoton(infoton),"UTF-8")}")

  override def prevUUID: Option[String] = None

  override def path = infoton.systemFields.path

  override def lastModified: DateTime = infoton.systemFields.lastModified

  override def lastModifiedBy: String = infoton.systemFields.lastModifiedBy
}

case class StatusTracking(tid: String, numOfParts: Int)

sealed trait IndexCommand extends Command {
  def path: String

  def trackingIDs: Seq[StatusTracking]

  def uuid: String

  def indexName: String

}

/**
  * A command for the Indexer to index a new infoton
  *
  * @param uuid      of the new infoton
  * @param path      infoton's path (used for kafka partitioning)
  * @param isCurrent whether this is the latest
  */
case class IndexNewInfotonCommand(uuid: String,
                                  isCurrent: Boolean,
                                  path: String,
                                  infotonOpt: Option[Infoton] = None,
                                  indexName: String,
                                  trackingIDs: Seq[StatusTracking] = Nil)
  extends IndexCommand

/**
  * A commnad for the Indexer to update existing infoton
  *
  * @param uuid   of the existing infoton
  * @param weight infoton's weight
  * @param path   infoton's path (used for kafka partitioning
  */
case class IndexExistingInfotonCommand(uuid: String,
                                       weight: Long,
                                       path: String,
                                       indexName: String,
                                       trackingIDs: Seq[StatusTracking] = Nil)
  extends IndexCommand

/**
  * A command for the Indexer to index a new infoton
  *
  * @param uuid      of the new infoton
  * @param path      infoton's path (used for kafka partitioning)
  * @param isCurrent whether this is the latest
  * @param persistOffsets the offset in persist topic whom the command was made of
  */
case class IndexNewInfotonCommandForIndexer(uuid: String,
                                            isCurrent: Boolean,
                                            path: String,
                                            infotonOpt: Option[Infoton] = None,
                                            indexName: String,
                                            persistOffsets: Seq[Offset],
                                            trackingIDs: Seq[StatusTracking] = Nil)
  extends IndexCommand

/**
  * A commnad for the Indexer to update existing infoton
  *
  * @param uuid   of the existing infoton
  * @param weight infoton's weight
  * @param path   infoton's path (used for kafka partitioning
  * @param persistOffsets the offset in persist topic whom the command was made of
  */
case class IndexExistingInfotonCommandForIndexer(uuid: String,
                                                 weight: Long,
                                                 path: String,
                                                 indexName: String,
                                                 persistOffsets: Seq[Offset],
                                                 trackingIDs: Seq[StatusTracking] = Nil)
  extends IndexCommand

case class NullUpdateCommandForIndexer(uuid: String = "",
                                       path: String,
                                       indexName: String = "",
                                       persistOffsets: Seq[Offset],
                                       trackingIDs: Seq[StatusTracking] = Nil)
  extends IndexCommand

object CommandSerializer {

  def encode(cmd: Command): Array[Byte] = {
    JsonSerializer.encodeCommand(cmd)
  }

  def decode(payload: Array[Byte]): Command = {
    JsonSerializer.decodeCommand(payload)
  }

}
