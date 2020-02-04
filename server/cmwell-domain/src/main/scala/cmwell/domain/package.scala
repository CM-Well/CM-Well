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
package cmwell

import com.typesafe.scalalogging.LazyLogging

/**
  * Created by gilad on 8/6/15.
  */
package object domain extends LazyLogging {

  def addIndexTime(infoton: Infoton, indexTime: Option[Long], force: Boolean = false): Infoton = infoton match {
    // format: off
    case i:ObjectInfoton  if force || i.systemFields.indexTime.isEmpty => i.copy(i.systemFields.copy(indexTime = indexTime))
    case i:FileInfoton    if force || i.systemFields.indexTime.isEmpty => i.copy(i.systemFields.copy(indexTime = indexTime))
    case i:LinkInfoton    if force || i.systemFields.indexTime.isEmpty => i.copy(i.systemFields.copy(indexTime = indexTime))
    case i:DeletedInfoton if force || i.systemFields.indexTime.isEmpty => i.copy(i.systemFields.copy(indexTime = indexTime))
    // format: on
    case i if i.systemFields.indexTime.isDefined => {
      logger.warn(
        s"was asked to add indextime, but one is already supplied! uuid=${i.uuid}, path=${i.systemFields.path}, indexTime=${i.systemFields.indexTime.get}"
      )
      i
    }
    case _ => ???
  }

  def addIndexInfo(infoton: Infoton, indexTime: Option[Long], indexName: String, force: Boolean = false): Infoton =
    infoton match {
      // format: off
    case i:ObjectInfoton  if force || i.systemFields.indexTime.isEmpty => i.copy(i.systemFields.copy(indexTime = indexTime, indexName = indexName))
    case i:FileInfoton    if force || i.systemFields.indexTime.isEmpty => i.copy(i.systemFields.copy(indexTime = indexTime, indexName = indexName))
    case i:LinkInfoton    if force || i.systemFields.indexTime.isEmpty => i.copy(i.systemFields.copy(indexTime = indexTime, indexName = indexName))
    case i:DeletedInfoton if force || i.systemFields.indexTime.isEmpty => i.copy(i.systemFields.copy(indexTime = indexTime, indexName = indexName))
    // format: on
      case i if i.systemFields.indexTime.isDefined => {
        logger.warn(
          s"was asked to add indextime, but one is already supplied! uuid=${i.uuid}, path=${i.systemFields.path}, indexTime=${i.systemFields.indexTime.get}"
        )
        i
      }
      case _ => ???
    }

  def addDc(infoton: Infoton, dc: String, force: Boolean = false): Infoton = infoton match {
    // format: off
    case i:ObjectInfoton  if force || i.systemFields.dc=="na" => i.copy(i.systemFields.copy(dc = dc))
    case i:FileInfoton    if force || i.systemFields.dc=="na" => i.copy(i.systemFields.copy(dc = dc))
    case i:LinkInfoton    if force || i.systemFields.dc=="na" => i.copy(i.systemFields.copy(dc = dc))
    case i:DeletedInfoton if force || i.systemFields.dc=="na" => i.copy(i.systemFields.copy(dc = dc))
    // format: on
    case i if i.systemFields.dc != "na" => {
      logger.warn(s"was asked to add dc, but one is already supplied! uuid=${i.uuid}, path=${i.systemFields.path}, dc=${i.systemFields.dc}")
      i
    }
    case _ => ???
  }

  def addDcAndIndexTimeForced(infoton: Infoton, dc: String, indexTime: Long): Infoton = infoton match {
    // format: off
    case i:ObjectInfoton  => i.copy(i.systemFields.copy(dc = dc, indexTime = Some(indexTime)))
    case i:FileInfoton    => i.copy(i.systemFields.copy(dc = dc, indexTime = Some(indexTime)))
    case i:LinkInfoton    => i.copy(i.systemFields.copy(dc = dc, indexTime = Some(indexTime)))
    case i:DeletedInfoton => i.copy(i.systemFields.copy(dc = dc, indexTime = Some(indexTime)))
    // format: on
    case _ => ???
  }

  def autoFixDcAndIndexTime(i: Infoton, dcIfNeeded: String): Option[Infoton] = {
    if (i.systemFields.dc == "na" || i.systemFields.indexTime.isEmpty) {

      val idxT = i.systemFields.indexTime.orElse(Some(i.systemFields.lastModified.getMillis))

      val dc = if (i.systemFields.dc == "na") dcIfNeeded else i.systemFields.dc

      i match {
        // format: off
        case i: ObjectInfoton  => Some(i.copy(i.systemFields.copy(dc = dc, indexTime = idxT)))
        case i: FileInfoton    => Some(i.copy(i.systemFields.copy(dc = dc, indexTime = idxT)))
        case i: LinkInfoton    => Some(i.copy(i.systemFields.copy(dc = dc, indexTime = idxT)))
        case i: DeletedInfoton => Some(i.copy(i.systemFields.copy(dc = dc, indexTime = idxT)))
        // format: on
        case _ => ???
      }
    } else None
  }
}
