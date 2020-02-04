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
package cmwell.bg

import cmwell.domain._
import cmwell.common._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.{DateTime, DateTimeZone}

import scala.annotation.tailrec
import scala.collection.immutable.SortedSet

/**
  * Created by israel on 12/8/15.
  */
object Merger {
  def apply() = new Merger(ConfigFactory.load())
}

sealed trait MergeResponse {
  def tids: Seq[String]
  def evictions: Seq[(String, Option[String])]
  def merged: Option[Infoton]
  def path: String
  def extra: Option[String]
}
final case class NullUpdate(path: String, tids: Seq[String], evictions: Seq[(String, Option[String])], extra: Option[String])
  extends MergeResponse {
  override def merged = None
}
final case class RealUpdate(infoton: Infoton, tids: Seq[String], evictions: Seq[(String, Option[String])], extra: Option[String])
  extends MergeResponse {
  override lazy val merged = Some(infoton)

  override def path: String = infoton.systemFields.path
}

class Merger(config: Config) extends LazyLogging {

  val defaultDC = config.getString("cmwell.dataCenter.id")

  private def merge_f(last_fields: Option[Map[String, Set[FieldValue]]],
                      current_fields: Option[Map[String, Set[FieldValue]]]): Option[Map[String, Set[FieldValue]]] = {

    val previousFields = if (last_fields.exists(_.isEmpty)) None else last_fields
    val currentFields = if (current_fields.exists(_.isEmpty)) None else current_fields

    (previousFields, currentFields) match {
      case (None, current)  => current
      case (previous, None) => previous
      case (Some(last), Some(curr)) => {
        val keys = last.keySet ++ curr.keySet
        val f: Map[String, Set[FieldValue]] = keys.view.map { k =>
          val v1 = last.getOrElse(k, Set.empty[FieldValue])
          val v2 = curr.getOrElse(k, Set.empty[FieldValue])
          k -> (v1 ++ v2)
        }.to(Map)
        Some(f)
      }
    }
  }

  private def delete_f(current_fields: Option[Map[String, Set[FieldValue]]],
                       deleteFields: Map[String, Set[FieldValue]]): Option[Map[String, Set[FieldValue]]] = {
    val (asterix, filterQuad): (Boolean, Option[String] => Boolean) = deleteFields.get("*") match {
      case Some(quads) if quads.contains(FNull(Some("*"))) =>
        true -> { _ =>
          true
        }
      case Some(quads) => true -> { quads.collect { case FNull(q) => q } }
      case None =>
        false -> { _ =>
          false
        }
    }
    val delete_fields: Map[String, Set[FieldValue]] = deleteFields.view.filterKeys(_ != "*").toMap

    current_fields.map(_.map {
      case (k, vs) =>
        //if we have a wild card field delete, we must check every value for it's quad (that's why we added the `|| asterix`)
        if (delete_fields.keySet(k) || asterix) {
          if (delete_fields.get(k).collect { case xs => xs.isEmpty }.getOrElse(false))
            (k, Set.empty[FieldValue])
          else {
            val v = vs.filterNot(
              fv =>
                filterQuad(fv.quad) || (delete_fields.contains(k) && delete_fields(k).exists {
                  case FNull(Some("*")) => true
                  case FNull(q)         => fv.quad == q
                  case fvtd =>
                    fvtd.value == fv.value && (fvtd.quad match {
                      case None      => fv.quad.isEmpty
                      case Some("*") => true
                      case Some(q)   => fv.quad.fold(false)(q.==)
                    })
                })
            )
            (k, v)
          }
        } else {
          (k, vs)
        }
    }.filterNot { case (_, s) => s.isEmpty })
  }

  private def update_f(current_fields: Option[Map[String, Set[FieldValue]]],
                       delete_fields: Map[String, Set[FieldValue]],
                       add_fields: Map[String, Set[FieldValue]]): Option[Map[String, Set[FieldValue]]] = {
    val after_delete = delete_f(current_fields, delete_fields)
    val after_update = merge_f(after_delete, Some(add_fields))
    after_update
  }

  private def delete_merge(prev_infoton: Infoton,
                           fields: Map[String, Set[FieldValue]],
                           lastModified: DateTime): Infoton = {
    prev_infoton match {
      case ObjectInfoton(systemFields, current_fields) =>
        val newFields = delete_f(current_fields, fields)
        if (newFields.nonEmpty)
            ObjectInfoton(systemFields.copy(dc= defaultDC, indexTime = None, lastModified = lastModified), newFields)
          else DeletedInfoton(systemFields.copy(dc = defaultDC,indexTime = None, lastModified = lastModified))
      case f @ FileInfoton(_, current_fields, _) =>
        f.copy(f.systemFields.copy(indexTime = None, lastModified = lastModified), fields = delete_f(current_fields, fields))
      case l @ LinkInfoton(_, current_fields, _, _) =>
        l.copy(l.systemFields.copy(indexTime = None, lastModified = lastModified), fields = delete_f(current_fields, fields))
      case i: DeletedInfoton => i // if we got a delete on delete we need ignore the create of delete
      case j =>
        throw new NotImplementedError(s"kind [${j.kind}] uuid [${prev_infoton.uuid}] info [$j]")
    }
  }

  private def update_merge(current_infoton: Infoton,
                           delete_fields: Map[String, Set[FieldValue]],
                           add_fields: Map[String, Set[FieldValue]],
                           lastModified: DateTime,
                           protocol: String): Infoton = {
    val u_f = update_f(current_infoton.fields, delete_fields, add_fields)
    current_infoton match {
      case ObjectInfoton(sysFields, _) if u_f.exists(_.nonEmpty) =>
        ObjectInfoton(sysFields.copy(dc = defaultDC, indexTime = None, lastModified = lastModified, protocol = protocol), u_f)
      case ObjectInfoton(sysFields, _) =>
        DeletedInfoton(sysFields.copy(dc = defaultDC, indexTime = None, lastModified = lastModified, protocol = protocol))
      case FileInfoton(sysFields, _, c_fc) =>
        FileInfoton(sysFields.copy(dc = defaultDC, indexTime = None, lastModified = lastModified, protocol = protocol), u_f, c_fc)
      case LinkInfoton(sysFields, _, c_to, c_linkType) =>
        LinkInfoton(sysFields.copy(dc = defaultDC, indexTime = None, lastModified = lastModified, protocol = protocol), u_f, c_to, c_linkType)
      case DeletedInfoton(sysFields) if u_f.exists(_.nonEmpty) =>
        // if we got update after a delete infoton we create a new one
        ObjectInfoton(sysFields.copy(dc = defaultDC, indexTime = None, lastModified = lastModified, protocol = protocol), u_f)
      case _ =>
        // might happen when e.g: writing a "skeleton" on top of a deleted infoton.
        logger.warn(s"kind [${current_infoton.kind}] uuid [${current_infoton.uuid}] info [$current_infoton]")
        current_infoton
    }
  }

  private def write_merge(prev_infoton: Infoton, current_infoton: Infoton): Infoton = {
    // we build the new infoton based on the old one first we need to merge the fields
    current_infoton match {
      case ObjectInfoton(systemFields, current_fields) =>
        prev_infoton match {
          case ObjectInfoton(_, prev_fields) =>
            ObjectInfoton(systemFields.copy(dc = defaultDC, indexTime = None), merge_f(prev_fields, current_fields))
          case FileInfoton(prev_sysFields, prev_fields, perv_fc) =>
            FileInfoton(systemFields.copy(dc = defaultDC, indexTime = None, protocol = prev_sysFields.protocol), merge_f(prev_fields, current_fields), perv_fc)
          case LinkInfoton(prev_sysFields, prev_fields, prev_to, prev_linkType) =>
            LinkInfoton(systemFields.copy(dc = defaultDC, indexTime = None, protocol = prev_sysFields.protocol),
              merge_f(prev_fields, current_fields), prev_to, prev_linkType)
          case DeletedInfoton(_) =>
            current_infoton
          case _ =>
            throw new NotImplementedError(s"was trying to write_merge o[ $current_infoton ] on top of[ $prev_infoton ]")
        }
      case FileInfoton(systemFields, current_fields, c_fc) =>
        prev_infoton match {
          case ObjectInfoton(_, prev_fields) =>
            FileInfoton(systemFields.copy(dc = defaultDC, indexTime = None), merge_f(prev_fields, current_fields), c_fc)
          case FileInfoton(_, prev_fields, prev_fc) =>
            FileInfoton(systemFields.copy(dc = defaultDC, indexTime = None),
              merge_f(if (prev_fields.exists(_.nonEmpty)) prev_fields else None, current_fields),
              c_fc.orElse(prev_fc)
            )
          case LinkInfoton(_, prev_fields, _, _) if prev_fields.exists(_.nonEmpty) =>
            FileInfoton(systemFields.copy(dc = defaultDC, indexTime = None), merge_f(prev_fields, current_fields), c_fc)
          case _: LinkInfoton | _: DeletedInfoton =>
            current_infoton
          case _ =>
            throw new NotImplementedError(s"was trying to write_merge f[ $current_infoton ] on top of[ $prev_infoton ]")

        }
      case LinkInfoton(systemFields, current_fields, c_to, c_linkType) =>
        prev_infoton match {
          case ObjectInfoton(_, prev_fields) =>
            LinkInfoton(systemFields.copy(dc = defaultDC, indexTime = None), merge_f(prev_fields, current_fields), c_to, c_linkType)
          case LinkInfoton(_, prev_fields, _, _) =>
            LinkInfoton(systemFields.copy(dc = defaultDC, indexTime = None),merge_f(prev_fields, current_fields), c_to, c_linkType)
          case FileInfoton(_, prev_fields, _) if prev_fields.exists(_.nonEmpty) =>
            LinkInfoton(systemFields.copy(dc = defaultDC, indexTime = None), merge_f(prev_fields, current_fields), c_to, c_linkType)
          case _: DeletedInfoton | _: FileInfoton =>
            current_infoton
          case _ =>
            throw new NotImplementedError(s"was trying to write_merge l[ $current_infoton ] on top of[ $prev_infoton ]")
        }
      case _ =>
        throw new NotImplementedError(s"was trying to write_merge [ $current_infoton ] on top of[ $prev_infoton ]")
    }
  }

  def merge(baseInfoton: Option[Infoton], cmds: Seq[SingleCommand]): MergeResponse = {

    def ensurePrevUUID(base: Infoton, prevUUID: Option[String])(
      getMerged: Infoton => Option[Infoton]
    ): (Option[Infoton], Option[String]) = {
      lazy val merged: (Option[Infoton], Option[String]) = getMerged(base) -> None
      prevUUID.fold(merged) { pUUID =>
        if (base.uuid == pUUID) merged
        else Some(base) -> Some(s"expected:$pUUID,actual:${base.uuid}")
      }
    }

    def ensurePrevNone(prevUUID: Option[String])(getApplied: => Option[Infoton]): (Option[Infoton], Option[String]) =
      prevUUID match {
        case None | Some("")      => getApplied -> None
        case Some(someMadeUpUUID) => None -> Some(s"expected:$someMadeUpUUID,actual:none")
      }

    /**
      * @return a tuple of the merged infoton (or prev version if wasn't merged)
      *         and option containing eviction reson if was evicted
      */
    def do_merge(base: Option[Infoton], cmd: SingleCommand): (Option[Infoton], Option[String]) = {
      cmd match {
        case WriteCommand(currInfoton, _, prevUUID) =>
          base match {
            case Some(last_infoton) => ensurePrevUUID(last_infoton, prevUUID)(i => Some(write_merge(i, currInfoton)))
            case None               => ensurePrevNone(prevUUID)(Some(currInfoton))
          }
        case DeleteAttributesCommand(_, fields, lastModified, _, _, prevUUID) =>
          base match {
            case Some(last_infoton) =>
              ensurePrevUUID(last_infoton, prevUUID)(i => Some(delete_merge(i, fields, lastModified)))
            case None => ensurePrevNone(prevUUID)(None)
          }
        case UpdatePathCommand(path, deleteFields, updateFields, lastModified, _, _, prevUUID, protocol) =>
          base match {
            case Some(last_infoton) =>
              ensurePrevUUID(last_infoton, prevUUID)(
                i => Some(update_merge(i, deleteFields, updateFields, lastModified, protocol))
              )
            case None =>
              ensurePrevNone(prevUUID)(Some(ObjectInfoton(SystemFields(path, lastModified, cmd.lastModifiedBy, defaultDC, None, "", protocol),
                Some(updateFields))))
          }
        case DeletePathCommand(path, lastModified, _, _, prevUUID) =>
          base match {
            case sdi @ Some(di: DeletedInfoton) => ensurePrevUUID(di, prevUUID)(_ => sdi)
            case Some(infoton) =>
              ensurePrevUUID(infoton, prevUUID)(_ => Some(DeletedInfoton(infoton.systemFields.copy(path = path, lastModified = lastModified,
                dc = defaultDC, indexTime = None))))
            case None => ensurePrevNone(prevUUID)(None)
          }
        case _ => throw new NotImplementedError(s"no impl for [ $cmd ]")
      }
    }

    /**
      * @param prev optional previous infoton
      * @param cmds commands to apply on prev
      * @param evictionsAcc evictions accumulator
      * @return a tuple of the merged infoton and:
      *         list of either:
      *         - eviction reason with optional TID
      *         - TID
      *
      *         note that the returned list may be smaller than the number of commands.
      *         a command which went smoothly and is not tracked,
      *         won't contribute any element to this list.
      */
    @tailrec
    def merge_recurse(
                       prev: Option[Infoton],
                       cmds: Seq[SingleCommand],
                       evictionsAcc: List[Either[(String, Option[String]), String]] = Nil
                     ): (Option[Infoton], List[Either[(String, Option[String]), String]]) = {

      if (cmds.isEmpty) None -> evictionsAcc
      else {
        val (merged, eviction) = do_merge(prev, cmds.head)
        val tid = cmds.head.trackingID
        val evAcc = eviction.fold(tid.fold(evictionsAcc)(Right(_) :: evictionsAcc))(e => Left((e, tid)) :: evictionsAcc)
        if (cmds.size == 1)
          merged -> evAcc
        else
          merge_recurse(merged, cmds.tail, evAcc)
      }
    }

    val lastModifiedBy = (SortedSet[String](cmds.map(_.lastModifiedBy):_*)).mkString(",")

    val (merged, evictionsAndTIDs) = merge_recurse(baseInfoton, cmds)

    val (evictions, trackingIds) = cmwell.util.collections.partitionWith(evictionsAndTIDs)(identity)

    merged match {
      case Some(i) if !baseInfoton.exists(_.isSameAs(i)) =>
        val (infoton, extraData) = baseInfoton.fold(i.copyInfoton(i.systemFields.copy(lastModifiedBy = lastModifiedBy)) -> Option.empty[String])
        { j =>
          if (j.systemFields.lastModified.getMillis < i.systemFields.lastModified.getMillis)
            i.copyInfoton(i.systemFields.copy(lastModifiedBy = lastModifiedBy)) -> None
          else {
            logger.info(s"PlusDebug: There was an infoton [$j] in the system that is not the same as the merged one [$i] but has earlier lastModified. " +
              s"Adding 1 milli")
            val newLastModified = new DateTime(j.systemFields.lastModified.getMillis + 1L, DateTimeZone.UTC)
            i.copyInfoton(i.systemFields.copy(lastModified = newLastModified, lastModifiedBy = lastModifiedBy)) -> Some(newLastModified.getMillis.toString)
          }
        }
        RealUpdate(infoton, trackingIds, evictions, extraData)
      case Some(i) if baseInfoton.exists(bi => bi.isSameAs(i)
        && bi.systemFields.indexTime.isEmpty
        && new DateTime(bi.systemFields.lastModified, DateTimeZone.UTC) == new DateTime(cmds.last.lastModified, DateTimeZone.UTC)) =>
        //We are also testing for indexTime in order to handle BG recovery mode.
        //The merge process sets the last modified to be as the base one (in case they are the same).
        //If the merged infoton is the same as the the base one but the "should be" lastModified is different it means it's a null update
        //and not a replay after crash (it happens a lot with parents in clustered env.). This is the reason the the last command modified check
        logger.warn(s"Merged infoton [$i] is the same as the base infoton [${baseInfoton.get}] but the base infoton doesn't have index time!")
        RealUpdate(i.copyInfoton(i.systemFields.copy(lastModified = baseInfoton.get.systemFields.lastModified,
          lastModifiedBy = baseInfoton.get.systemFields.lastModifiedBy)), trackingIds, evictions, extra = None)
      case _ =>
        NullUpdate(baseInfoton.fold(cmds.head.path)(_.systemFields.path), trackingIds, evictions, extra = None)
    }
  }
}
