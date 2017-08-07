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


package cmwell.ws.adt

import cmwell.fts.{ MultiFieldFilter, SingleFieldFilter, FieldFilter }
import cmwell.ws.Settings
import cmwell.ws.qp.Encoder
import cmwell.util.collections._
import org.apache.poi.hssf.record.cf.Threshold
import wsutil._
import scala.util.Try
import scala.language.reflectiveCalls

sealed trait ConsumeState {
  def from: Long
  def path: Option[String]
  def withHistory: Boolean
  def withDeleted: Boolean
  def withDescendants: Boolean
  def qpOpt: Option[FieldFilter]
}

case class SortedConsumeState(from: Long, path: Option[String], withHistory: Boolean, withDeleted: Boolean, withDescendants: Boolean, qpOpt: Option[FieldFilter]) extends ConsumeState {
  def asBulk(threshold: Long = Settings.consumeBulkThreshold) = BulkConsumeState(from, None, path, withHistory, withDeleted, withDescendants, threshold, qpOpt)
}
object SortedConsumeState {
  implicit val parser = ConsumeState.parser.sortedConsumeState
}

case class BulkConsumeState(from: Long, to: Option[Long], path: Option[String], withHistory: Boolean, withDeleted: Boolean, withDescendants: Boolean, threshold: Long, qpOpt: Option[FieldFilter]) extends ConsumeState {
  def asRegular = SortedConsumeState(from, path, withHistory, withDeleted, withDescendants, qpOpt)
}
object BulkConsumeState {
  implicit val parser = ConsumeState.parser.bulkConsumeState
}

object ConsumeState {

  def formatRange(cs: ConsumeState): String = cs match {
    case BulkConsumeState(from, Some(to), _, _, _, _, _, _) => s"$from,$to"
    case _ => cs.from.toString
  }

  def encode(cs: ConsumeState): String = {
    val bare = format(cs)
    val bArr = cmwell.util.string.Zip.compress(bare)
    cmwell.util.string.Base64.encodeBase64URLSafeString(bArr)
  }

  def format(cs: ConsumeState): String = {
    val rangeStr = formatRange(cs)
    val pathStr = cs.path.fold("")("|" + _.toString)
    val hStr = if (cs.withHistory) "|h" else ""
    val dStr = if (cs.withDeleted) "|d" else ""
    val rStr = if (cs.withDescendants) "|r" else ""
    val thresholdStr = cs match {
      case BulkConsumeState(_, _, _, _, _, _, threshold, _) => s"|$threshold"
      case _ => ""
    }
    val qpStr = cs.qpOpt.fold("")(ff => "|" + Encoder.encodeFieldFilter(ff))
    rangeStr + pathStr + hStr + dStr + rStr + thresholdStr + qpStr
  }

  def decode[T <: ConsumeState : parser.Parser](base64: String): Try[T] = {
    if (base64.isEmpty) scala.util.Failure(new IllegalArgumentException("position cannot be empty"))
    else Try {
      val bArr = cmwell.util.string.Base64.decodeBase64(base64)
      cmwell.util.string.Zip.decompress(bArr)
    }.flatMap(parse[T])
  }

  def parse[T <: ConsumeState : parser.Parser](s: String): Try[T] = {
    import parser._
    val p: Parser[T] = implicitly[parser.Parser[T]]
    parser.parseAll(p, s) match {
      case NoSuccess(msg, _) => scala.util.Failure(new IllegalArgumentException(msg))
      case Success(res, _) => res.qpOpt.filter(fieldFilterHasKey("system.indexTime")).fold[Try[T]](scala.util.Success(res)){ _ =>
        scala.util.Failure(new IllegalArgumentException("consume API's qp must not contain \"system.indexTime\""))
      }
    }
  }

  def fieldFilterHasKey(key: String)(fieldFilter: FieldFilter): Boolean = fieldFilter match {
    case SingleFieldFilter(_,_,fieldName,_) => fieldName == key
    case MultiFieldFilter(_,fieldFilters) => fieldFilters.exists(fieldFilterHasKey(key))
  }

  val parser = new cmwell.ws.util.FieldFilterParser {

    object Inner {

      import scala.util.{ Failure => F, Success => S }

      private[this] def extractRawKeyAsFieldName(rff: RawFieldFilter): Try[FieldFilter] = rff match {
        case RawMultiFieldFilter(fo, rffs) => Try.traverse(rffs)(extractRawKeyAsFieldName).map(MultiFieldFilter(fo, _))
        case RawSingleFieldFilter(fo, vo, Right(dfk), v) => S(SingleFieldFilter(fo, vo, dfk.internalKey, v))
        case UnevaluatedQuadFilter(_,_,alias) => F {
          new IllegalArgumentException(s"supplied fields must be direct. system.quad with alias[$alias] is not direct and needs to be resolved (use fully qualified URI instead).")
        }
        case RawSingleFieldFilter(fo, vo, Left(rfk), v) => F {
          new IllegalArgumentException(s"supplied fields must be direct. ${rfk.externalKey} needs to be resolved.")
        }
      }

      private[this] def charBoolean(c: Char): Parser[Boolean] = opt(s"|$c") ^^ {
        _.isDefined
      }

      val long: Parser[Long] = wholeNumber ^^ {
        _.toLong
      }
      val range /*: Parser[(Long,Option[Long])]*/ = long ~ ("," ~> long).?
      val path: Parser[Option[String]] = opt("|" ~> "/[^|$]*".r)
      val h: Parser[Boolean] = charBoolean('h')
      val d: Parser[Boolean] = charBoolean('d')
      val r: Parser[Boolean] = charBoolean('r')
      val threshold: Parser[Option[Long]] = opt("|" ~> long)
      val qp: Parser[Try[FieldFilter]] = "|" ~> unwrappedFieldFilters.^^(extractRawKeyAsFieldName)
      val ff: Parser[Option[FieldFilter]] = opt(qp ^? ({
        case S(fieldFilter) => fieldFilter
      }, f => f.failed.get.getMessage))

    }

    import Inner._

    val innerSortedConsumeState = long ~ path ~ h ~ d ~ r ~ ff ^^ {
      case from ~ pathOpt ~ history ~ deleted ~ recursive ~ qpOpt =>
        SortedConsumeState(from, pathOpt, history, deleted, recursive, qpOpt)
    }

    val innerBulkConsumeState = range ~ path ~ h ~ d ~ r ~ threshold ~ ff ^? {
      case from ~ toOpt ~ pathOpt ~ history ~ deleted ~ recursive ~ Some(thresholdConcrete) ~ qpOpt =>
        BulkConsumeState(from, toOpt, pathOpt, history, deleted, recursive, thresholdConcrete, qpOpt)
    }

    val sortedConsumeState = innerSortedConsumeState ||| innerBulkConsumeState.^^(_.asRegular)

    val bulkConsumeState = innerBulkConsumeState | innerSortedConsumeState.^^(_.asBulk(Settings.consumeBulkThreshold))
  }
}