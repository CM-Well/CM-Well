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
import java.util.concurrent.TimeoutException

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cmwell.domain._
import cmwell.formats.{FormatExtractor, Formatter}
import cmwell.fts._
import cmwell.tracking.PathStatus
import cmwell.util.collections._
import cmwell.util.concurrent.{FutureTimeout, SimpleScheduler, travset}
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.exceptions.{UnretrievableIdentifierException, UnsupportedURIException}
import cmwell.ws.Settings
import cmwell.ws.util.{ExpandGraphParser, FieldNameConverter, PathGraphExpansionParser}
import com.typesafe.scalalogging.LazyLogging
import controllers.SpaMissingException
import filters.Attrs
import ld.cmw.PassiveFieldTypesCache
import ld.exceptions.{BadFieldTypeException, ConflictingNsEntriesException, ServerComponentNotAvailableException, TooManyNsRequestsException}
import logic.CRUDServiceFS
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.ISODateTimeFormat
import play.api.http.{HttpChunk, HttpEntity}
import play.api.libs.json.Json
import play.api.mvc.Results._
import play.api.mvc.{Headers, Request, ResponseHeader, Result}
import play.utils.InvalidUriEncodingException

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

package object wsutil extends LazyLogging {

  val Uuid = "([a-f0-9]{32})".r
  val zeroTime = new DateTime(0L, DateTimeZone.UTC)
  lazy val dtf = ISODateTimeFormat.dateTime()

  /**
    * Normalize Path.
    *
    * {{{
    * # Scala REPL style
    * scala> wsutil.normalizePath("")
    * res0: String = /
    *
    * scala> wsutil.normalizePath("/")
    * res1: String = /
    *
    * scala> wsutil.normalizePath("/xyz/")
    * res2: String = /xyz
    *
    * scala> wsutil.normalizePath("xyz/")
    * res3: String = /xyz
    *
    * scala> wsutil.normalizePath("/x/yz/")
    * res4: String = /x/yz
    *
    * scala> wsutil.normalizePath("x/yz/")
    * res5: String = /x/yz
    *
    * scala> wsutil.normalizePath("/x//yz/")
    * res6: String = /x/yz
    *
    * scala> wsutil.normalizePath("x//yz/")
    * res7: String = /x/yz
    *
    * scala> wsutil.normalizePath("/x///yz/")
    * res8: String = /x/yz
    *
    * scala> wsutil.normalizePath("x///yz/")
    * res9: String = /x/yz
    *
    * scala> wsutil.normalizePath("/x/yz//////")
    * res10: String = /x/yz
    *
    * scala> wsutil.normalizePath("/////x/yz//////")
    * res11: String = /x/yz
    *
    * scala> wsutil.normalizePath("x/yz//////")
    * res12: String = /x/yz
    *
    * scala> wsutil.normalizePath("/////x////yz//////")
    * res13: String = /x/yz
    *
    * scala> wsutil.normalizePath("x////yz//////")
    * res14: String = /x/yz
    *
    * scala> wsutil.normalizePath("/////x////y/z//////")
    * res15: String = /x/y/z
    *
    * scala> wsutil.normalizePath("x////y/z//////")
    * res16: String = /x/y/z
    *
    * scala> wsutil.normalizePath("/xyz")
    * res17: String = /xyz
    *
    * scala> wsutil.normalizePath("xyz")
    * res18: String = /xyz
    *
    * scala> wsutil.normalizePath("/x/yz")
    * res19: String = /x/yz
    *
    * scala> wsutil.normalizePath("x/yz")
    * res20: String = /x/yz
    *
    * scala> wsutil.normalizePath("/x//yz")
    * res21: String = /x/yz
    *
    * scala> wsutil.normalizePath("x//yz")
    * res22: String = /x/yz
    *
    * scala> wsutil.normalizePath("/x///yz")
    * res23: String = /x/yz
    *
    * scala> wsutil.normalizePath("x///yz")
    * res24: String = /x/yz
    *
    * scala> wsutil.normalizePath("/////x/yz")
    * res25: String = /x/yz
    *
    * scala> wsutil.normalizePath("/////x////yz")
    * res26: String = /x/yz
    *
    * scala> wsutil.normalizePath("x////yz")
    * res27: String = /x/yz
    *
    * scala> wsutil.normalizePath("/////x////y/z")
    * res28: String = /x/y/z
    *
    * scala> wsutil.normalizePath("x////y/z")
    * res29: String = /x/y/z
    *
    * scala> wsutil.normalizePath("//x/y/z")
    * res30: String = /x/y/z
    * }}}
    *
    * OK, so this is an overkill. (thanks Mark...!!!)
    * original code was:
    *     if(path.length > 1 && path.charAt(path.length-1) == '/') path.dropRight(1) else path
    *
    * but it wasn't good enough, so we changed it to the following idiomatic code:
    *     if (path.forall(_ == '/')) "/"
    *     else if (path.length > 1 && path.last == '/') path.dropTrailingChars('/')
    *     else path
    *
    * but then, Mark said it can be optimized further.
    * now (after optimization challenge accepted),
    * we resulted with the following bloated code.
    * if you read whole this comment, please add at list one doctest above.
    */
  def normalizePath(path: String): String = {
    var i, j: Int = 0
    var k: Int = path.length
    var lastIsSlash: Boolean = false
    var last2AreSlash: Boolean = false
    var initialized: Boolean = false
    var starting: Boolean = true
    val pre: String = if (path.isEmpty || path.head != '/') "/" else ""
    var chr: Char = '\n'

    lazy val sb = new StringBuilder(path.length)

    while (i < path.length) {
      chr = path(i)
      if (chr == '/') {
        if (!starting) {
          if (initialized) {
            if (!lastIsSlash) {
              lastIsSlash = true
            }
          } else {
            if (lastIsSlash) {
              last2AreSlash = true
            } else {
              lastIsSlash = true
              k = i
            }
          }
        }
      } else {
        if (!starting) {
          if (initialized) {
            if (lastIsSlash) {
              sb += '/'
            }
            sb += chr
          } else {
            if (last2AreSlash) {
              sb ++= pre
              sb ++= path.substring(j, k + 1)
              sb += chr
              initialized = true
              last2AreSlash = false
            }
          }
        } else {
          starting = false
          if (i == 0) { //if path didn't start with a slash
            j = 0
          } else {
            j = i - 1
          }
        }
        lastIsSlash = false
      }
      i += 1
    }

    if (initialized) sb.mkString
    else if (lastIsSlash) pre + path.substring(j, k)
    else pre + path.substring(j, i)
  }

  implicit class StringExtensions(s: String) {
    def dropRightWhile(p: (Char) => Boolean): String = s.dropRight(s.reverseIterator.takeWhile(p).length)
    def dropTrailingChars(c: Char*): String = dropRightWhile(c.toSet[Char](_))
  }

  def overrideMimetype(default: String, req: Request[_]): (String, String) =
    overrideMimetype(default, req.getQueryString("override-mimetype"))

  def overrideMimetype(default: String, overrideMime: Option[String]): (String, String) = overrideMime match {
    case Some(mimetype) => (play.api.http.HeaderNames.CONTENT_TYPE, mimetype)
    case _              => (play.api.http.HeaderNames.CONTENT_TYPE, default)
  }

  def filterInfoton(f: SingleFieldFilter, i: Infoton): Boolean = {
    require(f.valueOperator == Contains || f.valueOperator == Equals, s"unsupported ValueOperator: ${f.valueOperator}")

    val valOp: (String, String) => Boolean = f.valueOperator match {
      case Contains =>
        (infotonValue, inputValue) =>
          infotonValue.contains(inputValue)
      case Equals =>
        (infotonValue, inputValue) =>
          infotonValue == inputValue
      case _ => ???
    }

    f.fieldOperator match {
      case Should | Must =>
        i.fields
          .flatMap(_.get(f.name).map(_.exists(fv => f.value.forall(v => valOp(fv.value.toString, v)))))
          .getOrElse(false)
      case MustNot =>
        i.fields
          .flatMap(_.get(f.name).map(_.forall(fv => !f.value.exists(v => valOp(fv.value.toString, v)))))
          .getOrElse(true)
    }
  }

  type RawField[Op <: FieldValeOperator] = (Op, Either[UnresolvedFieldKey, DirectFieldKey])
  sealed trait RawAggregationFilter
  case class RawStatsAggregationFilter(name: String = "Statistics Aggregation", field: RawField[FieldValeOperator])
      extends RawAggregationFilter

  case class RawTermAggregationFilter(name: String = "Term Aggregation",
                                      field: RawField[FieldValeOperator],
                                      size: Int = 10,
                                      subFilters: Seq[RawAggregationFilter] = Seq.empty)
      extends RawAggregationFilter

  case class RawHistogramAggregationFilter(name: String = "Histogram Aggregation",
                                           field: RawField[FieldValeOperator],
                                           interval: Int,
                                           minDocCount: Int,
                                           extMin: Option[Double],
                                           extMax: Option[Double],
                                           subFilters: Seq[RawAggregationFilter] = Seq.empty)
      extends RawAggregationFilter

  case class RawSignificantTermsAggregationFilter(name: String = "Signigicant Terms Aggregation",
                                                  field: RawField[FieldValeOperator],
                                                  backgroundTerm: Option[(String, String)],
                                                  minDocCount: Int,
                                                  size: Int,
                                                  subFilters: Seq[RawAggregationFilter] = Seq.empty)
      extends RawAggregationFilter

  case class RawCardinalityAggregationFilter(name: String,
                                             field: RawField[FieldValeOperator],
                                             precisionThreshold: Option[Long])
      extends RawAggregationFilter

  object RawAggregationFilter {
    private def uniq(fn: String, name: String) = {
      if (fn.length > 1 && fn.tail.head == '$') s"-${fn.head}- $name"
      else "-s- " + name
    }
    def eval(af: RawAggregationFilter,
             cache: PassiveFieldTypesCache,
             cmwellRDFHelper: CMWellRDFHelper,
             timeContext: Option[Long])(implicit ec: ExecutionContext): Future[List[AggregationFilter]] = af match {
      case RawStatsAggregationFilter(name, (op, fk)) =>
        FieldKey.eval(fk, cache, cmwellRDFHelper, timeContext).map { fns =>
          fns.view.map { fn =>
            val uname = {
              if (fns.size == 1) name
              else uniq(fn, name)
            }
            StatsAggregationFilter(uname, Field(op, fn))
          }.to(List)
        }
      case RawTermAggregationFilter(name, (op, fk), size, rawSubFilters) if rawSubFilters.nonEmpty => {
        val ff = FieldKey.eval(fk, cache, cmwellRDFHelper, timeContext)
        Future.traverse(rawSubFilters)(eval(_, cache, cmwellRDFHelper, timeContext)).flatMap { subFilters =>
          ff.map { fns =>
            fns.view.map { fn =>
              val uname = {
                if (fns.size == 1) name
                else uniq(fn, name)
              }
              TermAggregationFilter(uname, Field(op, fn), size, subFilters.flatten)
            }.to(List)
          }
        }
      }
      case RawTermAggregationFilter(name, (op, fk), size, rawSubFilters) if rawSubFilters.isEmpty =>
        FieldKey.eval(fk, cache, cmwellRDFHelper, timeContext).map { fns =>
          fns.view.map { fn =>
            val uname = {
              if (fns.size == 1) name
              else uniq(fn, name)
            }
            TermAggregationFilter(uname, Field(op, fn), size)
          }.to(List)
        }
      case RawHistogramAggregationFilter(name, (op, fk), interval, minDocCount, extMin, extMax, rawSubFilters)
          if rawSubFilters.nonEmpty => {
        val ff = FieldKey.eval(fk, cache, cmwellRDFHelper, timeContext)
        Future.traverse(rawSubFilters)(eval(_, cache, cmwellRDFHelper, timeContext)).flatMap { subFilters =>
          ff.map { fns =>
            fns.view.map { fn =>
              val uname = {
                if (fns.size == 1) name
                else uniq(fn, name)
              }
              HistogramAggregationFilter(uname,
                                         Field(op, fn),
                                         interval,
                                         minDocCount,
                                         extMin,
                                         extMax,
                                         subFilters.flatten)
            }.to(List)
          }
        }
      }
      case RawHistogramAggregationFilter(name, (op, fk), interval, minDocCount, extMin, extMax, rawSubFilters)
          if rawSubFilters.isEmpty =>
        FieldKey.eval(fk, cache, cmwellRDFHelper, timeContext).map { fns =>
          fns.view.map { fn =>
            val uname = {
              if (fns.size == 1) name
              else uniq(fn, name)
            }
            HistogramAggregationFilter(uname, Field(op, fn), interval, minDocCount, extMin, extMax)
          }.to(List)
        }
      case RawSignificantTermsAggregationFilter(name, (op, fk), None, minDocCount, size, rawSubFilters)
          if rawSubFilters.nonEmpty => {
        val ff = FieldKey.eval(fk, cache, cmwellRDFHelper, timeContext)
        Future.traverse(rawSubFilters)(eval(_, cache, cmwellRDFHelper, timeContext)).flatMap { subFilters =>
          ff.map { fns =>
            fns.view.map { fn =>
              val uname = {
                if (fns.size == 1) name
                else uniq(fn, name)
              }
              SignificantTermsAggregationFilter(uname, Field(op, fn), None, minDocCount, size, subFilters.flatten)
            }.to(List)
          }
        }
      }
      case RawSignificantTermsAggregationFilter(name, (op, fk), None, minDocCount, size, rawSubFilters)
          if rawSubFilters.isEmpty =>
        FieldKey.eval(fk, cache, cmwellRDFHelper, timeContext).map { fns =>
          fns.view.map { fn =>
            val uname = {
              if (fns.size == 1) name
              else uniq(fn, name)
            }
            SignificantTermsAggregationFilter(uname, Field(op, fn), None, minDocCount, size)
          }.to(List)
        }
      //TODO: backgroundTerms should also be unevaluated FieldKey. need to fix the parser.
      case RawSignificantTermsAggregationFilter(_, _, Some(_), _, _, _) => ???
      case RawCardinalityAggregationFilter(name, (op, fk), precisionThreshold) =>
        FieldKey.eval(fk, cache, cmwellRDFHelper, timeContext).map { fns =>
          fns.view.map { fn =>
            val uname = {
              if (fns.size == 1) name
              else uniq(fn, name)
            }
            CardinalityAggregationFilter(uname, Field(op, fn), precisionThreshold)
          }.to(List)
        }
    }
  }

  trait FieldPattern
  trait NsPattern

  case class HashedNsPattern(hash: String) extends NsPattern
  trait ResolvedNsPattern extends NsPattern {
    def resolve(cmwellRDFHelper: CMWellRDFHelper, timeContext: Option[Long])(
      implicit ec: ExecutionContext
    ): Future[String]
  }
  case class NsUriPattern(nsUri: String) extends ResolvedNsPattern {
    override def resolve(cmwellRDFHelper: CMWellRDFHelper, timeContext: Option[Long])(implicit ec: ExecutionContext) =
      cmwellRDFHelper.urlToHashAsync(nsUri, timeContext)
  }
  case class PrefixPattern(prefix: String) extends ResolvedNsPattern {
    override def resolve(cmwellRDFHelper: CMWellRDFHelper, timeContext: Option[Long])(implicit ec: ExecutionContext) =
      cmwellRDFHelper.getIdentifierForPrefixAsync(prefix, timeContext)
  }

  case object JokerPattern extends FieldPattern
  case class NsWildCard(nsPattern: NsPattern) extends FieldPattern
  case class FieldKeyPattern(fieldKey: Either[UnresolvedFieldKey, DirectFieldKey]) extends FieldPattern

  case class FilteredField[FP <: FieldPattern](fieldPattern: FP, rawFieldFilterOpt: Option[RawFieldFilter])
  case class LevelExpansion(filteredFields: List[FilteredField[FieldPattern]])

  sealed abstract class DirectedExpansion
  case class ExpandUp(filteredFields: List[FilteredField[FieldKeyPattern]]) extends DirectedExpansion
  case class ExpandIn(filteredFields: List[FilteredField[FieldPattern]]) extends DirectedExpansion

  case class PathExpansion(pathSegments: List[DirectedExpansion])
  case class PathsExpansion(paths: List[PathExpansion])

  //Some convenience methods & types
  def getByPath(protocol: String, path: String, crudServiceFS: CRUDServiceFS)(implicit ec: ExecutionContext): Future[Infoton] =
    crudServiceFS.irwService.readPathAsync(path, crudServiceFS.level).map(_.getOrElse(GhostInfoton.ghost(path, protocol)))
  type F[X] = (X, Option[List[RawFieldFilter]])
  type EFX = Either[F[Future[Infoton]], F[Infoton]]

  def filterByRawFieldFiltersTupled(
    cache: PassiveFieldTypesCache,
    cmwellRDFHelper: CMWellRDFHelper,
    timeContext: Option[Long]
  )(tuple: (Infoton, Option[List[RawFieldFilter]]))(implicit ec: ExecutionContext): Future[Boolean] = tuple match {
    case (i, None)          => Future.successful(true)
    case (i, Some(filters)) => filterByRawFieldFilters(cache, cmwellRDFHelper, timeContext)(i, filters)
  }

  def filterByRawFieldFilters(
    cache: PassiveFieldTypesCache,
    cmwellRDFHelper: CMWellRDFHelper,
    timeContext: Option[Long]
  )(infoton: Infoton, filters: List[RawFieldFilter])(implicit ec: ExecutionContext): Future[Boolean] = {

    val p = Promise[Boolean]()

    val futures = for {
      filter <- filters
      future = filterByRawFieldFilter(infoton, filter, cache, cmwellRDFHelper, timeContext)
    } yield
      future.andThen {
        case Success(true) if !p.isCompleted => p.trySuccess(true)
      }

    cmwell.util.concurrent.successes(futures).foreach {
      case Nil =>
        Future.traverse(futures)(_.failed).foreach { err =>
          p.tryFailure(new cmwell.util.exceptions.MultipleFailures(err))
        }
      case list =>
        if (!p.isCompleted) {
          p.trySuccess(list.exists(identity))
        }
    }

    p.future
  }

//  //if needed: TODO: IMPLEMENT!!!!!
//  def filterByRawFieldFilter(infoton: Infoton, filter: RawFieldFilter): Future[Boolean] = filter match {
//    case RawSingleFieldFilter(fo,vo,fk,v) => fk.internalKey.map{ internalFieldName =>
//      (fo,vo) match {
//        case (Must|Should,op) => infoton.fields.exists(_.exists {
//          case (fieldName,values) => fieldName == internalFieldName && v.fold(true){ value =>
//            op match {
//              case Equals => values.exists(_.toString == value)
//              case Contains => values.exists(_.toString.contains(value))
//            }
//          }
//        })
//      }
//    }
//  }

  def filterByRawFieldFilter(infoton: Infoton,
                             filter: RawFieldFilter,
                             cache: PassiveFieldTypesCache,
                             cmwellRDFHelper: CMWellRDFHelper,
                             timeContext: Option[Long])(implicit ec: ExecutionContext): Future[Boolean] =
    RawFieldFilter.eval(filter, cache, cmwellRDFHelper, timeContext).transform {
      case Success(ff)                        => Try(ff.filter(infoton).value)
      case Failure(_: NoSuchElementException) => Success(false)
      case failure                            => failure.asInstanceOf[Failure[Boolean]]
    }

  def expandIn(filteredFields: List[FilteredField[FieldPattern]],
               infotonsToExpand: Seq[Infoton],
               infotonsRetrievedCache: Map[String, Infoton],
               cmwellRDFHelper: CMWellRDFHelper,
               cache: PassiveFieldTypesCache,
               timeContext: Option[Long])(implicit ec: ExecutionContext): Future[(Seq[Infoton], Seq[Infoton])] = {
    val expansionFuncsFut = Future.traverse(filteredFields) {
      case FilteredField(JokerPattern, rffo) =>
        Future.successful({ (internalFieldName: String) =>
          true
        } -> rffo)
      case FilteredField(FieldKeyPattern(Right(dfk)), rffo) => Future.successful((dfk.internalKey == _, rffo))
      case FilteredField(FieldKeyPattern(Left(rfk)), rffo) =>
        FieldKey.resolve(rfk, cmwellRDFHelper, timeContext).map(fk => (fk.internalKey == _, rffo))
      case FilteredField(NsWildCard(HashedNsPattern(hash)), rffo) =>
        Future.successful({ (internalFieldName: String) =>
          internalFieldName.endsWith(s".$hash")
        } -> rffo)
      case FilteredField(NsWildCard(rnp: ResolvedNsPattern), rffo) =>
        rnp
          .resolve(cmwellRDFHelper, timeContext)
          .map(hash => { (internalFieldName: String) =>
            internalFieldName.endsWith(s".$hash")
          } -> rffo)
      case x @ FilteredField(_, _) => logger.error(s"Unexpected input. Received: $x"); ???
    }
    expansionFuncsFut.flatMap { funs =>
      // all the infotons' fields
      val fieldsMaps = infotonsToExpand.map(_.fields).collect { case Some(m) => m }

      // maps reduced into 1 aggregated fields `Map`
      val fieldsReduced = if (fieldsMaps.nonEmpty) fieldsMaps.reduce { (m1, m2) =>
        m1.foldLeft(m2) {
          case (acc, (fieldName, values)) if acc.contains(fieldName) =>
            acc.updated(fieldName, acc(fieldName).union(values))
          case (acc, fieldNameValsTuple) => acc + fieldNameValsTuple
        }
      } else Map.empty[String, Set[FieldValue]]

      // build a list that pairs cmwell paths to retrieval with raw field filters to be applied on
      val cmwPathFilterOptionPairs = fieldsReduced.view.flatMap {
        case (fieldName, values) =>
          funs.flatMap {
            case (func, _) if !func(fieldName) => Nil
            case (_, rffo) =>
              values.view.collect {
                case fr: FReference => (fr.getProtocol -> normalizePath(fr.getCmwellPath)) -> rffo
              }.to(List)
          }
      }.to(List)

      // value are `Option[List[...]]` because `None` means no filtering (pass all)
      // which is different from emptylist which means at least 1 filter should apply (i.e: block all)
      val pathToFiltersMap = cmwPathFilterOptionPairs.groupBy(_._1).mapValues(ps => Option.sequence(ps.unzip._2)).toSeq

      // get infotons from either `infotonsRetrievedCache` or from cassandra, and pair with filters option
      val (l, r) = partitionWith(pathToFiltersMap) {
        case ((protocol,path), rffso) => {
          infotonsRetrievedCache.get(path).fold[EFX](Left(getByPath(protocol, path, cmwellRDFHelper.crudServiceFS) -> rffso)) {
            i =>
              Right(i -> rffso)
          }
        }
      }

      // after retrieval, filter out what is not needed
      val lInfotonsFut = Future
        .traverse(l) {
          case (fi, None) => fi.map(Some.apply)
          case (fi, Some(filters)) =>
            fi.flatMap {
              case i =>
                filterByRawFieldFilters(cache, cmwellRDFHelper, timeContext)(i, filters).map {
                  case true  => Some(i)
                  case false => None
                }
            }
        }
        .map(_.collect { case Some(i) => i })

      // also filter the infotons retrieved from "cache"
      val rInfotonsFut = Future
        .traverse(r) {
          case t @ (i, _) =>
            filterByRawFieldFiltersTupled(cache, cmwellRDFHelper, timeContext)(t).map {
              case true  => Some(i)
              case false => None
            }
        }
        .map(_.collect { case Some(i) => i })

      // combine results
      lInfotonsFut.zip(rInfotonsFut)
    }
  }

  def expandUp(filteredFields: List[FilteredField[FieldKeyPattern]],
               population: Seq[Infoton],
               cmwellRDFHelper: CMWellRDFHelper,
               cache: Map[String, Infoton],
               typesCache: PassiveFieldTypesCache,
               infotonsSample: Seq[Infoton],
               pattern: String,
               chunkSize: Int,
               timeContext: Option[Long])(implicit ec: ExecutionContext): Future[(Seq[Infoton], Seq[Infoton])] = {

    def mkFieldFilters2(ff: FilteredField[FieldKeyPattern],
                        outerFieldOperator: FieldOperator,
                        pathsAndProtocols: List[(String,String)]): Future[FieldFilter] = {

      val FilteredField(fkp, rffo) = ff
      val internalFieldNameFut = fkp match {
        case FieldKeyPattern(Right(dfk)) => Future.successful(dfk.internalKey)
        case FieldKeyPattern(Left(unfk)) => FieldKey.resolve(unfk, cmwellRDFHelper, timeContext).map(_.internalKey)
      }
      val filterFut: Future[FieldFilter] = internalFieldNameFut.map { internalFieldName =>
        if(pathsAndProtocols.isEmpty) {
          val sb = new StringBuilder
          sb ++= "empty urls in expandUp("
          sb ++= filteredFields.toString
          sb ++= ",population[size="
          sb ++= population.size.toString
          sb ++= "],cache[size="
          sb ++= cache.size.toString
          sb ++= "])\nfor pattern: "
          sb ++= pattern
          sb ++= "\nand infotons.take(3) = "
          sb += '['
          infotonsSample.headOption.foreach { i =>
            sb ++= i.toString
            infotonsSample.tail.foreach { j =>
              sb += ','
              sb ++= j.toString
            }
          }
          sb += ']'
          throw new IllegalStateException(sb.result())
        } else {
          val shoulds = pathsAndProtocols.flatMap { case (path,protocol) => pathToUris(protocol, path) }.
            map(url => SingleFieldFilter(Should, Equals, internalFieldName, Some(url)))
          MultiFieldFilter(rffo.fold[FieldOperator](outerFieldOperator)(_ => Must), shoulds)
        }
      }
      rffo.fold[Future[FieldFilter]](filterFut) { rawFilter =>
        RawFieldFilter.eval(rawFilter, typesCache, cmwellRDFHelper, timeContext).flatMap { filter =>
          filterFut.map(ff => MultiFieldFilter(outerFieldOperator, List(ff, filter)))
        }
      }
    }

    Future
      .traverse(population.grouped(chunkSize)) { infotonsChunk =>
        val pathsAndProtocols: List[(String,String)] = infotonsChunk.view.map { i =>
          i.systemFields.path -> i.systemFields.protocol
        }.to(List)

        val fieldFilterFut = filteredFields match {
          case Nil =>
            val i = infotonsSample.mkString("[", ",", "]")
            val c = cache.size
            val p = population.size
            throw new IllegalStateException(s"expandUp($filteredFields,population[size=$p],cache[size=$c])\nfor pattern: $pattern\nand infotons.take(3) = $i")
          case ff :: Nil => mkFieldFilters2(ff, Must, pathsAndProtocols)
          case _         => Future.traverse(filteredFields)(mkFieldFilters2(_, Should, pathsAndProtocols)).map(MultiFieldFilter(Must, _))
        }
        fieldFilterFut.transformWith {
          case Failure(_: NoSuchElementException) => Future.successful(Nil -> Nil)
          case Success(ffs) =>
            cmwellRDFHelper.crudServiceFS
              .thinSearch(None,
                          Some(ffs),
                          None,
                          PaginationParams(0, Settings.expansionLimit),
                          withHistory = false,
                          NullSortParam,
                          debugInfo = false,
                          withDeleted = false)
              .flatMap(sr => {
                val (inCache, toFetch) = sr.thinResults.partition(i => cache.contains(i.path))
                cmwellRDFHelper.crudServiceFS.getInfotonsByUuidAsync(toFetch.map(_.uuid)).map {
                  _ -> inCache.map(i => cache(i.path))
                }
              })
          case anotherFailures => Future.fromTry(anotherFailures.asInstanceOf[Try[(Seq[Infoton], Seq[Infoton])]])
        }
      }
      .map {
        case tuples if tuples.isEmpty => (Seq.empty, Seq.empty)
        case tuples =>
          tuples.reduce[(Seq[Infoton], Seq[Infoton])] {
            case ((la, ra), (lb, rb)) => (la ++ lb) -> (ra ++ rb)
          }
      }
  }

  def deepExpandGraph(xgPattern: String,
                      infotons: Seq[Infoton],
                      cmwellRDFHelper: CMWellRDFHelper,
                      cache: PassiveFieldTypesCache,
                      timeContext: Option[Long])(implicit ec: ExecutionContext): Future[(Boolean, Seq[Infoton])] = {

    def expandDeeper(expanders: List[LevelExpansion],
                     infotonsToExpand: Seq[Infoton],
                     infotonsRetrievedCache: Map[String, Infoton]): Future[(Boolean, Seq[Infoton])] = expanders match {
      case Nil => Future.successful(true -> infotonsRetrievedCache.values.filterNot(_.isInstanceOf[GhostInfoton]).toSeq)
      case f :: fs if infotonsRetrievedCache.size > Settings.expansionLimit =>
        Future.successful(false -> infotonsRetrievedCache.values.toSeq)
      case f :: fs => {
        expandIn(f.filteredFields, infotonsToExpand, infotonsRetrievedCache, cmwellRDFHelper, cache, timeContext)
          .flatMap {
            case (lInfotons, rInfotons) =>
              expandDeeper(
                fs,
                lInfotons ++ rInfotons,
                infotonsRetrievedCache ++ lInfotons.view.map(i => i.systemFields.path -> i).toMap
              )
          }
      }
    }

    val t = ExpandGraphParser.getLevelsExpansionFunctions(xgPattern).map { fs =>
      expandDeeper(fs, infotons, infotons.map(i => i.systemFields.path -> i).toMap)
    }

    t match {
      case Success(future) => future
      case Failure(error)  => Future.failed(error)
    }
  }

  def gqpFilter(gqpPattern: String,
                infotons: Seq[Infoton],
                cmwellRDFHelper: CMWellRDFHelper,
                typesCache: PassiveFieldTypesCache,
                chunkSize: Int,
                timeContext: Option[Long])(implicit ec: ExecutionContext): Future[Seq[Infoton]] = {

    logger.trace(s"gqpFilter with infotons: [${infotons.map(_.systemFields.path).mkString(", ")}]")

    def filterByDirectedExpansion(
      dexp: DirectedExpansion
    )(iv: (Infoton, Vector[Infoton])): Future[(Infoton, Vector[Infoton])] = {
      logger.trace(
        s"filterByDirectedExpansion($dexp): with original[${iv._1.systemFields.path}] and current-pop[${iv._2.map(_.systemFields.path).mkString(", ")}]"
      )
      dexp match {
        case ExpandIn(filteredFields) =>
          expandIn(
            filteredFields,
            iv._2,
            iv._2.view.map(i => i.systemFields.path -> i).toMap,
            cmwellRDFHelper,
            typesCache,
            timeContext
          ).map {
            case (l, r) =>
              iv._1 -> {
                val rv = l.toVector ++ r
                logger.trace(
                  s"filterByDirectedExpansion($dexp): after expandIn($filteredFields), finished with result[${rv.map(_.systemFields.path).mkString(", ")}]"
                )
                rv
              }
          }
        case ExpandUp(filteredFields) =>
          expandUp(
            filteredFields,
            iv._2,
            cmwellRDFHelper,
            iv._2.view.map(i => i.systemFields.path -> i).toMap,
            typesCache,
            infotons.take(3),
            gqpPattern,
            chunkSize,
            timeContext
          ).map {
            case (l, r) =>
              iv._1 -> {
                val rv = l.toVector ++ r
                logger.trace(
                  s"filterByDirectedExpansion($dexp): after expandIn($filteredFields), finished with result[${rv.map(_.systemFields.path).mkString(", ")}]"
                )
                rv
              }
          }
      }
    }

    def nextFilteringHop(dexp: DirectedExpansion,
                         dexps: List[DirectedExpansion],
                         survivors: Vector[(Infoton, Vector[Infoton])]): Future[Vector[Infoton]] = {
      Future
        .traverse(survivors)(filterByDirectedExpansion(dexp))
        .flatMap { s =>
          val newSurvivors = s.filter(_._2.nonEmpty)
          if (newSurvivors.isEmpty || dexps.isEmpty) Future.successful(newSurvivors.map(_._1))
          else nextFilteringHop(dexps.head, dexps.tail, newSurvivors)
        }
        .andThen {
          case Success(is) =>
            logger.trace(s"nextFilteringHop: finished with survivors[${is.map(_.systemFields.path).mkString(", ")}]")
          case Failure(ex) => logger.error(s"nextFilteringHop($dexp,$dexps,$survivors)", ex)
        }
    }

    if (gqpPattern.isEmpty) Future.successful(infotons)
    else
      PathGraphExpansionParser.getGQPs(gqpPattern).map {
        case PathsExpansion(paths) =>
          paths.foldLeft(Future.successful(Vector.empty[Infoton])) {
            case (vecFut, PathExpansion(segments)) if segments.isEmpty => vecFut
            case (vecFut, PathExpansion(segments)) =>
              vecFut.flatMap { vec =>
                val candidates: Vector[(Infoton, Vector[Infoton])] = infotons.view.collect {
                  case i if !vec.contains(i) =>
                    i -> Vector(i)
                }.to(Vector)
                logger.trace(s"appending: [${segments.mkString(", ")}] to vec[${vec
                  .map(_.systemFields.path)
                  .mkString(", ")}] with candidates[${candidates.map(_._1.systemFields.path).mkString(", ")}]")
                nextFilteringHop(segments.head, segments.tail, candidates).map(_ ++: vec)
              }
          }
      } match {
        case Success(future) => future
        case Failure(error)  => Future.failed(error)
      }
  }

  def pathExpansionParser(ygPattern: String,
                          infotons: Seq[Infoton],
                          chunkSize: Int,
                          cmwellRDFHelper: CMWellRDFHelper,
                          typesCache: PassiveFieldTypesCache,
                          timeContext: Option[Long])(implicit ec: ExecutionContext): Future[(Boolean, Seq[Infoton])] = {

    type Expander = (DirectedExpansion, List[DirectedExpansion], Seq[Infoton])

    /**
      *
      * @param infoCachedOrRetrieved
      * @param tail
      * @return
      */
    def adjustResults(infoCachedOrRetrieved: (Seq[Infoton], Seq[Infoton]),
                      tail: List[DirectedExpansion]): (Option[Expander], Seq[Infoton]) = {
      val (retrieved, cached) = infoCachedOrRetrieved
      tail.headOption.map[Expander](h => (h, tail.tail, cached ++ retrieved)) -> retrieved
    }

    def expandDeeper(expanders: List[Option[Expander]],
                     cache: Map[String, Infoton]): Future[(Boolean, Seq[Infoton])] = {
      if (expanders.forall(_.isEmpty))
        Future.successful(true -> cache.values.filterNot(_.isInstanceOf[GhostInfoton]).toSeq)
      else if (cache.count(!_._2.isInstanceOf[GhostInfoton]) > Settings.expansionLimit)
        Future.successful(false -> cache.values.toSeq)
      else
        Future
          .traverse(expanders) {
            case None => Future.successful(None -> Seq.empty)
            case Some((ExpandIn(ffs), tail, population)) =>
              expandIn(ffs, population, cache, cmwellRDFHelper, typesCache, timeContext).map(
                newPop => adjustResults(newPop, tail)
              )
            case Some((ExpandUp(ffs), tail, population)) =>
              expandUp(ffs,
                       population,
                       cmwellRDFHelper,
                       cache,
                       typesCache,
                       infotons.take(3),
                       ygPattern,
                       chunkSize,
                       timeContext).map(newPop => adjustResults(newPop, tail))
          }
          .flatMap { expanderRetrievedInfotonPairs =>
            val (newExpanders, retrievedInfotons) = expanderRetrievedInfotonPairs.unzip
            val newCache = retrievedInfotons.foldLeft(cache) {
              case (accache, additions) => accache ++ additions.map(i => i.systemFields.path -> i)
            }
            expandDeeper(newExpanders, newCache)
          }
    }

    PathGraphExpansionParser.getPathsExpansionFunctions(ygPattern).map {
      case PathsExpansion(paths) => {
        val perPathHeadTail = paths.map {
          case PathExpansion(segments) =>
            segments.headOption.map { head =>
              (head, segments.tail, infotons)
            }
        }
        expandDeeper(perPathHeadTail, infotons.view.map(i => i.systemFields.path -> i).toMap)
      }
    } match {
      case Success(future) => future
      case Failure(error)  => Future.failed(error)
    }
  }

  def isPathADomain(path: String): Boolean = path.dropWhile(_ == '/').takeWhile(_ != '/').contains('.')

  def pathToUris(protocol: String, path: String): Seq[String] = {
    if (isPathADomain(path))
      List(s"http:/$path", s"https:/$path") //TODO When it is safe to undo WombatUpdate2, return this: s"$protocol:/$path"
    else List(s"cmwell:/$path")
  }

  def exceptionToResponse(throwable: Throwable): Result = {
    val (status, eHandler): (Status, Throwable => String) = throwable match {
      case _: ServerComponentNotAvailableException                      => ServiceUnavailable -> { _.getMessage }
      case _: TooManyNsRequestsException                                => ServiceUnavailable -> { _.getMessage }
      case _: ConflictingNsEntriesException                             => ExpectationFailed -> { _.getMessage }
      case _: TimeoutException                                          => ServiceUnavailable -> { _.getMessage }
      case _: FutureTimeout[_]                                          =>
        ServiceUnavailable -> { _ => "An internal timeout happened. Please try again later." }
      case _: SpaMissingException                                       => ServiceUnavailable -> { _.getMessage }
      case _: UnretrievableIdentifierException                          => UnprocessableEntity -> { _.getMessage }
      case _: security.UnauthorizedException                            => Forbidden -> { _.getMessage }
      case _: org.apache.jena.shared.JenaException                      => BadRequest -> { _.getMessage }
      case _: cmwell.web.ld.exceptions.ParsingException                 => BadRequest -> { _.getMessage }
      case _: NumberFormatException                                     =>
        BadRequest -> { e => s"Bad number format: ${e.getMessage}" }
      case _: IllegalArgumentException                                  => BadRequest -> { _.getMessage }
      case _: UnsupportedURIException                                   => BadRequest -> { _.getMessage }
      case _: InvalidUriEncodingException                               => BadRequest -> { _.getMessage }
      case _: BadFieldTypeException                                     => BadRequest -> { _.getMessage }
      case _: com.datastax.driver.core.exceptions.InvalidQueryException => ExpectationFailed -> { _.getMessage }
      case _: com.datastax.driver.core.exceptions.DriverException       => ServiceUnavailable -> { _.getMessage }
      case e: org.elasticsearch.transport.RemoteTransportException
          if e.getCause.isInstanceOf[org.elasticsearch.action.search.ReduceSearchPhaseException] =>
        ServiceUnavailable -> { _.getCause.getMessage }
      case e: org.elasticsearch.transport.RemoteTransportException
          if e.getCause.isInstanceOf[org.elasticsearch.action.search.SearchPhaseExecutionException] =>
        BadRequest -> { _.getCause.getMessage }
      case e: Throwable => {
        logger.error("unexpected error occurred", e)
        InternalServerError -> { _.getMessage }
      }
    }
    status(Json.obj("success" -> false, "error" -> eHandler(throwable)))
  }

  def extractFieldsMask(req: Request[_],
                        cache: PassiveFieldTypesCache,
                        cmwellRDFHelper: CMWellRDFHelper,
                        timeContext: Option[Long])(implicit ec: ExecutionContext): Future[Set[String]] = {
    extractFieldsMask(req.getQueryString("fields"), cache, cmwellRDFHelper, timeContext)
  }

  def extractFieldsMask(fieldsOpt: Option[String],
                        cache: PassiveFieldTypesCache,
                        cmwellRDFHelper: CMWellRDFHelper,
                        timeContext: Option[Long])(implicit ec: ExecutionContext): Future[Set[String]] = {
    fieldsOpt.map(FieldNameConverter.toActualFieldNames) match {
      case Some(Success(fields)) =>
        travset(fields) {
          case Right(x) => Future.successful(x.internalKey)
          case Left(u)  => FieldKey.resolve(u, cmwellRDFHelper, timeContext).map(_.internalKey)
        }
      case Some(Failure(e)) => Future.failed(e)
      case None             => Future.successful(Set.empty[String])
    }
  }

  val noDataFormat: String => Boolean = Set("tsv", "tab", "text", "path").apply

  def extractInferredFormatWithData(req: Request[_], defaultFormat: String = "json"): (String, Boolean) = {
    val format = req.getQueryString("format").getOrElse(defaultFormat)
    val withData = req.getQueryString("with-data").fold(!noDataFormat(format))(_ != "false")
    format -> withData
  }

  val endln = ByteString(cmwell.util.os.Props.endln)

  def guardHangingFutureByExpandingToSource[T, U, V](futureThatMayHang: Future[T],
                                                     initialGraceTime: FiniteDuration,
                                                     injectInterval: FiniteDuration)(
    backOnTime: T => V,
    prependInjections: () => U,
    injectOriginalFutureWith: T => U,
    continueWithSource: Source[U, NotUsed] => V
  )(implicit ec: ExecutionContext): Future[V] = {
    val p1 = Promise[V]()
    p1.tryCompleteWith(futureThatMayHang.map(backOnTime))

    SimpleScheduler.schedule(initialGraceTime) {
      if (!futureThatMayHang.isCompleted) {
        //unfoldAsync(s: S)(f: S => Future[Option[(S,E)]]): Source[E,_]
        //
        // s: initial state
        // f: function that takes the current state to:
        //    a Future of:
        //      None (if it's the end of Source's output)
        //      Some of:
        //        S: next state to operate on
        //        E: next Element to output
        val source = Source.unfoldAsync(false) { isCompleted =>
          {
            if (isCompleted) Future.successful(None)
            else {
              val p2 = Promise[Option[(Boolean, U)]]()
              p2.tryCompleteWith(futureThatMayHang.map(t => Some(true -> injectOriginalFutureWith(t))))
              SimpleScheduler.schedule(injectInterval) {
                p2.trySuccess(Some(false -> prependInjections()))
              }
              p2.future
            }
          }
        }
        p1.trySuccess(continueWithSource(source))
      }
    }

    p1.future
  }

  /**false
    * @param response The original response to be wrapped
    * @return a new response that will drip "\n" drops every 3 seconds,
    *         UNLESS the original Future[Result] is completed within the first 3 seconds.
    *         One can use the latter case for validation before long execution, and return 400 or so,
    *         prior to the decision of Chunked 200 OK.
    */
  def keepAliveByDrippingNewlines(
    response: Future[Result],
    extraHeaders: Seq[(String, String)] = Nil,
    extraTrailers: Future[Seq[(String, String)]] = Future.successful(Nil),
    expandedResponseContentType: Option[String] = None
  )(implicit ec: ExecutionContext): Future[Result] = {
    val isEmptySuccessfulFuture = extraTrailers.value.fold(false)(_.fold(err => false, _.isEmpty))
    if (isEmptySuccessfulFuture)
      guardHangingFutureByExpandingToSource[Result, Source[ByteString, _], Result](response, 7.seconds, 3.seconds)(
        identity,
        () => Source.single(endln),
        _.body.dataStream,
        source => {
          val r = Ok.chunked(source.flatMapConcat(x => x)).withHeaders(extraHeaders: _*)
          expandedResponseContentType.fold(r)(r.as)
        }
      )
    else {
      val prependInjections = () => Source.single(HttpChunk.Chunk(endln))
      val injectOriginalFutureWith: Result => Source[HttpChunk, _] = _.body.dataStream
        .map(HttpChunk.Chunk)
        .concat(
          Source
            .fromFuture(extraTrailers)
            .filter(_.nonEmpty)
            .map(nonEmptyTrailers => HttpChunk.LastChunk(new Headers(nonEmptyTrailers)))
        )
      val continueWithSource: Source[Source[HttpChunk, _], NotUsed] => Result = source => {
        Result(
          header = ResponseHeader(200),
          body = HttpEntity.Chunked(
            source.flatMapConcat(x => x),
            expandedResponseContentType
          )
        ).withHeaders(extraHeaders: _*)
      }
      guardHangingFutureByExpandingToSource[Result, Source[HttpChunk, _], Result](response, 7.seconds, 3.seconds)(
        identity,
        prependInjections,
        injectOriginalFutureWith,
        continueWithSource
      )
    }
  }

  def formattableToByteString(formatter: Formatter)(i: Formattable) = {
    val body = formatter.render(i)
    if (body.endsWith(cmwell.util.os.Props.endln)) ByteString(body, "utf-8")
    else ByteString(body, "utf-8") ++ endln
  }

  // format: off
  // scalastyle:off
  def metaOpRegex(metaOpType: String): String = {

    /* ******************* *
     *  REGEX explanation: *
     * ******************* *

(                                                                          1.  start a group to capture protocol + optional domain
 (cmwell:/)                                                                2.  if protocol is `cmwell:` no need to specify domain
           |                                                               3.  OR
            (https?://                                                     4.  if protocol is `http:` or `https:` (we need a domain)
                      (                                                    5.  start a group to capture multiple domain parts that end with a '.'
                       (?!-)                                               6.  domain part cannot begin with hyphen '-'
                            [A-Za-z0-9-]{1,63}                             7.  domain part is composed of letters, digits & hyphen and can be in size of at least 1 but less than 64
                                              (?<!-)                       8.  domain part cannot end with an hyphen (look back)
                                                    \.)*                   9.  all parts except the last must end with a dot '.'
                      (                                                    10. start same regex as before for the last domain part which must not end with a dot '.'
                       (?!-)                                               11. same as 6
                            [A-Za-z0-9-]{1,63}                             12. same as 7
                                              (?<!-)                       13. same as 8
                                                    )                      14. end domain group
                                                     (:\d+)?               15. optional port
                                                            )              16. end http/https with domain and optional port group
                                                             )             17. end cmwell OR http group
                                                              (/meta/$op#) 18. path must be `/meta/sys#` / `/meta/ns#` / etc'...
     */
    """((cmwell:/)|(http://((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)*((?!-)[A-Za-z0-9-]{1,63}(?<!-))(:\d+)?))(/meta/""" + metaOpType + """#)"""
  }
  // scalastyle:on
  // format: on

  def pathStatusAsInfoton(ps: PathStatus): Infoton = {
    val PathStatus(path, status) = ps
    val fields: Option[Map[String, Set[FieldValue]]] = Some(Map("trackingStatus" -> Set(FString(status.toString))))
    VirtualInfoton(ObjectInfoton(SystemFields(path, zeroTime, "VirtualInfoton", Settings.dataCenter, None, "", "http"), fields = fields))
  }

  def getFormatter(request: Request[_],
                   formatterManager: FormatterManager,
                   defaultFormat: String,
                   withoutMeta: Boolean = false): Formatter =
    request.getQueryString("format").getOrElse(defaultFormat) match {
      case FormatExtractor(formatType) =>
        formatterManager.getFormatter(
          format = formatType,
          timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp),
          host = request.host,
          uri = request.uri,
          pretty = request.queryString.keySet("pretty"),
          callback = request.queryString.get("callback").flatMap(_.headOption),
          withData = request.getQueryString("with-data"),
          withoutMeta = withoutMeta
        )
      case unknownFormat => throw new IllegalArgumentException(s"Format $unknownFormat is not supported")
    }

  def isSystemProperty(firstDotLast: (String, String)): Boolean = firstDotLast match {
    case ("system.parent", "parent_hierarchy") => true
    case ("system.parent", "parent")           => true
    case ("system", "lastModified")            => true
    case ("system", "kind")                    => true
    case ("system", "path")                    => true
    case ("system", "uuid")                    => true
    case ("system", "quad")                    => true
    case ("system", "dc")                      => true
    case ("system", "indexTime")               => true
    case ("system", "current")                 => true
    case ("system", "parent")                  => true
    case ("system", "protocol")                => true
    case ("link", "to")                        => true
    case ("link", "kind")                      => true
    case ("content", "data")                   => true
    case ("content", "mimeType")               => true
    case ("content", "length")                 => true
    case _                                     => false
  }

  val errorHandler: PartialFunction[Throwable, Result] = { case t => exceptionToResponse(t) }

  val asyncErrorHandler: PartialFunction[Throwable, Future[Result]] = {
    val res2fut: Result => Future[Result] = Future.successful[Result]
    val err2res: Throwable => Result = exceptionToResponse
    ({ case t => res2fut.compose(err2res)(t) }): PartialFunction[Throwable, Future[Result]]
  }
}
