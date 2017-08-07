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


import java.util.concurrent.TimeoutException

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cmwell.domain._
import cmwell.formats.{FormatExtractor, Formatter}
import cmwell.fts._
import cmwell.tracking.PathStatus
import cmwell.util.collections._
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.exceptions.{PrefixAmbiguityException, UnretrievableIdentifierException, UnsupportedURIException}
import cmwell.ws.Settings
import cmwell.ws.util.{ExpandGraphParser, FieldNameConverter, PathGraphExpansionParser, TypeHelpers}
import com.typesafe.scalalogging.LazyLogging
import ld.exceptions.BadFieldTypeException
import logic.CRUDServiceFS
import cmwell.util.concurrent.SimpleScheduler
import ld.cmw.PassiveFieldTypesCache
import play.api.libs.json.Json
import play.api.mvc.Results._
import play.api.mvc.{Request, Result}
import play.utils.InvalidUriEncodingException

import scala.collection.breakOut
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

package object wsutil extends LazyLogging {

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
  def normalizePath(path:String): String = {
    var i,j: Int = 0
    var k: Int = path.length
    var lastIsSlash: Boolean = false
    var last2AreSlash: Boolean = false
    var initialized: Boolean = false
    var starting: Boolean = true
    val pre: String = if(path.isEmpty || path.head != '/') "/" else ""
    var chr: Char = '\n'

    lazy val sb = new StringBuilder(path.length)

    //println("\n\n")

    while(i < path.length) {
      chr = path(i)
      //println(s"pre=$pre, chr=$chr, starting=$starting, initialized=$initialized, lastIsSlash=$lastIsSlash last2AreSlash=$last2AreSlash, i=$i, j=$j, k=$k")
      if(chr == '/') {
        if(!starting){
          if(initialized) {
            if (!lastIsSlash) {
              lastIsSlash = true
            }
          } else {
            if(lastIsSlash) {
              last2AreSlash = true
            } else {
              lastIsSlash = true
              k = i
            }
          }
        }
      }
      else {
        if(!starting){
          if(initialized) {
            if(lastIsSlash) {
              sb += '/'
            }
            sb += chr
          } else {
            if(last2AreSlash) {
              sb ++= pre
              sb ++= path.substring(j,k + 1)
              sb += chr
              initialized = true
              last2AreSlash = false
            }
          }
        } else {
          starting = false
          if(i == 0) { //if path didn't start with a slash
            j = 0
          } else {
            j = i - 1
          }
        }
        lastIsSlash = false
      }
      i += 1
    }


    if(initialized) sb.mkString
    else if(lastIsSlash) pre + path.substring(j,k)
    else pre + path.substring(j,i)
  }

  implicit class StringExtensions(s: String) {
    def dropRightWhile(p: (Char) => Boolean): String = s.dropRight(s.reverseIterator.takeWhile(p).length)
    def dropTrailingChars(c: Char *): String = dropRightWhile(c.toSet[Char](_))
  }

  def overrideMimetype(default: String, req: Request[_]): (String, String) = overrideMimetype(default,req.getQueryString("override-mimetype"))

  def overrideMimetype(default: String, overrideMime: Option[String]): (String, String) = overrideMime match {
    case Some(mimetype) => (play.api.http.HeaderNames.CONTENT_TYPE, mimetype)
    case _ => (play.api.http.HeaderNames.CONTENT_TYPE, default)
  }

  def filterInfoton(f:SingleFieldFilter, i: Infoton): Boolean = {
    require(f.valueOperator == Contains || f.valueOperator == Equals,s"unsupported ValueOperator: ${f.valueOperator}")

    val valOp: (String,String) => Boolean = f.valueOperator match {
      case Contains => (infotonValue,inputValue) => infotonValue.contains(inputValue)
      case Equals => (infotonValue,inputValue) => infotonValue == inputValue
      case _ => ???
    }

    f.fieldOperator match {
      case Should | Must => i.fields.flatMap(_.get(f.name).map(_.exists(fv => f.value.forall(v => valOp(fv.value.toString,v))))).getOrElse(false)
      case MustNot => i.fields.flatMap(_.get(f.name).map(_.forall(fv => !f.value.exists(v => valOp(fv.value.toString,v))))).getOrElse(true)
    }
  }

  type RawField[Op <: FieldValeOperator] = (Op,Either[UnresolvedFieldKey,DirectFieldKey])
  sealed trait RawAggregationFilter
  case class RawStatsAggregationFilter(name:String = "Statistics Aggregation", field:RawField[FieldValeOperator]) extends RawAggregationFilter

  case class RawTermAggregationFilter(name:String = "Term Aggregation", field:RawField[FieldValeOperator], size:Int = 10,
                                 subFilters:Seq[RawAggregationFilter] = Seq.empty) extends RawAggregationFilter

  case class RawHistogramAggregationFilter(name:String = "Histogram Aggregation", field:RawField[FieldValeOperator], interval:Int, minDocCount:Int,
                                      extMin:Option[Long], extMax:Option[Long],
                                      subFilters:Seq[RawAggregationFilter] = Seq.empty) extends RawAggregationFilter

  case class RawSignificantTermsAggregationFilter(name:String = "Signigicant Terms Aggregation", field:RawField[FieldValeOperator],
                                             backgroundTerm:Option[(String, String)], minDocCount:Int, size:Int,
                                             subFilters:Seq[RawAggregationFilter] = Seq.empty) extends RawAggregationFilter

  case class RawCardinalityAggregationFilter(name:String, field:RawField[FieldValeOperator], precisionThreshold:Option[Long]) extends RawAggregationFilter

  object RawAggregationFilter {
    private[this] val lbo = scala.collection.breakOut[Set[String],AggregationFilter,List[AggregationFilter]]
    private def uniq(fn: String, name: String) = {
      if(fn.length > 1 && fn.tail.head == '$') s"-${fn.head}- $name"
      else "-s- " + name
    }
    def eval(af: RawAggregationFilter, cache: PassiveFieldTypesCache, cmwellRDFHelper: CMWellRDFHelper,nbg: Boolean)(implicit ec: ExecutionContext): Future[List[AggregationFilter]] = af match {
      case RawStatsAggregationFilter(name, (op,fk)) => FieldKey.eval(fk,cache,cmwellRDFHelper,nbg).map { fns =>
        fns.map { fn =>
          val uname = {
            if(fns.size == 1) name
            else uniq(fn, name)
          }
          StatsAggregationFilter(uname, Field(op, fn))
        }(lbo)
      }
      case RawTermAggregationFilter(name,(op,fk),size,rawSubFilters) if rawSubFilters.nonEmpty => {
        val ff = FieldKey.eval(fk,cache,cmwellRDFHelper,nbg)
        Future.traverse(rawSubFilters)(eval(_,cache,cmwellRDFHelper,nbg)).flatMap { subFilters =>
          ff.map { fns =>
            fns.map { fn =>
              val uname = {
                if(fns.size == 1) name
                else uniq(fn, name)
              }
              TermAggregationFilter(uname,Field(op,fn),size,subFilters.flatten)
            }(lbo)
          }
        }
      }
      case RawTermAggregationFilter(name,(op,fk),size,rawSubFilters) if rawSubFilters.isEmpty => FieldKey.eval(fk,cache,cmwellRDFHelper,nbg).map { fns =>
        fns.map { fn =>
          val uname = {
            if(fns.size == 1) name
            else uniq(fn, name)
          }
          TermAggregationFilter(uname, Field(op, fn), size)
        }(lbo)
      }
      case RawHistogramAggregationFilter(name,(op,fk),interval,minDocCount,extMin,extMax,rawSubFilters) if rawSubFilters.nonEmpty => {
        val ff = FieldKey.eval(fk,cache,cmwellRDFHelper,nbg)
        Future.traverse(rawSubFilters)(eval(_,cache,cmwellRDFHelper,nbg)).flatMap { subFilters =>
          ff.map { fns =>
            fns.map { fn =>
              val uname = {
                if(fns.size == 1) name
                else uniq(fn, name)
              }
              HistogramAggregationFilter(uname,Field(op,fn),interval,minDocCount,extMin,extMax,subFilters.flatten)
            }(lbo)
          }
        }
      }
      case RawHistogramAggregationFilter(name,(op,fk),interval,minDocCount,extMin,extMax,rawSubFilters) if rawSubFilters.isEmpty => FieldKey.eval(fk,cache,cmwellRDFHelper,nbg).map { fns =>
        fns.map { fn =>
          val uname = {
            if(fns.size == 1) name
            else uniq(fn, name)
          }
          HistogramAggregationFilter(uname, Field(op, fn),interval,minDocCount,extMin,extMax)
        }(lbo)
      }
      case RawSignificantTermsAggregationFilter(name,(op,fk),None,minDocCount,size,rawSubFilters) if rawSubFilters.nonEmpty => {
        val ff = FieldKey.eval(fk,cache,cmwellRDFHelper,nbg)
        Future.traverse(rawSubFilters)(eval(_,cache,cmwellRDFHelper,nbg)).flatMap { subFilters =>
          ff.map { fns =>
            fns.map { fn =>
              val uname = {
                if(fns.size == 1) name
                else uniq(fn, name)
              }
              SignificantTermsAggregationFilter(uname,Field(op,fn),None,minDocCount,size,subFilters.flatten)
            }(lbo)
          }
        }
      }
      case RawSignificantTermsAggregationFilter(name,(op,fk),None,minDocCount,size,rawSubFilters) if rawSubFilters.isEmpty => FieldKey.eval(fk,cache,cmwellRDFHelper,nbg).map { fns =>
        fns.map { fn =>
          val uname = {
            if(fns.size == 1) name
            else uniq(fn, name)
          }
          SignificantTermsAggregationFilter(uname, Field(op, fn),None,minDocCount,size)
        }(lbo)
      }
      //TODO: backgroundTerms should also be unevaluated FieldKey. need to fix the parser.
      case RawSignificantTermsAggregationFilter(_,_,Some(_),_,_,_) => ???
      case RawCardinalityAggregationFilter(name, (op,fk),precisionThreshold) => FieldKey.eval(fk,cache,cmwellRDFHelper,nbg).map { fns =>
        fns.map { fn =>
          val uname = {
            if(fns.size == 1) name
            else uniq(fn, name)
          }
          CardinalityAggregationFilter(uname, Field(op, fn),precisionThreshold)
        }(lbo)
      }
    }
  }

  trait FieldPattern
  trait NsPattern

  case class HashedNsPattern(hash: String) extends NsPattern
  trait ResolvedNsPattern extends NsPattern {def resolve(cmwellRDFHelper: CMWellRDFHelper,nbg: Boolean)(implicit ec: ExecutionContext): Future[String]}
  case class NsUriPattern(nsUri: String) extends ResolvedNsPattern {
    override def resolve(cmwellRDFHelper: CMWellRDFHelper,nbg: Boolean)(implicit ec: ExecutionContext) = cmwellRDFHelper.urlToHashAsync(nsUri,nbg)
  }
  case class PrefixPattern(prefix: String) extends ResolvedNsPattern {
    override def resolve(cmwellRDFHelper: CMWellRDFHelper,nbg: Boolean)(implicit ec: ExecutionContext) = cmwellRDFHelper.getUrlAndLastForPrefixAsync(prefix,nbg).map(_._2)
  }

  case object JokerPattern                       extends FieldPattern
  case class NsWildCard(nsPattern: NsPattern)    extends FieldPattern
  case class FieldKeyPattern(fieldKey: Either[UnresolvedFieldKey,DirectFieldKey]) extends FieldPattern

  case class FilteredField[FP <: FieldPattern](fieldPattern: FP, rawFieldFilterOpt: Option[RawFieldFilter])
  case class LevelExpansion(filteredFields: List[FilteredField[FieldPattern]])

  sealed abstract class DirectedExpansion
  case class ExpandUp(filteredFields: List[FilteredField[FieldKeyPattern]]) extends DirectedExpansion
  case class ExpandIn(filteredFields: List[FilteredField[FieldPattern]]) extends DirectedExpansion

  case class PathExpansion(pathSegments: List[DirectedExpansion])
  case class PathsExpansion(paths: List[PathExpansion])

  //Some convenience methods & types
  def getByPath(path: String, crudServiceFS: CRUDServiceFS, nbg: Boolean)(implicit ec: ExecutionContext): Future[Infoton] = crudServiceFS.irwService(nbg).readPathAsync(path, crudServiceFS.level).map(_.getOrElse(GhostInfoton(path)))
  type F[X] = (X,Option[List[RawFieldFilter]])
  type EFX = Either[F[Future[Infoton]],F[Infoton]]

  def filterByRawFieldFiltersTupled(cache: PassiveFieldTypesCache,cmwellRDFHelper: CMWellRDFHelper,nbg: Boolean)(tuple: (Infoton,Option[List[RawFieldFilter]]))(implicit ec: ExecutionContext): Future[Boolean] = tuple match {
    case (i,None) => Future.successful(true)
    case (i,Some(filters)) => filterByRawFieldFilters(cache,cmwellRDFHelper,nbg)(i,filters)
  }

  def filterByRawFieldFilters(cache: PassiveFieldTypesCache,cmwellRDFHelper: CMWellRDFHelper,nbg: Boolean)(infoton: Infoton, filters: List[RawFieldFilter])(implicit ec: ExecutionContext): Future[Boolean] = {

    val p = Promise[Boolean]()

    val futures = for {
      filter <- filters
      future = filterByRawFieldFilter(infoton,filter,cache,cmwellRDFHelper,nbg)
    } yield future.andThen {
      case Success(true) if !p.isCompleted => p.trySuccess(true)
    }

    cmwell.util.concurrent.successes(futures).foreach {
      case Nil => Future.traverse(futures)(_.failed).foreach { err =>
        p.tryFailure(new cmwell.util.exceptions.MultipleFailures(err))
      }
      case list => if(!p.isCompleted){
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

  def filterByRawFieldFilter(infoton: Infoton, filter: RawFieldFilter, cache: PassiveFieldTypesCache, cmwellRDFHelper: CMWellRDFHelper,nbg: Boolean)(implicit ec: ExecutionContext): Future[Boolean] =
    RawFieldFilter.eval(filter,cache,cmwellRDFHelper,nbg).map(_.filter(infoton).value)

  def expandIn(filteredFields: List[FilteredField[FieldPattern]],
               infotonsToExpand: Seq[Infoton],
               infotonsRetrievedCache: Map[String, Infoton],
               cmwellRDFHelper: CMWellRDFHelper,
               cache: PassiveFieldTypesCache,
               nbg: Boolean)(implicit ec: ExecutionContext): Future[(Seq[Infoton],Seq[Infoton])] = {
    val expansionFuncsFut = Future.traverse(filteredFields) {
      case FilteredField(JokerPattern, rffo) => Future.successful({ (internalFieldName: String) => true } -> rffo)
      case FilteredField(FieldKeyPattern(Right(dfk)), rffo) => Future.successful((dfk.internalKey == _,rffo))
      case FilteredField(FieldKeyPattern(Left(rfk)), rffo) => FieldKey.resolve(rfk,cmwellRDFHelper,nbg).map(fk => (fk.internalKey == _,rffo))
      case FilteredField(NsWildCard(HashedNsPattern(hash)), rffo) => Future.successful({ (internalFieldName: String) => internalFieldName.endsWith(s".$hash") } -> rffo)
      case FilteredField(NsWildCard(rnp: ResolvedNsPattern), rffo) => rnp.resolve(cmwellRDFHelper,nbg).map(hash => { (internalFieldName: String) => internalFieldName.endsWith(s".$hash") } -> rffo)
    }
    expansionFuncsFut.flatMap { funs =>

      // all the infotons' fields
      val fieldsMaps = infotonsToExpand.map(_.fields).collect { case Some(m) => m }

      // maps reduced into 1 aggregated fields `Map`
      val fieldsReduced = if(fieldsMaps.nonEmpty) fieldsMaps.reduce { (m1, m2) =>
        m1.foldLeft(m2) {
          case (acc, (fieldName, values)) if acc.contains(fieldName) => acc.updated(fieldName, acc(fieldName) union values)
          case (acc, fieldNameValsTuple) => acc + fieldNameValsTuple
        }
      } else Map.empty[String,Set[FieldValue]]

      // build a list that pairs cmwell paths to retrieval with raw field filters to be applied on
      val cmwPathFilterOptionPairs = fieldsReduced.flatMap {
        case (fieldName, values) => funs.flatMap {
          case (fun, rffo) if !fun(fieldName) => Nil
          case (fun, rffo) => values.collect {
            case fr: FReference => normalizePath(fr.getCmwellPath) -> rffo
          }(breakOut[Set[FieldValue], (String, Option[RawFieldFilter]), List[(String, Option[RawFieldFilter])]])
        }
      }(breakOut[Map[String, Set[FieldValue]], (String, Option[RawFieldFilter]), List[(String, Option[RawFieldFilter])]])

      // value are `Option[List[...]]` because `None` means no filtering (pass all)
      // which is different from emptylist which means at least 1 filter should apply (i.e: block all)
      val pathToFiltersMap = cmwPathFilterOptionPairs.groupBy(_._1).mapValues(ps => Option.sequence(ps.unzip._2)).toSeq

      // get infotons from either `infotonsRetrievedCache` or from cassandra, and pair with filters option
      val (l, r) = partitionWith(pathToFiltersMap) {
        case (path, rffso) => {
          infotonsRetrievedCache.get(path).fold[EFX](Left(getByPath(path,cmwellRDFHelper.crudServiceFS,nbg) -> rffso)){
            i => Right(i -> rffso)
          }
        }
      }

      // after retrieval, filter out what is not needed
      val lInfotonsFut = Future.traverse(l) {
        case (fi, None) => fi.map(Some.apply)
        case (fi, Some(filters)) => fi.flatMap {
          case i => filterByRawFieldFilters(cache,cmwellRDFHelper,nbg)(i, filters).map {
            case true => Some(i)
            case false => None
          }
        }
      }.map(_.collect { case Some(i) => i })

      // also filter the infotons retrieved from "cache"
      val rInfotonsFut = Future.traverse(r) {
        case t@(i, _) => filterByRawFieldFiltersTupled(cache,cmwellRDFHelper,nbg)(t).map {
          case true => Some(i)
          case false => None
        }
      }.map(_.collect { case Some(i) => i})

      // combine results
      lInfotonsFut.zip(rInfotonsFut)
    }
  }


  def deepExpandGraph(xgPattern: String, infotons: Seq[Infoton],cmwellRDFHelper: CMWellRDFHelper,cache: PassiveFieldTypesCache,nbg: Boolean)(implicit ec: ExecutionContext): Future[(Boolean,Seq[Infoton])] = {

    def expandDeeper(expanders: List[LevelExpansion], infotonsToExpand: Seq[Infoton], infotonsRetrievedCache: Map[String, Infoton]): Future[(Boolean,Seq[Infoton])] = expanders match {
      case Nil => Future.successful(true -> infotonsRetrievedCache.values.filterNot(_.isInstanceOf[GhostInfoton]).toSeq)
      case f :: fs if infotonsRetrievedCache.size > Settings.expansionLimit => Future.successful(false -> infotonsRetrievedCache.values.toSeq)
      case f :: fs => {
        expandIn(f.filteredFields,infotonsToExpand,infotonsRetrievedCache,cmwellRDFHelper,cache,nbg).flatMap {
          case (lInfotons,rInfotons) => 
            expandDeeper(fs, 
              lInfotons ++ rInfotons, 
              infotonsRetrievedCache ++ lInfotons.map(i => i.path -> i)(scala.collection.breakOut[Seq[Infoton],(String,Infoton),Map[String,Infoton]]))
        }
      }
    }

    val t = ExpandGraphParser.getLevelsExpansionFunctions(xgPattern).map { fs =>
      expandDeeper(fs, infotons, infotons.map(i => i.path -> i).toMap)
    }

    t match {
      case Success(future) => future
      case Failure(error) => Future.failed(error)
    }
  }

  def pathExpansionParser(ygPattern: String, infotons: Seq[Infoton], chunkSize: Int, cmwellRDFHelper: CMWellRDFHelper, typesCache: PassiveFieldTypesCache, nbg: Boolean)(implicit ec: ExecutionContext): Future[(Boolean,Seq[Infoton])] = {

    type Expander = (DirectedExpansion,List[DirectedExpansion],Seq[Infoton])

     def expandUp(filteredFields: List[FilteredField[FieldKeyPattern]],
                  population: Seq[Infoton],
                  cache: Map[String, Infoton]): Future[(Seq[Infoton],Seq[Infoton])] = {

       def mkFieldFilters2(ff: FilteredField[FieldKeyPattern], outerFieldOperator: FieldOperator, urls: List[String]): Future[FieldFilter] = {

         val FilteredField(fkp, rffo) = ff
         val internalFieldNameFut = fkp match {
           case FieldKeyPattern(Right(dfk)) => Future.successful(dfk.internalKey)
           case FieldKeyPattern(Left(unfk)) => FieldKey.resolve(unfk, cmwellRDFHelper,nbg).map(_.internalKey)
         }
         val filterFut: Future[FieldFilter] = internalFieldNameFut.map { internalFieldName =>
           urls match {
             case Nil => throw new IllegalStateException(s"empty urls in expandUp($filteredFields,population[size=${population.size}],cache[size=${cache.size}])\nfor pattern: $ygPattern\nand infotons.take(3) = ${infotons.take(3).mkString("[", ",", "]")}")
             case url :: Nil => SingleFieldFilter(rffo.fold[FieldOperator](outerFieldOperator)(_ => Must), Equals, internalFieldName, Some(url))
             case _ => {
               val shoulds = urls.map(url => SingleFieldFilter(Should, Equals, internalFieldName, Some(url)))
               MultiFieldFilter(rffo.fold[FieldOperator](outerFieldOperator)(_ => Must), shoulds)
             }
           }
         }
         rffo.fold[Future[FieldFilter]](filterFut) { rawFilter =>
           RawFieldFilter.eval(rawFilter, typesCache, cmwellRDFHelper,nbg).flatMap { filter =>
             filterFut.map(ff => MultiFieldFilter(outerFieldOperator, List(ff, filter)))
           }
         }
       }

       Future.traverse(population.grouped(chunkSize)) { infotonsChunk =>
         val urls: List[String] = infotonsChunk.map(i => pathToUri(i.path))(breakOut)
         val fieldFilterFut = filteredFields match {
           case Nil => throw new IllegalStateException(s"expandUp($filteredFields,population[size=${population.size}],cache[size=${cache.size}])\nfor pattern: $ygPattern\nand infotons.take(3) = ${infotons.take(3).mkString("[",",","]")}")
           case ff :: Nil => mkFieldFilters2(ff,Must,urls)
           case _ => Future.traverse(filteredFields)(mkFieldFilters2(_,Should,urls)).map(MultiFieldFilter(Must,_))
         }
         fieldFilterFut.flatMap{ffs =>
           cmwellRDFHelper.crudServiceFS.thinSearch(None,
             Some(ffs),
             None,
             PaginationParams(0, Settings.expansionLimit),
             withHistory = false,
             NullSortParam,
             debugInfo = false,
             withDeleted = false,
             nbg = nbg).flatMap(sr => {
             val (inCache, toFetch) = sr.thinResults.partition(i => cache.contains(i.path))
             cmwellRDFHelper.crudServiceFS.getInfotonsByUuidAsync(toFetch.map(_.uuid),nbg).map {
               _ -> inCache.map(i => cache(i.path))
             }
           })
         }
       }.map {
         case tuples if tuples.isEmpty => (Seq.empty, Seq.empty)
         case tuples => tuples.reduce[(Seq[Infoton], Seq[Infoton])] {
           case ((la, ra), (lb, rb)) => (la ++ lb) -> (ra ++ rb)
         }
       }
     }

    /**
      *
      * @param infoCachedOrRetrieved
      * @param tail
      * @return
      */
    def adjustResults(infoCachedOrRetrieved: (Seq[Infoton],Seq[Infoton]), tail: List[DirectedExpansion]): (Option[Expander],Seq[Infoton]) = {
      val (retrieved,cached) = infoCachedOrRetrieved
      tail.headOption.map[Expander](h => (h,tail.tail,cached ++ retrieved)) -> retrieved
    }

    def expandDeeper(expanders: List[Option[Expander]], cache: Map[String, Infoton]): Future[(Boolean,Seq[Infoton])] = {
      if (expanders.forall(_.isEmpty)) Future.successful(true -> cache.values.filterNot(_.isInstanceOf[GhostInfoton]).toSeq)
      else if (cache.count(!_._2.isInstanceOf[GhostInfoton]) > Settings.expansionLimit) Future.successful(false -> cache.values.toSeq)
      else Future.traverse(expanders) {
        case None => Future.successful(None -> Seq.empty)
        case Some((ExpandIn(ffs), tail, population)) => expandIn(ffs, population, cache, cmwellRDFHelper, typesCache, nbg).map(newPop => adjustResults(newPop, tail))
        case Some((ExpandUp(ffs), tail, population)) => expandUp(ffs, population, cache).map(newPop => adjustResults(newPop, tail))
      }.flatMap { expanderRetrievedInfotonPairs =>

        val (newExpanders, retrievedInfotons) = expanderRetrievedInfotonPairs.unzip
        val newCache = retrievedInfotons.foldLeft(cache) {
          case (accache, additions) => accache ++ additions.map(i => i.path -> i)
        }
        expandDeeper(newExpanders, newCache)
      }
    }

    PathGraphExpansionParser.getPathsExpansionFunctions(ygPattern).map {
      case PathsExpansion(paths) => {
        val perPathHeadTail = paths.map {
          case PathExpansion(segments) => segments.headOption.map { head =>
            (head, segments.tail, infotons)
          }
        }
        expandDeeper(perPathHeadTail,infotons.map(i => i.path -> i)(scala.collection.breakOut[Seq[Infoton],(String,Infoton),Map[String,Infoton]]))
      }
    } match {
      case Success(future) => future
      case Failure(error) => Future.failed(error)
    }
  }

  def isPathADomain(path: String): Boolean = path.dropWhile(_ == '/').takeWhile(_ != '/').contains('.')

  def pathToUri(path: String): String = {
    if(path.startsWith("/https.")) s"https:/${path.drop("/https.".length)}"
    else if(isPathADomain(path)) s"http:/$path"
    else s"cmwell:/$path"
  }

  def exceptionToResponse(throwable: Throwable): Result = {
    val (status,eHandler): (Status,Throwable => String) = throwable match {
      case _: TimeoutException => ServiceUnavailable -> {_.getMessage}
      case _: UnretrievableIdentifierException => UnprocessableEntity -> {_.getMessage}
      case _: PrefixAmbiguityException => UnprocessableEntity -> {_.getMessage}
      case _: security.UnauthorizedException => Forbidden -> {_.getMessage}
      case _: org.apache.jena.shared.JenaException => BadRequest -> {_.getMessage}
      case _: cmwell.web.ld.exceptions.ParsingException => BadRequest -> {_.getMessage}
      case _: IllegalArgumentException => BadRequest -> {_.getMessage}
      case _: UnsupportedURIException => BadRequest -> {_.getMessage}
      case _: InvalidUriEncodingException => BadRequest -> {_.getMessage}
      case _: BadFieldTypeException => BadRequest -> {_.getMessage}
      case _: com.datastax.driver.core.exceptions.InvalidQueryException => ExpectationFailed -> {_.getMessage}
      case _: com.datastax.driver.core.exceptions.DriverException => ServiceUnavailable -> {_.getMessage}
      case e: org.elasticsearch.transport.RemoteTransportException if e.getCause.isInstanceOf[org.elasticsearch.action.search.ReduceSearchPhaseException] => ServiceUnavailable -> {_.getCause.getMessage}
      case e: org.elasticsearch.transport.RemoteTransportException if e.getCause.isInstanceOf[org.elasticsearch.action.search.SearchPhaseExecutionException] => BadRequest -> {_.getCause.getMessage}
      case e: Throwable => {
        logger.error("unexpected error occurred",e)
        InternalServerError -> {_.getMessage}
      }
    }
    status(Json.obj("success" -> false, "error" -> eHandler(throwable)))
  }

  def extractFieldsMask(req: Request[_],cache: PassiveFieldTypesCache, cmwellRDFHelper: CMWellRDFHelper,nbg: Boolean)(implicit ec: ExecutionContext): Future[Set[String]] = {
    extractFieldsMask(req.getQueryString("fields"),cache,cmwellRDFHelper,nbg)
  }

  def extractFieldsMask(fieldsOpt: Option[String],cache: PassiveFieldTypesCache, cmwellRDFHelper: CMWellRDFHelper,nbg: Boolean)(implicit ec: ExecutionContext): Future[Set[String]] = {
    fieldsOpt.map(FieldNameConverter.toActualFieldNames) match {
      case Some(Success(fields)) => Future.traverse(fields)(FieldKey.eval(_,cache,cmwellRDFHelper,nbg)).map(_.reduce(_ | _))
      case Some(Failure(e)) => Future.failed(e)
      case None => Future.successful(Set.empty[String])
    }
  }

  val noDataFormat: String => Boolean = Set("tsv","tab","text","path").apply

  def extractInferredFormatWithData(req: Request[_], defaultFormat: String = "json"): (String,Boolean) = {
    val format = req.getQueryString("format").getOrElse(defaultFormat)
    val withData = req.getQueryString("with-data").fold(!noDataFormat(format))(_ != "false")
    format -> withData
  }

  val endln = ByteString(cmwell.util.os.Props.endln)

  def guardHangingFutureByExpandingToSource[T,U,V](futureThatMayHang: Future[T],
                                                 initialGraceTime: FiniteDuration,
                                                 injectInterval: FiniteDuration)
                                                (backOnTime: T => V,
                                                 prependInjections: () => U,
                                                 injectOriginalFutureWith: T => U,
                                                 continueWithSource: Source[U,NotUsed] => V)
                                                (implicit ec: ExecutionContext): Future[V] = {
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
        val source = Source.unfoldAsync(false) {
          isCompleted => {
            if (isCompleted) Future.successful(None)
            else {
              val p2 = Promise[Option[(Boolean,U)]]()
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
  def keepAliveByDrippingNewlines(response: Future[Result], extraHeaders: Seq[(String,String)] = Nil)(implicit ec: ExecutionContext): Future[Result] = {
    guardHangingFutureByExpandingToSource[Result, Source[ByteString, _], Result](response, 7.seconds, 3.seconds)(identity, () => Source.single(endln), _.body.dataStream, source => Ok.chunked(source.flatMapConcat(x => x)).withHeaders(extraHeaders: _*))
  }

  def formattableToByteString(formatter: Formatter)(i: Formattable) = {
    val body = formatter.render(i)
    if (body.endsWith(cmwell.util.os.Props.endln)) ByteString(body, "utf-8")
    else ByteString(body, "utf-8") ++ endln
  }

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


  def pathStatusAsInfoton(ps: PathStatus): Infoton = {
    val PathStatus(path, status) = ps
    val fields: Option[Map[String, Set[FieldValue]]] = Some(Map("trackingStatus" -> Set(FString(status.toString))))
    VirtualInfoton(ObjectInfoton(path, Settings.dataCenter, None, fields = fields))
  }

  def getFormatter(request: Request[_], formatterManager: FormatterManager, defaultFormat: String, nbg: Boolean, withoutMeta: Boolean = false): Formatter =
    request.getQueryString("format").getOrElse(defaultFormat) match {
      case FormatExtractor(formatType) => formatterManager.getFormatter(
        format = formatType,
        host = request.host,
        uri = request.uri,
        pretty = request.queryString.keySet("pretty"),
        callback = request.queryString.get("callback").flatMap(_.headOption),
        withData = request.getQueryString("with-data"),
        withoutMeta = withoutMeta,
        nbg = nbg)
      case unknownFormat => throw new IllegalArgumentException(s"Format $unknownFormat is not supported")
    }

  val errorHandler = {
    val err2res: Throwable => Result = exceptionToResponse
    PartialFunction(err2res)
  }

  val asyncErrorHandler = {
    val res2fut: Result => Future[Result] = Future.successful[Result]
    val err2res: Throwable => Result = exceptionToResponse
    PartialFunction(res2fut compose err2res)
  }
}
