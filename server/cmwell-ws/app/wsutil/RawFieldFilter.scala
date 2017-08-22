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


package wsutil

import javax.inject.Inject

import cmwell.domain.{FReference, FString}
import cmwell.fts._
import cmwell.util.concurrent.retry
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.exceptions.{PrefixAmbiguityException, UnretrievableIdentifierException}
import cmwell.ws.util.PrefixRequirement
import com.typesafe.scalalogging.LazyLogging
import cmwell.syntaxutils._
import ld.cmw.{PassiveFieldTypesCache, PassiveFieldTypesCacheTrait}
import logic.CRUDServiceFS

import scala.concurrent.{ExecutionContext, Future, Promise, duration}
import ExecutionContext.Implicits.global
import duration.DurationInt
import scala.util.{Failure, Success, Try}

sealed trait RawFieldFilter {
  def fieldOperator: FieldOperator
}

sealed trait UnresolvedFieldKey {
  def externalKey: String
}
sealed trait FieldKey {
  def externalKey: String
  def internalKey: String
  def metaPath: String
}
sealed trait DirectFieldKey extends FieldKey {
  def infoPath: String
  override def metaPath: String = infoPath
}

case class UnresolvedURIFieldKey(uri: String) extends UnresolvedFieldKey {
  override val externalKey = "$" + uri + "$"
}
case class URIFieldKey(uri: String, first: String, last: String) extends FieldKey {
  //override val firstLast = retry(7,1.seconds)(Future.fromTry(FieldKey.namespaceUri(uri)))
  override val internalKey = s"$first.$last"
  override val externalKey = s"$first.$$$last"
  override val metaPath = s"/meta/ns/$last/$first"
}

case class UnresolvedPrefixFieldKey(first: String,prefix: String) extends UnresolvedFieldKey {
  override val externalKey = first + "." + prefix
}
case class PrefixFieldKey(first: String, last: String, prefix: String) extends FieldKey {
  //override lazy val firstLast = retry(7,1.seconds)(FieldKey.resolvePrefix(first,prefix))
  override val internalKey = s"$first.$last"
  override val externalKey = s"$first.$$$last"
  override val metaPath = s"/meta/ns/$last/$first"
}

case class NnFieldKey(externalKey: String) extends DirectFieldKey {
  override def internalKey = externalKey
  override def infoPath = {
    if(externalKey.startsWith("system.") || externalKey.startsWith("content.") || externalKey.startsWith("link.")) s"/meta/sys/${externalKey.drop("system.".length)}"
    else s"/meta/nn/$externalKey"
  }
}
case class HashedFieldKey(first: String,hash: String) extends DirectFieldKey {
  override val internalKey = first + "." + hash
  override val externalKey = first + ".$" + hash
  override def infoPath = s"/meta/ns/$hash/$first"
}
case class UnevaluatedQuadFilter(override val fieldOperator: FieldOperator = Must,
                                 valueOperator: ValueOperator,
                                 quadAlias: String) extends RawFieldFilter


case class RawSingleFieldFilter(override val fieldOperator: FieldOperator = Must,
                                valueOperator: ValueOperator,
                                key: Either[UnresolvedFieldKey,DirectFieldKey],
                                value: Option[String]) extends RawFieldFilter

case class RawMultiFieldFilter(override val fieldOperator: FieldOperator = Must,
                               filters:Seq[RawFieldFilter]) extends RawFieldFilter

object RawFieldFilter extends PrefixRequirement {
  private[this] val bo1 = scala.collection.breakOut[Seq[RawFieldFilter],FieldFilter,Vector[FieldFilter]]
  private[this] val bo2 = scala.collection.breakOut[Set[String],FieldFilter,Vector[FieldFilter]]
  def eval(rff: RawFieldFilter, cache: PassiveFieldTypesCacheTrait, cmwellRDFHelper: CMWellRDFHelper,nbg: Boolean)(implicit ec: ExecutionContext): Future[FieldFilter] = rff match {
    case UnevaluatedQuadFilter(fo,vo,alias) => {
      val fieldFilterWithExplicitUrlOpt = cmwellRDFHelper.getQuadUrlForAlias(alias,nbg).map(v => SingleFieldFilter(fo, vo, "system.quad", Some(v)))
      prefixRequirement(fieldFilterWithExplicitUrlOpt.nonEmpty, s"The alias '$alias' provided for quad in search does not exist. Use explicit quad URL, or register a new alias using `graphAlias` meta operation.")
      Future.successful(fieldFilterWithExplicitUrlOpt.get)
    }
    case RawMultiFieldFilter(fo,rs) => Future.traverse(rs)(eval(_,cache,cmwellRDFHelper,nbg))(bo1,ec).map(MultiFieldFilter(fo, _))
    case RawSingleFieldFilter(fo,vo,fk,v) => FieldKey.eval(fk,cache,cmwellRDFHelper,nbg).map{
      case s if s.isEmpty => !!!
      case s if s.size == 1 => mkSingleFieldFilter(fo,vo,s.head,v)
      case s => MultiFieldFilter(fo,s.map(mkSingleFieldFilter(Should,vo,_,v))(bo2))
    }
  }

  def mkSingleFieldFilter(fieldOp: FieldOperator, valueOp: ValueOperator, fieldName: String, value: Option[String]) = valueOp match {
    case Equals if fieldName.indexOf('$') == 1     ||
                   fieldName.startsWith("system.") ||
                   fieldName.startsWith("content.") => SingleFieldFilter(fieldOp,Contains,fieldName,value)
    case _ => SingleFieldFilter(fieldOp,valueOp,fieldName,value)
  }
}

sealed trait RawSortParam
case class RawFieldSortParam(rawFieldSortParam: List[RawSortParam.RawFieldSortParam]) extends RawSortParam
case object RawNullSortParam extends RawSortParam

object RawSortParam extends LazyLogging {
  type RawFieldSortParam = (Either[UnresolvedFieldKey,DirectFieldKey], FieldSortOrder)

  val empty = RawFieldSortParam(Nil)
  private[this] val bo = scala.collection.breakOut[Set[String],SortParam.FieldSortParam,List[SortParam.FieldSortParam]]

//  private[this] val indexedFieldsNamesCache =
//    new SingleElementLazyAsyncCache[Set[String]](Settings.fieldsNamesCacheTimeout.toMillis,Set.empty)(CRUDServiceFS.ftsService.getMappings(withHistory = true))(scala.concurrent.ExecutionContext.Implicits.global)

  def eval(rsps: RawSortParam, crudServiceFS: CRUDServiceFS, cache: PassiveFieldTypesCache, cmwellRDFHelper: CMWellRDFHelper, nbg: Boolean)(implicit ec: ExecutionContext): Future[SortParam] = rsps match {
    case RawNullSortParam => Future.successful(NullSortParam)
    case RawFieldSortParam(rfsp) => {

      val indexedFieldsNamesFut = crudServiceFS.ESMappingsCache(nbg).getAndUpdateIfNeeded

      Future.traverse(rfsp) {
        case (fk, ord) => FieldKey.eval(fk,cache,cmwellRDFHelper,nbg).map(_.map(_ -> ord)(bo))
        // following code could gives precedence to mangled fields over unmangled ones
      }.flatMap(pairs => indexedFieldsNamesFut.map {
        indexedFieldsNamesWithTypeConcatenation => {
          val indexedFieldsNames = indexedFieldsNamesWithTypeConcatenation.map(_.takeWhile(':'.!=))
          FieldSortParams(pairs.foldRight(List.empty[SortParam.FieldSortParam]) {
            (currentFieldMangledList, reduced) => {
              val (mangled, unmangled) = {
                val filtered = currentFieldMangledList.filter {
                  case (cur, _) => {
                    (cur.length > 1 && cur(1) == '$') ||
                      cur.startsWith("system.") ||
                      cur.startsWith("content.") ||
                      cur.startsWith("link.") ||
                      indexedFieldsNames(cur)
                  }
                }
                val prePartition = if(filtered.nonEmpty)  filtered else {
                  logger.warn(s"currentFieldMangledList was filtered up to an empty list: $currentFieldMangledList ,\n$indexedFieldsNames")
                  currentFieldMangledList
                }
                prePartition.partition {
                  case (name, order) => name.length > 1 && name.charAt(1) == '$'
                }
              }
              mangled.foldRight(unmangled.foldRight(reduced)(_ :: _))(_ :: _)
            }
          })
        }
      })
    }
  }
}

object FieldKey extends LazyLogging with PrefixRequirement  {
  
  def eval(fieldKey: Either[UnresolvedFieldKey,DirectFieldKey], cache: PassiveFieldTypesCacheTrait, cmwellRDFHelper: CMWellRDFHelper,nbg:Boolean)(implicit ec: ExecutionContext): Future[Set[String]] = fieldKey match {
    case Right(NnFieldKey(key)) if key.startsWith("system.") || key.startsWith("content.") || key.startsWith("link.")  => Future.successful(Set(key))
    case Right(dFieldKey) => enrichWithTypes(dFieldKey, cache)
    case Left(uFieldKey) => resolve(uFieldKey, cmwellRDFHelper,nbg).flatMap(enrichWithTypes(_,cache))
  }

  def enrichWithTypes(fk: FieldKey, cache: PassiveFieldTypesCacheTrait): Future[Set[String]] = {
    cache.get(fk).map(_.collect {
      case c if c != 's' => s"$c$$${fk.internalKey}"
    } + fk.internalKey )
  }

  def resolve(ufk: UnresolvedFieldKey, cmwellRDFHelper: CMWellRDFHelper,nbg: Boolean): Future[FieldKey] = ufk match {
    case UnresolvedPrefixFieldKey(first,prefix) => resolvePrefix(cmwellRDFHelper,first,prefix,nbg).map{
      case (first,hash) => PrefixFieldKey(first,hash,prefix)
    }
    case UnresolvedURIFieldKey(uri) => Future.fromTry(namespaceUri(cmwellRDFHelper,uri,nbg).map{
      case (first,hash) => URIFieldKey(uri,first,hash)
    })
  }

  def namespaceUri(cmwellRDFHelper: CMWellRDFHelper,u: String,nbg: Boolean): Try[(String,String)] = {
    val p = org.apache.jena.rdf.model.ResourceFactory.createProperty(u)
    val first = p.getLocalName
    val ns = p.getNameSpace
    cmwellRDFHelper.urlToHash(ns,nbg) match {
      case None => Failure(new UnretrievableIdentifierException(s"could not find namespace URI: $ns"))
      case Some(internalIdentifier) => Success(first -> internalIdentifier)
    }
  }

  def resolvePrefix(cmwellRDFHelper: CMWellRDFHelper, first: String, requestedPrefix: String,nbg: Boolean)(implicit ec: ExecutionContext): Future[(String,String)] = {
    val p = Promise[String]()

    // easier, but we want better error messages returned
    //            val (_,last) = CMWellRDFHelper.getUrlAndLastForPrefix(s)(Settings.esTimeout)
    // or:
    //            CMWellRDFHelper.prefixToHash(s)

    val f = Try(cmwellRDFHelper.getUrlAndLastForPrefixAsync(requestedPrefix, nbg, withFallBack = false)).recover {
      case t: Throwable =>
        Future.failed[(String,String)](t)
    }.get

    //first, try old API, assuming prefix == hash
    cmwellRDFHelper.hashToInfoton(requestedPrefix,nbg) match {
      case None => f.onComplete {
        case scala.util.Success((_, last)) => p.success(last)
        case scala.util.Failure(e: UnretrievableIdentifierException) => p.failure(e)
        case scala.util.Failure(e: IllegalArgumentException) => p.failure(new UnretrievableIdentifierException(e.getMessage, e))
        case scala.util.Failure(e) => {
          logger.error(s"couldn't find the prefix: $requestedPrefix", e)
          p.failure(new UnretrievableIdentifierException(s"couldn't find the prefix: $requestedPrefix", e))
        }
      }
      case Some(infoton) => f.onComplete {
        case Success((url, last)) => infoton.fields.flatMap(_.get("url")) match {
          case None => {
            logger.warn(s"infoton has empty fields? $infoton")
            p.success(last)
          }
          case Some(urlSet) if urlSet.size != 1 => p.failure(new UnretrievableIdentifierException(s"multiple/no url values in: $infoton"))
          case Some(urlSet) => {

            val url22Try = urlSet.head match {
              case FString(v, _, _) => Success(v)
              case FReference(v, _) => Success(v)
              case _ => Failure(new UnretrievableIdentifierException(s"url must be string in: $infoton"))
            }

            p.complete(url22Try.flatMap { url22 =>

              if (url22 == url && last == infoton.path.drop("/meta/ns/".length)) {
                //                    val path = infoton.path.drop("/meta/ns/".length)
                lazy val prefixOpt = infoton.fields.flatMap(_.get("prefix").flatMap(_.headOption.collect {
                  case f: FString => f.value
                }))

                if (requestedPrefix == last) Success(requestedPrefix)
                else if (prefixOpt.isEmpty || prefixOpt.get != requestedPrefix) {
                  logger.warn(s"false namespace ambiguity detected. prefix is empty for path: /meta/ns/$requestedPrefix & infoton: $infoton")
                  Success(requestedPrefix)
                }
                else {
                  //requestedPrefix == prefixOpt.get
                  Failure(new PrefixAmbiguityException(s"prefix $requestedPrefix is ambiguous. search explicitly, i.e: (1) $first.$$$requestedPrefix or (2) $first.$$$last "))
                }
              }
              else if (url22 == url) Failure(new PrefixAmbiguityException(s"prefix $requestedPrefix with the url $url is backed by both new API and old API." +
                " as a workaround, you can explicitly use the 2 APIs." +
                s" just specify your query predicate twice with '$$'. e.g: $first.$$$last & $first.$$$requestedPrefix"))
              else Failure(new PrefixAmbiguityException(s"prefix $requestedPrefix is ambiguous. used by URLs: (1) $url , (2) $url22 , search explicitly, i.e: (1) $first.$$$requestedPrefix or (2) $first.$$$last "))
            })
          }
        }
        case Failure(e: IllegalArgumentException) => p.failure(new UnretrievableIdentifierException(e.getMessage))
        case Failure(e: UnretrievableIdentifierException) => p.failure(e)
        case Failure(e) => {
          logger.info("CMWellRDFHelper.getUrlAndLastForPrefixAsync failed", e)
          p.success(infoton.path.drop("/meta/ns/".length))
        }
      }
    }

    p.future.map(first -> _)
  }
}