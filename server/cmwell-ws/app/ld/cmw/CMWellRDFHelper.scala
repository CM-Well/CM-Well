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
package cmwell.web.ld.cmw

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import cmwell.domain._
import cmwell.fts.{Equals, FieldFilter, PathFilter, Should}
import cmwell.util.string.Hash._
import cmwell.util.string._
import cmwell.ws.Settings
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import javax.inject._

import akka.util.Timeout
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import ld.cmw.TimeBasedAccumulatedNsCache
import ld.exceptions.{ConflictingNsEntriesException, ServerComponentNotAvailableException}
import logic.CRUDServiceFS

import scala.concurrent._
import scala.concurrent.ExecutionContext.{global => globalExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object CMWellRDFHelper {

  sealed trait PrefixResolvingError extends RuntimeException
  object PrefixResolvingError {
    def apply(prefix: String) = new RuntimeException(s"Failed to resolve prefix [$prefix]") with PrefixResolvingError
    def apply(prefix: String, cause: Throwable) =
      new RuntimeException(s"Failed to resolve prefix [$prefix]", cause) with PrefixResolvingError
  }

  sealed trait PrefixState //to perform
  case object Create extends PrefixState
  case object Exists extends PrefixState
//  case object Update extends PrefixState

  private sealed trait ByAlg
  private case object ByBase64 extends ByAlg
  private case object ByCrc32 extends ByAlg
}
@Singleton
class CMWellRDFHelper @Inject()(val crudServiceFS: CRUDServiceFS,
                                injectedExecutionContext: ExecutionContext,
                                actorSystem: ActorSystem)
    extends LazyLogging {

  import CMWellRDFHelper._

  implicit val timeout = akka.util.Timeout(10.seconds) // TODO IS THIS OK?
  implicit val ec = injectedExecutionContext // TODO IS THIS OK?

  val newestGreatestMetaNsCacheImpl =
    TimeBasedAccumulatedNsCache(Map.empty, 0L, 2.minutes, crudServiceFS)(injectedExecutionContext, actorSystem)

  private def validateInfoton(infoton: Infoton): Try[(String, String)] = {
    if (!infoton.systemFields.path.matches("/meta/ns/[^/]+"))
      Failure(new IllegalStateException(s"weird looking path for /meta/ns infoton [${infoton.systemFields.path}/${infoton.uuid}]"))
    else if (infoton.fields.isEmpty)
      Failure(new IllegalStateException(s"no fields found for /meta/ns infoton [${infoton.systemFields.path}/${infoton.uuid}]"))
    else {
      val f = infoton.fields.get
      metaNsFieldsValidator(infoton, f, "prefix").flatMap { p =>
        metaNsFieldsValidator(infoton, f, "url").map(_ -> p)
      }
    }
  }

  private def metaNsFieldsValidator(i: Infoton, fields: Map[String, Set[FieldValue]], field: String): Try[String] = {
    fields
      .get(field)
      .fold[Try[String]](
        Failure(new IllegalStateException(s"$field field not found for /meta/ns infoton [${i.systemFields.path}/${i.uuid}]"))
      ) { values =>
        if (values.isEmpty)
          Failure(
            new IllegalStateException(s"empty value set for $field field in /meta/ns infoton [${i.systemFields.path}/${i.uuid}]")
          )
        else if (values.size > 1)
          Failure(
            new IllegalStateException(
              s"multiple values ${values.mkString("[,", ",", "]")} for $field field in /meta/ns infoton [${i.systemFields.path}/${i.uuid}]"
            )
          )
        else
          values.head.value match {
            case s: String => Success(s)
            case x =>
              Failure(
                new IllegalStateException(
                  s"found a weird /meta/ns infoton without a string value [${x.getClass.getSimpleName}] for prefix: [$i]"
                )
              )
          }
      }
  }

  private[this] val looksLikeHashedNsIDRegex = "[A-Za-z0-9\\-_]{5,7}"
  private[this] val transformFuncURL2URL: Try[String] => Option[String] = {
    case Success(url)                       => Some(url)
    case Failure(_: NoSuchElementException) => None
    case Failure(e)                         => throw e
  }
  private[this] val transformFuncURLAndPrefix2URLAndPrefix: Try[(String, String)] => Option[(String, String)] = {
    case Success(urlAndPrefix)              => Some(urlAndPrefix)
    case Failure(_: NoSuchElementException) => None
    case Failure(e)                         => throw e
  }

  @inline def invalidate(nsID: String)(implicit timeout: Timeout): Future[Unit] =
    newestGreatestMetaNsCacheImpl.invalidate(nsID)
  @inline def invalidateAll()(implicit timeout: Timeout): Future[Unit] = newestGreatestMetaNsCacheImpl.invalidateAll()

  @deprecated("API may falsely return None on first calls for some value", "Quetzal")
  def hashToUrl(nsID: String, timeContext: Option[Long]): Option[String] = {
    val f = hashToUrlAsync(nsID, timeContext)
    f.value.fold[Option[String]] {
      if (nsID.matches(looksLikeHashedNsIDRegex))
        // Await is OK here, as it is very rare (first fetch of an id which was not found in cache)
        Await.ready(f, 9.seconds).value.flatMap(transformFuncURL2URL)
      else
        None
    }(transformFuncURL2URL)
  }

  def hashToUrlAsync(hash: String, timeContext: Option[Long])(implicit ec: ExecutionContext): Future[String] =
    newestGreatestMetaNsCacheImpl
      .get(hash, timeContext)
      .transform {
        case f @ Failure(_: NoSuchElementException)        => f.asInstanceOf[Try[String]]
        case f @ Failure(_: ConflictingNsEntriesException) => f.asInstanceOf[Try[String]]
        case Success((url, _))                             => Success(url)
        case Failure(e)                                    => Failure(ServerComponentNotAvailableException(s"hashToUrlAsync failed for [$hash]", e))
      }(ec)

  @deprecated("API may falsely return None on first calls for some value", "Quetzal")
  def urlToHash(url: String, timeContext: Option[Long]): Option[String] =
    newestGreatestMetaNsCacheImpl.getByURL(url, timeContext).value match {
      case None => throw ServerComponentNotAvailableException("Internal old API (urlToHash) used on id [" + url +
        "], which was not yet in cache. subsequent requests should succeed eventually. Call should be migrated to new API")
      case Some(Success(nsID))                      => Some(nsID)
      case Some(Failure(_: NoSuchElementException)) => None
      case Some(Failure(e))                         => throw e
    }

  def urlToHashAsync(url: String, timeContext: Option[Long])(implicit ec: ExecutionContext): Future[String] =
    newestGreatestMetaNsCacheImpl
      .getByURL(url, timeContext)
      .transform {
        case f @ Failure(_: NoSuchElementException)        => f.asInstanceOf[Try[String]]
        case f @ Failure(_: ConflictingNsEntriesException) => f.asInstanceOf[Try[String]]
        case Failure(e)                                    => Failure(ServerComponentNotAvailableException(s"urlToHashAsync failed for [$url]", e))
        case success                                       => success
      }(ec)

  def getIdentifierForPrefixAsync(prefix: String,
                                  timeContext: Option[Long])(implicit ec: ExecutionContext): Future[String] =
    newestGreatestMetaNsCacheImpl
      .getByPrefix(prefix, timeContext)
      .transform {
        case f @ Failure(_: NoSuchElementException)        => f.asInstanceOf[Try[String]]
        case f @ Failure(_: ConflictingNsEntriesException) => f.asInstanceOf[Try[String]]
        case Failure(e) =>
          Failure(ServerComponentNotAvailableException(s"getIdentifierForPrefixAsync failed for [$prefix]", e))
        case success => success
      }(ec)

  @deprecated("API may falsely return None on first calls for some value", "Quetzal")
  def hashToUrlAndPrefix(nsID: String, timeContext: Option[Long]): Option[(String, String)] = {
    val f = newestGreatestMetaNsCacheImpl.get(nsID, timeContext)
    f.value.fold[Option[(String, String)]] {
      if (nsID.matches(looksLikeHashedNsIDRegex))
        // Await is OK here, as it is very rare (first fetch of an id which was not found in cache)
        Await.ready(f, 9.seconds).value.flatMap(transformFuncURLAndPrefix2URLAndPrefix)
      else
        None
    }(transformFuncURLAndPrefix2URLAndPrefix)
  }

  /**
    * @param url as plain string
    * @return corresponding hash, or if it's a new namespace, will return an available hash to register the meta infoton at,
    *         paired with a boolean indicating if this is new or not/
    */
  def nsUrlToHash(url: String, timeContext: Option[Long]): (String, PrefixState) = {

    def inner(hash: String): Future[(String, PrefixState)] =
      hashToUrlAsync(hash, timeContext)(globalExecutionContext).transformWith {
        case Success(`url`)                     => Future.successful(hash -> Exists)
        case Failure(_: NoSuchElementException) => Future.successful(hash -> Create)
        case Failure(err) =>
          Future.failed(new Exception(s"nsUrlToHash.inner failed for url [$url] and hash [$hash]", err))
        case Success(notSameUrl /* not same url */ ) => {
          val doubleHash = crc32base64(hash)
          logger.warn(s"double hashing url's [$url] hash [$hash] to [$doubleHash] because not same as [$notSameUrl]")
          inner(doubleHash)
        }
      }(globalExecutionContext)

    Await.result(
      urlToHashAsync(url, timeContext).transformWith {
        case Success(nsIdentifier)              => Future.successful(nsIdentifier -> Exists)
        case Failure(_: NoSuchElementException) => inner(crc32base64(url))
        case Failure(somethingBad) =>
          logger.error(s"nsUrlToHash failed for url [$url]", somethingBad)
          Future.failed(somethingBad)
      },
      Duration.Inf
    ) //FIXME: Await...
  }

  @inline def hashIterator(url: String) =
    Iterator.iterate(cmwell.util.string.Hash.crc32base64(url))(cmwell.util.string.Hash.crc32base64)

  // in case of ambiguity between meta/ns infotons with same url, this will return the one that was not auto-generated
  def getTheFirstGeneratedMetaNsInfoton(url: String,
                                        infotons: Seq[Infoton],
                                        timeContext: Option[Long]): Future[Infoton] = {
    require(infotons.nonEmpty)

    val hashSet = infotons.view.map(_.systemFields.name).to(Set)
    val hashChain = hashIterator(url).take(infotons.length + 5).toStream

    // find will return the first (shortest compute chain) hash
    hashChain.find(hashSet) match {
      case Some(h) =>
        Future.successful(infotons.find(_.systemFields.name == h).get) //get is safe here because `hashSet` was built from infotons names
      case None =>
        /* if we were not able to find a suitable hash
         * that back a /meta/ns infoton from the given
         * Seq, it means one of two things:
         * Either we have so many collisions in /meta/ns
         * that all the hashes computed points to other
         * namespaces,
         * or that we have old style unhashed identifiers
         * in /meta/ns.
         * Giving precedence to hashed versions, since from
         * now on (Jan 2018) old style isn't supported,
         * and should have been migrated to hashed identifiers.
         * Only if we fail to find such, we will arbitrarily
         * choose the first in lexicographic order from the Seq
         */
        logger.warn(s"hashChain ${hashChain.mkString("[", ", ", "]")} did not contain a valid identifier for ${hashSet
          .mkString("[", ", ", "]")}")
        getFirstHashForNsURL(url, infotons, timeContext).transform {
          case Success(Right(i)) => Success(i)
          case Success(Left(hash)) =>
            Failure(
              new IllegalStateException(
                s"There's an unoccupied hash [$hash] that can fit [$url]. Manual data repair is required. please also consider ns ambiguities [$infotons]"
              )
            )
          case Failure(err) =>
            val first = infotons.minBy(_.systemFields.name)
            logger.warn(
              s"Was unable to validate any of the given infotons [$infotons], choosing the first in lexicographic order [${first.systemFields.path}]",
              err
            )
            Success(first)
        }(globalExecutionContext)
    }
  }

  def hashToInfotonAsync[T](hash: String)(out: (Infoton, (String, String)) => T)(ec: ExecutionContext): Future[T] =
    crudServiceFS
      .getInfotonByPathAsync(s"/meta/ns/$hash")
      .transform {
        case Failure(err)      => Failure(new IllegalStateException(s"could not load /meta/ns/$hash", err))
        case Success(EmptyBox) => Failure(new NoSuchElementException(s"could not load /meta/ns/$hash"))
        case Success(BoxedFailure(err)) =>
          Failure(new IllegalStateException(s"could not load /meta/ns/$hash from IRW", err))
        case Success(FullBox(infoton)) =>
          validateInfoton(infoton).transform(
            urlPrefix => Try(out(infoton, urlPrefix)),
            e => Failure(new IllegalStateException(s"loaded invalid infoton for [$hash] / [$infoton]", e))
          )
      }(ec)

  def getFirstHashForNsURL(url: String,
                           infotons: Seq[Infoton],
                           timeContext: Option[Long]): Future[Either[String, Infoton]] = {
    val it = hashIterator(url)
    def foldWhile(usedHashes: Set[String]): Future[Either[String, Infoton]] = {
      val hash = it.next()
      if (usedHashes(hash))
        Future.failed(
          new IllegalStateException(
            s"found a hash cycle starting with [$hash] without getting a proper infoton for [$url]"
          )
        )
      else
        hashToUrlAsync(hash, timeContext)(globalExecutionContext).transformWith {
          case Success(`url`) =>
            infotons
              .find(_.systemFields.name == hash)
              .fold[Future[Either[String, Infoton]]] {
                logger.error(s"hash [$hash] returned the right url [$url], but was not found in original seq?!?!?")
                // getting the correct infoton anyway:
                hashToInfotonAsync(hash)((infoton, _) => infoton)(globalExecutionContext)
                  .map(Right.apply)(globalExecutionContext)
              }((Future.successful[Either[String, Infoton]] _).compose(Right.apply))
          case Success(someOtherUrl) =>
            // Yes. I am aware the log will be printed in every iteration of the recursion. That's the point.
            logger.warn(
              s"ns collision detected. Hash [$hash] points to [$someOtherUrl] but can be computed from [$url]. " +
                s"This might be the result of abusing the namespace mechanism, which is not supposed to be used with too many namespaces. " +
                s"Since current implementation uses a hash with 32 bits of entropy, it means that if you have more than 64K namespaces, " +
                s"you'll have over 50% chance of collision. This is way above what should be necessary, and unless you are very unlucky, " +
                s"which in this case you'll have a single namespace causing this log to be printed once in a while for the same namespace, " +
                s"but can probably ignore it, it is likely that you are abusing CM-Well in ways this humble developer didn't thought reasonable. " +
                s"In this case, either refactor using a hash function with more bits of entropy is needed (may I recommend `xxhash64`, " +
                s"which you'll probably find at `cmwell.util.string.Hash.xxhash64`, assuming 64 bits of entropy will suffice), or, " +
                s"reconsider your use-case as it is probably wrong, or buggy. Please inspect the ns bookkeeping infotons under /meta/ns."
            )
            foldWhile(usedHashes + hash)
          case Failure(err) =>
            logger.warn(s"could not load hash [$hash] for [$infotons]", err)
            Future.successful(Left(hash))
        }(globalExecutionContext)
    }
    foldWhile(Set.empty)
  }

//   ^
//  /|\
//   |
//   └-- NS caching section
//
//   ┌-- QUADS caching section
//   |
//  \|/
//   v

  private[this] val graphToAliasCache: LoadingCache[String, String] = CacheBuilder
    .newBuilder()
    .maximumSize(Settings.quadsCacheSize)
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build {
      new CacheLoader[String, String] {
        override def load(url: String): String = {
          val f = getAliasForQuadUrlAsyncActual(url)(injectedExecutionContext)
          f.onComplete {
            case Success(Some(alias)) => aliasToGraphCache.put(alias, url)
            case Success(None)        => logger.trace(s"load for graph: $url is empty")
            case Failure(e)           => logger.error(s"load for $url failed", e)
          }(scala.concurrent.ExecutionContext.Implicits.global)
          Await.result(f, 10.seconds).get
        }
      }
    }

  private[this] val aliasToGraphCache: LoadingCache[String, String] = CacheBuilder
    .newBuilder()
    .maximumSize(Settings.quadsCacheSize)
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build {
      new CacheLoader[String, String] {
        override def load(alias: String): String = {
          val f = getQuadUrlForAliasAsyncActual(alias)(injectedExecutionContext)
          f.onComplete {
            case Success(graph) => graphToAliasCache.put(graph, alias)
            case Failure(e)     => logger.error(s"load for $alias failed", e)
          }(scala.concurrent.ExecutionContext.Implicits.global)
          Await.result(f, 10.seconds)
        }
      }
    }

  def getAliasForQuadUrl(graphName: String): Option[String] = Try(graphToAliasCache.get(graphName)).toOption

  def getAliasForQuadUrlAsync(graph: String)(implicit ec: ExecutionContext): Future[Option[String]] = {
    val f = getAliasForQuadUrlAsyncActual(graph)
    f.onComplete {
      case Success(Some(alias)) => {
        graphToAliasCache.put(graph, alias)
        aliasToGraphCache.put(alias, graph)
      }
      case Success(None) => logger.info(s"graph: $graph could not be retrieved")
      case Failure(e)    => logger.error(s"getAliasForQuadUrlAsync for $graph failed", e)
    }
    f
  }

  def getQuadUrlForAlias(alias: String): Option[String] = Try(aliasToGraphCache.get(alias)).toOption

  def getQuadUrlForAliasAsync(alias: String)(implicit ec: ExecutionContext): Future[String] = {
    val f = getQuadUrlForAliasAsyncActual(alias)
    f.onComplete {
      case Success(graph) => {
        aliasToGraphCache.put(alias, graph)
        graphToAliasCache.put(graph, alias)
      }
      case Failure(e) => logger.error(s"getQuadUrlForAliasAsync for $alias failed", e)
    }
    f
  }

  def getQuadUrlForAliasAsyncActual(alias: String)(implicit ec: ExecutionContext): Future[String] = {
    crudServiceFS
      .search(pathFilter = Some(PathFilter("/meta/quad", false)),
              fieldFilters = Some(FieldFilter(Should, Equals, "alias", alias)),
              datesFilter = None,
              withData = true)
      .map {
        case SearchResults(_, _, total, _, length, infotons, _) => {
          require(total != 0, s"the alias $alias is not associated to any graph")

          val url = {
            val byUrl = infotons.groupBy(_.fields.flatMap(_.get("url")))
            require(byUrl.size == 1, s"group by url must be unambiguous: $byUrl")

            //TODO: eliminate intentional duplicates (same graph in `/meta/quad/crc32` & `/meta/quad/base64`), i.e. delete crc32 version as a side effect.
            //TODO: if only crc32 version exists, replace it with base64 version
            //i.e: byUrl.valuesIterator.foreach(...)

            val urlSet = byUrl.keys.headOption.flatMap(identity)
            require(urlSet.isDefined, s"url must have keys defined: $byUrl, $urlSet")

            val urls = urlSet.toSeq.flatMap(identity)
            require(urls.size == 1,
                    s"must have exactly 1 URI: ${urls.mkString("[", ",", "]")}, fix any of this by using")

            require(urls.head.value.isInstanceOf[String], "url value not a string")
            urls.head.value.asInstanceOf[String]
          }
          url
        }
      }
  }

  private[this] def getAliasForQuadUrlAsyncActual(
    graphName: String
  )(implicit ec: ExecutionContext): Future[Option[String]] = {
    getAliasForQuadUrlAsync(graphName, ByBase64).flatMap {
      case some: Some[String] => Future.successful(some)
      case None               => getAliasForQuadUrlAsync(graphName, ByCrc32)
    }
  }

  private[this] def getAliasForQuadUrlAsync(graphName: String, byAlg: ByAlg = ByBase64)(
    implicit ec: ExecutionContext
  ): Future[Option[String]] = {
    val hashByAlg: String = byAlg match {
      case ByBase64 => Base64.encodeBase64URLSafeString(graphName)
      case ByCrc32  => Hash.crc32(graphName)
    }

    crudServiceFS.getInfoton("/meta/quad/" + hashByAlg, None, None).flatMap {
      case Some(Everything(i)) =>
        Future.successful[Option[String]] {
          i.fields.flatMap(_.get("alias").flatMap { set =>
            {
              val aliases = set.collect {
                case FString(value, _, _) => value
              }
              if (aliases.size > 1) {
                logger.warn(s"quads ambiguity alert: $aliases")
              }
              aliases.headOption
            }
          })
        }
      case _ => Future.successful(None)
    }
  }

}
