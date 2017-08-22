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


package cmwell.web.ld.cmw

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.actor.Actor.Receive
import cmwell.domain._
import cmwell.fts.{Settings => _, _}
import cmwell.util.string.Hash._
import cmwell.util.string._
import cmwell.util.concurrent._
import cmwell.util.collections.LoadingCacheExtensions
import cmwell.web.ld.exceptions.UnretrievableIdentifierException
import cmwell.ws.Settings
import com.google.common.cache.{Cache, CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import javax.inject._
import ld.cmw.PassiveFieldTypesCache
import logic.CRUDServiceFS
import wsutil.DirectFieldKey

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.parsing.json.JSON.{parseFull => parseJson}
import scala.util.{Failure, Success, Try}
import scala.collection.mutable.{Set => MSet}


object CMWellRDFHelper {

  class NoFallbackException extends RuntimeException("No fallback...")

  sealed trait PrefixState //to perform
  case object Create extends PrefixState
  case object Exists extends PrefixState
  case object Update extends PrefixState

  private sealed trait ByAlg
  private case object ByBase64 extends ByAlg
  private case object ByCrc32 extends ByAlg
}


@Singleton
class CMWellRDFHelper @Inject()(val crudServiceFS: CRUDServiceFS, injectedExecutionContext: ExecutionContext) extends LazyLogging {

  import CMWellRDFHelper._


  private[this] val oHashToUrlPermanentCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .build {
      new CacheLoader[String, String] {
        override def load(hash: String): String = {
          val vSet = hashToInfoton(hash, false).get.fields.get("url")
          require(vSet.size == 1, "only 1 url value is allowed!")
          vSet.head match {
            case FReference(url,_) => url
            case FString(url,_,_) => {
              logger.warn(s"got $url for 'url' field for hash=$hash, but it is `FString` instead of `FReference`")
              url
            }
            case weirdFValue => {
              logger.error(s"got weird value: $weirdFValue")
              weirdFValue.value.toString
            }
          }
        }
      }
    }

  private[this] val oUrlToHashPermanentCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .build {
      new CacheLoader[String, String] {
        override def load(url: String): String = {
          urlToInfoton(url, false).get.path.drop("/meta/ns/".length)
        }
      }
    }

  private[this] val oHashToMetaNsInfotonCache: LoadingCache[String,Infoton] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
      new CacheLoader[String,Infoton] {
        override def load(hash: String): Infoton = {
          val f = getMetaNsInfotonForHash(hash, false)(injectedExecutionContext)
          f.onComplete {
            case Success(Some(infoton)) => {
              infoton.fields.foreach { fields =>
                fields.get("prefix").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 prefix ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => oPrefixToHashCache.put(fv.value.asInstanceOf[String], hash)
                      case fv => logger.error(s"found a weird /meta/ns infoton without a string value: $infoton")
                    }
                  }
                }
                fields.get("url").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 url ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => oUrlToMetaNsInfotonCache.put(fv.value.asInstanceOf[String], infoton)
                      case fv => logger.error(s"found a weird /meta/ns infoton without a string value: $infoton")
                    }
                  }
                }
              }
            }
            case Success(None) => logger.trace(s"load for /meta/ns/$hash is empty")
            case Failure(e) => logger.error(s"load for $hash failed",e)
          }(scala.concurrent.ExecutionContext.Implicits.global)
          Await.result(f, 10.seconds).get
        }
      }
    }

  private[this] val oUrlToMetaNsInfotonCache: LoadingCache[String,Infoton] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build {
      new CacheLoader[String,Infoton] {
        override def load(url: String): Infoton = {
          val f = getMetaNsInfotonForUrl(url, false)(injectedExecutionContext)
          f.onComplete{
            case Success(Some(infoton)) => {
              val hash = infoton.path.drop("/meta/ns/".length)
              infoton.fields.foreach { fields =>
                fields.get("prefix").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 prefix ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => oPrefixToHashCache.put(fv.value.asInstanceOf[String], hash)
                      case fv => logger.error(s"found a weird /meta/ns infoton without a string value: $infoton")
                    }
                  }
                }
                fields.get("url").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 url ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => oHashToMetaNsInfotonCache.put(hash, infoton)
                      case fv => logger.error(s"found a weird /meta/ns infoton without a string value: $infoton")
                    }
                  }
                }
              }
            }
            case Success(None) => logger.trace(s"load for url = $url is empty")
            case Failure(e) => logger.error(s"load for url = $url failed",e)
          }(scala.concurrent.ExecutionContext.Implicits.global)
          Await.result(f, 10.seconds).get
        }
      }
    }

  private[this] val oPrefixToHashCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
      new CacheLoader[String,String] {
        override def load(prefix: String): String = {
          val f = getUrlAndLastForPrefixAsync(prefix, false)(injectedExecutionContext)
          Await.result(f, 10.seconds)._1
        }
      }
    }

  private[this] val oGraphToAliasCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .maximumSize(Settings.quadsCacheSize)
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
      new CacheLoader[String,String] {
        override def load(url: String): String = {
          val f = getAliasForQuadUrlAsyncActual(url, false)(injectedExecutionContext)
          f.onComplete{
            case Success(Some(alias)) => oAliasToGraphCache.put(alias,url)
            case Success(None) => logger.trace(s"load for graph: $url is empty")
            case Failure(e) => logger.error(s"load for $url failed",e)
          }(scala.concurrent.ExecutionContext.Implicits.global)
          Await.result(f, 10.seconds).get
        }
      }
    }

  private[this] val oAliasToGraphCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .maximumSize(Settings.quadsCacheSize)
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
      new CacheLoader[String,String] {
        override def load(alias: String): String = {
          val f = getQuadUrlForAliasAsyncActual(alias, false)(injectedExecutionContext)
          f.onComplete{
            case Success(graph) => oGraphToAliasCache.put(graph,alias)
            case Failure(e) => logger.error(s"load for $alias failed",e)
          }(scala.concurrent.ExecutionContext.Implicits.global)
          Await.result(f, 10.seconds)
        }
      }
    }

  private[this] val nHashToUrlPermanentCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .build {
      new CacheLoader[String, String] {
        override def load(hash: String): String = {
          val vSet = hashToInfoton(hash, true).get.fields.get("url")
          require(vSet.size == 1, "only 1 url value is allowed!")
          vSet.head match {
            case FReference(url,_) => url
            case FString(url,_,_) => {
              logger.warn(s"got $url for 'url' field for hash=$hash, but it is `FString` instead of `FReference`")
              url
            }
            case weirdFValue => {
              logger.error(s"got weird value: $weirdFValue")
              weirdFValue.value.toString
            }
          }
        }
      }
    }

  private[this] val nUrlToHashPermanentCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .build {
      new CacheLoader[String, String] {
        override def load(url: String): String = {
          urlToInfoton(url, true).get.path.drop("/meta/ns/".length)
        }
      }
    }

  private[this] val nHashToMetaNsInfotonCache: LoadingCache[String,Infoton] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
      new CacheLoader[String,Infoton] {
        override def load(hash: String): Infoton = {
          val f = getMetaNsInfotonForHash(hash, true)(injectedExecutionContext)
          f.onComplete {
            case Success(Some(infoton)) => {
              infoton.fields.foreach { fields =>
                fields.get("prefix").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 prefix ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => nPrefixToHashCache.put(fv.value.asInstanceOf[String], hash)
                      case fv => logger.error(s"found a weird /meta/ns infoton without a string value: $infoton")
                    }
                  }
                }
                fields.get("url").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 url ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => nUrlToMetaNsInfotonCache.put(fv.value.asInstanceOf[String], infoton)
                      case fv => logger.error(s"found a weird /meta/ns infoton without a string value: $infoton")
                    }
                  }
                }
              }
            }
            case Success(None) => logger.trace(s"load for /meta/ns/$hash is empty")
            case Failure(e) => logger.error(s"load for $hash failed",e)
          }(scala.concurrent.ExecutionContext.Implicits.global)
          Await.result(f, 10.seconds).get
        }
      }
    }

  private[this] val nUrlToMetaNsInfotonCache: LoadingCache[String,Infoton] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build {
      new CacheLoader[String,Infoton] {
        override def load(url: String): Infoton = {
          val f = getMetaNsInfotonForUrl(url, true)(injectedExecutionContext)
          f.onComplete{
            case Success(Some(infoton)) => {
              val hash = infoton.path.drop("/meta/ns/".length)
              infoton.fields.foreach { fields =>
                fields.get("prefix").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 prefix ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => nPrefixToHashCache.put(fv.value.asInstanceOf[String], hash)
                      case fv => logger.error(s"found a weird /meta/ns infoton without a string value: $infoton")
                    }
                  }
                }
                fields.get("url").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 url ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => nHashToMetaNsInfotonCache.put(hash, infoton)
                      case fv => logger.error(s"found a weird /meta/ns infoton without a string value: $infoton")
                    }
                  }
                }
              }
            }
            case Success(None) => logger.trace(s"load for url = $url is empty")
            case Failure(e) => logger.error(s"load for url = $url failed",e)
          }(scala.concurrent.ExecutionContext.Implicits.global)
          Await.result(f, 10.seconds).get
        }
      }
    }

  private[this] val nPrefixToHashCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
      new CacheLoader[String,String] {
        override def load(prefix: String): String = {
          val f = getUrlAndLastForPrefixAsync(prefix, true)(injectedExecutionContext)
          Await.result(f, 10.seconds)._1
        }
      }
    }

  private[this] val nGraphToAliasCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .maximumSize(Settings.quadsCacheSize)
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
      new CacheLoader[String,String] {
        override def load(url: String): String = {
          val f = getAliasForQuadUrlAsyncActual(url, true)(injectedExecutionContext)
          f.onComplete{
            case Success(Some(alias)) => nAliasToGraphCache.put(alias,url)
            case Success(None) => logger.trace(s"load for graph: $url is empty")
            case Failure(e) => logger.error(s"load for $url failed",e)
          }(scala.concurrent.ExecutionContext.Implicits.global)
          Await.result(f, 10.seconds).get
        }
      }
    }

  private[this] val nAliasToGraphCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .maximumSize(Settings.quadsCacheSize)
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
      new CacheLoader[String,String] {
        override def load(alias: String): String = {
          val f = getQuadUrlForAliasAsyncActual(alias, true)(injectedExecutionContext)
          f.onComplete{
            case Success(graph) => nGraphToAliasCache.put(graph,alias)
            case Failure(e) => logger.error(s"load for $alias failed",e)
          }(scala.concurrent.ExecutionContext.Implicits.global)
          Await.result(f, 10.seconds)
        }
      }
    }

  private[this] def hashToUrlPermanentCache(nbg: Boolean): LoadingCache[String,String] = {
    if(nbg) nHashToUrlPermanentCache
    else oHashToUrlPermanentCache
  }

  private[this] def urlToHashPermanentCache(nbg: Boolean): LoadingCache[String,String] = {
    if(nbg) nUrlToHashPermanentCache
    else oUrlToHashPermanentCache
  }

  private[this] def hashToMetaNsInfotonCache(nbg: Boolean): LoadingCache[String,Infoton] = {
    if(nbg) nHashToMetaNsInfotonCache
    else oHashToMetaNsInfotonCache
  }

  private[this] def urlToMetaNsInfotonCache(nbg: Boolean): LoadingCache[String,Infoton] = {
    if(nbg) nUrlToMetaNsInfotonCache
    else oUrlToMetaNsInfotonCache
  }

  private[this] def prefixToHashCache(nbg: Boolean): LoadingCache[String,String] = {
    if(nbg) nPrefixToHashCache
    else oPrefixToHashCache
  }

  private[this] def graphToAliasCache(nbg: Boolean): LoadingCache[String,String] = {
    if(nbg) nGraphToAliasCache
    else oGraphToAliasCache
  }

  private[this] def aliasToGraphCache(nbg: Boolean): LoadingCache[String,String] = {
    if(nbg) nAliasToGraphCache
    else oAliasToGraphCache
  }

  def loadNsCachesWith(infotons: Seq[Infoton], nbg: Boolean): Unit = infotons.foreach{ i =>

    def fieldsToOpt(fieldName: String)(fMap: Map[String,Set[FieldValue]]): Option[String] = {
      val uValSetOpt = fMap.get(fieldName)
      uValSetOpt.flatMap{ vSet =>
        if(vSet.size == 1) vSet.head match {
          case FString(value,_,_) => Some(value)
          case FReference(value,_) => Some(value)
          case weirdValue => {
            logger.error(s"weird value [$weirdValue] from meta ns for field [$fieldName]")
            None
          }
        }
        else {
          logger.error(s"ns $fieldName loading failed because amount != 1: $vSet")
          None
        }
      }
    }

    val urlOpt = i.fields.flatMap(fieldsToOpt("url"))
    val prefixOpt = i.fields.flatMap(fieldsToOpt("prefix"))
    val nsIdentifier = i.path.drop("/meta/ns/".length)

    hashToMetaNsInfotonCache(nbg).put(nsIdentifier,i)

    urlOpt.foreach{url =>
      hashToUrlPermanentCache(nbg).put(nsIdentifier,url)
      urlToMetaNsInfotonCache(nbg).put(url,i)
      urlToHashPermanentCache(nbg).put(url,nsIdentifier)
    }

    prefixOpt.foreach{prefix =>
      prefixToHashCache(nbg).put(prefix,nsIdentifier)
    }
  }

  def hashToUrl(hash: String, nbg: Boolean): Option[String] = Try(hashToUrlPermanentCache(nbg).getBlocking(hash)).toOption

  def urlToHash(url: String, nbg: Boolean): Option[String] = Try(urlToHashPermanentCache(nbg).getBlocking(url)).toOption

  def urlToHashAsync(url: String, nbg: Boolean)(implicit ec: ExecutionContext): Future[String] = urlToHashPermanentCache(nbg).getAsync(url)(ec)

  def getUrlAndLastForPrefix(prefix: String, nbg: Boolean)(implicit ec: ExecutionContext, awaitTimeout: FiniteDuration = Settings.esTimeout): (String,String) = {
    Await.result(getUrlAndLastForPrefixAsync(prefix,nbg),awaitTimeout)
  }

  def getUrlAndLastForPrefixAsync(prefix: String, nbg: Boolean, withFallBack: Boolean = true)(implicit ec: ExecutionContext): Future[(String,String)] = {
    val f = getUrlForPrefixAsyncActual(prefix,nbg,withFallBack)
    f.onComplete{
      case Success((url,last,Right(infoton))) => {
        hashToUrlPermanentCache(nbg).put(last,url)
        urlToHashPermanentCache(nbg).put(url,last)
        hashToMetaNsInfotonCache(nbg).put(last, infoton)
      }
      case Success((url,last,Left(infoton))) => {
        hashToUrlPermanentCache(nbg).put(last, url)
        urlToHashPermanentCache(nbg).put(url, last)
        logger.info(s"search for prefix $prefix succeeded but resulted with infoton: $infoton")
      }
      case Failure(nf: NoFallbackException) => logger.warn(s"getHashForPrefixAsync for $prefix failed (without fallback)")
      case Failure(e) => logger.error(s"getHashForPrefixAsync for $prefix failed",e)
    }
    f.map(t => t._1 -> t._2)
  }

  def hashToInfoton(hash: String, nbg: Boolean): Option[Infoton] = Try(hashToMetaNsInfotonCache(nbg).getBlocking(hash)).toOption

  def urlToInfoton(url: String, nbg: Boolean): Option[Infoton] = Try(urlToMetaNsInfotonCache(nbg).getBlocking(url)).toOption

  def hashToUrlAndPrefix(hash: String, nbg: Boolean): Option[(String,String)] =
    hashToInfoton(hash,nbg).flatMap {
      infoton => {
        infoton.fields.flatMap { fieldsMap =>
          fieldsMap.get("url").map { valueSet =>
            require(valueSet.size == 1, s"/meta/ns infoton must contain a single url value: $infoton")
            val uri = valueSet.head.value match {
              case url: String => {
                hashToUrlPermanentCache(nbg).put(hash,url)
                urlToHashPermanentCache(nbg).put(url,hash)
                url
              }
              case any => throw new RuntimeException(s"/meta/ns infoton's url field must contain a string value (got: ${any.getClass} for $infoton)")
            }
            val prefix = fieldsMap.get("prefix").map{ prefixSet =>
              require(valueSet.size == 1, s"/meta/ns infoton must contain a single prefix value: $infoton")
              prefixSet.head.value match {
                case prefix: String => prefix
                case any => throw new RuntimeException(s"/meta/ns infoton's url field must contain a string value (got: ${any.getClass} for $infoton)")
              }
            }
            uri -> prefix.getOrElse{
              logger.debug(s"retrieving prefix in the old method. we need to replace the infoton: $infoton")
              infoton.path.drop("/meta/ns/".length)
            }
          }
        }
      }
    }

  def prefixToHash(prefix: String, nbg: Boolean): Option[String] = Try(prefixToHashCache(nbg).getBlocking(prefix)).toOption

  /**
   * @param url as plain string
   * @return corresponding hash, or if it's a new namespace, will return an available hash to register the meta infoton at,
   *         paired with a boolean indicating if this is new or not/
   */
  def nsUrlToHash(url: String, nbg: Boolean): (String,PrefixState) = {
    def inner(hash: String): (String,PrefixState) = hashToInfoton(hash,nbg) match {
      case None => hash -> Create
      case Some(i) => {
        require(i.fields.isDefined, s"must have non empty fields ($i)")
        require(i.fields.get.contains("url"), s"must have url defined ($i)")
        val valSet = i.fields.get("url")
        require(valSet.size == 1, s"must have only 1 url ($i)")
        valSet.head match {
          case fv if fv.value.isInstanceOf[String] && fv.value.asInstanceOf[String] == url => hash -> Exists
          case fv if fv.value.isInstanceOf[String] && fv.value.asInstanceOf[String] != url => inner(crc32base64(hash))
          case fv => throw new RuntimeException(s"got weird value: $fv")
        }
      }
    }


    urlToHash(url,nbg) match {
      case Some(nsIdentifier) => nsIdentifier -> Exists
      case None => inner(crc32base64(url))
    }

//    //TODO: temp code (the `.map(...) expression`), once all meta is stabilized with prefixes, use the permanent cache method: urlToHash
//    urlToInfoton(url).map(
//      i => {//if prefix field is not defined (for old style /meta/ns infotons) return true, which means the field will be added
//        val prefixState = i.fields.flatMap(_.get("prefix")) match {
//          case None => Update
//          case _ => Exists
//        }
//        i.path.drop("/meta/ns/".length) -> prefixState
//      }
//    ).getOrElse(inner(crc32base64(url)))
  }

  def getAliasForQuadUrl(graphName: String, nbg: Boolean): Option[String] = Try(graphToAliasCache(nbg).get(graphName)).toOption

  def getAliasForQuadUrlAsync(graph: String, nbg: Boolean)(implicit ec: ExecutionContext): Future[Option[String]] = {
    val f = getAliasForQuadUrlAsyncActual(graph, nbg)
    f.onComplete{
      case Success(Some(alias)) => {
        graphToAliasCache(nbg).put(graph,alias)
        aliasToGraphCache(nbg).put(alias,graph)
      }
      case Success(None) => logger.info(s"graph: $graph could not be retrieved")
      case Failure(e) => logger.error(s"getAliasForQuadUrlAsync for $graph failed",e)
    }
    f
  }

  def getQuadUrlForAlias(alias: String, nbg: Boolean): Option[String] = Try(aliasToGraphCache(nbg).get(alias)).toOption

  def getQuadUrlForAliasAsync(alias: String, nbg: Boolean)(implicit ec: ExecutionContext): Future[String] = {
    val f = getQuadUrlForAliasAsyncActual(alias,nbg)
    f.onComplete{
      case Success(graph) => {
        aliasToGraphCache(nbg).put(alias,graph)
        graphToAliasCache(nbg).put(graph,alias)
      }
      case Failure(e) => logger.error(s"getQuadUrlForAliasAsync for $alias failed",e)
    }
    f
  }

  def getQuadUrlForAliasAsyncActual(alias: String, nbg: Boolean)(implicit ec: ExecutionContext): Future[String] = {
    crudServiceFS.search(
      pathFilter = Some(PathFilter("/meta/quad",false)),
      fieldFilters = Some(FieldFilter(Should,Equals,"alias",alias)),
      datesFilter = None,
      withData = true,
      nbg = nbg).map {
      case SearchResults(_,_,total,_,length,infotons,_) => {
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
          require(urls.size == 1, s"must have exactly 1 URI: ${urls.mkString("[",",","]")}, fix any of this by using")

          require(urls.head.value.isInstanceOf[String], "url value not a string")
          urls.head.value.asInstanceOf[String]
        }
        url
      }
    }
  }
  
  // in case of ambiguity between meta/ns infotons with same url, this will return the one that was not auto-generated
  def getTheNonGeneratedMetaNsInfoton(url: String, infotons: Seq[Infoton], nbg: Boolean): Infoton = {
    require(infotons.nonEmpty)
    val it = Iterator.iterate(cmwell.util.string.Hash.crc32base64(url))(cmwell.util.string.Hash.crc32base64)
    it.take(infotons.length+1).foldLeft(infotons) { case (z,h) => if(z.size == 1) z else infotons.filterNot(_.name==h) }.head
  }

  // private[this] section:

  private[this] def getUrlForPrefixAsyncActual(prefix: String, nbg: Boolean, withFallBack: Boolean = true)(implicit ec: ExecutionContext): Future[(String,String,Either[Infoton,Infoton])] = {

    @inline def prefixRequirement(requirement: Boolean, message: => String): Unit = {
      if (!requirement)
        throw new UnretrievableIdentifierException(message)
    }

    def ensureRequirementsAndOutputPair(infotons: Seq[Infoton], infotonToEither: Infoton => Either[Infoton,Infoton]): (String,String,Either[Infoton,Infoton]) = {
      prefixRequirement(infotons.nonEmpty, s"the prefix $prefix is not associated to any namespace")

      val triple = {
        val byUrl = infotons.groupBy(_.fields.flatMap(_.get("url")))
        prefixRequirement(byUrl.forall(_._2.size == 1), s"group by url must be unique: $byUrl")

        lazy val nsUris = {
          val xs = byUrl.flatMap{case (uriOpt,is) => is.map(uriOpt -> _)}
          xs.collect{
            case (Some(kSet),i) => {
              val name = i.name
              val uri = kSet.collect{
                case f: FString => f.value
                case f: FReference => f.value
              }.head
              "id: \"" + name + "\" referenced by: " + uri
            }
          }
        }

        prefixRequirement(byUrl.size == 1, s"namespace URIs must be unambiguous: ${nsUris.mkString("[",", ","]")}")

        val urlSet = byUrl.keys.headOption.flatMap(identity)
        prefixRequirement(urlSet.isDefined, s"url must have keys defined: $byUrl, $urlSet")

        val iSeq = byUrl(urlSet)
        prefixRequirement(iSeq.size == 1, s"got more than 1 infoton ??? $iSeq")

        val urls = urlSet.toSeq.flatMap(identity)
        prefixRequirement(urls.size == 1, s"""must have exactly 1 URI: ${urls.mkString("[", ",", "]")}, fix any of this by using the meta operation: (POST to _in `<> <cmwell://meta/ns#NEW_PREFIX> "NS_URI" .`)""")

        prefixRequirement(urls.head.value.isInstanceOf[String], "url value not a string")
        val url = urls.head.value.asInstanceOf[String]
        val i = iSeq.head
        val last = i.path.drop("/meta/ns/".length)
        val either = infotonToEither(i)
        (url, last, either)
      }
      triple
    }

    crudServiceFS.search(
      pathFilter = Some(PathFilter("/meta/ns", false)),
      fieldFilters = Some(FieldFilter(Should, Equals, "prefix", prefix)),
      datesFilter = None,
      withData = true,
      nbg = nbg).flatMap {
      case SearchResults(_, _, _, _, _, infotons, _) if infotons.exists(_.fields.isDefined) =>
        Future(ensureRequirementsAndOutputPair(infotons.filter(_.fields.isDefined),Right.apply))
      case SearchResults(_, _, _, _, _, infotons, _) if withFallBack =>
        crudServiceFS.getInfoton("/meta/ns/" + prefix, None, None, nbg).map { iOpt =>
          ensureRequirementsAndOutputPair(iOpt.map(_.infoton).toSeq, Left.apply)
        }
      case _ => throw new NoFallbackException
    }
  }

  private[this] def getMetaNsInfotonForHash(hash: String, nbg: Boolean)(implicit ec: ExecutionContext): Future[Option[Infoton]] =
    crudServiceFS.getInfoton("/meta/ns/" + hash, None, None, nbg).map(_.map(_infoton))

  private[this] def getMetaNsInfotonForUrl(url: String, nbg: Boolean)(implicit ec: ExecutionContext): Future[Option[Infoton]] =
    crudServiceFS.search(
      pathFilter =  Some(PathFilter("/meta/ns", descendants = false)),
      fieldFilters = Some(FieldFilter(Must,Equals,"url",url)),
      datesFilter = None,
      withData = true,
      nbg = nbg).map{
      searchResults => {
        searchResults.infotons match {
          case Nil => None
          case infotons => Some(getTheNonGeneratedMetaNsInfoton(url, infotons, nbg))
        }
      }
    }

  // todo there must be a better way to achieve this. making ContentPortion abstract case class or such.
  private[this] def _infoton(content: ContentPortion) = content match {
    case Everything(i) => i
    case UnknownNestedContent(i) => i
    case _ => ???
  }

  private[this] def getAliasForQuadUrlAsyncActual(graphName: String, nbg: Boolean)(implicit ec: ExecutionContext): Future[Option[String]] = {
    getAliasForQuadUrlAsync(graphName,nbg,ByBase64).flatMap{
      case some: Some[String] => Future.successful(some)
      case None => getAliasForQuadUrlAsync(graphName,nbg,ByCrc32)
    }
  }

  private[this] def getAliasForQuadUrlAsync(graphName: String, nbg: Boolean, byAlg: ByAlg = ByBase64)(implicit ec: ExecutionContext): Future[Option[String]] = {
    val hashByAlg: String = byAlg match {
      case ByBase64 => Base64.encodeBase64URLSafeString(graphName)
      case ByCrc32 => Hash.crc32(graphName)
    }

    crudServiceFS.getInfoton("/meta/quad/" + hashByAlg, None, None, nbg).flatMap{
      case Some(Everything(i)) => Future.successful[Option[String]]{
        i.fields.flatMap(_.get("alias").flatMap {
          set => {
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

