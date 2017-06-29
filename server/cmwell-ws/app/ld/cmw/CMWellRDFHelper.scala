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

import ld.cmw.PassiveFieldTypesCache
import logic.CRUDServiceFS
import wsutil.DirectFieldKey

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.parsing.json.JSON.{parseFull => parseJson}
import scala.util.{Failure, Success, Try}
import scala.collection.mutable.{Set => MSet}


/**
 * Created with IntelliJ IDEA.
 * User: gilad
 * Date: 7/14/13
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 */
object CMWellRDFHelper extends LazyLogging {

  def loadNsCachesWith(infotons: Seq[Infoton]): Unit = infotons.foreach{ i =>

    def fieldsToOpt(fieldName: String)(fMap: Map[String,Set[FieldValue]]): Option[String] = {
      val uValSetOpt = fMap.get(fieldName)
      uValSetOpt.flatMap{ vSet =>
        if(vSet.size == 1) vSet.headOption.flatMap {
          case FString(value,_,_) => Some(value)
          case FReference(value,_) => Some(value)
          case weirdValue => {
            logger.error(s"weird value from meta ns: $weirdValue")
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

    hashToMetaNsInfotonCache.put(nsIdentifier,i)

    urlOpt.foreach{url =>
      hashToUrlPermanentCache.put(nsIdentifier,url)
      urlToMetaNsInfotonCache.put(url,i)
      urlToHashPermanentCache.put(url,nsIdentifier)
    }

    prefixOpt.foreach{prefix =>
      prefixToHashCache.put(prefix,nsIdentifier)
    }
  }

  /**
   * 
   */
  private[this] val hashToMetaNsInfotonCache: LoadingCache[String,Infoton] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
      new CacheLoader[String,Infoton] {
        override def load(hash: String): Infoton = {
          val f = getMetaNsInfotonForHash(hash)
          f.onComplete {
            case Success(Some(infoton)) => {
              infoton.fields.foreach { fields =>
                fields.get("prefix").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 prefix ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => prefixToHashCache.put(fv.value.asInstanceOf[String], hash)
                      case fv => logger.error(s"found a weird /meta/ns infoton without a string value: $infoton")
                    }
                  }
                }
                fields.get("url").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 url ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => urlToMetaNsInfotonCache.put(fv.value.asInstanceOf[String], infoton)
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

  private[this] val hashToUrlPermanentCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .build {
    new CacheLoader[String, String] {
      override def load(hash: String): String = {
        val vSet = hashToInfoton(hash).get.fields.get("url")
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

  private[this] val urlToMetaNsInfotonCache: LoadingCache[String,Infoton] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build {
      new CacheLoader[String,Infoton] {
        override def load(url: String): Infoton = {
          val f = getMetaNsInfotonForUrl(url)
          f.onComplete{
            case Success(Some(infoton)) => {
              val hash = infoton.path.drop("/meta/ns/".length)
              infoton.fields.foreach { fields =>
                fields.get("prefix").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 prefix ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => prefixToHashCache.put(fv.value.asInstanceOf[String], hash)
                      case fv => logger.error(s"found a weird /meta/ns infoton without a string value: $infoton")
                    }
                  }
                }
                fields.get("url").foreach {
                  vSet => {
                    require(vSet.size == 1, s"must have only 1 url ($infoton)")
                    vSet.head match {
                      case fv if fv.value.isInstanceOf[String] => hashToMetaNsInfotonCache.put(hash, infoton)
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

  private[this] val urlToHashPermanentCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .build {
    new CacheLoader[String, String] {
      override def load(url: String): String = {
        urlToInfoton(url).get.path.drop("/meta/ns/".length)
      }
    }
  }

  def hashToUrl(hash: String): Option[String] = Try(hashToUrlPermanentCache.getBlocking(hash)).toOption

  def urlToHash(url: String): Option[String] = Try(urlToHashPermanentCache.getBlocking(url)).toOption

  def urlToHashAsync(url: String)(implicit ec: ExecutionContext): Future[String] = urlToHashPermanentCache.getAsync(url)(ec)

  /**
   * 
   */
  private[this] val prefixToHashCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
      new CacheLoader[String,String] {
        override def load(prefix: String): String = {
          val f = getUrlAndLastForPrefixAsync(prefix)
          Await.result(f, 10.seconds)._1
        }
      }
    }

  /**
   * 
   * @param prefix
   * @param awaitTimeout
   * @return
   */
  def getUrlAndLastForPrefix(prefix: String)(implicit awaitTimeout: FiniteDuration = Settings.esTimeout): (String,String) = {
    Await.result(getUrlAndLastForPrefixAsync(prefix),awaitTimeout)
  }

  /**
   * 
   * @param prefix
   * @param withFallBack
   * @return
   */
  def getUrlAndLastForPrefixAsync(prefix: String, withFallBack: Boolean = true): Future[(String,String)] = {
    val f = getUrlForPrefixAsyncActual(prefix,withFallBack)
    f.onComplete{
      case Success((url,last,Right(infoton))) => {
        hashToUrlPermanentCache.put(last,url)
        urlToHashPermanentCache.put(url,last)
        hashToMetaNsInfotonCache.put(last, infoton)
      }
      case Success((url,last,Left(infoton))) => {
        hashToUrlPermanentCache.put(last, url)
        urlToHashPermanentCache.put(url, last)
        logger.info(s"search for prefix $prefix succeeded but resulted with infoton: $infoton")
      }
      case Failure(nf: NoFallbackException) => logger.warn(s"getHashForPrefixAsync for $prefix failed (without fallback)")
      case Failure(e) => logger.error(s"getHashForPrefixAsync for $prefix failed",e)
    }(scala.concurrent.ExecutionContext.Implicits.global)
    f.map(t => t._1 -> t._2)(scala.concurrent.ExecutionContext.Implicits.global)
  }

  /**
   * 
   * @param prefix
   * @param withFallBack
   * @return
   */
  private[this] def getUrlForPrefixAsyncActual(prefix: String, withFallBack: Boolean = true): Future[(String,String,Either[Infoton,Infoton])] = {

    @inline def prefixRequirement(requirement: Boolean, message: => String): Unit = {
      if (!requirement)
        throw new UnretrievableIdentifierException(message)
    }

    import scala.concurrent.ExecutionContext.Implicits.global

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

    CRUDServiceFS.search(
      pathFilter = Some(PathFilter("/meta/ns", false)),
      fieldFilters = Some(/*MultiFieldFilter(Must,List(*/FieldFilter(Should, Equals, "prefix", prefix)/*,FieldFilter(Should, Equals, "s$prefix", prefix)))*/),
      datesFilter = None,
      withData = true).flatMap {
      case SearchResults(_, _, _, _, _, infotons, _) if infotons.exists(_.fields.isDefined) =>
        Future(ensureRequirementsAndOutputPair(infotons.filter(_.fields.isDefined),Right.apply))
      case SearchResults(_, _, _, _, _, infotons, _) if withFallBack =>
        CRUDServiceFS.getInfoton("/meta/ns/" + prefix, None, None).map { iOpt =>
          ensureRequirementsAndOutputPair(iOpt.map(_.infoton).toSeq, Left.apply)
        }
      case _ => throw new NoFallbackException
    }
  }

  class NoFallbackException extends RuntimeException("No fallback...")

  private[this] def getMetaNsInfotonForHash(hash: String): Future[Option[Infoton]] =
    CRUDServiceFS.getInfoton("/meta/ns/" + hash, None, None).map(_.map(_infoton))(scala.concurrent.ExecutionContext.Implicits.global)

  private[this] def getMetaNsInfotonForUrl(url: String): Future[Option[Infoton]] =
    CRUDServiceFS.search(
      pathFilter =  Some(PathFilter("/meta/ns", descendants = false)),
      fieldFilters = Some(/*MultiFieldFilter(Must,List(*/FieldFilter(Should,Equals,"url",url)/*,FieldFilter(Should,Equals,"s$url",url)))*/),
      datesFilter = None,
      withData = true).map{
      searchResults => {
        searchResults.infotons match {
          case Nil => None
          case infotons => Some(getTheNonGeneratedMetaNsInfoton(url, infotons))
        }
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)

  // todo there must be a better way to achieve this. making ContentPortion abstract case class or such.
  private def _infoton(content: ContentPortion) = content match {
    case Everything(i) => i
    case UnknownNestedContent(i) => i
    case _ => ???
  }




  def hashToInfoton(hash: String): Option[Infoton] = Try(hashToMetaNsInfotonCache.getBlocking(hash)).toOption

  def urlToInfoton(url: String): Option[Infoton] = Try(urlToMetaNsInfotonCache.getBlocking(url)).toOption


  /**
   *
   * @param hash
   * @return url
   */
  def hashToUrlAndPrefix(hash: String): Option[(String,String)] =
    hashToInfoton(hash).flatMap {
      infoton => {
        infoton.fields.flatMap { fieldsMap =>
          fieldsMap.get("url").map { valueSet =>
            require(valueSet.size == 1, s"/meta/ns infoton must contain a single url value: $infoton")
            val uri = valueSet.head.value match {
              case url: String => {
                hashToUrlPermanentCache.put(hash,url)
                urlToHashPermanentCache.put(url,hash)
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

  /**
   *
   * @param prefix
   * @return
   */
  def prefixToHash(prefix: String): Option[String] = Try(prefixToHashCache.getBlocking(prefix)).toOption

  
  sealed trait PrefixState //to perform
  case object Create extends PrefixState
  case object Exists extends PrefixState
  case object Update extends PrefixState
  /**
   *
   * @param url as plain string
   * @return corresponding hash, or if it's a new namespace, will return an available hash to register the meta infoton at,
   *         paired with a boolean indicating if this is new or not/
   */
  def nsUrlToHash(url: String): (String,PrefixState) = {
    def inner(hash: String): (String,PrefixState) = hashToInfoton(hash) match {
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


    urlToHash(url) match {
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



  private[this] sealed trait ByAlg
  private[this] case object ByBase64 extends ByAlg
  private[this] case object ByCrc32 extends ByAlg


  def getAliasForQuadUrl(graphName: String): Option[String] = Try(graphToAliasCache.get(graphName)).toOption

  private[this] val graphToAliasCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .maximumSize(Settings.quadsCacheSize)
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
    new CacheLoader[String,String] {
      override def load(url: String): String = {
        val f = getAliasForQuadUrlAsyncActual(url)
        f.onComplete{
          case Success(Some(alias)) => aliasToGraphCache.put(alias,url)
          case Success(None) => logger.trace(s"load for graph: $url is empty")
          case Failure(e) => logger.error(s"load for $url failed",e)
        }(scala.concurrent.ExecutionContext.Implicits.global)
        Await.result(f, 10.seconds).get
      }
    }
  }

  def getAliasForQuadUrlAsync(graph: String): Future[Option[String]] = {
    val f = getAliasForQuadUrlAsyncActual(graph)
    f.onComplete{
      case Success(Some(alias)) => {
        graphToAliasCache.put(graph,alias)
        aliasToGraphCache.put(alias,graph)
      }
      case Success(None) => logger.info(s"graph: $graph could not be retrieved")
      case Failure(e) => logger.error(s"getAliasForQuadUrlAsync for $graph failed",e)
    }(scala.concurrent.ExecutionContext.Implicits.global)
    f
  }
  
  private[this] def getAliasForQuadUrlAsyncActual(graphName: String): Future[Option[String]] = {
    getAliasForQuadUrlAsync(graphName,ByBase64).flatMap{
      case some: Some[String] => Future.successful(some)
      case None => getAliasForQuadUrlAsync(graphName,ByCrc32)
    }(scala.concurrent.ExecutionContext.Implicits.global)
  }

  private[this] def getAliasForQuadUrlAsync(graphName: String, byAlg: ByAlg = ByBase64): Future[Option[String]] = {
    val hashByAlg: String = byAlg match {
      case ByBase64 => Base64.encodeBase64URLSafeString(graphName)
      case ByCrc32 => Hash.crc32(graphName)
    }

    CRUDServiceFS.getInfoton("/meta/quad/" + hashByAlg, None, None).flatMap{
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
    }(scala.concurrent.ExecutionContext.Implicits.global)
  }

  def getQuadUrlForAlias(alias: String): Option[String] = Try(aliasToGraphCache.get(alias)).toOption

  private[this] val aliasToGraphCache: LoadingCache[String,String] = CacheBuilder
    .newBuilder()
    .maximumSize(Settings.quadsCacheSize)
    .expireAfterWrite(2, TimeUnit.MINUTES)
    .build{
    new CacheLoader[String,String] {
      override def load(alias: String): String = {
        val f = getQuadUrlForAliasAsyncActual(alias)
        f.onComplete{
          case Success(graph) => graphToAliasCache.put(graph,alias)
          case Failure(e) => logger.error(s"load for $alias failed",e)
        }(scala.concurrent.ExecutionContext.Implicits.global)
        Await.result(f, 10.seconds)
      }
    }
  }
  
  def getQuadUrlForAliasAsync(alias: String): Future[String] = {
    val f = getQuadUrlForAliasAsyncActual(alias)
    f.onComplete{
      case Success(graph) => {
        aliasToGraphCache.put(alias,graph)
        graphToAliasCache.put(graph,alias)
      }
      case Failure(e) => logger.error(s"getQuadUrlForAliasAsync for $alias failed",e)
    }(scala.concurrent.ExecutionContext.Implicits.global)
    f
  }

  def getQuadUrlForAliasAsyncActual(alias: String): Future[String] = {
    CRUDServiceFS.search(
      pathFilter = Some(PathFilter("/meta/quad",false)),
      fieldFilters = Some(FieldFilter(Should,Equals,"alias",alias)),
      datesFilter = None,
      withData = true).map {
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
    }(scala.concurrent.ExecutionContext.Implicits.global)
  }
  
  // in case of ambiguity between meta/ns infotons with same url, this will return the one that was not auto-generated
  def getTheNonGeneratedMetaNsInfoton(url: String, infotons: Seq[Infoton]): Infoton = {
    require(infotons.nonEmpty)
    val it = Iterator.iterate(cmwell.util.string.Hash.crc32base64(url))(cmwell.util.string.Hash.crc32base64)
    it.take(infotons.length+1).foldLeft(infotons) { case (z,h) => if(z.size == 1) z else infotons.filterNot(_.name==h) }.head
  }
}

