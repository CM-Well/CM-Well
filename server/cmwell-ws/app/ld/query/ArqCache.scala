package ld.query

import java.util.concurrent.TimeUnit

import cmwell.domain.{Everything, Infoton}
import cmwell.fts._
import cmwell.syntaxutils.!!!
import cmwell.web.ld.query.NamespaceException
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import logic.CRUDServiceFS
import org.apache.jena.sparql.core.Quad

import scala.annotation.switch
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Try

// TODO find out a way to use Futures within ARQ
object ArqCache { // one day this will be a distributed cache ... // todo zStore all the caches! Actually, think twice, because serialization. Thrift...?

  private sealed trait Input
  private case class Uri(value: String) extends Input
  private case class Hash(value: String) extends Input
  private case class NsResult(uri: String, hash: String, prefix: String)

  private lazy val searchesCache: LoadingCache[String, Seq[Quad]] =
    CacheBuilder.newBuilder().maximumSize(100000).expireAfterWrite(100, TimeUnit.SECONDS).
      build(new CacheLoader[String, Seq[Quad]] { override def load(key: String) = !!! })
}

class ArqCache(crudServiceFS: CRUDServiceFS, nbg: Boolean) extends LazyLogging {

  import ld.query.ArqCache._

  def getInfoton(path: String): Option[Infoton] = infotonsCache.get(path)
  def namespaceToHash(ns: String): String = identifiersCache.get(ns)
  def hashToNamespace(hash: String): String = namespacesCache.get(hash).uri
  def hashToPrefix(hash: String): String = namespacesCache.get(hash).prefix
  def putSearchResults(key: String, value: Seq[Quad]): Unit = searchesCache.put(key, value)
  def getSearchResults(key: String): Seq[Quad] = Try(searchesCache.get(key)).toOption.getOrElse(Seq())

  private lazy val infotonsCache: LoadingCache[String, Option[Infoton]] =
    CacheBuilder.newBuilder().maximumSize(100000).expireAfterAccess(5, TimeUnit.MINUTES).
      build(new CacheLoader[String, Option[Infoton]] {
        override def load(key: String) = Await.result(crudServiceFS.getInfoton(key, None, None, nbg), 9.seconds) match { case Some(Everything(i)) => Option(i) case _ => None }
      })

  private lazy val identifiersCache: LoadingCache[String, String] =
    CacheBuilder.newBuilder().maximumSize(1024).expireAfterAccess(5, TimeUnit.MINUTES).
      build(new CacheLoader[String, String] { override def load(key: String) = translateAndFetch(Uri(key)).hash })

  private lazy val namespacesCache: LoadingCache[String, NsResult] =
    CacheBuilder.newBuilder().maximumSize(1024).expireAfterAccess(5, TimeUnit.MINUTES).
      build(new CacheLoader[String, NsResult] { override def load(key: String) = translateAndFetch(Hash(key)) })

  private def translateAndFetch(input: Input): NsResult = {
    val (hash,knownUri) = input match {
      case Uri(uri) => cmwell.util.string.Hash.crc32base64(uri) -> Some(uri)
      case Hash(h) => h -> None
    }

    def extractFieldValue(i: Infoton, fieldName: String) = i.fields.getOrElse(Map()).getOrElse(fieldName,Set()).mkString(",")

    // /meta/ns/{hash} if fields.url == pred.getURI return hash. else search over meta/ns?qp=url::pred.getURI -> get identifier from path of infoton
    Await.result(crudServiceFS.getInfoton(s"/meta/ns/$hash", None, None, nbg), 9.seconds) match {
      case Some(Everything(i)) if knownUri.isEmpty => {
        NsResult(extractFieldValue(i, "url"), hash, extractFieldValue(i, "prefix"))
      }
      case Some(Everything(i)) if extractFieldValue(i, "url") == knownUri.get => {
        logger.info(s"[arq] $hash was a good guess")
        NsResult(knownUri.get, hash, extractFieldValue(i, "prefix"))
      }
      case _ if knownUri.isDefined => {
        val searchFutureRes = crudServiceFS.search(
          pathFilter = Some(PathFilter("/", descendants = true)),
          fieldFilters = Some(SingleFieldFilter(Must, Equals, "url", knownUri)),
          paginationParams = PaginationParams(0, 10),
          nbg = nbg)
        val infotonsWithThatUrl = Await.result(searchFutureRes, 9.seconds).infotons
        (infotonsWithThatUrl.length: @switch) match {
          case 0 =>
            logger.info(s"[arq] ns for $hash was not found")
            throw new NamespaceException(s"Namespace ${knownUri.get} does not exist")
          case 1 =>
            logger.info(s"[arq] Fetched proper namespace for $hash")
            val infoton = infotonsWithThatUrl.head
            val path = infoton.path
            val actualHash = path.substring(path.lastIndexOf('/') + 1)
            val uri = knownUri.getOrElse(extractFieldValue(infoton, "url"))
            NsResult(uri, actualHash, extractFieldValue(infoton, "prefix"))
          case _ =>
            logger.info(s"[arq] this should never happen: same URL [${knownUri.get}] cannot be more than once in meta/ns [${infotonsWithThatUrl.map(i => i.path + "[" + i.uuid + "]").mkString(",")}]")
            !!!
        }
      }
      case _ =>
        logger.info(s"[arq] this should never happen: given a hash [$hash], when the URI is unknown, and there's no infoton under meta/ns/ - it means corrupted data")
        !!!
    }
  }
}
