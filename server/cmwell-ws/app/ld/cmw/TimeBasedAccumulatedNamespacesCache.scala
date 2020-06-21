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
package ld.cmw

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import cmwell.domain.{FieldValue, Infoton}
import cmwell.fts._
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import cmwell.util.collections.{partitionWith, spanWith, subtractedDistinctMultiMap, updatedDistinctMultiMap}
import cmwell.util.exceptions.MultipleFailures
import cmwell.util.string.Hash.crc32base64
import com.typesafe.scalalogging.LazyLogging
import ld.exceptions.{ConflictingNsEntriesException, ServerComponentNotAvailableException, TooManyNsRequestsException}
import logic.CRUDServiceFS
import org.elasticsearch.search.SearchHit

import scala.unchecked
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * TODO:
  * - refine consume with pagination
  * - add invalidate by id (and by url/prefix ?) - make sure to invalidate auxiliary caches.
  * - guard blacklist with a size cap (many non-existing prefixes searched => DOS-Attack)
  * - implement ???s
  * - tests?
  */
trait TimeBasedAccumulatedNsCacheTrait {

  // TODO Don't we want those to get an implicit Execution  Context?

  def getByURL(url: NsURL, timeContext: Option[Long])(implicit timeout: Timeout): Future[NsID]
  def getByPrefix(prefix: NsPrefix, timeContext: Option[Long])(implicit timeout: Timeout): Future[NsID]
  def get(key: NsID, timeContext: Option[Long])(implicit timeout: Timeout): Future[(NsURL, NsPrefix)]
  def invalidate(key: NsID)(implicit timeout: Timeout): Future[Unit]
  def invalidateAll()(implicit timeout: Timeout): Future[Unit]
  def getStatus(quick: Boolean)(implicit timeout: Timeout): Future[String]
  def init(time: Long): Unit
}

class TimeBasedAccumulatedNsCache private (private[this] var mainCache: Map[NsID, (NsURL, NsPrefix)],
                                           private[this] var urlCache: Map[NsURL, Set[NsID]],
                                           private[this] var prefixCache: Map[NsPrefix, Set[NsID]],
                                           seedTimestamp: Long,
                                           coolDownMillis: Long,
                                           crudService: CRUDServiceFS)(implicit ec: ExecutionContext, sys: ActorSystem)
    extends TimeBasedAccumulatedNsCacheTrait { _: LazyLogging =>

  import TimeBasedAccumulatedNsCache.Messages._

  private[this] var checkTime: Long = 0L

  // TODO: load from config
  val incrementingWaitTimeMillis = 1000L
  val maxCountIncrements = 30
  val cacheSizeCap = 10000

  // public API

  @inline override def getByURL(url: NsURL, timeContext: Option[Long])(implicit timeout: Timeout): Future[NsID] = {
    timeContext.foreach { t =>
      if (mainCache.size < cacheSizeCap && !updatedRecently(t)) {
        actor ! UpdateRequest(t)
      }
    }
    urlCache
      .get(url)
      .fold((actor ? GetByURL(url)).mapTo[Set[NsID]].transform {
        case f @ Failure(_: NoSuchElementException) => f.asInstanceOf[Try[NsID]]
        case Failure(e)                             => Failure(ServerComponentNotAvailableException(s"failed to getByURL($url) from ns cache", e))
        case Success(s) if s.isEmpty =>
          Failure(new NoSuchElementException(s"getByURL failed with EmptySet stored for [$url]"))
        case Success(s) if s.size == 1 => Success(s.head)
        case Success(many)             => Failure(ConflictingNsEntriesException.byURL(url, many))
      }) {
        case s if s.isEmpty =>
          Future.failed(new NoSuchElementException(s"getByURL failed with EmptySet stored for [$url]"))
        case s if s.size == 1 => Future.successful(s.head)
        case many             => Future.failed(ConflictingNsEntriesException.byURL(url, many))
      }
  }

  @inline override def getByPrefix(prefix: NsPrefix,
                                   timeContext: Option[Long])(implicit timeout: Timeout): Future[NsID] = {
    timeContext.foreach { t =>
      if (mainCache.size < cacheSizeCap && !updatedRecently(t)) {
        actor ! UpdateRequest(t)
      }
    }
    prefixCache
      .get(prefix)
      .fold((actor ? GetByPrefix(prefix)).mapTo[Set[NsID]].transform {
        case f @ Failure(_: NoSuchElementException) => f.asInstanceOf[Try[NsID]]
        case Failure(e) =>
          Failure(ServerComponentNotAvailableException(s"failed to getByPrefix($prefix) from ns cache", e))
        case Success(s) if s.isEmpty =>
          Failure(new NoSuchElementException(s"getByPrefix failed with EmptySet stored for [$prefix]"))
        case Success(s) if s.size == 1 => Success(s.head)
        case Success(many)             => Failure(ConflictingNsEntriesException.byPrefix(prefix, many))
      }) {
        case s if s.isEmpty =>
          Future.failed(new NoSuchElementException(s"getByPrefix failed with EmptySet stored for [$prefix]"))
        case s if s.size == 1 => Future.successful(s.head)
        case many             => Future.failed(ConflictingNsEntriesException.byPrefix(prefix, many))
      }
  }

  @inline override def get(key: NsID,
                           timeContext: Option[Long])(implicit timeout: Timeout): Future[(NsURL, NsPrefix)] = {
    timeContext.foreach { t =>
      if (mainCache.size < cacheSizeCap && !updatedRecently(t)) {
        actor ! UpdateRequest(t)
      }
    }
    mainCache
      .get(key)
      .fold((actor ? GetByID(key)).mapTo[(NsURL, NsPrefix)].transform {
        case f @ Failure(_: NoSuchElementException) => f
        case Failure(e)                             => Failure(new Exception(s"failed to get($key) from ns cache", e))
        case success                                => success
      })(Future.successful[(NsURL, NsPrefix)])
  }

  @inline override def invalidate(key: NsID)(implicit timeout: Timeout): Future[Unit] =
    mainCache.get(key).fold(Future.successful(()))(_ => (actor ? Invalidate(key)).mapTo[Done].map(_ => ()))

  @inline override def invalidateAll()(implicit timeout: Timeout): Future[Unit] = {
    if (mainCache.isEmpty && urlCache.isEmpty && prefixCache.isEmpty) Future.successful(())
    else (actor ? InvalidateAll).mapTo[Done].map(_ => ())
  }

  @inline override def getStatus(quick: Boolean)(implicit timeout: Timeout): Future[String] =
    if (quick) {
      val now = System.currentTimeMillis()
      val sb = new StringBuilder("{\"checkTime\":")
      sb ++= checkTime.toString
      sb ++= ",\"checkTimeDiff\":"
      sb ++= (now - checkTime).toString
      sb ++= ",\"mainCache\":"
      renderMapWithStringBuilder(mainCache, sb, false) {
        case (url, prefix) =>
          Seq(
            ("url", sb => {
              sb += '"'
              sb ++= url
              sb += '"'
            }),
            ("prefix", sb => {
              sb += '"'
              sb ++= prefix
              sb += '"'
            })
          )
      }
      sb ++= ",\"prefixCache\":"
      renderMapWithStringBuilder(prefixCache, sb, true) { set: Set[NsID] =>
        set.toSeq.sorted.map((_, (_: StringBuilder) => {}))
      }
      sb ++= ",\"urlCache\":"
      renderMapWithStringBuilder(urlCache, sb, true) { set: Set[NsID] =>
        set.toSeq.sorted.map((_, (_: StringBuilder) => {}))
      }
      Future.successful(sb.append('}').result())
    } else (actor ? GetStatus).mapTo[String]

  @inline override def init(time: Long): Unit = actor ! UpdateRequest(time)

  // private section

  private[cmw] class TimeBasedAccumulatedNsCacheActor extends Actor {

    private[this] var timestamp: Long = seedTimestamp
    private[this] var isConsuming: Boolean = false

    private[this] var nsIDBlacklist: Map[NsID, (Long, Int, Throwable)] = Map.empty
    private[this] var nsURLBlacklist: Map[NsURL, (Long, Int, Throwable)] = Map.empty
    private[this] var nsPrefixBlacklist: Map[NsPrefix, (Long, Int, Throwable)] = Map.empty

    private[this] var nsIDFutureList: Map[NsID, Future[(NsURL, NsPrefix)]] = Map.empty
    private[this] var nsURLFutureList: Map[NsURL, Future[Set[NsID]]] = Map.empty
    private[this] var nsPrefixFutureList: Map[NsPrefix, Future[Set[NsID]]] = Map.empty

    override def receive: Receive = {
      case GetByID(id)                                      => handleGetByID(id)
      case UpdateAfterSuccessfulFetch(id, tuple)            => handleUpdateAfterSuccessfulFetch(id, tuple)
      case UpdateAfterFailedFetch(id, count, err)           => handleUpdateAfterFailedFetch(id, count, err)
      case GetByURL(url)                                    => handleGetByURL(url)
      case UpdateAfterFailedURLFetch(url, count, err)       => handleUpdateAfterFailedURLFetch(url, count, err)
      case UpdateAfterSuccessfulURLFetch(url, miup)         => handleUpdateAfterSuccessfulURLFetch(url, miup)
      case GetByPrefix(prefix)                              => handleGetByPrefix(prefix)
      case UpdateAfterFailedPrefixFetch(prefix, count, err) => handleUpdateAfterFailedPrefixFetch(prefix, count, err)
      case UpdateAfterSuccessfulPrefixFetch(prefix, miup)   => handleUpdateAfterSuccessfulPrefixFetch(prefix, miup)
      case Invalidate(id)                                   => handleInvalidate(id)
      case InvalidateAll                                    => handleInvalidateAll()
      case UpdateRequest(time, shouldContinue)              => handleUpdateRequest(time, shouldContinue)
      case GetStatus                                        => sender() ! handleShowStatus
      case UpdateAfterSuccessfulConsume(newIdxTime, data, shoudCont) =>
        handleUpdateAfterSuccessfulConsume(newIdxTime, data, shoudCont)
      case UpdateAfterFailedConsume(origTime, e) => handleUpdateAfterFailedConsume(origTime, e)
    }

    private[this] def handleGetByID(id: NsID): Unit = {
      val sndr = sender()
      mainCache.get(id) match {
        case Some(tuple) => sndr ! tuple
        case None =>
          nsIDFutureList.get(id) match {
            case Some(future) => future.pipeTo(sndr)
            case None => {
              val currentSize = nsIDFutureList.size
              if (currentSize >= cacheSizeCap)
                sndr ! Status.Failure(
                  ServerComponentNotAvailableException(
                    s"asked to resolve ns id [$id], but too many [$currentSize] resolving requests are in flight"
                  )
                )
              else
                nsIDBlacklist.get(id) match {
                  case None => doFetch(id, sndr, 1)
                  case Some((time, count, err)) => {
                    if (hasEnoughIncrementalWaitTimeElapsed(time, count))
                      doFetch(id, sndr, math.min(maxCountIncrements, count + 1))
                    else
                      sndr ! Status.Failure(err)
                  }
                }
            }
          }
      }
    }

    private[this] def doFetch(id: NsID, sndr: ActorRef, failureCount: Int): Unit = {
      val f = wrappedGetByNsID(id)
      nsIDFutureList = nsIDFutureList.updated(id, f)
      f.pipeTo(sndr)
      f.transform {
          case Failure(e) => Success(UpdateAfterFailedFetch(id, failureCount, e))
          case success    => success.map(UpdateAfterSuccessfulFetch(id, _))
        }
        .pipeTo(self)
    }

    private[this] def wrappedGetByNsID(id: NsID): Future[(NsURL, NsPrefix)] =
      crudService.getInfotonByPathAsync(s"/meta/ns/$id").transform {
        case Failure(e)               => Failure(new Exception(s"failed to get /meta/ns/$id", e))
        case Success(EmptyBox)        => Failure(new NoSuchElementException(s"No such ns identifier: /meta/ns/$id"))
        case Success(BoxedFailure(e)) => Failure(new Exception(s"failed to get /meta/ns/$id from IRW", e))
        case Success(FullBox(i))      => extractMetadataFromInfoton(i)
      }

    private[this] def extractMetadataFromInfoton(infoton: Infoton): Try[(NsURL, NsPrefix)] = {
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

    private[this] def handleUpdateAfterSuccessfulFetch(id: NsID, tuple: (NsURL, NsPrefix)): Unit = {
      nsIDFutureList -= id
      nsIDBlacklist -= id
      handleUpdateAfterSuccessfulFetchPerTriple(id, tuple)
    }

    private[this] val handleUpdateAfterSuccessfulFetchPerTripleValue: (NsID, (NsURL, NsPrefix)) => Unit =
      handleUpdateAfterSuccessfulFetchPerTriple

    private[this] def handleUpdateAfterSuccessfulFetchPerTriple(id: NsID, tuple: (NsURL, NsPrefix)): Unit = {
      val (u, p) = tuple
      // scalastyle:off
      mainCache.get(id) match {
        // all is good. nothing needs to be changed. use of `return` avoids mainCache redundant update
        // or else it would have to be repeated in all other cases
        //TODO: comment about why this case is needed
        case Some(`tuple`) => return
        case Some((`u`, oldPrefix)) =>
          prefixCache = updatedDistinctMultiMap(subtractedDistinctMultiMap(prefixCache, oldPrefix, id), p, id)
        case Some((oldURL, `p`)) =>
          // todo log warning - this should almost never happen
          urlCache = updatedDistinctMultiMap(subtractedDistinctMultiMap(urlCache, oldURL, id), u, id)
        case Some((oldU, oldP)) =>
          // todo log warning - this should almost never happen
          prefixCache = updatedDistinctMultiMap(subtractedDistinctMultiMap(prefixCache, oldP, id), p, id)
          urlCache = updatedDistinctMultiMap(subtractedDistinctMultiMap(urlCache, oldU, id), u, id)
        case None =>
          prefixCache = updatedDistinctMultiMap(prefixCache, p, id)
          urlCache = updatedDistinctMultiMap(urlCache, u, id)
      }
      // scalastyle:on
      mainCache = mainCache.updated(id, tuple)
    }

    private[this] def handleUpdateAfterFailedFetch(id: NsID, count: Int, err: Throwable): Unit = {
      nsIDFutureList -= id
      // every new namespace ingested, is first checked for existence.
      // so the first time we search a namespace identifier, is actually OK.
      // it just means we will create it now.
      // So we are not logging the first time to avoid junk in logs
      if (count > 1) {
        logger.error(s"failure to retrieve /meta/ns/$id [fail #$count]", err)
      }
      // an if (with == instead of >=) might suffice, but we do it in a while loop to be on the safe side
      while (nsIDBlacklist.size >= cacheSizeCap) {
        // remove earliest entry by timestamp until cache size is smaller than size cap
        nsIDBlacklist -= nsIDBlacklist.minBy(_._2._1)._1
      }
      nsIDBlacklist = nsIDBlacklist.updated(id, (System.currentTimeMillis(), count, err))
    }

    private[this] def handleUpdateAfterFailedURLFetch(url: NsURL, count: Int, err: Throwable): Unit = {
      nsURLFutureList -= url
      // every new namespace ingested, is first checked for existence.
      // so the first time we search a namespace URL, is actually OK.
      // it just means we will create it now.
      // So we are not logging the first time to avoid junk in logs
      if (count > 1) {
        logger.error(s"failure to retrieve or search data for URL $url [fail #$count]", err)
      }
      // an if (with == instead of >=) might suffice, but we do it in a while loop to be on the safe side
      while (nsURLBlacklist.size >= cacheSizeCap) {
        // remove earliest entry by timestamp until cache size is smaller than size cap
        nsURLBlacklist -= nsURLBlacklist.minBy(_._2._1)._1
      }
      nsURLBlacklist = nsURLBlacklist.updated(url, (System.currentTimeMillis(), count, err))
    }

    private[this] def handleUpdateAfterFailedPrefixFetch(prefix: NsPrefix, count: Int, err: Throwable): Unit = {
      nsPrefixFutureList -= prefix
      logger.error(s"failure to retrieve or search data for prefix $prefix [fail #$count]", err)
      // an if (with == instead of >=) might suffice, but we do it in a while loop to be on the safe side
      while (nsPrefixBlacklist.size >= cacheSizeCap) {
        // remove earliest entry by timestamp until cache size is smaller than size cap
        nsPrefixBlacklist -= nsPrefixBlacklist.minBy(_._2._1)._1
      }
      nsPrefixBlacklist = nsPrefixBlacklist.updated(prefix, (System.currentTimeMillis(), count, err))
    }

    private[this] def handleGetByURL(url: NsURL): Unit = {
      val sndr = sender()
      urlCache.get(url) match {
        // todo explain why this case can happen
        case Some(set) => sndr ! set
        case None =>
          nsURLFutureList.get(url) match {
            case Some(future) => future.pipeTo(sndr)
            case None => {
              val currentSize = nsURLFutureList.size
              if (currentSize >= cacheSizeCap)
                sndr ! Status.Failure(
                  ServerComponentNotAvailableException(
                    s"asked to resolve ns url [$url], but too many [$currentSize] resolving requests are in flight"
                  )
                )
              else
                nsURLBlacklist.get(url) match {
                  case None => doFetchURL(url, sndr, 1)
                  case Some((time, count, err)) => {
                    if (hasEnoughIncrementalWaitTimeElapsed(time, count))
                      doFetchURL(url, sndr, math.min(maxCountIncrements, count + 1))
                    else
                      sndr ! Status.Failure(err)
                  }
                }
            }
          }
      }
    }

    private[this] def doFetchURL(url: NsID, sndr: ActorRef, failureCount: Int): Unit = {
      val f: Future[Map[NsID, (NsURL, NsPrefix)]] = wrappedGetByNsURL(url)
      val g: Future[Set[NsID]] = f.map(_.keySet)
      nsURLFutureList = nsURLFutureList.updated(url, g)
      g.pipeTo(sndr)
      f.transform {
          case Failure(e) => Success(UpdateAfterFailedURLFetch(url, failureCount, e))
          case success    => success.map(UpdateAfterSuccessfulURLFetch(url, _))
        }
        .pipeTo(self)
    }

    private[this] def wrappedGetByNsURL(url: NsURL): Future[Map[NsID, (NsURL, NsPrefix)]] = {
      val f1 = nsSearchBy("url", url)
      val f2 = nsURLToNsIDByHash(url)
      // TODO What if only one of the futures succeeds? Do we really want to yield a failed future?
      for {
        nsIDs <- f1
        anotherID <- f2
      } yield nsIDs + anotherID
    }

    // todo   1. wire the search future in here as well. in case there are multiple hash collisions,
    // todo      it might be a good idea to be combined with the search future rather than many recursive steps.
    // todo
    // todo   2. Add a maximum cap for recursive steps
    // todo      Alternatively (or not), keep a Set of previous NsIDs, to handle a fixed point in hash (... srsly?)
    def nsURLToNsIDByHash(url: NsURL): Future[(NsID, (NsURL, NsPrefix))] = {

      def inner(nsID: String): Future[(NsID, (NsURL, NsPrefix))] = wrappedGetByNsID(nsID).transformWith {
        case Success(tuple @ (`url`, _))        => Future.successful(nsID -> tuple)
        case Failure(e: NoSuchElementException) => Future.failed(e)
        case Failure(err) =>
          Future.failed(new Exception(s"nsURLToNsID.inner failed for url [$url] and hash [$nsID]", err))
        case Success(notSameUrl) => {
          val doubleHash = crc32base64(nsID)
          logger.warn(
            s"double hashing url's [$url] hash [$nsID] to [$doubleHash] because not same as [${notSameUrl._1}]"
          )
          inner(doubleHash)
        }
      }
      inner(crc32base64(url))
    }

    private[this] val pathFilter = Some(PathFilter("/meta/ns", false))
    private[this] val fieldsForSearch = Array("system.path", "fields.nn.prefix", "fields.nn.url")
    def nsSearchBy(fieldName: String, fieldValue: String): Future[Map[NsID, (NsURL, NsPrefix)]] = {

      import cmwell.util.collections.TryOps

      crudService
        .fullSearch(pathFilter,
                    fieldFilters = Some(SingleFieldFilter(Must, Equals, fieldName, Some(fieldValue))),
                    storedFields = Seq.empty,
                    fieldsFromSource = fieldsForSearch) { (sr, _) =>
          val hits = sr.getHits.getHits.toSeq
          Try
            .traverse(hits) { hit =>
              import io.circe._
              import io.circe.parser._
              val source = parse(hit.getSourceAsString).right.get
              val systemPart = source.hcursor.downField("system")
              val fieldsPart = source.hcursor.downField("fields").downField("nn")
              val prefixList = fieldsPart.get[List[String]]("prefix").right.get
              val urlList = fieldsPart.get[List[String]]("url").right.get

              if (prefixList.size != 1 || urlList.size != 1) {
                val path = systemPart.get[String]("path") match {
                  case Left(failure) => s"path not available due to $failure"
                  case Right(pathTmp) => pathTmp
                }
                Failure(new RuntimeException(s"More than one prefix or URL values encountered when trying to resolve [" +
                                             fieldName+"] value for ["+fieldValue+"] in one of the ["+hits.size.toString+
                                             "] results found! (bad path is ["+path+"])"))
              } else {
                val pathOpt = systemPart.get[String]("path").toOption
                val urlOpt = fieldsPart.downField("url").downArray.as[String].toOption
                val prefixOpt = fieldsPart.downField("prefix").downArray.as[String].toOption
                (for {
                  path <- pathOpt
                  url <- urlOpt
                  prefix <- prefixOpt
                } yield {
                  val nsID = path.drop("/meta/ns/".length)
                  nsID -> (url, prefix)
                }).fold[Try[(NsID, (NsURL, NsPrefix))]](
                  Failure(new IllegalStateException(s"invalid /meta/ns infoton [$pathOpt,$urlOpt,$prefixOpt]"))
                )(Success.apply)
              }
            }
            .map(_.toMap)
        }
        .transform(_.flatMap(identity))
    }

    private[this] val fieldsForIndexTimeSearch = fieldsForSearch ++ Array("system.indexTime")
    private[this] val paginationParamsForIndexTimeSearch = PaginationParams(0, 512)
    def nsSearchByIndexTime(indexTime: Long): Future[(Boolean, Long, Map[NsID, (NsURL, NsPrefix)])] = {

      import cmwell.util.collections.TryOps

      crudService
        .fullSearch(
          pathFilter,
          fieldFilters = Some(SingleFieldFilter(Must, GreaterThan, "system.indexTime", Some(indexTime.toString))),
          storedFields = Seq.empty,
          fieldsFromSource = fieldsForIndexTimeSearch,
          paginationParams = paginationParamsForIndexTimeSearch,
          fieldSortParams = SortParam("system.indexTime" -> Asc)
        ) { (sr, _) =>
          val hits = sr.getHits.getHits

          val entries: Seq[(Long, Try[(NsID, Long, NsURL, NsPrefix)])] = hits.map { hit =>
            import io.circe._
            import io.circe.parser._
            val source = parse(hit.getSourceAsString).right.get
            val systemPart = source.hcursor.downField("system")
            val fieldsPart = source.hcursor.downField("fields").downField("nn")
            val indexTimeOpt = systemPart.get[Long]("indexTime").toOption
            val timeToSortBy = indexTimeOpt.getOrElse(Long.MaxValue)

            val prefixList = fieldsPart.get[List[String]]("prefix").right.get
            val urlList = fieldsPart.get[List[String]]("url").right.get

            val tryValue =
              if (prefixList.size != 1 || urlList.size != 1) {
                val path = systemPart.get[String]("path") match {
                  case Left(failure) => s"path not available due to $failure"
                  case Right(path) => path
                }
                Failure(new RuntimeException("More than one prefix or URL values encountered when trying to consume latest changes in one of the [" +
                                             hits.size.toString + "] results found! (bad path is [" + path + "])"))
              } else {
                val pathOpt = systemPart.get[String]("path").toOption
                val urlOpt = fieldsPart.downField("url").downArray.as[String].toOption
                val prefixOpt = fieldsPart.downField("prefix").downArray.as[String].toOption
                (for {
                  path <- pathOpt
                  infotonIndexTime <- indexTimeOpt
                  url <- urlOpt
                  prefix <- prefixOpt
                } yield {
                  val nsID = path.drop("/meta/ns/".length)
                  (nsID, infotonIndexTime, url, prefix)
                }).fold[Try[(NsID, Long, NsURL, NsPrefix)]](
                  Failure(new IllegalStateException(s"invalid /meta/ns infoton [$pathOpt,$urlOpt,$prefixOpt]"))
                )(Success.apply)
              }

            timeToSortBy -> tryValue
          }

          val (ok, ko) = spanWith(entries.sortBy(_._1)) {
            case (_, tryQuadruple) => tryQuadruple.toOption
          }

          val shouldContinue = sr.getHits.getTotalHits.value > hits.length && ko.isEmpty
          val err: Throwable = {
            if (ko.nonEmpty) {
              val errors = ko.view.collect {
                case (_, Failure(e)) => e
              }.to(List)
              val error = {
                if (errors.length == 1) errors.head
                else new MultipleFailures(errors)
              }
              val msg =
                s"nsSearchByIndexTime([$indexTime]) failed with [${ko.length}] infotons omitted from results, and passing [${ok.length}] infotons"
              logger.error(msg, error)
              error
            } else null
          }

          if (ok.isEmpty && ko.nonEmpty) Failure(err)
          else if (ok.isEmpty) Success((false, indexTime, Map.empty[NsID, (NsURL, NsPrefix)]))
          else {
            val (maxOkIndexTime, mapBuilder) = ok.foldLeft(indexTime -> Map.newBuilder[NsID, (NsURL, NsPrefix)]) {
              case ((maxTime, b), (id, ts, url, pref)) =>
                math.max(maxTime, ts) -> b.+=((id, url -> pref))
            }

            Success((shouldContinue, maxOkIndexTime, mapBuilder.result()))
          }
        }
        .transform(_.flatMap(identity))
    }

    private[this] def handleUpdateAfterSuccessfulURLFetch(url: NsURL, miup: Map[NsID, (NsURL, NsPrefix)]): Unit = {
      nsURLFutureList -= url
      nsURLBlacklist -= url
      miup.foreach {
        case (id, tuple) => handleUpdateAfterSuccessfulFetchPerTriple(id, tuple)
      }
    }

    private[this] def handleUpdateAfterSuccessfulPrefixFetch(prefix: NsPrefix,
                                                             miup: Map[NsID, (NsURL, NsPrefix)]): Unit = {
      nsPrefixFutureList -= prefix
      nsPrefixBlacklist -= prefix
      miup.foreach {
        case (id, tuple) => handleUpdateAfterSuccessfulFetchPerTriple(id, tuple)
      }
    }

    private[this] def handleGetByPrefix(prefix: NsPrefix): Unit = {
      val sndr = sender()
      prefixCache.get(prefix) match {
        // todo explain why this case can happen
        case Some(set) => sndr ! set
        case None =>
          nsPrefixFutureList.get(prefix) match {
            case Some(future) => future.pipeTo(sndr)
            case None => {
              val currentSize = nsPrefixFutureList.size
              if (currentSize >= cacheSizeCap)
                sndr ! Status.Failure(
                  ServerComponentNotAvailableException(
                    s"asked to resolve ns prefix [$prefix], but too many [$currentSize] resolving requests are in flight"
                  )
                )
              else
                nsPrefixBlacklist.get(prefix) match {
                  case None => doFetchPrefix(prefix, sndr, 1)
                  case Some((time, count, err)) => {
                    if (hasEnoughIncrementalWaitTimeElapsed(time, count))
                      doFetchPrefix(prefix, sndr, math.min(maxCountIncrements, count + 1))
                    else
                      sndr ! Status.Failure(err)
                  }
                }
            }
          }
      }
    }

    private[this] def doFetchPrefix(prefix: NsPrefix, sndr: ActorRef, failureCount: Int): Unit = {
      val f: Future[Map[NsID, (NsURL, NsPrefix)]] = nsSearchBy("prefix", prefix)
      val g: Future[Set[NsID]] = f.map(_.keySet)
      nsPrefixFutureList = nsPrefixFutureList.updated(prefix, g)
      g.pipeTo(sndr)
      f.transform {
          case Failure(e) => Success(UpdateAfterFailedPrefixFetch(prefix, failureCount, e))
          case success    => success.map(UpdateAfterSuccessfulPrefixFetch(prefix, _))
        }
        .pipeTo(self)
    }

    private[this] def handleInvalidateAll(): Unit = {
      mainCache = Map.empty
      urlCache = Map.empty
      prefixCache = Map.empty
      sender() ! Done
    }

    private[this] def handleInvalidate(key: NsID): Unit = {
      mainCache.get(key).fold(logger.warn(s"tried to invalidate NsID [$key], but it's not in cache!")) {
        case (url, prefix) =>
          mainCache -= key
          urlCache -= url
          prefixCache -= prefix
      }
      sender() ! Done
    }

    private[this] def handleShowStatus: String = {

      val now = System.currentTimeMillis()
      val sb = new StringBuilder("{\"checkTime\":")
      sb ++= checkTime.toString
      sb ++= ",\"checkTimeDiff\":"
      sb ++= (now - checkTime).toString
      sb ++= ",\"timestamp\":"
      sb ++= timestamp.toString
      sb ++= ",\"timestampDiff\":"
      sb ++= (now - timestamp).toString
      sb ++= ",\"mainCache\":"
      renderMapWithStringBuilder(mainCache, sb, false) {
        case (url, prefix) =>
          Seq(
            ("url", sb => {
              sb += '"'
              sb ++= url
              sb += '"'
            }),
            ("prefix", sb => {
              sb += '"'
              sb ++= prefix
              sb += '"'
            })
          )
      }
      sb ++= ",\"prefixCache\":"
      renderMapWithStringBuilder(prefixCache, sb, true) { set: Set[NsID] =>
        set.toSeq.sorted.map((_, (_: StringBuilder) => {}))
      }
      sb ++= ",\"urlCache\":"
      renderMapWithStringBuilder(urlCache, sb, true) { set: Set[NsID] =>
        set.toSeq.sorted.map((_, (_: StringBuilder) => {}))
      }
      val blackListFormattingFunction: ((Long, Int, Throwable)) => Seq[(String, StringBuilder => Unit)] = {
        case (time, count, err) =>
          Seq[(String, StringBuilder => Unit)](
            ("time", _.append(time)),
            ("timeDiff", _.append(now - time)),
            ("count", _.append(count)),
            ("error", _.append('"').append(err.getClass.getSimpleName).append('"')),
            ("errorMsg", _.append('"').append(org.json.simple.JSONValue.escape(err.getMessage)).append('"'))
          ) ++ Option(err.getCause).fold(Seq.empty[(String, StringBuilder => Unit)]) { cause =>
            Seq(
              ("errorCause", _.append('"').append(cause.getClass.getSimpleName).append('"')),
              ("errorCauseMsg", _.append('"').append(org.json.simple.JSONValue.escape(cause.getMessage)).append('"'))
            )
          }
      }
      sb ++= ",\"nsIDBlacklist\":"
      renderMapWithStringBuilder(nsIDBlacklist, sb, false)(blackListFormattingFunction)
      sb ++= ",\"nsURLBlacklist\":"
      renderMapWithStringBuilder(nsURLBlacklist, sb, false)(blackListFormattingFunction)
      sb ++= ",\"nsPrefixBlacklist\":"
      renderMapWithStringBuilder(nsPrefixBlacklist, sb, false)(blackListFormattingFunction)
      val futureListFormattingFunction: Future[_] => Seq[(String, StringBuilder => Unit)] = { f =>
        Seq(
          ("isCompleted", _.append(f.isCompleted)),
          ("futureValue", _.append('"').append(f.value).append('"'))
        )
      }
      sb ++= ",\"nsIDFutureList\":"
      renderMapWithStringBuilder(nsIDFutureList, sb, false)(futureListFormattingFunction)
      sb ++= ",\"nsURLFutureList\":"
      renderMapWithStringBuilder(nsURLFutureList, sb, false)(futureListFormattingFunction)
      sb ++= ",\"nsPrefixFutureList\":"
      renderMapWithStringBuilder(nsPrefixFutureList, sb, false)(futureListFormattingFunction)
      sb.append('}').result()
    }

    private[this] def handleUpdateRequest(time: Long, shouldContinue: Boolean): Unit =
      if (!isConsuming && (shouldContinue || !updatedRecently(time))) {
        val orig = checkTime
        // assignment must come first, to reduce update calls to actor as much as possible
        checkTime = time

        isConsuming = true
        nsSearchByIndexTime(timestamp)
          .transform {
            case Success((sc, newIndexTime, results)) =>
              Success(UpdateAfterSuccessfulConsume(newIndexTime, results, sc && mainCache.size < cacheSizeCap))
            case Failure(e) => Success(UpdateAfterFailedConsume(orig, e))
          }
          .pipeTo(self)
      }

    private[this] def handleUpdateAfterSuccessfulConsume(indexTime: Long,
                                                         data: Map[NsID, (NsURL, NsPrefix)],
                                                         shouldContinue: Boolean) = {
      timestamp = indexTime
      data.foreach(handleUpdateAfterSuccessfulFetchPerTripleValue.tupled)
      if (shouldContinue)
        handleUpdateRequest(checkTime, shouldContinue = true)
      else
        isConsuming = false
    }

    private[this] def handleUpdateAfterFailedConsume(originalCheckTime: Long, e: Throwable) = {
      logger.error(
        s"Failed to conusme NS Deltas, checkTime was $checkTime and it is now reverted to $originalCheckTime",
        e
      )
      checkTime = originalCheckTime
      isConsuming = false
    }

    private[this] def hasEnoughIncrementalWaitTimeElapsed(since: Long, cappedCount: Int): Boolean =
      (System.currentTimeMillis() - since) > (cappedCount * incrementingWaitTimeMillis)

  }

  def renderValueWithStringBuilder[T](value: T, sb: StringBuilder, isSequential: Boolean)(
    f: T => Seq[(String, StringBuilder => Unit)]
  ): Unit = {
    val xs = f(value)
    if (isSequential) sb += '['
    else sb += '{'
    if (xs.nonEmpty) {
      var isFirst = true
      xs.foreach {
        case (k, v) => {
          if (isFirst) isFirst = false
          else sb += ','
          sb += '"'
          sb ++= k
          if (isSequential) sb += '"'
          else {
            sb ++= "\":"
            v(sb)
          }
        }
      }
    }
    if (isSequential) sb += ']'
    else sb += '}'
  }

  def renderMapWithStringBuilder[T](m: Map[String, T], sb: StringBuilder, isSequential: Boolean)(
    f: T => Seq[(String, StringBuilder => Unit)]
  ): Unit = {
    var notFirst = false
    sb += '{'
    m.foreach {
      case (k, v) =>
        if (notFirst) sb += ','
        else notFirst = true

        sb += '"'
        sb ++= k
        sb ++= "\":"
        renderValueWithStringBuilder(v, sb, isSequential)(f)
    }
    sb += '}'
  }

  private[this] val actor = sys.actorOf(Props(new TimeBasedAccumulatedNsCacheActor), "TimeBasedAccumulatedNsCacheActor")

  private[this] def updatedRecently(timeContext: Long): Boolean =
    timeContext - checkTime <= coolDownMillis
}

object TimeBasedAccumulatedNsCache extends LazyLogging {

  private def validateLogAndGetInvertedCaches(
    m: Map[NsID, (NsURL, NsPrefix)],
    urls: Map[NsURL, Set[NsID]] = Map.empty,
    prefs: Map[NsPrefix, Set[NsID]] = Map.empty
  ): (Map[NsURL, Set[NsID]], Map[NsPrefix, Set[NsID]]) = {
    val (errors, mUrls, mPrefixes) = m.iterator.foldLeft((List.empty[Either[String, String]], urls, prefs)) {
      case ((errs, _urls, _prefs), (id, (url, prefix))) => {
        val (errs1, urls1) = if (_urls.contains(url)) {
          (Left(url) :: errs) -> updatedDistinctMultiMap(_urls, url, id)
        } else {
          errs -> updatedDistinctMultiMap(_urls, url, id)
        }
        val (errs2, prefs1) = if (_prefs.contains(prefix)) {
          (Right(prefix) :: errs1) -> updatedDistinctMultiMap(_prefs, prefix, id)
        } else {
          errs1 -> updatedDistinctMultiMap(_prefs, prefix, id)
        }
        (errs2, urls1, prefs1)
      }
    }

    val rv = mUrls -> mPrefixes
    if (errors.isEmpty) rv
    else if (errors.forall(_.isRight)) {
      val msg = errors.map(_.right.get).distinct.sorted.mkString("Multiple prefix ambiguities detected: [", ",", "]")
      logger.error(msg)
      rv
    } else {
      val (u, p) = partitionWith(errors)(identity)
      val msgU = u.distinct.sorted.mkString("Multiple URL ambiguities detected: [", ",", "]")
      val msg =
        if (p.isEmpty) msgU
        else msgU + p.distinct.sorted.mkString(", and multiple prefix ambiguities detected: [", ",", "]")
      logger.error(msg)
      rv
    }
  }

  object Messages {
    case class GetByID(id: NsID)
    case class GetByURL(url: NsURL)
    case class GetByPrefix(prefix: NsPrefix)
    case class Invalidate(id: NsID)
    case object InvalidateAll
    case class UpdateRequest(time: Long, shouldContinue: Boolean = false)
    case object GetStatus
    case class UpdateAfterSuccessfulConsume(newIndexTime: Long,
                                            data: Map[NsID, (NsURL, NsPrefix)],
                                            shouldContinue: Boolean)
    case class UpdateAfterFailedConsume(originalCheckTime: Long, e: Throwable)

    sealed trait UpdateAfterFetch
    case class UpdateAfterSuccessfulFetch(id: NsID, tuple: (NsURL, NsPrefix)) extends UpdateAfterFetch
    case class UpdateAfterFailedFetch(id: NsID, count: Int, cause: Throwable) extends UpdateAfterFetch
    case class UpdateAfterSuccessfulURLFetch(url: NsURL, nsIDs: Map[NsID, (NsURL, NsPrefix)]) extends UpdateAfterFetch
    case class UpdateAfterFailedURLFetch(url: NsURL, count: Int, cause: Throwable) extends UpdateAfterFetch
    case class UpdateAfterSuccessfulPrefixFetch(prefix: NsPrefix, nsIDs: Map[NsID, (NsURL, NsPrefix)])
        extends UpdateAfterFetch
    case class UpdateAfterFailedPrefixFetch(prefix: NsPrefix, count: Int, cause: Throwable) extends UpdateAfterFetch
  }

  def apply(
    seed: Map[NsID, (NsURL, NsPrefix)],
    seedTimestamp: Long,
    coolDown: FiniteDuration,
    crudService: CRUDServiceFS
  )(implicit ec: ExecutionContext, sys: ActorSystem): TimeBasedAccumulatedNsCacheTrait = {

    val (urlToID, prefixToID) = validateLogAndGetInvertedCaches(seed)
    new TimeBasedAccumulatedNsCache(seed, urlToID, prefixToID, seedTimestamp, coolDown.toMillis, crudService)
    with LazyLogging
  }
}
