package ld.cmw

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import cmwell.domain.Infoton
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import cmwell.util.collections.{partitionWith, subtractedDistinctMultiMap, updatedDistinctMultiMap}
import cmwell.web.ld.exceptions.{ConflictingNsEntriesException, TooManyNsRequestsException}
import com.typesafe.scalalogging.LazyLogging
import logic.CRUDServiceFS

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * TODO:
  * - implement singular fetches by URL (guarded by blacklisting repeated failed queries)
  * - implement singular fetches by Prefix (guarded by blacklisting repeated failed queries)
  * - add full incremental update (consume style) - only if mainCache size is under size cap
  * - add invalidate by id (and by url/prefix ?) - make sure to invalidate auxiliary caches.
  * - add status reporting endpoint (see [[PassiveFieldTypesCache.getState]] for reference)
  * - implement ???s
  * - replace CMWellRDFHelper instantiazation with this class
  * - rm util "generic" version
  * - tests?
  */
trait TimeBasedAccumulatedNsCacheTrait {
  def getByURL(url: NsURL)(implicit timeout: Timeout): Future[NsID]
  def getByPrefix(prefix: NsPrefix)(implicit timeout: Timeout): Future[NsID]
  def get(key: NsID)(implicit timeout: Timeout): Future[(NsURL,NsPrefix)]
}

class TimeBasedAccumulatedNsCache private(private[this] var mainCache: Map[NsID,(NsURL,NsPrefix)],
                                          private[this] var urlCache: Map[NsURL,Set[NsID]],
                                          private[this] var prefixCache: Map[NsPrefix,Set[NsID]],
                                          seedTimestamp: Long,
                                          coolDownMillis: Long,
                                          crudService: CRUDServiceFS)
                                         (implicit ec: ExecutionContext, sys: ActorSystem) extends TimeBasedAccumulatedNsCacheTrait { _: LazyLogging =>

  import TimeBasedAccumulatedNsCache.Messages._

  // TODO: load from config
  val incrementingWaitTimeMillis = 1000L
  val maxCountIncrements = 30

  // public API

  @inline def getByURL(url: NsURL)(implicit timeout: Timeout): Future[NsID] = urlCache.get(url).fold((actor ? GetByURL(url)).mapTo[Set[NsID]].transform{
    case Failure(e) => Failure(new Exception(s"failed to getByURL($url) from ns cache",e))
    case Success(s) if s.isEmpty => Failure(new IllegalStateException(s"getByURL failed with EmptySet stored for [$url]"))
    case Success(s) if s.size == 1 => Success(s.head)
    case Success(many) => Failure(ConflictingNsEntriesException.byURL(url,many))
  }){
    case s if s.isEmpty => Future.failed(new IllegalStateException(s"getByURL failed with EmptySet stored for [$url]"))
    case s if s.size == 1 => Future.successful(s.head)
    case many => Future.failed(ConflictingNsEntriesException.byURL(url,many))
  }

  @inline def getByPrefix(prefix: NsPrefix)(implicit timeout: Timeout): Future[NsID] = prefixCache.get(prefix).fold((actor ? GetByPrefix(prefix)).mapTo[Set[NsID]].transform{
    case Failure(e) => Failure(new Exception(s"failed to getByPrefix($prefix) from ns cache",e))
    case Success(s) if s.isEmpty => Failure(new IllegalStateException(s"getByPrefix failed with EmptySet stored for [$prefix]"))
    case Success(s) if s.size == 1  => Success(s.head)
    case Success(many) => Failure(ConflictingNsEntriesException.byPrefix(prefix,many))
  }){
    case s if s.isEmpty => Future.failed(new IllegalStateException(s"getByPrefix failed with EmptySet stored for [$prefix]"))
    case s if s.size == 1  => Future.successful(s.head)
    case many => Future.failed(ConflictingNsEntriesException.byPrefix(prefix,many))
  }

  @inline def get(key: NsID)(implicit timeout: Timeout): Future[(NsURL,NsPrefix)] = mainCache.get(key).fold((actor ? GetByID(key)).mapTo[(NsURL,NsPrefix)].transform{
    case Failure(e) => Failure(new Exception(s"failed to get($key) from ns cache",e))
    case success => success
  })(Future.successful[(NsURL,NsPrefix)])

  // private section

  private[this] def props = Props(classOf[TimeBasedAccumulatedNsCacheActor])

  private[cmw] class TimeBasedAccumulatedNsCacheActor extends Actor {

    private[this] var timestamp: Long = seedTimestamp
    private[this] var checkTime: Long = 0L

    private[this] var nsIDBlacklist:     Map[NsID,(Long,Int)]     = Map.empty
    private[this] var nsURLBlacklist:    Map[NsURL,(Long,Int)]    = Map.empty
    private[this] var nsPrefixBlacklist: Map[NsPrefix,(Long,Int)] = Map.empty

    private[this] var nsIDFutureList:     Map[NsID,Future[(NsURL,NsPrefix)]] = Map.empty
    private[this] var nsURLFutureList:    Map[NsURL,Future[NsID]]            = Map.empty
    private[this] var nsPrefixFutureList: Map[NsPrefix,Future[NsID]]         = Map.empty

    override def receive: Receive = {
      case GetByID(id) => handleGetByID(id)
      case UpdateAfterSuccessfulFetch(id,tuple) => handleUpdateAfterSuccessfulFetch(id,tuple)
      case UpdateAfterFailedFetch(id,count,err) => handleUpdateAfterFailedFetch(id,count,err)
    }

    private[this] def handleGetByID(id: NsID): Unit = {
      val sndr = sender()
      mainCache.get(id) match {
        case Some(tuple) => sndr ! tuple
        case None => nsIDFutureList.get(id) match {
          case Some(future) => future.pipeTo(sndr)
          case None => nsIDBlacklist.get(id) match {
            case None => doFetch(id, sndr, 1)
            case Some((time, count)) => {
              if (hasEnoughIncrementalWaitTimePassed(time, count))
                doFetch(id, sndr, math.min(maxCountIncrements,count+1))
              else
                sndr ! Status.Failure(new TooManyNsRequestsException(s"Too frequent failed request after [$count] failures to resolve ns identifier [$id]"))
            }
          }
        }
      }
    }

    private[this] def doFetch(id: NsID, sndr: ActorRef, failureCount: Int): Unit = {
      val f = wrappedGetByNsID(id)
      nsIDFutureList = nsIDFutureList.updated(id, f)
      f.pipeTo(sndr)
      f.transform{
        case Failure(e) => Success(UpdateAfterFailedFetch(id, failureCount,e))
        case success => success.map(UpdateAfterSuccessfulFetch(id,_))
      }.pipeTo(self)
    }

    private[this] def wrappedGetByNsID(id: NsID): Future[(NsURL,NsPrefix)] =
      crudService.getInfotonByPathAsync(s"/meta/ns/$id").transform {
        case Failure(e) => Failure(new Exception(s"failed to get /meta/ns/$id", e))
        case Success(EmptyBox) => Failure(new NoSuchElementException(s"No such ns identifier: /meta/ns/$id"))
        case Success(BoxedFailure(e)) => Failure(new Exception(s"failed to get /meta/ns/$id fromIRW", e))
        case Success(FullBox(i)) => extractMetadataFromInfoton(i)
      }

    private[this] def extractMetadataFromInfoton(i: Infoton): Try[(NsURL,NsPrefix)] = {
      ???
    }

    private[this] def handleUpdateAfterSuccessfulFetch(id: NsID, tuple: (NsURL,NsPrefix)): Unit = {
      nsIDFutureList -= id
      val (u,p) = tuple
      mainCache.get(id) match {
        // all is good. nothing needs to be changed. use of `return` avoids mainCache redundant update
        // or else it would have to be repeated in all other cases
        case Some(`tuple`) => return
        case Some((`u`,oldPrefix)) =>
          prefixCache = updatedDistinctMultiMap(subtractedDistinctMultiMap(prefixCache,oldPrefix,id),p,id)
        case Some((oldURL,`p`)) =>
          urlCache = updatedDistinctMultiMap(subtractedDistinctMultiMap(urlCache,oldURL,id),u,id)
        case Some((oldU,oldP)) =>
          prefixCache = updatedDistinctMultiMap(subtractedDistinctMultiMap(prefixCache,oldP,id),p,id)
          urlCache = updatedDistinctMultiMap(subtractedDistinctMultiMap(urlCache,oldU,id),u,id)
        case None =>
          prefixCache = updatedDistinctMultiMap(prefixCache,p,id)
          urlCache = updatedDistinctMultiMap(urlCache,u,id)
      }
      mainCache = mainCache.updated(id,tuple)
    }

    private[this] def handleUpdateAfterFailedFetch(id: NsID, count: Int, err: Throwable): Unit = {
      nsIDFutureList -= id
      logger.error(s"failure to retrieve /meta/ns/$id [fail #$count]",err)
      nsIDBlacklist = nsIDBlacklist.updated(id,System.currentTimeMillis() -> count)
    }

    private[this] def hasEnoughIncrementalWaitTimePassed(since: Long, cappedCount: Int): Boolean =
      (System.currentTimeMillis() - since) > (cappedCount * incrementingWaitTimeMillis)

    private[this] def updatedRecently: Boolean =
      System.currentTimeMillis() - checkTime <= coolDownMillis

  }

  val actor = sys.actorOf(props)
}

object TimeBasedAccumulatedNsCache extends LazyLogging {


  private def getInvertedCaches(m: Map[NsID,(NsURL,NsPrefix)]): (Map[NsURL,NsID],Map[NsPrefix,NsID]) = {
    m.foldLeft(Map.empty[NsURL,NsID] -> Map.empty[NsPrefix,NsID]) {
      case ((accv1,accv2),(k,(v1,v2))) =>
        accv1.updated(v1, k) -> accv2.updated(v2, k)
    }
  }

  private def validateLogAndGetInvertedCaches(m: Map[NsID,(NsURL,NsPrefix)],
                                              urls: Map[NsURL,Set[NsID]] = Map.empty,
                                              prefs: Map[NsPrefix,Set[NsID]] = Map.empty): (Map[NsURL,Set[NsID]],Map[NsPrefix,Set[NsID]]) = {
    val (errors,mUrls,mPrefixes) = m.iterator.foldLeft((List.empty[Either[String,String]],urls,prefs)){
      case ((errs,_urls,_prefs),(id,(url,prefix))) => {
        val (errs1,urls1) = if (_urls.contains(url)) {
          (Left(url) :: errs) -> updatedDistinctMultiMap(_urls, url, id)
        } else {
          errs -> updatedDistinctMultiMap(_urls, url, id)
        }
        val (errs2,prefs1) = if(_prefs.contains(prefix)) {
          (Right(prefix) :: errs1) -> updatedDistinctMultiMap(_prefs, prefix, id)
        } else {
          errs1 -> updatedDistinctMultiMap(_prefs, prefix, id)
        }
        (errs2,urls1,prefs1)
      }
    }

    val rv = mUrls -> mPrefixes
    if(errors.isEmpty) rv
    else if(errors.forall(_.isRight)) {
      val msg = errors.map(_.right.get).distinct.sorted.mkString("Multiple prefix ambiguities detected: [",",","]")
      logger.error(msg)
      rv
    } else {
      val (u,p) = partitionWith(errors)(identity)
      val msgU = u.distinct.sorted.mkString("Multiple URL ambiguities detected: [",",","]")
      val msg =
        if(p.isEmpty) msgU
        else msgU + p.distinct.sorted.mkString(", and multiple prefix ambiguities detected: [",",","]")
      logger.error(msg)
      rv
    }
  }

  object Messages {
    case class GetByID(id: NsID)
    case class GetByURL(id: NsURL)
    case class GetByPrefix(id: NsPrefix)

    trait UpdateAfterFetch
    case class UpdateAfterSuccessfulFetch(id: NsID, tuple: (NsURL,NsPrefix)) extends UpdateAfterFetch
    case class UpdateAfterFailedFetch(id: NsID, count: Int, cause: Throwable) extends UpdateAfterFetch
  }


  def apply(seed: Map[NsID,(NsURL,NsPrefix)], seedTimestamp: Long, coolDown: FiniteDuration, crudService: CRUDServiceFS)
           (implicit ec: ExecutionContext, sys: ActorSystem): TimeBasedAccumulatedNsCache = {

    val (urlToID,prefixToID) = validateLogAndGetInvertedCaches(seed)
    new TimeBasedAccumulatedNsCache(
      seed,
      urlToID,
      prefixToID,
      seedTimestamp,
      coolDown.toMillis,
      crudService) with LazyLogging
  }
}
