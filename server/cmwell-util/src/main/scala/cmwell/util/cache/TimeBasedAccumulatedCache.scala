//package cmwell.util.cache
//
//import java.util.concurrent.atomic.AtomicBoolean
//
//import com.typesafe.scalalogging.LazyLogging
//
//import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
//import scala.concurrent.{Await, ExecutionContext, Future}
//import ExecutionContext.{global => globalExecutionContext}
//import scala.util.{Failure, Success, Try}
//
///**
//  * Proj: server
//  * User: gilad
//  * Date: 2/4/18
//  * Time: 11:38 AM
//  */
//class TimeBasedAccumulatedCache[T,K,V1,V2] private(/*@volatile*/ private[this] var mainCache: Map[K,(V1,V2)],
//                                                   /*@volatile*/ private[this] var v1Cache: Map[V1,K],
//                                                   /*@volatile*/ private[this] var v2Cache: Map[V2,K],
//                                                   seedTimestamp: Long,
//                                                   coolDownMillis: Long,
//                                                   accumulateSince: Long => Future[T],
//                                                   timestampAndEntriesFromResults: T => (Long,Map[K,(V1,V2)]),
//                                                   getSingle: K => Future[(V1,V2)]) { self: LazyLogging =>
//
//  private[this] val isBeingUpdated = new AtomicBoolean(false)
//  /*@volatile*/ private[this] var timestamp: Long = seedTimestamp
//  /*@volatile*/ private[this] var checkTime: Long = System.currentTimeMillis()
//  private[this] var blacklist: Map[K,(Long,Int)] = Map.empty
//
//  @inline def getByV1(v1: V1): Option[K] = v1Cache.get(v1)
//  @inline def getByV2(v2: V2): Option[K] = v2Cache.get(v2)
//  @inline def get(key: K): Option[(V1,V2)] = mainCache.get(key).orElse(blacklist.get(key) match {
//    case Some((time,count)) if System.currentTimeMillis() - time > 1000L*count => getAnywayBlockingOnGlobal(key, math.min(30,count+1))
//    case None => getAnywayBlockingOnGlobal(key, 1)
//    case _ => None // last time we forcibly checked for `key` was less than `count` seconds ago
//  })
//
//  @inline private[this] def getAnywayBlockingOnGlobal(key: K, count: Int): Option[(V1,V2)] = {
//    val rv = Await.result(getAndUpdateSingle(key,true)(globalExecutionContext), 1.minute)
//    rv match {
//      case None => blacklist = blacklist.updated(key, System.currentTimeMillis() -> count)
//      case Some(_) => blacklist -= key
//    }
//    rv
//  }
//
//  @inline def getOrElseUpdate(key: K)(implicit ec: ExecutionContext): Future[Option[(V1,V2)]] =
//    get(key).fold(updateAndGet(key))(Future.successful[Option[(V1,V2)]] _ compose Some.apply)
//
//  private[this] def updateAndGet(key: K)(implicit ec: ExecutionContext): Future[Option[(V1,V2)]] = {
//    if(updatedRecently || !isBeingUpdated.compareAndSet(false,true)) Future.successful(mainCache.get(key))
//    else Try(accumulateSince(timestamp)).fold(Future.failed,identity).flatMap { results =>
//      val (newTimestamp, newVals) = timestampAndEntriesFromResults(results)
//
//      checkTime = System.currentTimeMillis()
//      if(newVals.isEmpty) Future.successful(mainCache.get(key))
//      else {
//        mainCache ++= newVals
//        val (v1Additions,v2Additions) = TimeBasedAccumulatedCache.getInvertedCaches(newVals)
//        v1Cache ++= v1Additions
//        v2Cache ++= v2Additions
//        timestamp = newTimestamp
//        newVals.get(key).fold[Future[Option[(V1,V2)]]] {
//          getAndUpdateSingle(key,false)
//        }(Future.successful[Option[(V1,V2)]] _ compose Some.apply)
//      }
//    }.andThen { case _ => isBeingUpdated.set(false) }
//  }
//
//  @inline private[this] def getAndUpdateSingle(key: K, shouldAcquireLock: Boolean)(implicit ec: ExecutionContext): Future[Option[(V1,V2)]] = {
//    val whenLocked = {
//      if (shouldAcquireLock) acquireLock()
//      else Future.successful(())
//    }
//    val f = whenLocked.flatMap { _ =>
//      Try(getSingle(key)).fold(Future.failed, identity).transform {
//        case Failure(_: NoSuchElementException) => Success(None)
//        case t => t map {
//          case v@(v1, v2) =>
//            mainCache += key -> v
//            v1Cache += v1 -> key
//            v2Cache += v2 -> key
//            Some(v)
//        }
//      }
//    }
//
//    if(!shouldAcquireLock) f
//    else f.andThen{ case _ => isBeingUpdated.set(false) }
//  }
//
//  def invalidate(k: K)(implicit ec: ExecutionContext): Future[Unit] = {
//    mainCache.get(k).fold(Future.successful(())) { case (v1, v2) =>
//      acquireLock().map { _ =>
//        if (mainCache.contains(k)) {
//          mainCache -= k
//          v1Cache -= v1
//          v2Cache -= v2
//        }
//        isBeingUpdated.set(false)
//      }
//    }
//  }
//
//  private[this] def updatedRecently: Boolean =
//    System.currentTimeMillis() - checkTime <= coolDownMillis
//
//  private[this] def acquireLock(maxDepth: Int = 16)(implicit ec: ExecutionContext): Future[Unit] = {
//    if(maxDepth == 0) Future.failed(new IllegalStateException(s"too many tries to acquire TimeBasedAccumulatedCache lock failed"))
//    else if(isBeingUpdated.compareAndSet(false,true)) Future.successful(())
//    else cmwell.util.concurrent.SimpleScheduler.scheduleFuture(40.millis)(acquireLock(maxDepth - 1))
//  }
//}
//
//
//object TimeBasedAccumulatedCache {
//
//
//  private def getInvertedCaches[K,V1,V2](m: Map[K,(V1,V2)]): (Map[V1,K],Map[V2,K]) = {
//    m.foldLeft(Map.empty[V1,K] -> Map.empty[V2,K]) {
//      case ((accv1,accv2),(k,(v1,v2))) =>
//        accv1.updated(v1, k) -> accv2.updated(v2, k)
//    }
//  }
//
//  def apply[T,K,V1,V2](seed: Map[K,(V1,V2)], seedTimestamp: Long, coolDown: FiniteDuration)
//                  (accumulateSince: Long => Future[T])
//                  (timestampAndEntriesFromResults: T => (Long,Map[K,(V1,V2)]))
//                  (getSingle: K => Future[(V1,V2)]): TimeBasedAccumulatedCache[T,K,V1,V2] = {
//    val (v1,v2) = getInvertedCaches(seed)
//    new TimeBasedAccumulatedCache(seed,v1,v2,
//      seedTimestamp,
//      coolDown.toMillis,
//      accumulateSince,
//      timestampAndEntriesFromResults,
//      getSingle) with LazyLogging
//  }
//}
