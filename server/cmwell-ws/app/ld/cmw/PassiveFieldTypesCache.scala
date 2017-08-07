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


package ld.cmw

import akka.actor.{Actor, ActorPath, ActorRef, ActorSystem, Cancellable, Props}
import akka.pattern._
import cmwell.domain.{FString, Infoton}
import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.scalalogging.LazyLogging
import logic.CRUDServiceFS
import wsutil.{FieldKey, NnFieldKey}

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{Set => MSet}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// TODO: indexTime based search for changes since last change?
object PassiveFieldTypesCache {
  private[cmw] lazy val uniqueIdentifierForActorName = {
    val n = cmwell.util.os.Props.machineName
    if(ActorPath.isValidPathElement(n)) n
    else cmwell.util.string.Hash.crc32(n)
  }

  private[cmw] case object UpdateCache
  private[cmw] case object UpdateCompleted
  private[cmw] case class RequestUpdateFor(field: FieldKey)
  private[cmw] case class UpdateAndGet(field: FieldKey)
  private[cmw] case class Put(field: String, types: Set[Char], reportWhenDone: Boolean = false, reportTo: Option[ActorRef] = None)

  private[cmw] class PassiveFieldTypesCacheActor(crudService: CRUDServiceFS, nbg: Boolean, cache: Cache[String,Either[Future[Set[Char]],(Long, Set[Char])]], updatingExecutionContext: ExecutionContext) extends Actor with LazyLogging {

    var requestedCacheUpdates: MSet[FieldKey] = _
    var cancellable: Cancellable = _

    override def preStart() = {
      requestedCacheUpdates = MSet.empty[FieldKey]
      cancellable = context.system.scheduler.schedule(1.second, 2.minutes, self, UpdateCache)(updatingExecutionContext, self)
    }

    override def receive: Receive = {
      case RequestUpdateFor(field) => requestedCacheUpdates += field
      case UpdateCache => if (requestedCacheUpdates.nonEmpty) {
        requestedCacheUpdates.foreach { fk =>
          val maybe = cache.getIfPresent(fk.internalKey)
          if (maybe eq null) {
            val lefuture = Left(getMetaFieldInfoton(fk).map(infoptToChars)(updatingExecutionContext))
            cache.put(fk.internalKey, lefuture)
          }
          else maybe.right.foreach {
            case (oTime, chars) => {
              getMetaFieldInfoton(fk).foreach { infopt =>
                val types = infoptToChars(infopt)
                if (types.diff(chars).nonEmpty) {
                  self ! Put(fk.internalKey, types union chars)
                }
              }(updatingExecutionContext)
            }
          }
        }
        requestedCacheUpdates.clear()
      }
      case Put(internalKey,types,reportWhenDone,reportTo) => {
        lazy val sendr = reportTo.getOrElse(sender())
        val maybe = cache.getIfPresent(internalKey)
        if(maybe eq null) {
          cache.put(internalKey,Right(System.currentTimeMillis() -> types))
          if(reportWhenDone) {
            sendr ! UpdateCompleted
          }
        }
        else maybe match {
          case Left(future) => future.onComplete {
            case Failure(error) => self ! Put(internalKey,types,reportWhenDone,Some(sendr))
            case Success(chars) => {
              if ((types diff chars).nonEmpty) self ! Put(internalKey, types union chars, reportWhenDone, Some(sendr))
              else if (reportWhenDone) sendr ! UpdateCompleted
            }
          }(updatingExecutionContext)
          case Right((_,chars)) => {
            if (types.diff(chars).nonEmpty) {
              cache.put(internalKey, Right(System.currentTimeMillis() -> (chars union types)))
            }
            if(reportWhenDone) {
              sendr ! UpdateCompleted
            }
          }
        }
      }
      case UpdateAndGet(field: FieldKey) => {
        val sndr = sender()
        val rv = getMetaFieldInfoton(field).map(infoptToChars)(updatingExecutionContext)
        rv.onComplete {      //field.metaPath is already completed as it is memoized in a lazy val if it is truly async
          case Failure(e) => logger.error(s"failed to update cache for: ${field.metaPath}", e)
          case Success(types) => {
            val nTime = System.currentTimeMillis()
            lazy val right = Right(nTime->types)
            sndr ! types
            // provided cache should have passed a cache that has concurrencyLevel set to 1.
            // So we should avoid useless updates, nevertheless,
            // it's okay to risk blocking on the cache's write lock here,
            // because writes are rare (once every 2 minutes, and on first-time asked fields)
            val internalKey = field.internalKey
            val maybe = cache.getIfPresent(internalKey)
            if (maybe eq null) cache.put(internalKey,right)
            else maybe match {
              case Right((oTime,chars)) if types.diff(chars).isEmpty => if(nTime > oTime) cache.put(internalKey,right)
              case Right((oTime,chars)) => cache.put(internalKey,Right(System.currentTimeMillis → chars.union(types)))
              case Left(charsFuture) => charsFuture.onComplete {
                case Failure(error) => {
                  logger.error("future stored in types cache failed",error)
                  self ! Put(internalKey,types)
                }
                case Success(chars) => if (types diff chars nonEmpty) {
                  self ! Put(internalKey, types union chars)
                }
              }(updatingExecutionContext)
            }
          }
        }(updatingExecutionContext)
      }
    }

    private def infoptToChars(infopt: Option[Infoton]) = {
      val typesOpt = infopt.flatMap(_.fields.flatMap(_.get("mang")))
      typesOpt.fold(Set.empty[Char])(_.collect{
        case FString(t, _, _) if t.length == 1 => t.head
      })
    }

    private def getMetaFieldInfoton(field: FieldKey): Future[Option[Infoton]] =
      crudService.getInfoton(field.metaPath, None, None, nbg).map(_.map(_.infoton))(updatingExecutionContext)
  }
}

trait PassiveFieldTypesCacheTrait {
  def get(fieldKey: FieldKey, forceUpdateForType: Option[Set[Char]] = None)(implicit ec: ExecutionContext): Future[Set[Char]]
  def update(fieldKey: FieldKey, types: Set[Char])(implicit ec: ExecutionContext): Future[Unit]
}

abstract class PassiveFieldTypesCache(val cache: Cache[String,Either[Future[Set[Char]],(Long, Set[Char])]]) extends PassiveFieldTypesCacheTrait { this: LazyLogging =>

  import PassiveFieldTypesCache._

  implicit val timeout = akka.util.Timeout(10.seconds)
  private val cbf = implicitly[CanBuildFrom[MSet[FieldKey],(String,FieldKey),MSet[(String,FieldKey)]]]

  def get(fieldKey: FieldKey, forceUpdateForType: Option[Set[Char]] = None)(implicit ec: ExecutionContext): Future[Set[Char]] = fieldKey match {
    // TODO: instead of checking a `FieldKey` for `NnFieldKey(k) if k.startsWith("system.")` maybe it is better to add `SysFieldKey` ???
    case NnFieldKey(k) if k.startsWith("system.") || k.startsWith("content.") || k.startsWith("link.") => Future.successful(Set.empty)
    case field => Try {
      val key = field.internalKey
      val maybeEither = cache.getIfPresent(key)
      if (maybeEither eq null) (actor ? UpdateAndGet(field)).mapTo[Set[Char]]
      else maybeEither match {
        case Right((ts, types)) => forceUpdateForType match {
          case None =>
            if (System.currentTimeMillis() - ts > 30000) {
              actor ! RequestUpdateFor(field)
            }
            Future.successful(types)
          case Some(forcedTypes) =>
            if(forcedTypes.diff(types).nonEmpty || (System.currentTimeMillis() - ts > 30000))
              (actor ? UpdateAndGet(field)).mapTo[Set[Char]]
            else Future.successful(types)
        }
        case Left(fut) => fut
      }
    }.recover{
      case t: Throwable => Future.failed[Set[Char]](t)
    }.get
  }

  def update(fieldKey: FieldKey, types: Set[Char])(implicit ec: ExecutionContext): Future[Unit] = fieldKey match {
    case NnFieldKey(k) if k.startsWith("system.") || k.startsWith("content.") || k.startsWith("link.") => Future.successful(())
    case field => {
      val key = field.internalKey
      lazy val doneFut = (actor ? Put(key, types, true)).map(_ => ())
      val maybeEither = cache.getIfPresent(key)
      if (maybeEither eq null) doneFut
      else maybeEither match {
        case Right((_, set)) =>
          if ((types diff set).nonEmpty) doneFut
          else Future.successful(())
        case Left(future) => future.flatMap { set =>
          if ((types diff set).nonEmpty) doneFut
          else future.map(_ => ())
        }.recoverWith {
          case err: Throwable => {
            logger.error("cannot update cache. internalKey failure.", err)
            doneFut
          }
        }
      }
    }
  }

  def getState: String = {
    import scala.collection.JavaConverters._
    val m = cache.asMap().asScala
    val sb = new StringBuilder("[\n")
    m.foreach{
      case (k,v) =>
        sb.append(s"\t$k : $v\n")
    }
    sb.append("]").result()
  }

  protected def createActor: ActorRef = null.asInstanceOf[ActorRef]

  private[this] lazy val actor: ActorRef = createActor
}

/** !!!!!!!!!!!!!!!
  * !!! WARNING !!!
  * !!!!!!!!!!!!!!!
  *
  * Following 2 implementation classes of cache define an actor using a const name (per machine).
  * This means each of these classes may only be instantiated ONCE per [[ActorSystem]]!
  */
class NbgPassiveFieldTypesCache(crud: CRUDServiceFS, ec: ExecutionContext, sys: ActorSystem) extends
  // cache's concurrencyLevel set to 1, so we should avoid useless updates,
  // nevertheless, it's okay to risk blocking on the cache's write lock here,
  // because writes are rare (once every 2 minutes, and on first-time asked fields)
  PassiveFieldTypesCache(CacheBuilder.newBuilder().concurrencyLevel(1).build()) with LazyLogging {

  private val props = Props(classOf[PassiveFieldTypesCache.PassiveFieldTypesCacheActor], crud, true, cache, ec)
  override def createActor: ActorRef = sys.actorOf(props,"NbgPassiveFieldTypesCache_" + PassiveFieldTypesCache.uniqueIdentifierForActorName)
}
class ObgPassiveFieldTypesCache(crud: CRUDServiceFS, ec: ExecutionContext, sys: ActorSystem) extends
  // cache's concurrencyLevel set to 1, so we should avoid useless updates,
  // nevertheless, it's okay to risk blocking on the cache's write lock here,
  // because writes are rare (once every 2 minutes, and on first-time asked fields)
  PassiveFieldTypesCache(CacheBuilder.newBuilder().concurrencyLevel(1).build())  with LazyLogging {

  private val props = Props(classOf[PassiveFieldTypesCache.PassiveFieldTypesCacheActor], crud, false, cache, ec)
  override def createActor: ActorRef = sys.actorOf(props,"ObgPassiveFieldTypesCache_" + PassiveFieldTypesCache.uniqueIdentifierForActorName)
}