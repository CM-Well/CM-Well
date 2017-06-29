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

import akka.actor.{Cancellable, ActorRef, Actor}
import akka.pattern._
import cmwell.domain.{Infoton, FString}
import cmwell.util.concurrent._
import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid
import logic.CRUDServiceFS
import wsutil.{NnFieldKey, FieldKey}
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{Set => MSet}
import scala.collection.parallel.mutable
import scala.concurrent.duration._
import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.util.{Try, Failure, Success}

object PassiveFieldTypesCache extends LazyLogging {

  // TODO: indexTime based search for changes since last change
  // TODO: instead of checking a `FieldKey` for `NnFieldKey(k) if k.startsWith("system.")` maybe it is better to add `SysFieldKey` ???

  case object UpdateCache
  case object UpdateCompleted
  case class RequestUpdateFor(field: FieldKey)
  case class UpdateAndGet(field: FieldKey)
  case class Put(field: String, types: Set[Char], reportWhenDone: Boolean = false, reportTo: Option[ActorRef] = None)

  implicit val timeout = akka.util.Timeout(10.seconds)
  private val cbf = implicitly[CanBuildFrom[MSet[FieldKey],(String,FieldKey),MSet[(String,FieldKey)]]]
//  private val cbf = scala.collection.breakOut[MSet[FieldKey],(String,FieldKey),Map[String,FieldKey]]

  def get(fieldKey: FieldKey, forceUpdateForType: Option[Set[Char]] = None)(implicit ec: ExecutionContext): Future[Set[Char]] = fieldKey match {
    case NnFieldKey(k) if k.startsWith("system.") || k.startsWith("content.") || k.startsWith("link.") => Future.successful(Set.empty)
    case field => Try {
      field.internalKey.flatMap { key =>
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
      }
    }.recover{
      case t: Throwable => Future.failed[Set[Char]](t)
    }.get
  }

  def update(fieldKey: FieldKey, types: Set[Char])(implicit ec: ExecutionContext): Future[Unit] = fieldKey match {
    case NnFieldKey(k) if k.startsWith("system.") || k.startsWith("content.") || k.startsWith("link.") => Future.successful(())
    case field => field.internalKey.flatMap { key =>
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

  //TODO: think of using a different more suitable `ExecutionContext` instead of `global`
  private[this] val actor: ActorRef = Grid.createAnon(classOf[PassiveFieldTypesCacheActor],scala.concurrent.ExecutionContext.Implicits.global)
  private[this] val cache: Cache[String,Either[Future[Set[Char]],(Long, Set[Char])]] = CacheBuilder.newBuilder().concurrencyLevel(1).build()

  class PassiveFieldTypesCacheActor(updatingExecutionContext: ExecutionContext) extends Actor {

    var requestedCacheUpdates: MSet[FieldKey] = _
    var cancellable: Cancellable = _

    override def preStart() = {
      requestedCacheUpdates = MSet.empty[FieldKey]
      cancellable = context.system.scheduler.schedule(1.second, 2.minutes, self, UpdateCache)(updatingExecutionContext, self)
    }

    override def receive: Receive = {
      case RequestUpdateFor(field) => requestedCacheUpdates += field
      case UpdateCache => if (requestedCacheUpdates.nonEmpty) {
        val keysFut = Future.traverse(requestedCacheUpdates)(fk => fk.internalKey.map(_ -> fk)(updatingExecutionContext))(cbf,updatingExecutionContext)
        keysFut.foreach { keys =>
          keys.toMap.foreach {
            case (internalKey, fk) => {
              val maybe = cache.getIfPresent(internalKey)
              if (maybe eq null) {
                val lefuture = Left(getMetaFieldInfoton(fk).map(infoptToChars)(updatingExecutionContext))
                cache.put(internalKey, lefuture)
              }
              else maybe.right.foreach {
                case (oTime, chars) =>  {
                  getMetaFieldInfoton(fk).foreach { infopt =>
                    val types = infoptToChars(infopt)
                    if (types.diff(chars).nonEmpty) {
                      self ! Put(internalKey, types union chars)
                    }
                  }(updatingExecutionContext)
                }
              }
            }
          }
        }(updatingExecutionContext)
        requestedCacheUpdates.clear()// = MSet.empty[FieldKey]
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
              if ((types diff chars).nonEmpty) actor ! Put(internalKey, types union chars, reportWhenDone, Some(sendr))
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
          case Failure(e) => logger.error(s"failed to update cache for: ${field.metaPath.value.get}", e)
          case Success(types) => {
            val nTime = System.currentTimeMillis()
            lazy val right = Right(nTime->types)
            sndr ! types
            // cache's concurrencyLevel set to 1, so we should avoid useless updates,
            // nevertheless, it's okay to risk blocking on the cache's write lock here,
            // because writes are rare (once every 2 minutes, and on first-time asked fields)
            field.internalKey.foreach { internalKey =>
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
            }(updatingExecutionContext)
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

    private def getMetaFieldInfoton(field: FieldKey): Future[Option[Infoton]] = field.metaPath.flatMap { path =>
      CRUDServiceFS.getInfoton(path, None, None).map(_.map(_.infoton))(updatingExecutionContext)
    }(updatingExecutionContext)
  }
}