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
package cmwell.tracking

import java.util.NoSuchElementException

import akka.actor.ActorRef
import akka.pattern.ask
import cmwell.common.StatusTracking
import cmwell.ctrl.config.Jvms
import cmwell.domain.Infoton
import cmwell.driver.Dao
import cmwell.irw.IRWService
import cmwell.util.FullBox
import cmwell.util.concurrent.{retry, travector}
import cmwell.zcache.L1Cache
import cmwell.zstore.ZStore
import com.typesafe.scalalogging.LazyLogging
import k.grid.{Grid, GridJvm}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Created by yaakov on 3/15/17.
  */
trait TrackingUtil {

  val zStore: ZStore
  val irw: IRWService

  def spawn(actorName: String,
            data: Set[String] = Set.empty,
            createTime: Long = System.currentTimeMillis()): Future[(ActorRef, TrackingId)]
  def update(trackingId: TrackingId)(pss: PathStatus*): Future[TrackingOperationResponse]
  def readStatus(trackingId: TrackingId): Future[Seq[PathStatus]]
  def debug(trackingId: TrackingId)(msg: Any): Unit

  private val malformedTidResponse = Future.successful[TrackingOperationResponse](LogicalFailure("malformed tid"))

  // done is syntactic sugar for update(Done) with Strings
  def done(tid: String, path: String): Future[TrackingOperationResponse] =
    TrackingId
      .unapply(tid)
      .fold(malformedTidResponse)(update(_)(PathStatus(path, Done)))

  def update(path: String, st: StatusTracking): Future[TrackingOperationResponse] =
    TrackingId
      .unapply(st.tid)
      .fold(malformedTidResponse)(update(_)(PathStatus(path, PartialDone(1, st.numOfParts))))

  def cleanDirtyData(data: Seq[PathStatus], createTime: Long): Future[Seq[PathStatus]]

  def updateEvicted(path: String, st: StatusTracking, reason: String): Future[TrackingOperationResponse] =
    updateEvicted(path, st.tid, reason)

  def updateEvicted(path: String, tid: String, reason: String): Future[TrackingOperationResponse] =
    TrackingId
      .unapply(tid)
      .fold(malformedTidResponse)(update(_)(PathStatus(path, Evicted(reason))))

  // todo merge PathsStatus and StatusTracking case classes...
  def updateSeq(path: String, statusTrackings: Seq[StatusTracking]): Future[TrackingOperationResponse] =
    statusTrackings
      .map(update(path, _))
      .foldLeft(Future.successful(Successful)) {
        case (f1, f2) => f1.zip(f2).map(_._1)
      }
      .recoverWith { case t: NoSuchElementException => Future.successful(LogicalFailure(t.getMessage)) }

  /**
    * {{{
    * scala> cmwell.tracking.TrackingUtil().actorIdFromActorPath("akka://tracking/user/LTIyNzkyMzc1Mg")
    * res0: String = LTIyNzkyMzc1Mg
    * }}}
    */
  def actorIdFromActorPath[T: Stringer](actorPath: T): String = {
    val stringActorPath = implicitly[Stringer[T]].stringify(actorPath)
    stringActorPath.drop(stringActorPath.lastIndexOf('/') + 1).takeWhile(_ != '#')
  }
}

trait Stringer[T] {
  def stringify(t: T): String
}

object Stringer {
  implicit val identityStringer = new Stringer[String] {
    override def stringify(s: String): String = s
  }

  implicit val actorRefStringer = new Stringer[ActorRef] {
    override def stringify(ar: ActorRef): String = ar.path.toSerializationFormat
  }
}

object TrackingUtil {
  def zStore: ZStore = TrackingUtilImpl.zStore
  def apply(): TrackingUtil = TrackingUtilImpl
}

object TrackingUtilImpl extends TrackingUtil with LazyLogging {

  private lazy val dao: Dao =
    Dao(Settings.irwServiceDaoClusterName, Settings.irwServiceDaoKeySpace2, Settings.irwServiceDaoHostName, 9042, initCommands = None)
  override lazy val zStore: ZStore = ZStore.apply(dao)
  override lazy val irw: IRWService = IRWService.newIRW(dao)

  implicit val timeout = akka.util.Timeout(3.seconds)

  private lazy val system = Grid.system

  private val toOpResponse: Future[Unit] => Future[TrackingOperationResponse] =
    _.map { _ =>
      Successful
    }.recover { case t: NoSuchElementException => LogicalFailure(t.getMessage) }

  def spawn(actorName: String, initialData: Set[String], createTime: Long): Future[(ActorRef, TrackingId)] = {
    Try {
      (resurrector ? Spawn(actorName, initialData, Seq.empty[PathStatus], createTime)).map {
        case ta: ActorRef =>
          ta -> TrackingId(actorIdFromActorPath(ta), createTime)
      }
    } match {
      case Success(x) => x
      case Failure(e) => logger.error(s"Tracking: failed to spawn", e); throw e
    }
  }

  def update(trackingId: TrackingId)(pss: PathStatus*): Future[TrackingOperationResponse] = {
    val result = resolveActor(trackingId).map(actor => pss.foreach(actor.!))
    toOpResponse(result)
  }

  def readStatus(trackingId: TrackingId): Future[Seq[PathStatus]] =
    resolveActor(trackingId).flatMap { actor =>
      logger.debug(s"Tracking: Going to Read from $actor...")
      (actor ? Read).mapTo[Seq[PathStatus]]
    }

  def debug(trackingId: TrackingId)(msg: Any): Unit =
    resolveActor(trackingId).foreach(_ ! msg)

  private val resolveActor = {
    val task = (trackingId: TrackingId) => {
      val TrackingId(actorAddr, _) = trackingId
      logger.debug(s"Resolving Actor: $actorAddr")
      Grid.selectActor(actorIdFromActorPath(actorAddr), GridJvm(Jvms.WS)).resolveOne().recoverWith {
        case t: Throwable =>
          logger.warn(s"Tracking: Could not resolve TrackingActor($actorAddr), will try to resurrect.", t)
          ressurect(trackingId)
      }
    }
    L1Cache.memoize[TrackingId, ActorRef](task)(_.token)(l1Size = 256, ttlSeconds = 8)
  }

  private def ressurect(trackingId: TrackingId): Future[ActorRef] = {
    retry(3, 10.millis)((resurrector ? Resurrect(trackingId)).mapTo[Option[ActorRef]].andThen {
      case Failure(t) =>
        logger.warn(
          s"Tracking: [ressurect.Failure] Could not resolve TrackingActor($trackingId), will try to resurrect.",
          t
        )
    }).flatMap {
      case Some(ref) => Future.successful(ref)
      case None      => Future.failed(new NoSuchElementException("could not resolve actor"))
    }
  }

  def cleanDirtyData(data: Seq[PathStatus], createTime: Long): Future[Seq[PathStatus]] = {
    def isNew(i: Infoton) = (i.systemFields.lastModified.isAfter(createTime)) && i.systemFields.indexTime.isDefined

    val (inProgress, notInProgress) = data.partition(_.status == InProgress)
    if (inProgress.isEmpty) Future.successful(data)
    else {

      val inProgressPaths: Vector[String] = inProgress.view.map(_.path).to(Vector)
      val infotonsFut = travector(inProgressPaths)(irw.readPathAsync(_))
      val alreadyDonePathsFut = infotonsFut.map { infotons =>
        infotons.collect {
          case FullBox(infoton) if isNew(infoton) => infoton.systemFields.path
        }.toSet
      }
      alreadyDonePathsFut.map { alreadyDonePaths =>
        notInProgress ++ alreadyDonePaths.map(PathStatus(_, Done)) ++ inProgress.filterNot(
          ps => alreadyDonePaths(ps.path)
        )
      }
    }
  }

  private def resurrector = Grid.serviceRef("Resurrector")
}

sealed trait TrackingOperationResponse
case object Successful extends TrackingOperationResponse // in future, might as well be case class Success(value), if needed
case class LogicalFailure(reason: String) extends TrackingOperationResponse
