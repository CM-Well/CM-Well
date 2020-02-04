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
package cmwell.ctrl.checkers

import java.net.InetSocketAddress
import java.util.concurrent.TimeoutException

import akka.stream.{ActorMaterializer, ActorMaterializerSettings, KillSwitches}
import akka.stream.scaladsl.{Keep, Sink, Source, Tcp}
import akka.util.ByteString
import cmwell.ctrl.config.Config
import cmwell.ctrl.hc.ZookeeperUtils
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid

import scala.concurrent.{blocking, Future, Promise}
import scala.sys.process._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

/**
  * Created by michael on 9/25/16.
  */
object ZookeeperChecker extends Checker with LazyLogging {
  implicit val system = Grid.system
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))

  def isZkNode = ZookeeperUtils.isZkNode

  override def check: Future[ComponentState] = {
    val p = Promise[ComponentState]()
    singleTcp("localhost", 2181, "ruok").map(_.trim).onComplete {
      case Success("imok") =>
        singleTcp("localhost", 2181, "isro").map(_.trim).onComplete {
          case Success("rw") => p.success(ZookeeperOk())
          case Success("ro") => p.success(ZookeeperReadOnly())
          case Success(response) =>
            logger.warn(s"Zookeeper: unexpected answer to isro: $response")
            p.success(ZookeeperNotOk())
          case Failure(ex) => p.failure(ex)
        }
      case Success(response) =>
        logger.warn(s"Zookeeper: unexpected answer to ruok: $response")
        p.success(ZookeeperNotOk())
      case _ =>
        Try(isZkNode) match {
          case Success(true)  => p.success(ZookeeperSeedNotRunning())
          case Success(false) => p.success(ZookeeperNotRunning())
          case Failure(err) =>
            p.failure(err)
            logger.warn("Checking if zookeeper node failed with", err)
        }
    }
    p.future
  }

  private def singleTcp(host: String, port: Int, request: String): Future[String] = {
    val (killSwitch, futureByteString) = Source
      .single(ByteString(request))
      .via(Tcp().outgoingConnection(new InetSocketAddress(host, port), None, Nil, true, 5.seconds, 5.seconds))
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.head)(Keep.both)
      .run()
    val p = Promise[Unit]()
    akka.pattern.after(7.seconds, system.scheduler)(Future.successful(p.trySuccess(())))
    val scheduleFuture = p.future.map(Left.apply)
    val responseFuture = futureByteString.map(Right.apply)
    Future.firstCompletedOf[Either[Unit, ByteString]](List(scheduleFuture, responseFuture)).onComplete {
      case Success(Left(_)) =>
        logger
          .warn("Zookeeper: timeout checking state. Cancelling check. It will be checked again on next scheduled check")
        killSwitch.abort(new TimeoutException("Zookeeper server didn't respond in time"))
      case _ =>
    }
    scheduleFuture.onComplete {
      case Failure(err) =>
        // scalastyle:off
        logger.warn(s"Zookeeper: $err occurred in status checker timeout scheduler. Cancelling check (only if it didn't finish already). It will be checked again on next scheduled check")
        // scalastyle:on
        killSwitch.abort(new TimeoutException("Zookeeper error occurred in status checker timeout scheduler and server didn't respond in time"))
      case _ =>
    }
    futureByteString.map(_.utf8String)
  }
  /*
    private def singleTcp(host: String, port: Int, request: String): Future[String] = {
      Source.single(ByteString(request))
        .via(Tcp().outgoingConnection(new InetSocketAddress(host, port), None, Nil, true, 5.seconds, 5.seconds))
        .toMat(Sink.head)(Keep.right)
        .run()
        .map(_.utf8String)
    }
 */
}
