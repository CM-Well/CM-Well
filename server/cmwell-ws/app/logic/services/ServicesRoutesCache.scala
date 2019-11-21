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
package logic.services

import akka.actor.{Actor, ActorSystem, Props}
import cmwell.domain.Infoton
import cmwell.fts.PathFilter
import cmwell.ws.Settings
import com.typesafe.scalalogging.LazyLogging
import javax.inject.{Inject, Singleton}
import logic.CRUDServiceFS

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration.DurationInt

@Singleton
class ServicesRoutesCache @Inject()(crudService: CRUDServiceFS)(implicit ec: ExecutionContext, sys: ActorSystem) extends LazyLogging {
  private var services: Map[String, ServiceDefinition] = Map.empty
  private val helperActor = sys.actorOf(Props(new ServicesRoutesCacheHelperActor), "ServicesRoutesCacheHelperActor")
  private implicit val timeout: Timeout = 60.seconds

  def find(path: String): Option[ServiceDefinition] =
    services.find { case (route, _) => path.startsWith(route) }.map(_._2)

  def list: Set[String] = services.keySet

  def refresh(): Future[Unit] = (helperActor ? PopulateOnce).mapTo[Unit]

  // Letting WS some initial delay to start and warm up, then fetching data from CRUD:
  sys.scheduler.scheduleOnce(Settings.servicesRoutesCacheInitialDelay, helperActor, Populate)

  class ServicesRoutesCacheHelperActor extends Actor {
    private def populate(): Future[Unit] = {
      def desrialize(infoton: Infoton): Try[ServiceDefinition] = Try {
        val fields = infoton.fields.getOrElse(throw new RuntimeException(s"Infoton with no fields was not expected (path=${infoton.path})"))

        def field(name: String): String = fields(name).head.value.toString

        val route = field("route")
        field("type.lzN1FA") match {
          case "cmwell://meta/sys#Redirection" =>
            val sourcePattern = field("sourcePattern")
            val replacement = field("replacement")
            val replceFunc = (input: String) => sourcePattern.r.replaceAllIn(input, replacement)
            RedirectionService(route, sourcePattern, replceFunc)
          case "cmwell://meta/sys#Source" => ??? //TODO implement the unimplemented
          case "cmwell://meta/sys#Binary" => ??? //TODO implement the unimplemented
          case other => throw new RuntimeException(s"Infoton with type $other was not expected (path=${infoton.path})")
        }
      }

      //TODO use consume API, don't get everything each time
      crudService.search(Some(PathFilter("/meta/services", descendants = false)), withData = true).andThen {
        case Success(sr) => sr.infotons.
          map(desrialize).
          collect { case Success(sd) => sd }.
          foreach { sd => services += sd.route -> sd }
        case Failure(t) => logger.error("Could not load Services from /meta/services", t)
      }.map(_ => ())
    }

    override def receive: Receive = {
      case Populate =>
        populate().andThen { case _ =>
            context.system.scheduler.scheduleOnce(Settings.servicesRoutesCacheRefreshInterval, self, Populate)
        }

      case PopulateOnce =>
        val s = sender()
        populate().andThen { case _ => s ! () }
    }
  }

  case object Populate
  case object PopulateOnce

}
