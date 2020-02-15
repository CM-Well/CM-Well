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
package logic.services

import akka.actor.ActorSystem
import cmwell.domain.{DeletedInfoton, Infoton, ObjectInfoton}
import cmwell.fts.PathFilter
import cmwell.ws.Settings
import com.typesafe.scalalogging.LazyLogging
import javax.inject.{Inject, Singleton}
import logic.CRUDServiceFS

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

@Singleton
class ServicesRoutesCache @Inject()(crudService: CRUDServiceFS)(implicit ec: ExecutionContext, sys: ActorSystem) extends LazyLogging {
  private var services: Map[String, ServiceDefinition] = Map.empty
  private val (initialDelay, interval) = Settings.servicesRoutesCacheInitialDelay -> Settings.servicesRoutesCacheRefreshInterval

  sys.scheduler.schedule(initialDelay, interval)(populate())

  def find(path: String): Option[ServiceDefinition] =
    services.values.find(sd => path.startsWith(sd.route))

  def list: Set[String] = services.values.map(_.route).toSet

  def populate(): Future[Unit] = {
    //TODO use consume API, don't get everything each time
    crudService.search(Some(PathFilter("/meta/services", descendants = false)), withData = true, withDeleted = true).andThen {
      case Success(sr) =>
        val toAddOrUpdate = sr.infotons.collect { case oi: ObjectInfoton => oi }
        val toRemove = sr.infotons.collect { case di: DeletedInfoton => di }

        toAddOrUpdate.map(desrialize).
          collect { case Success(se) => se }.
          foreach { se => services += se.infotonPath -> se.serviceDefinition }

        toRemove.foreach(services -= _.systemFields.path)

      case Failure(t) => logger.error("Could not load Services from /meta/services", t)
    }.map(_ => ())
  }

  private def desrialize(infoton: Infoton): Try[ServiceEntry] = Try {
    val fields = infoton.fields.getOrElse(throw new RuntimeException(s"Infoton with no fields was not expected (path=${infoton.systemFields.path})"))

    def field(name: String): String = fields(name).head.value.toString

    val route = field("route")
    field("type.lzN1FA") match {
      case "cmwell://meta/sys#Redirection" =>
        val sourcePattern = field("sourcePattern")
        val replacement = field("replacement")
        val replceFunc = (input: String) => sourcePattern.r.replaceAllIn(input, replacement)
        ServiceEntry(infoton.systemFields.path, RedirectionService(route, sourcePattern, replceFunc))
      case "cmwell://meta/sys#Source" => ??? //TODO implement the unimplemented
      case "cmwell://meta/sys#Binary" => ??? //TODO implement the unimplemented
      case other => throw new RuntimeException(s"Infoton with type $other was not expected (path=${infoton.systemFields.path})")
    }
  }

  case class ServiceEntry(infotonPath: String, serviceDefinition: ServiceDefinition)
}
