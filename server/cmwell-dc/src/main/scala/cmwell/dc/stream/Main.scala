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
package cmwell.dc.stream

import akka.actor.ActorSystem
import cmwell.ctrl.hc.HealthControl
import cmwell.dc.{LazyLogging, Settings}
import cmwell.tools.data.sparql.{SparqlProcessorManager, SparqlProcessorManagerSettings}
import cmwell.tracking.ResurrectorActor
import k.grid.service.ServiceTypes
import k.grid.{Grid, GridConnection}
import org.rogach.scallop.ScallopConf
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J

/**
  * Created by gilad on 1/4/16.
  */
object Main extends App with LazyLogging {
  import Settings._
  logger.info("Starting Dc-Sync using stream")
  //SLF4J initialization is not thread safe, so it's "initialized" by writing some log and only then using sendSystemOutAndErrToSLF4J.
  //Without it there will be en error in stderr and some log line at the beginning will be lost
  SysOutOverSLF4J.sendSystemOutAndErrToSLF4J()

  Grid.setGridConnection(GridConnection(memberName = "dc"))
  Grid.declareServices(
    ServiceTypes()
      .add("DataCenterSyncManager", classOf[DataCenterSyncManager], destinationHostsAndPorts(rawTarget), None)
      .add(HealthControl.services)
      .add(SparqlProcessorManager.name, classOf[SparqlProcessorManager], new SparqlProcessorManagerSettings)
      .add("Resurrector", classOf[ResurrectorActor])
  )
  Grid.joinClient
  HealthControl.init
  Thread.sleep(10000)
}

object MainStandAlone extends App with LazyLogging {
  import Settings._

  implicit val sys = ActorSystem("ExtrenalSystem")

  val conf = new Conf(args)

  val ar =
    sys.actorOf(DataCenterSyncManager.props(destinationHostsAndPorts(conf.destinationHosts()), Some(conf.syncJson())))
}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//  val syncJson = opt[String](required = true)
  val syncJson = trailArg[String]()
  val destinationHosts = trailArg[String]()
  verify()
}
