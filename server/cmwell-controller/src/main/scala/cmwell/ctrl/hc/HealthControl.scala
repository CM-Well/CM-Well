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
package cmwell.ctrl.hc

import akka.util.Timeout
import cmwell.ctrl.config.Config._
import cmwell.ctrl.controllers.CmwellController
import cmwell.ctrl.server._
import cmwell.ctrl.tasks.TaskExecutorActor
import k.grid.dmap.impl.inmem.InMemDMap
import k.grid.service.ServiceInitInfo
import k.grid.Grid
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 4/11/16.
  */
object HealthControl {
  private val clusterLogger = LoggerFactory.getLogger("cluster_state")

  lazy val services = Map(
    "CassandraMonitorActor" -> ServiceInitInfo(classOf[CassandraMonitorActor],
                                               None,
                                               Grid.hostName,
                                               0,
                                               hcSampleInterval),
    "DcMonitorActor" -> ServiceInitInfo(classOf[DcMonitorActor], None, Grid.hostName, 0, hcSampleInterval),
    "HealthActor" -> ServiceInitInfo(classOf[HealthActor], None)
  )

  def init: Unit = {
    import akka.pattern.ask
    implicit val timeout = Timeout(3.seconds)

    def joinCtrlGrid: Unit = {
      val forceMajor = sys.env.get("FORCE").isDefined
      clusterLogger.info(s"force majour is $forceMajor")
      Try(
        Await.result(
          cmwell.util.concurrent.retry(10, 5.seconds) { HealthActor.ref ? NodeJoinRequest(host) }.mapTo[JoinResponse],
          30.seconds
        )
      ) match {
        case Success(r) =>
          clusterLogger.info(s"$host got $r from the cluster $clusterName")
          r match {
            case response if forceMajor =>
              clusterLogger.info(
                s"$host received response $response from the cluster $clusterName. Doing force majour will boot cm-well components."
              )
              HealthActor.ref ! RemoveFromDownNodes(host)
              CmwellController.start
            case JoinOk =>
              CmwellController.start
            case JoinShutdown =>
              //CmwellController.clearData
              sys.exit(0)
            case JoinBootComponents =>
              CmwellController.start
          }
        case Failure(err) =>
          clusterLogger.error(err.getMessage, err)
          clusterLogger.info(
            s"$host didn't receive response from the cluster $clusterName, will boot cm-well components."
          )
          CmwellController.start
      }
    }

    Grid.create(classOf[CommandActor], commandActorName)

    Grid.create(classOf[WebMonitorActor], "WebMonitorActor", Grid.hostName, 0, hcSampleInterval)
    Grid.create(classOf[BgHealthMonitorActor], "BgHealthMonitorActor", Grid.hostName, 0, bgSampleInterval)
    Grid.create(classOf[ElasticsearchMonitorActor], "ElasticsearchMonitorActor", Grid.hostName, 0, hcSampleInterval)
    Grid.create(classOf[SystemMonitorActor], "SystemMonitorActor", Grid.hostName, 0, hcSampleInterval)
    Grid.create(classOf[ZookeeperMonitorActor], "ZookeeperMonitorActor", Grid.hostName, 0, hcSampleInterval)
    Grid.create(classOf[KafkaMonitorActor], "KafkaMonitorActor", Grid.hostName, 0, hcSampleInterval)

    Grid.create(classOf[PingActor], "PingActor")
    Grid.serviceRef(TaskExecutorActor.name)
    //Thread.sleep(20000)
    // Grid.system.scheduler.scheduleOnce(20.seconds) {
    joinCtrlGrid
    InMemDMap.aggregate("knownHosts", host)
    //}
  }
}
