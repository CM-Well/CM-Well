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


package cmwell.bg

import akka.pattern.ask
import cmwell.driver.Dao
import cmwell.fts.FTSServiceNew
import cmwell.irw.IRWService
import cmwell.common.ZStoreOffsetsService
import cmwell.zstore.ZStore
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import k.grid.service.{ServiceInitInfo, ServiceTypes}
import k.grid.{Grid, GridConnection}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by israel on 11/23/15.
  */
object Runner extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val irwServiceDaoClusterName = config.getString("irwServiceDao.clusterName")
    val irwServiceDaoKeySpace = config.getString("irwServiceDao.keySpace")
    val irwServiceDaoHostName = config.getString("irwServiceDao.hostName")
    val partition = config.getInt("cmwell.bg.persist.commands.partition") //main partition for this process
    val numOfPartitions = config.getInt("cmwell.kafka.numOfPartitions")
    val zkServers = config.getString("cmwell.kafka.zkServers")
    val casDao = Dao(irwServiceDaoClusterName,irwServiceDaoKeySpace,irwServiceDaoHostName)
    val irwService = {

      val irwServiceDaoClusterName = config.getString("irwServiceDao.clusterName")
      val irwServiceDaoKeySpace = config.getString("irwServiceDao.keySpace")
      val irwServiceDaoHostName = config.getString("irwServiceDao.hostName")
      val irwServiceDao = Dao(irwServiceDaoClusterName,irwServiceDaoKeySpace,irwServiceDaoHostName)

      IRWService.newIRW(irwServiceDao,false)
    }

    val ftsService = FTSServiceNew("bg.es.yml")

    val zStore = ZStore(casDao)

    val offsetsService = new ZStoreOffsetsService(zStore)

    Grid.setGridConnection(GridConnection(memberName = "bg", labels = Set("bg")))
    Grid.declareServices(ServiceTypes().
      add("KafkaMonitor", classOf[KafkaMonitorActor], zkServers, 15 * 60 * 1000L, concurrent.ExecutionContext.Implicits.global).
      addLocal(s"BGActor$partition", classOf[CMWellBGActor], partition, config, irwService, ftsService, zStore, offsetsService).
      //     This is part of a temp solution to make Grid service start current partition actor here and register for failover
      //     purposes rest of partitions
      add(
        (1 to numOfPartitions-1).toSet.filter(_ != partition).map{ par =>
          s"BGActor$par" -> ServiceInitInfo(classOf[CMWellBGActor], None, par, config, irwService, ftsService, zStore, offsetsService)
        }.toMap
      ))

    Grid.joinClient

    Thread.sleep(60000)

    val actorSystem = Grid.system

    val cmwellBGActor = Grid.serviceRef(s"BGActor$partition")

    sys.addShutdownHook {
      logger info s"shutting down cmwell-bg's Indexer and Imp flows before process is exiting"
      Await.result(ask(cmwellBGActor, ShutDown)(30.seconds), 30.seconds)
    }

  }
}


