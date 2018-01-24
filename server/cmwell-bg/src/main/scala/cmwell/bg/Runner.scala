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

import akka.actor.ActorRef
import akka.pattern.ask
import ch.qos.logback.classic.LoggerContext
import cmwell.driver.Dao
import cmwell.fts.FTSServiceNew
import cmwell.irw.IRWService
import cmwell.common.ZStoreOffsetsService
import cmwell.zstore.ZStore
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import k.grid.service.{ServiceInitInfo, ServiceTypes}
import k.grid.{Grid, GridConnection}
import org.slf4j.LoggerFactory
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import collection.JavaConverters._

/**
  * Created by israel on 11/23/15.
  */
object Runner extends LazyLogging {

  def main(args: Array[String]): Unit = {
    var cmwellBGActor:ActorRef = null
    try {
      logger.info("Starting BG process")
      //SLF4J initialization is not thread safe, so it's "initialized" by writing some log and only then using sendSystemOutAndErrToSLF4J.
      //Without it there will be en error in stderr and some log line at the beginning will be lost
      SysOutOverSLF4J.sendSystemOutAndErrToSLF4J()
      val config = ConfigFactory.load()
        logger info s"Loaded Configuration:\n ${config.entrySet().asScala.collect{case entry if entry.getKey.startsWith("cmwell") => s"${entry.getKey} -> ${entry.getValue.render()}"}.mkString("\n")}"
      val irwServiceDaoClusterName = config.getString("irwServiceDao.clusterName")
      val irwServiceDaoKeySpace = config.getString("irwServiceDao.keySpace")
      val irwServiceDaoHostName = config.getString("irwServiceDao.hostName")
      val partition = config.getInt("cmwell.bg.persist.commands.partition") //main partition for this process
      val numOfPartitions = config.getInt("cmwell.kafka.numOfPartitions")
      val zkServers = config.getString("cmwell.kafka.zkServers")
      val casDao = Dao(irwServiceDaoClusterName, irwServiceDaoKeySpace, irwServiceDaoHostName)
      val irwService = {

        val irwServiceDaoClusterName = config.getString("irwServiceDao.clusterName")
        val irwServiceDaoKeySpace = config.getString("irwServiceDao.keySpace")
        val irwServiceDaoHostName = config.getString("irwServiceDao.hostName")
        val irwServiceDao = Dao(irwServiceDaoClusterName, irwServiceDaoKeySpace, irwServiceDaoHostName)

        IRWService.newIRW(irwServiceDao, false, 120.seconds)
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
        (1 to numOfPartitions - 1).toSet.filter(_ != partition).map { par =>
          s"BGActor$par" -> ServiceInitInfo(classOf[CMWellBGActor], None, par, config, irwService, ftsService, zStore, offsetsService)
        }.toMap
      ))

      Grid.joinClient

      Thread.sleep(60000)

      val actorSystem = Grid.system

      cmwellBGActor = Grid.serviceRef(s"BGActor$partition")
    } catch {
      case t:Throwable =>
        logger error s"BG Process failed to start thus exiting. Reason:\n${cmwell.common.exception.getStackTrace(t)}"
        sys.exit(1)
    }

    sys.addShutdownHook {
      logger info s"shutting down cmwell-bg's Indexer and Imp flows before process is exiting"
      try {
        Await.result(ask(cmwellBGActor, ShutDown)(30.seconds), 30.seconds)
      } catch{
        case t:Throwable =>
          logger error "BG Process failed to send Shutdown message to BGActor during shutdownhook. " +
            s"Reason:\n${cmwell.common.exception.getStackTrace(t)}"
      }
      // Since logger is async, this is to ensure we don't miss any lines
      LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext].stop()
      println("existing BG Runner")
    }

  }
}


