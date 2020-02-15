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
package cmwell.bg

import cmwell.common.ZStoreOffsetsService
import cmwell.driver.Dao
import cmwell.fts.FTSService
import cmwell.irw.IRWService
import cmwell.zstore.ZStore
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import k.grid.service.ServiceTypes
import k.grid.{Grid, GridConnection}
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by israel on 11/23/15.
  */
object Runner extends LazyLogging {

  def main(args: Array[String]): Unit = {
    try {
      logger.info("Starting BG process")
      //SLF4J initialization is not thread safe, so it's "initialized" by writing some log and only then using sendSystemOutAndErrToSLF4J.
      //Without it there will be en error in stderr and some log line at the beginning will be lost
      SysOutOverSLF4J.sendSystemOutAndErrToSLF4J()
      val config = ConfigFactory.load()
      logger.info(s"Loaded Configuration:\n ${config
        .entrySet()
        .asScala
        .collect { case entry if entry.getKey.startsWith("cmwell") => s"${entry.getKey} -> ${entry.getValue.render()}" }
        .mkString("\n")}")

      val partition = config.getInt("cmwell.bg.persist.commands.partition") //main partition for this process
      val numOfPartitions = config.getInt("cmwell.kafka.numOfPartitions")
      val zkServers = config.getString("cmwell.kafka.zkServers")

      val casDao = {
        val irwServiceDaoClusterName = config.getString("irwServiceDao.clusterName")
        val irwServiceDaoKeySpace = config.getString("irwServiceDao.keySpace")
        val irwServiceDaoHostName = config.getString("irwServiceDao.hostName")
        Dao(irwServiceDaoClusterName, irwServiceDaoKeySpace, irwServiceDaoHostName, 9042, initCommands = None)
      }

      // casTimeout = Duration.Inf means not to use timeoutFuture in IRW.
      val irwService = IRWService.newIRW(casDao, disableReadCache = false, casTimeout = Duration.Inf)
      val zStore = ZStore(casDao)
      val ftsService = FTSService(config)
      val offsetsService = new ZStoreOffsetsService(zStore)

      val serviceTypes = {
        val neighborhoodPartitions = (partition to partition+2) map (_ % numOfPartitions)

        val basicServiceTypes = ServiceTypes()
/*NOTE: ZkUtils was removed from newer versions of Kafka. Also, this class never actually worked so it's not a regression to remove it.
          .add("KafkaMonitor",
            classOf[KafkaMonitorActor],
            zkServers,
            15 * 60 * 1000L,
            concurrent.ExecutionContext.Implicits.global)
*/
        neighborhoodPartitions.foldLeft(basicServiceTypes) { (st,par) =>
          st.add(s"BGActor$par", classOf[CMWellBGActor], par,
            config.withValue("cmwell.bg.persist.commands.partition", ConfigValueFactory.fromAnyRef(par))
              .withValue("cmwell.bg.index.commands.partition", ConfigValueFactory.fromAnyRef(par)),
            irwService, ftsService, zStore, offsetsService
          )
        }
      }

      Grid.setGridConnection(GridConnection(memberName = "bg", labels = Set("bg")))
      Grid.declareServices(serviceTypes)

      Grid.joinClient

      Thread.sleep(60000)
    } catch {
      case t: Throwable =>
        logger.error(s"BG Process failed to start thus exiting. Reason:\n${cmwell.common.exception.getStackTrace(t)}")
        sys.exit(1)
    }
  }
}
