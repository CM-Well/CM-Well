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


package cmwell.batch.boot

import cmwell.ctrl.config.Jvms
import cmwell.indexer.Indexer
import com.typesafe.config.ConfigFactory
import cmwell.tlog.{TLog, TLogState}
import cmwell.imp.IMPService
import cmwell.fts.FTSServiceES
import com.typesafe.scalalogging.LazyLogging

import concurrent._
import scala.compat.Platform._
import scala.concurrent.ExecutionContext.Implicits.global
import cmwell.driver.Dao

import scala.util.Success
import scala.util.Failure
import cmwell.irw.IRWService
import k.grid.{Grid, GridConnection}
import cmwell.rts.Publisher
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J

import scala.util.Failure
import scala.util.Success

/**
 * User: Israel
 * Date: 5/20/13
 * Time: 12:05 PM
 */
object Runner extends App with LazyLogging{

  import Settings._


//  val isIndexer:Boolean =  java.lang.Boolean.valueOf(Option(System.getProperty("isIndexer")).getOrElse("false"))

  logger info ("Running batch")
  //SLF4J initialization is not thread safe, so it's "initialized" by writing some log and only then using sendSystemOutAndErrToSLF4J.
  //Without it there will be en error in stderr and some log line at the beginning will be lost
  SysOutOverSLF4J.sendSystemOutAndErrToSLF4J()

  //logger info(s"Grid.join with clusterName [$clusterName] gridBindIP [$gridBindIP] gridSeeds [$gridSeeds] gridBindPort [$gridBindPort]")

  Grid.setGridConnection(GridConnection(memberName = "batch", labels = Set("publisher")))

  Grid.joinClient
  Thread.sleep(1000)
  Publisher.init

  val updatesTlog = TLog(updatesTLogName, updatesTLogPartition)
  updatesTlog.init()


  val uuidsTlog = TLog(uuidsTLogName, uuidsTLogPartition)
  uuidsTlog.init()

  val irwServiceDao = Dao(irwServiceDaoClusterName,irwServiceDaoKeySpace,irwServiceDaoHostName)

  logger.info("If you got nothing to log, Logan, don't log logan!")
  val irwService = IRWService(irwServiceDao,false)


  val ftsService = FTSServiceES.getOne("ftsService.yml")



  //val fIndexer = future {blocking{ indexer.index}}
  //fIndexer.onComplete{case Success(_) => logger info("Indexer exited"); case Failure(t) => logger error (t.getLocalizedMessage + "\n" + t.getCause.getStackTraceString + "\n" + t.getStackTraceString)}

  logger debug ("starting imp")
  val impState = TLogState("imp" , updatesTLogName , updatesTLogPartition)
  impState.loadState
  val impService = IMPService(updatesTlog, uuidsTlog, irwService, impState , impParallelism, impBatchSize)

  val fImp = Future {blocking{impService.process}   }
  fImp.onComplete{case Success(_) => logger info("IMP exited"); case Failure(t) => logger error (t.getCause.getMessage + "\n" + t.getLocalizedMessage + "\n" + t.getCause.getStackTrace().mkString("", EOL, EOL) + "\n" + t.getStackTrace().mkString("", EOL, EOL))}

//  logger info ("waiting 7 minutes before starting indexer")
//  Thread.sleep(1000 * 60 * 7)
  val indexerState = TLogState("indexer" , uuidsTLogName , updatesTLogPartition)
  //  val indexer = RateIndexer(uuidsTlog, irwService, ftsService, indexerState)
  //  indexer.start
  val akkaIndexer = new Indexer(uuidsTlog, updatesTlog, irwService, ftsService, indexerState)

  sys.addShutdownHook{
//    logger info ("stopping indexing service")
//    indexer.terminate
    logger info ("stopping imp service")
    impService.terminate


    // shutdown tlog
    updatesTlog.shutdown()

    uuidsTlog.shutdown()

    irwServiceDao.shutdown()
    // shutdown dao
    irwServiceDao.shutdown()

    Grid.shutdown

    logger info ("Asta La Vista Baby.")

  }


}

object Settings {
  val hostName = java.net.InetAddress.getLocalHost.getHostName

  val config = ConfigFactory.load()

  // tLogs DAO
  val tLogsDaoHostName = config.getString("tLogs.hostName")
  val tLogsDaoClusterName = config.getString("tLogs.cluster.name")
  val tLogsDaoKeySpace = config.getString("tLogs.keyspace")
  val tLogsDaoColumnFamily = config.getString("tLogs.columnFamilyName")
  val tLogsDaoMaxConnections = config.getInt("tLogs.maxConnections")

  // updates tLog
  val updatesTLogName = config.getString("updatesTlog.name")
  val updatesTLogPartition = try { config.getString("updatesTlog.partition") } catch { case _:Throwable => "updatesPar_" + hostName}

  // uuids tLog
  val uuidsTLogName = config.getString("uuidsTlog.name")
  val uuidsTLogPartition = try { config.getString("uuidsTlog.partition") } catch { case _:Throwable => "uuidsPar_" + hostName }

  // infotons DAO
  val irwServiceDaoHostName = config.getString("irwServiceDao.hostName")
  val irwServiceDaoClusterName = config.getString("irwServiceDao.clusterName")
  val irwServiceDaoKeySpace = config.getString("irwServiceDao.keySpace")

  val impParallelism = config.getInt("imp.parallelism")
  val impBatchSize = config.getInt("imp.batchSize")

  val gridBindIP = config.getString("cmwell.grid.bind.host")
  val gridBindPort = config.getInt("cmwell.grid.bind.port")
  val gridSeeds = Set.empty[String] ++ config.getString("cmwell.grid.seeds").split(";")
  val clusterName = config.getString("cmwell.clusterName")

//  val pollingInterval = config.getLong("indexer.pollingInterval")
//  val bucketsSize = config.getInt("indexer.bucketsSize")


}
