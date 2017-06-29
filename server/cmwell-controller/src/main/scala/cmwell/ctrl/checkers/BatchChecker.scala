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


package cmwell.ctrl.checkers

import cmwell.ctrl.checkers.WebChecker._
import cmwell.ctrl.controllers.{BatchController, WebserverController}
import cmwell.ctrl.utils.ProcUtil
import cmwell.tlog.{TLog, TLogState}
import com.typesafe.scalalogging.LazyLogging


import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import cmwell.ctrl.config.Config._
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

/**
 * Created by michael on 12/3/14.
 */
object BatchChecker extends Checker with RestarterChecker with LazyLogging {
  override val storedStates: Int = 10
  val impState = TLogState("imp" , updatesTLogName , updatesTLogPartition, true)
  val indexerState = TLogState("indexer" , uuidsTLogName , updatesTLogPartition, true )

  val updatesTlog = TLog(updatesTLogName, updatesTLogPartition, readOnly = true)
  updatesTlog.init()

  val uuidsTlog = TLog(uuidsTLogName, uuidsTLogPartition, readOnly = true)
  uuidsTlog.init()



  override def restartAfter: FiniteDuration = 10.minutes

  override def doRestart: Unit = {
    BatchController.restart
    logger.warn(s"Batch was restarted after $storedStates sequential failures.")
  }

  override def check: Future[ComponentState] = {
    if(ProcUtil.checkIfProcessRun("batch") > 0) {
      val lastState = getLastStates(1).headOption
      val newState = lastState match {
        case Some(cs : BatchState) =>

          if(cs.isIndexing(updatesTlog.size , impState.position, uuidsTlog.size , indexerState.position))
            BatchOk(updatesTlog.size , impState.position, uuidsTlog.size , indexerState.position, (impState.position - cs.impLocation) / (batchSampleInterval / 1000), (indexerState.position - cs.indexerLocation) / (batchSampleInterval / 1000))
          else
            BatchNotIndexing(updatesTlog.size , impState.position, uuidsTlog.size , indexerState.position)
        case None => BatchOk(updatesTlog.size , impState.position, uuidsTlog.size , indexerState.position)
        case _ =>
          logger.error("Wrong ComponentStatus is in BatchStates!")
          BatchOk(updatesTlog.size , impState.position, uuidsTlog.size , indexerState.position)
      }

      Future.successful(newState)
    } else
      Future.successful(BatchDown(updatesTlog.size , impState.position, uuidsTlog.size , indexerState.position))
  }
}
