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





import actions.RequestMonitor
import cmwell.ws.BGMonitorActor
import cmwell.ctrl.client.CtrlClient
import cmwell.fts.{PaginationParams, PathFilter, SortParam}
import cmwell.rts.Subscriber
import cmwell.common.ZStoreOffsetsService
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Settings._
import com.typesafe.scalalogging.LazyLogging
import k.grid.{Grid, GridConnection}
import k.grid.service.ServiceTypes
import logic.CRUDServiceFS
import play.api.{Logger, _}
import security.NoncesManager
import javax.inject._

import cmwell.domain.SearchResults

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J

/**
 * Created with IntelliJ IDEA.
 * User: gilad
 * Date: 11/26/13
 * Time: 12:16 PM
 * To change this template use File | Settings | File Templates.
 */
@Singleton
class Global @Inject()(crudServiceFS: CRUDServiceFS, cmwellRDFHelper: CMWellRDFHelper)(implicit ec: ExecutionContext) extends LazyLogging {

  onStart

  def onStart {

    //SLF4J initialization is not thread safe, so it's "initialized" by writing some log and only then using sendSystemOutAndErrToSLF4J.
    //Without it there will be en error in stderr and some log line at the beginning will be lost.
    //In this case some log lines are already printed and there is no need to write another one.
    SysOutOverSLF4J.sendSystemOutAndErrToSLF4J()

    Grid.setGridConnection(GridConnection(memberName = "ws", labels = Set("subscriber")))

    val offsetsService = new ZStoreOffsetsService(crudServiceFS.zStore)

    Grid.declareServices(ServiceTypes().
      add(BGMonitorActor.serviceName, classOf[BGMonitorActor], zkServers, offsetsService, concurrent.ExecutionContext.Implicits.global).
      add(classOf[NoncesManager].getName, classOf[NoncesManager])
    )

    logger.info("The known jvms are: " + Grid.jvmIdentities)

    Grid.joinClient

    CtrlClient.init

    Subscriber.init

    val recoverWithExitOnFail: PartialFunction[Throwable,Unit] = {
      case err : Throwable => {
        Logger.error("Failed to connect with CRUDService. Will exit now.",err)
        sys.exit(1)
      }
    }

    val recoverWithLogOnFail: Boolean => PartialFunction[Try[SearchResults],Unit] = nbg => {
      case Success(sr) => updateCaches(sr, nbg)
      case Failure(ex) => logger.error("Failed to connect with CRUDService. Will exit now.",ex)
    }

    // Y do we need this? do we want to check both new & old data paths? should we?
    // what if it's not enabled?
    // should we also inject NbgToggler, and only query against nbg = tbg.get?
    // Try(crudServiceFS.getInfoton("/", None, None)).recover(recoverWithExitOnFail)

    RequestMonitor.init

    import scala.concurrent.duration._


    scheduleAfterStart(30.seconds){
      Try{
        val fn = cmwell.util.concurrent.retry(3) {
          crudServiceFS.search(
            pathFilter = Some(PathFilter("/meta/ns", descendants = false)),
            fieldFilters = None,
            datesFilter = None,
            paginationParams = PaginationParams(0, initialMetaNsLoadingAmount),
            withHistory = false,
            withData = true,
            fieldSortParams = SortParam.empty,
            nbg = true)
        }

        val fo = cmwell.util.concurrent.retry(3) {
          crudServiceFS.search(
            pathFilter = Some(PathFilter("/meta/ns", descendants = false)),
            fieldFilters = None,
            datesFilter = None,
            paginationParams = PaginationParams(0, initialMetaNsLoadingAmount),
            withHistory = false,
            withData = true,
            fieldSortParams = SortParam.empty,
            nbg = false)
        }

        fn.andThen(recoverWithLogOnFail(true))
        fo.andThen(recoverWithLogOnFail(false))

      }.recover{
        case err: Throwable => logger.error("unexpected error occured in Global initialization",err)
      }
    }

    Logger.info("Application has started")
  }

  private def updateCaches(sr: SearchResults, nbg: Boolean) = {

    val groupedByUrls = sr.infotons.groupBy(_.fields.flatMap(_.get("url")))
    val goodInfotons = groupedByUrls.collect { case (Some(k),v) if k.size==1 =>
      val url = k.head.value.asInstanceOf[String]
      cmwellRDFHelper.getTheNonGeneratedMetaNsInfoton(url, v, nbg)
    }

    cmwellRDFHelper.loadNsCachesWith(goodInfotons.toSeq, nbg)
  }

  def onStop {
    Grid.shutdown
    Logger.info("Application has stopped")
  }

  def scheduleAfterStart(duration: FiniteDuration)(task: =>Unit): Unit = cmwell.util.concurrent.SimpleScheduler.schedule(duration)(task)
}
