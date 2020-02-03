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
package cmwell.crashableworker

import java.io.ByteArrayOutputStream

import actions.RequestMonitor
import akka.actor.{Actor, ActorRef, Props}
import cmwell.ctrl.config.Jvms
import cmwell.util.collections._
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.query.{Config, DataFetcher, DataFetcherImpl, JenaArqExtensions}
import com.google.inject.{AbstractModule, Guice}
import com.typesafe.scalalogging.LazyLogging
import controllers._
import k.grid.{Grid, GridConnection}
import ld.query.{ArqCache, JenaArqExtensionsUtils}
import ld.query.JenaArqExtensionsUtils.BakedSparqlQuery
import logic.CRUDServiceFS
import org.apache.jena.query.{QueryFactory, ResultSetFormatter}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.slf4j.LoggerFactory
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class CWModule extends AbstractModule {
  override def configure() {
    // bind singletons here, example:
    //bind(classOf[MySingleton]).asEagerSingleton()
  }
}

object WorkerMain extends App with LazyLogging {
  logger.info("Starting CW process")
  //SLF4J initialization is not thread safe, so it's "initialized" by writing some log and only then using sendSystemOutAndErrToSLF4J.
  //Without it there will be en error in stderr and some log line at the beginning will be lost
  SysOutOverSLF4J.sendSystemOutAndErrToSLF4J()

  Grid.extraDataCollector = () => QueryEvaluatorActor.getExtraData

  Grid.setGridConnection(GridConnection(memberName = "cw"))
  Grid.joinClient
  Thread.sleep(5000)
  RequestMonitor.init

//  Use injected singletons example:
//  val injector = Guice.createInjector(new CWModule())
//  val mySingleton = injector.getInstance(classOf[MySingleton])

  val crudServiceFS = new CRUDServiceFS()(implicitly, Grid.system)
  val cmwellRDFHelper = new CMWellRDFHelper(crudServiceFS, implicitly, Grid.system)
  val arqCache = new ArqCache(crudServiceFS)
  val dataFetcher = new DataFetcherImpl(Config.defaultConfig, crudServiceFS)
  val jenaArqExtensionsUtils =
    new JenaArqExtensionsUtils(arqCache, crudServiceFS.passiveFieldTypesCache, cmwellRDFHelper, dataFetcher)

  val jarsImporter = new JarsImporter(crudServiceFS)
  val queriesImporter = new QueriesImporter(crudServiceFS)
  val sourcesImporter = new SourcesImporter(crudServiceFS)

  val ref = Grid.create(
    classOf[QueryEvaluatorActor],
    "QueryEvaluatorActor",
    crudServiceFS,
    arqCache,
    jenaArqExtensionsUtils,
    dataFetcher,
    jarsImporter,
    queriesImporter,
    sourcesImporter
  )

  Grid.create(Props(classOf[QueryEvaluatorActorWatcher], ref), "QueryEvaluatorActorWatcher")

  val jenaArqExtensions = JenaArqExtensions.get(jenaArqExtensionsUtils)
}

sealed trait QueryResponse {
  def content: String
  def stats: Map[String, String]
}
case class Plain(content: String, stats: Map[String, String] = Map.empty) extends QueryResponse
case class Filename(content: String, stats: Map[String, String] = Map.empty) extends QueryResponse
case class RemoteFailure(failure: Throwable, stats: Map[String, String] = Map.empty) extends QueryResponse {
  override def content = throw failure
}
case class ThroughPipe(pipeName: String, stats: Map[String, String] = Map.empty) extends QueryResponse {
  override def content: String = ??? //read from pipe
}
case class ShortCircuitOverloaded(numActiveRequests: Int, stats: Map[String, String] = Map.empty)
    extends QueryResponse {
  override def content: String = ???
}
case class Status(counter: Int, stats: Map[String, String] = Map.empty) extends QueryResponse { // for debugging purposes
  override def content: String = s"numActiveQueries is $counter"
}

object QueryEvaluatorActor {
  private var activeQueryMap = Map.empty[String, Int]

  def get(name: String): Int = activeQueryMap.get(name).getOrElse(0)

  def set(name: String, value: Int) = activeQueryMap = activeQueryMap.updated(name, value)

  def getExtraData = {
    activeQueryMap
      .map {
        case (n, v) => s"#aq: $v"
      }
      .mkString("\n")
  }
}

class QueryEvaluatorActor(crudServiceFS: CRUDServiceFS,
                          arqCache: ArqCache,
                          jenaArqExtensionsUtils: JenaArqExtensionsUtils,
                          dataFetcher: DataFetcher,
                          jarsImporter: JarsImporter,
                          queriesImporter: QueriesImporter,
                          sourcesImporter: SourcesImporter)
    extends Actor
    with SpFileUtils {

  import QueryEvaluatorActor._
  private case class SpResponse(sender: ActorRef, queryResponse: QueryResponse)
  private case class SpFailure(sender: ActorRef, ex: Throwable)
  private def myName = self.path.name
  private def numActiveQueries = get(myName)
  private def updateActiveQueries(delta: Int) = set(myName, get(myName) + delta)

  val responseThreshold = 64 * 1024

  val ACTIVE_REQUESTS_DELAY_THRESHOLD = Runtime.getRuntime.availableProcessors
  val CIRCUIT_BREAKER = Runtime.getRuntime.availableProcessors * 4
//  var numActiveQueries = 0

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    logger.info("Creating actor QueryEvaluatorActor")
    deleteTempFiles()
  }

  private def isNeedToDelay = numActiveQueries > ACTIVE_REQUESTS_DELAY_THRESHOLD

  override def receive = watchedReceive()

  def watchedReceive(watcher: Option[ActorRef] = None): Receive = {
    case /*Luke,*/ IAmYourWatcher => {
      context.become(watchedReceive(Some(sender())))
    }
    case StatusRequest => sender ! Status(numActiveQueries) // for debugging purposes

    case paq: PopulateAndQuery if numActiveQueries > CIRCUIT_BREAKER =>
      watcher.foreach(_ ! Activate)
      sender() ! ShortCircuitOverloaded(numActiveQueries)

    case paq: PopulateAndQuery => {
      updateActiveQueries(+1)

      Try(paq.evaluate(jarsImporter, queriesImporter, sourcesImporter)) match {
        case Success(queryResults) => {
          val results = queryResults.flatMap {
            case (qr, stats) => rawDataToResponseMsg(qr, stats, paq.rp.forceUsingFile)
          }
          val originalSender = sender

          results.onComplete {
            case Success(res) => self ! SpResponse(originalSender, res)
            case Failure(err) => self ! SpFailure(originalSender, err)
          }
        }
        case Failure(err) => {
          updateActiveQueries(-1)
          watcher.foreach(_ ! Reset)
          sender() ! RemoteFailure(err)
        }
      }
    }

    case OverallSparqlQuery(_, _, _) if numActiveQueries > CIRCUIT_BREAKER =>
      sender() ! ShortCircuitOverloaded(numActiveQueries)
    case OverallSparqlQuery(query, host, rp) => {
      updateActiveQueries(+1)

      Try(QueryFactory.create(query)) match {
        case Failure(e) => sender() ! RemoteFailure(e)
        case Success(sprqlQuery) => {
          val config = Config(rp.doNotOptimize,
                              rp.intermediateLimit,
                              rp.resultsLimit,
                              rp.verbose,
                              SpHandler.queryTimeout,
                              Some(SpHandler.queryTimeout.fromNow),
                              rp.explainOnly)
          val JenaArqExtensionsUtils.BakedSparqlQuery(queryExecution, driver) =
            JenaArqExtensionsUtils.buildCmWellQueryExecution(sprqlQuery,
                                                             host,
                                                             config,
                                                             crudServiceFS,
                                                             arqCache,
                                                             jenaArqExtensionsUtils,
                                                             dataFetcher)

          if (!sprqlQuery.isConstructType && !sprqlQuery.isSelectType) {
            sender() ! RemoteFailure(new IllegalArgumentException("Query Type must be either SELECT or CONSTRUCT"))
          } else {
            val os = new ByteArrayOutputStream()

            if (config.explainOnly)
              driver.logMsg(
                "Expl",
                "AST:\n" + JenaArqExtensionsUtils.queryToSseString(sprqlQuery).lines.map("\t".+).mkString("\n")
              )

            driver.logVerboseMsg("Plan", "Planning started.")

            if (sprqlQuery.isSelectType)
              ResultSetFormatter.out(os, queryExecution.execSelect(), sprqlQuery)

            if (sprqlQuery.isConstructType)
              RDFDataMgr.write(os, queryExecution.execConstruct(), RDFFormat.NTRIPLES)

            driver.logVerboseMsg("Exec", "Executing completed.")
            val msgsBa = driver.msgs.map { case (k, v) => s"[$k] $v" }.mkString("", "\n", "\n\n").getBytes("UTF-8")

            val resultsBa =
              if (config.explainOnly) Array.emptyByteArray
              else os.toByteArray

            val results = rawDataToResponseMsg(msgsBa ++ resultsBa, Map.empty[String, String], forceWriteFile = false)
            val originalSender = sender

            results.onComplete {
              case Success(res) => self ! SpResponse(originalSender, res)
              case Failure(err) => self ! SpFailure(originalSender, err)
            }
          }
        }
      }
    }

    case SpFailure(originalSender, err) => {
      updateActiveQueries(-1)
      watcher.foreach(_ ! Reset)

      if (isNeedToDelay) {
        context.system.scheduler.scheduleOnce(3.seconds, originalSender, RemoteFailure(err))
      } else {
        originalSender ! RemoteFailure(err)
      }
    }

    case SpResponse(originalSender, queryResponse) => {
      updateActiveQueries(-1)
      watcher.foreach(_ ! Reset)

      if (isNeedToDelay) {
        context.system.scheduler.scheduleOnce(3.seconds, originalSender, queryResponse)
      } else {
        originalSender ! queryResponse
      }
    }
  }

  protected def rawDataToResponseMsg(qr: String,
                                     stats: Map[String, String],
                                     forceWriteFile: Boolean): Future[QueryResponse] =
    rawDataToResponseMsg(qr.getBytes("UTF-8"), stats, Some(qr), forceWriteFile)

  protected def rawDataToResponseMsg(data: Array[Byte],
                                     stats: Map[String, String],
                                     originalStringData: Option[String] = None,
                                     forceWriteFile: Boolean): Future[QueryResponse] = {
    if (forceWriteFile || data.length > responseThreshold) {
      Future {
        val path = generateTempFileName
        writeToFile(path)(data)
        Filename(path, stats)
      }
    } else {
      Future.successful(Plain(originalStringData.getOrElse(new String(data, "UTF-8")), stats))
    }
  }
}

// will HCN its CW once 66 seconds were passed from Activate without any Reset received
class QueryEvaluatorActorWatcher(qeaRef: ActorRef) extends Actor with LazyLogging {

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    logger.info("Creating actor QueryEvaluatorActorWatcher for " + qeaRef.path.name)
    qeaRef ! IAmYourWatcher
  }

  private val interval = 11.seconds
  private val threshold = 6

  private var counter = 0

  override def receive = inactive

  def inactive: Receive = {
    case Activate =>
      context.system.scheduler.scheduleOnce(interval, self, Tick)
      context.become(active)
  }

  def active: Receive = {
    case Tick =>
      counter += 1
      if (counter >= threshold) {
        logger.info(s"Watcher detected QueryEvaluatorActor[${qeaRef.path.name}] is hung. Goodbye!")
        System.exit(1)
      }
      context.system.scheduler.scheduleOnce(interval, self, Tick)

    case Reset =>
      counter = 0
      context.become(inactive)
  }
}

case object IAmYourWatcher
case object Activate
case object Tick
case object Reset
