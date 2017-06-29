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


package cmwell.crashableworker

import java.io.ByteArrayOutputStream

import actions.RequestMonitor
import akka.actor.{Actor, ActorRef}
import cmwell.ctrl.config.Jvms
import cmwell.util.collections
import cmwell.web.ld.query.JenaArqExtensionsUtils.BakedSparqlQuery
import cmwell.web.ld.query.{Config, JenaArqExtensions, JenaArqExtensionsUtils}
import com.typesafe.scalalogging.Logger
import controllers._
import k.grid.{Grid, GridConnection, GridJvm}
import org.apache.jena.query.{QueryFactory, ResultSetFormatter}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object WorkerMain extends App {
  Grid.extraDataCollector = () => QueryEvaluatorActor.getExtraData

  Grid.setGridConnection(GridConnection(memberName = "cw"))
  Grid.joinClient
  Thread.sleep(5000)
  RequestMonitor.init

  Grid.create(classOf[QueryEvaluatorActor], "QueryEvaluatorActor")
  Grid.create(classOf[QueryEvaluatorActorWatcher], "QueryEvaluatorActorWatcher")

  lazy val cwLogger: Logger = Logger(LoggerFactory.getLogger("cmwell.cw.app"))
}

import WorkerMain.{cwLogger => logger}

sealed trait QueryResponse {def content: String}
case class Plain(content: String) extends QueryResponse
case class Filename(content: String) extends QueryResponse
case class RemoteFailure(failure: Throwable) extends QueryResponse {
  override def content = throw failure
}
case class ThroughPipe(pipeName: String) extends QueryResponse {
  override def content: String = ??? //read from pipe
}
case class ShortCircuitOverloaded(numActiveRequests: Int) extends QueryResponse {
  override def content: String = ???
}
case class Status(counter: Int) extends QueryResponse { // for debugging purposes
  override def content: String = s"numActiveQueries is $counter"
}

object QueryEvaluatorActor {
  private var activeQueryMap = Map.empty[String, Int]

  def get(name : String) : Int = activeQueryMap.get(name).getOrElse(0)

  def set(name : String, value : Int) = activeQueryMap = activeQueryMap.updated(name, value)

  def getExtraData = {
    activeQueryMap.map {
      case (n,v) => s"#aq: $v"
    }.mkString("\n")
  }
}
class QueryEvaluatorActor extends Actor with SpFileUtils {
  import QueryEvaluatorActor._
  private case class SpResponse(sender: ActorRef, queryResponse: QueryResponse)
  private case class SpFailure(sender: ActorRef, ex: Throwable)
  private def myName = self.path.name
  private def numActiveQueries = get(myName)
  private def updateActiveQueries(delta : Int) = set(myName , get(myName) + delta)

  val responseThreshold = 64*1024

  val ACTIVE_REQUESTS_DELAY_THRESHOLD = Runtime.getRuntime.availableProcessors
  val CIRCUIT_BREAKER = Runtime.getRuntime.availableProcessors * 4
//  var numActiveQueries = 0

  val watcherActorFut = Grid.getRefFromSelection(Grid.selectActor("QueryEvaluatorActorWatcher", GridJvm(Jvms.CW)))

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    logger.info("Creating actor QueryEvaluatorActor")
    deleteTempFiles()
  }

  private def isNeedToDelay = numActiveQueries > ACTIVE_REQUESTS_DELAY_THRESHOLD

  override def receive: Receive = {
    case StatusRequest => sender ! Status(numActiveQueries) // for debugging purposes

    case paq: PopulateAndQuery if numActiveQueries > CIRCUIT_BREAKER =>
      watcherActorFut.map(_ ! Activate)
      sender ! ShortCircuitOverloaded(numActiveQueries)

    case paq: PopulateAndQuery => {
      updateActiveQueries(+1)


      Try(paq.evaluate()) match {
        case Success(queryResults) => {
          val results = queryResults.flatMap(rawDataToResponseMsg(_, paq.rp.forceUsingFile))
          val originalSender = sender

          results.onComplete {
            case Success(res) => self ! SpResponse(originalSender, res)
            case Failure(err) => self ! SpFailure(originalSender, err)
          }
        }
        case Failure(err) => {
          updateActiveQueries(-1)
          watcherActorFut.map(_ ! Reset)
          sender ! RemoteFailure(err)
        }
      }
    }

    case OverallSparqlQuery(_,_,_) if numActiveQueries > CIRCUIT_BREAKER => sender ! ShortCircuitOverloaded(numActiveQueries)
    case OverallSparqlQuery(query,host,rp) => {
      updateActiveQueries(+1)

      JenaArqExtensions.init

      Try(QueryFactory.create(query)) match {
        case Failure(e) => sender ! RemoteFailure(e)
        case Success(sprqlQuery) => {
          val config = Config(rp.doNotOptimize, rp.intermediateLimit, rp.resultsLimit, rp.verbose, SpHandler.queryTimeout.fromNow, rp.explainOnly)
          val BakedSparqlQuery(queryExecution, driver) = JenaArqExtensionsUtils.buildCmWellQueryExecution(sprqlQuery, host, config)

          if (!sprqlQuery.isConstructType && !sprqlQuery.isSelectType) {
            sender ! RemoteFailure(new IllegalArgumentException("Query Type must be either SELECT or CONSTRUCT"))
          } else {
            val os = new ByteArrayOutputStream()

            if(config.explainOnly)
              driver.logMsg("Expl", "AST:\n" + JenaArqExtensionsUtils.queryToSseString(sprqlQuery).lines.map("\t".+).mkString("\n"))

            driver.logVerboseMsg("Plan", "Planning started.")

            if(sprqlQuery.isSelectType)
              ResultSetFormatter.out(os, queryExecution.execSelect(), sprqlQuery)

            if(sprqlQuery.isConstructType)
              RDFDataMgr.write(os, queryExecution.execConstruct(), RDFFormat.NTRIPLES)

            driver.logVerboseMsg("Exec", "Executing completed.")
            val msgsBa = driver.msgs.map{case(k,v)=>s"[$k] $v"}.mkString("","\n","\n\n").getBytes("UTF-8")

            val resultsBa = if(config.explainOnly) Array.emptyByteArray
                            else os.toByteArray

            val results = rawDataToResponseMsg(msgsBa ++ resultsBa, forceWriteFile = false)
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
      watcherActorFut.map(_ ! Reset)

      if (isNeedToDelay) {
        context.system.scheduler.scheduleOnce(3.seconds, originalSender, RemoteFailure(err))
      } else {
        originalSender ! RemoteFailure(err)
      }
    }

    case SpResponse(originalSender, queryResponse) => {
      updateActiveQueries(-1)
      watcherActorFut.map(_ ! Reset)

      if (isNeedToDelay) {
        context.system.scheduler.scheduleOnce(3.seconds, originalSender, queryResponse)
      } else {
        originalSender ! queryResponse
      }
    }
  }

  protected def rawDataToResponseMsg(qr: String, forceWriteFile: Boolean): Future[QueryResponse] = rawDataToResponseMsg(qr.getBytes("UTF-8"), Some(qr), forceWriteFile)

  protected def rawDataToResponseMsg(data: Array[Byte], originalStringData: Option[String] = None, forceWriteFile: Boolean): Future[QueryResponse] = {
    if (forceWriteFile || data.length > responseThreshold) {
      Future {
        val path = generateTempFileName
        writeToFile(path)(data)
        Filename(path)
      }
    } else {
      Future.successful(Plain(originalStringData.getOrElse(new String(data, "UTF-8"))))
    }
  }
}

// will HCN its CW once 66 seconds were passed from Activate without any Reset received
class QueryEvaluatorActorWatcher extends Actor {

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    logger.info("Creating actor QueryEvaluatorActorWatcher")
  }

  private val interval =  11.seconds
  private val threshold = 6

  private var counter = 0

  override def receive = inactive

  def inactive: Receive = {
    case Activate =>
      context.system.scheduler.scheduleOnce(interval, self, Tick)
      context become active
  }

  def active: Receive = {
    case Tick =>
      counter += 1
      if (counter >= threshold) {
        logger.info("Watcher detected QueryEvaluatorActor is hung. Goodbye!")
        System.exit(1)
      }
      context.system.scheduler.scheduleOnce(interval, self, Tick)

    case Reset =>
      counter = 0
      context become inactive
  }
}

case object Activate
case object Tick
case object Reset
