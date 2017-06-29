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


package cmwell.indexer

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout.durationToTimeout
import cmwell.common.formats.JsonSerializer
import cmwell.domain._
import cmwell.fts.FTSServiceES
import cmwell.irw._
import cmwell.tlog.{TLog, TLogState}
import cmwell.util.jmx._
import cmwell.common.exception._
import cmwell.common.metrics.WithMetrics
import cmwell.common._
import cmwell.syntaxutils._
import cmwell.util.BoxedFailure
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.Requests
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{SECONDS, _}
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}


/**
 * User: israel
 * Date: 7/22/14
 * Time: 16:56
 */

object Retry {
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): Try[T] = {
    Try { fn } match {
      case x: Success[T] => x
      case _ if n > 1 => Thread.sleep(500); retry(n - 1)(fn)
      case f => f
    }
  }
}

trait IndexerMBean {
  def pause():Unit
  def resume():Unit
  def getState():String
}

class Indexer(uuidTlog:TLog, updatesTlog:TLog, irwService:IRWService, ftsService:FTSServiceES, indexerState:TLogState) extends LazyLogging with IndexerMBean {


  val system = ActorSystem("IndexerActorSystem")

  val stateManager = system.actorOf(StateManager.props(indexerState))

  val workProducer = system.actorOf(WorkProducer.props(uuidTlog, updatesTlog, irwService, ftsService, stateManager))

  val worker = system.actorOf(Worker.props(ftsService, stateManager, workProducer, irwService))

  // JMX methods Impl
  def pause() = worker ! Pause
  def resume() = worker ! Resume
  def getState() = {
    implicit val timeout:akka.util.Timeout = FiniteDuration(10, SECONDS)
    Await.result(worker ask GetState, timeout.duration).asInstanceOf[String]
  }

  // JMX registration
  jmxRegister(this, "cmwell.indexer:type=AkkaIndexer")

  sys.addShutdownHook{
    logger info "shutting down ActorSystem"
    val f = system.terminate()
    Await.result(f, Duration.Inf)
  }

}


object WorkProducer {
  def props(uuidTlog:TLog, updatesTLog:TLog, irwService:IRWService, ftsService:FTSServiceES, stateManager: ActorRef):Props = Props(new WorkProducer(uuidTlog, updatesTLog, irwService, ftsService, stateManager))
}

class WorkProducer(uuidTLog:TLog, updatesTLog:TLog, irwService:IRWService, ftsService:FTSServiceES, stateManager:ActorRef)
  extends Actor with LazyLogging with WithMetrics{
  var nextTimeStamp:Long = 0

  import Retry._
  import context._

  lazy val redlog: Logger = Logger(LoggerFactory.getLogger("redlog"))

  val bo = scala.collection.breakOut[Vector[(String,Long,Long)],String,Set[String]]
  val maxAggWeight = system.settings.config.getInt("indexer.max.agg.weight") * 1024
  val pollUnavailableIRWServiceInterval = system.settings.config.getInt("indexer.pollUnavailableIRWServiceInterval")

  /* Metrics */
  // In order to avoid collisions during actor restart, we'll try to unregister old metrics before creating new ones
  metrics.registry.remove(metrics.baseName.append("ES Actions Produces").name)
  val esActionsProducedMeter = metrics.meter("ES Actions Produced")
  metrics.registry.remove(metrics.baseName.append("Next uuid timestamp").name)
  val nextTimeStampGauge = metrics.gauge("Next uuid timestamp") {nextTimeStamp}
  metrics.registry.remove(metrics.baseName.append("Infotons Read Time").name)
  val infotonsReadTimer = metrics.timer("Infotons Read Time")
  metrics.registry.remove(metrics.baseName.append("InfotonsToESActions Time").name)
  val infotonsToESActionsTimer = metrics.timer("InfotonsToESActions Time")


  override def preStart(): Unit = {
    logger info s"asking StateManager for last saved timestamp"
    implicit val timeout:akka.util.Timeout = FiniteDuration(5, SECONDS)
    val savedTS = Await.result( stateManager ask GetState, timeout.duration).asInstanceOf[Long]
    logger info s"Received last saved timestamp: $savedTS to start with"
    nextTimeStamp = savedTS
  }


  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    val st = reason.getStackTrace.map(_.toString).mkString("\n\t","\n\t","\n")
    logger.error(s"Restarting due to exception: ${reason.getMessage} while processing message: ${message.toString} with stack trace: ${st}")
    super.preRestart(reason, message)
  }

  def irwUnavailable: Receive = {
    case ReceiveTimeout =>
      logger error "Switching to running mode, maybe IRWService is now available"
      become(receive)
  }

  def receive = {
    case ProduceFrom(startTimeStamp) =>
      nextTimeStamp = startTimeStamp

    case ProduceWork(numOfBulks, bulkSize) =>

      logger debug s"Got request to produce work. NumOfBulks:$numOfBulks, bulkSize:$bulkSize"
      var rounds = numOfBulks
      do {
        val commands:ArrayBuffer[IndexerCommand] = ArrayBuffer.empty
        var aggWeight = 0L
        var bulkCounter = 0
        var continue = true

        while(continue && bulkCounter < bulkSize && aggWeight < maxAggWeight) {
          uuidTLog.readOne(nextTimeStamp) match {
            case Some((ts, buffer)) => (CommandSerializer.decode(buffer): @unchecked) match {
              case nextCommand@MergedInfotonCommand(previousInfoton,currentInfoton,_) => {
                commands.append(nextCommand)
                bulkCounter += 1
                aggWeight += currentInfoton._2
                aggWeight += previousInfoton.map(_._2).getOrElse(0L)
                nextTimeStamp = ts
              }
              case nextCommand@OverwrittenInfotonsCommand(previousInfoton, currentInfoton, historicInfotons) => {
                commands.append(nextCommand)
                bulkCounter += 1 //TODO: should increment with 1 + historicInfotons.size?
                aggWeight += previousInfoton.map(_._2).getOrElse(0L)
                aggWeight += currentInfoton.map(_._2).getOrElse(0L)
                aggWeight += (0L /: historicInfotons){case (sum,(_,weight,_)) => sum + weight}
                nextTimeStamp = ts
              }
            }
            case None => continue = false
          }
        }

        logger debug s"agg weight of next bulk of infotons: $aggWeight"
        logger debug s"bulk size: $bulkCounter"

        if(commands.nonEmpty) {
          val esActionsTry = retry(5)(commandsToActions(commands))
          esActionsTry match {
            case Success(esActions) =>
              sender() ! Bulk(nextTimeStamp, esActions)
              esActionsProducedMeter.mark(esActions.size)
              rounds -= 1
            case Failure(t) =>
              logger.error("failed to convert commands to actions for 5 sequential times. becoming 'irw service unavailable'",t)
              context.setReceiveTimeout(FiniteDuration(pollUnavailableIRWServiceInterval, SECONDS))
              become(irwUnavailable)
          }

        } else {
          logger debug "tlog is empty, nothing to produce"
          rounds = 0
        }
      } while(rounds > 0 )
  }

  private def currentIndicesNames = {
    val currentAliasRes = ftsService.client.admin.indices().prepareGetAliases("cmwell_current").execute().actionGet()
    val indices = currentAliasRes.getAliases.keysIt().asScala.toSeq
    indices
  }

  def handleFailure(infoton: Infoton, errMsgOpt: Option[String]): Infoton = errMsgOpt match {
    case None => infoton
    case Some(errMsg) => {

      logger.info(s"[TEMP] handleFailure invoked with infoton $infoton and errMsg $errMsg")

      """MapperParsingException.*\[fields\.(.*?)\].*""".r.findFirstMatchIn(errMsg).map(_.group(1)) match {
        case None => !!!
        case Some(badFieldName) =>

          // write to RED-LOG file
          redlog.warn(s"Infoton(${infoton.path},${infoton.uuid}) was failed to index because $badFieldName has a bad type. Will now try to re-index without $badFieldName")

          // publish to /var/logs
          val fields: Map[String,Set[FieldValue]] = Seq(
            "path"->s"http:/${infoton.path}",
            "uuid"->infoton.uuid,
            "failedAt"->org.joda.time.DateTime.now.toDateTimeISO.toString,
            "badField"->badFieldName.takeWhile(_!='.'),
            "logType"->"index-error").
            map{ case (k,v) => val fv: FieldValue = FString(v); k->Set(fv) }.toMap
          val wc = WriteCommand(ObjectInfoton(s"/meta/logs/${infoton.uuid}","",None,fields))
          updatesTLog.write(CommandSerializer.encode(wc))

          val unMangledBadFieldName = badFieldName.substring(badFieldName.indexOf('$')+1)
          val modifiedInfoton = infoton.masked(infoton.fields.get.keySet -- Set(unMangledBadFieldName), allowEmpty = true)
          modifiedInfoton
      }
    }
  }


  def commandsToActions(commands:Seq[IndexerCommand]) : Seq[ActionBaker] = {
    var actions:ArrayBuffer[ActionBaker] = null
    val (previouses,histories,all,indexTimeMap,overwrites) = {
      val previouses = Set.empty[String]
      val histories = Set.empty[String]
      val arrSize = (0 /: commands){
        case (s,_:MergedInfotonCommand) => s+1
        case (s,OverwrittenInfotonsCommand(_, None, hs)) => s + hs.size
        case (s,OverwrittenInfotonsCommand(_, Some(_), hs)) => s + 1 + hs.size
      }
      val all:ArrayBuffer[String] = new ArrayBuffer[String](arrSize * 2)
      val indexTimeMap = Map.empty[String,Long]
      val overwrites = Set.empty[String]

      ((previouses,histories,all,indexTimeMap,overwrites) /: commands) {
        case ((p, h, a, m, ow), cmd) => {

          val (a1,p1) = cmd.previousInfoton.map { prev =>
            (a+=prev._1,p+prev._1)
          }.getOrElse(a,p)

          cmd match {
            case MergedInfotonCommand(previousInfoton, currentInfoton,_) => (p1,h,a1+=currentInfoton._1,m,ow)
            case OverwrittenInfotonsCommand(_, currentInfoton, histoticInfotons) => {

              val ows = {
                val ows = histoticInfotons.map(_._1)(bo)
                ow union currentInfoton.fold(ows)(ows + _._1)
              }

              val (a2,m1) = currentInfoton.map {
                case (u, _, idxTime) => (a1+=u) -> m.updated(u, idxTime)
              }.getOrElse(a1 -> m)

              val (x1,x2,x3) = ((h,a2,m1) /: histoticInfotons) {
                case ((hh,aa,mm),(u,_,t)) => (hh+u,aa+=u,mm.updated(u,t))
              }
              (p1,x1,x2,x3,ows)
            }
          }
        }
      }
    }

    def recurseToGetAll(uuids: Seq[String], level: cmwell.irw.ConsistencyLevel = cmwell.irw.QUORUM, triesLeft: Int = 3): Future[Vector[Infoton]] = {
      logger debug s"recurse to get all [${uuids.mkString(",")}]"

      cmwell.util.concurrent.travector(uuids){ uuid =>
        irwService.readUUIDAsync(uuid, level).map{
          case BoxedFailure(e) =>
            logger.error(s"failed to readUUIDAsync with [$uuid]",e)
            Left(uuid)
          case box => box.toOption.fold[Either[String,Infoton]](Left(uuid))(Right.apply)
        }
      }.flatMap{ eithers =>
        if(eithers.forall(_.isRight)) Future.successful(eithers.collect{case Right(infoton) => infoton})
        else {
          val (lefts, rights) = eithers.partition(_.isLeft)
          if (level != cmwell.irw.ONE) {
            val lvl = if (triesLeft == 0) cmwell.irw.ONE else level
            recurseToGetAll(lefts.collect { case Left(uuid) => uuid }, lvl, triesLeft - 1).map(_ ++ rights.collect { case Right(infoton) => infoton })
          }
          else {
            logger.error(s"irretrievable uuids from indexer tlog: ${lefts.collect{case Left(u) => u}.mkString("[",",","]")}")
            redlog.error(s"irretrievable uuids from indexer tlog: ${lefts.collect{case Left(u) => u}.mkString("[",",","]")}")
            Future.successful(rights.collect { case Right(infoton) => infoton })
          }
        }
      }
    }

    val f = recurseToGetAll(all) //irwService.readUUIDSAsync(all.toVector)
    val infotons = Await.result(f,Duration(20000, MILLISECONDS))
    if(infotons.size != all.size) {
      val notFound = all.filterNot(uuid => infotons.exists(_.uuid == uuid))
      logger.error(s"we missed something!!! ${notFound.mkString("[",",","]")}")
    }

//    val infotons = infotonsReadTimer.time{ irwService.read(Vector.empty ++ all) }
    //TODO: Assert instead of if
    if(infotons.nonEmpty) {
      logger debug s"got ${infotons.size} infotons from irwService"

      actions = new ArrayBuffer[ActionBaker](commands.size * 2)
      infotonsToESActionsTimer.time {
        infotons.foreach { i =>
          val infoton = i.indexTime match {
            case Some(idxT) => i
            //how can we read from cassandra a previous infoton with no indexTime?!?!?!?
            case None if previouses(i.uuid) => {
              val samePath = infotons.filter(_.path == i.path)
              val us = samePath.map(_.uuid).toSet
              val cmds = commands.collect {
                case cmd@MergedInfotonCommand(previousInfoton,currentInfoton,_) if previousInfoton.exists(t=>us(t._1)) || us(currentInfoton._1) => cmd
                case cmd@OverwrittenInfotonsCommand(previousInfoton, currentInfoton, histoticInfotons) if previousInfoton.exists(t=>us(t._1)) || currentInfoton.exists(t=>us(t._1)) || histoticInfotons.exists(t=>us(t._1)) => cmd
              }
              logger.warn(s"could not retrieve indexTime for a previous infoton, uuid=${i.uuid},path=${i.path}. will convert infotons lastModified into a fake indexTime.\ncommands containing uuid: $cmds\ninfotons with same path: $samePath")
              addIndexTime(i,Some(i.lastModified.getMillis))
            }
            //if not found, try to get from retrieved old infotons
            case None => indexTimeMap.get(i.uuid) match {
              case o@Some(idxT) => addIndexTime(i,o)
              case None if overwrites(i.uuid) => {
                logger.error(s"How can this be?!?!?!?!?!??!?! [${i.uuid}]:$i")
                i
              }
              case None => i
            }
          }

          if (previouses(infoton.uuid)) {
            val serializedInfoton = JsonSerializer.encodeInfoton(infoton, omitBinaryData = true, toEs = true)
            actions.append((_,_) => {(Requests.indexRequest("cmwell_history_latest").`type`("infoclone").id(infoton.uuid).create(true).source(serializedInfoton), None, infoton)})
            currentIndicesNames.foreach{index =>
              val bakedAction = (Requests.deleteRequest(index).`type`("infoclone").id(infoton.uuid), None, infoton)
              actions.append((_,_) => bakedAction)
            }
          } else if (overwrites(infoton.uuid) && (infoton.isInstanceOf[DeletedInfoton] || histories(infoton.uuid))) {
            val serializedInfoton = JsonSerializer.encodeInfoton(infoton, omitBinaryData = true, toEs = true)
            val tup = (Requests.indexRequest("cmwell_history_latest").`type`("infoclone").id(infoton.uuid).create(true).source(serializedInfoton), None, infoton)
            actions.append((_,_) => tup)
          }
          else if (infoton.isInstanceOf[DeletedInfoton]) {
            val fun = (infopt: Option[Infoton], maybeErrMsg: Option[String]) => {
              val currentTime = System.currentTimeMillis()
              val currentInfoton = addIndexTime(infopt.getOrElse(infoton), Some(currentTime))
              val modifiedCurrentInfoton = handleFailure(currentInfoton,maybeErrMsg)

              val serializedInfoton = JsonSerializer.encodeInfoton(modifiedCurrentInfoton, omitBinaryData = true, toEs = true)
              val ret = (Requests.indexRequest("cmwell_history_latest").`type`("infoclone").id(infoton.uuid).create(true).source(serializedInfoton), Some(currentTime -> infoton.uuid), modifiedCurrentInfoton)
              ret
            }
            actions.append(fun)
          }
          else if(overwrites(infoton.uuid)) {
            val serializedInfoton = JsonSerializer.encodeInfoton(infoton, omitBinaryData = true, toEs = true)
            val tup = (Requests.indexRequest("cmwell_current_latest").`type`("infoclone").id(infoton.uuid).create(true).source(serializedInfoton), None, infoton)
            actions.append((_,_) => tup)
          }
          else {
            val fun: ActionBaker = (infopt: Option[Infoton], maybeErrMsg: Option[String]) => {
              val current = System.currentTimeMillis()
              val currentInfoton = addIndexTime(infopt.getOrElse(infoton), Some(current))

              val modifiedCurrentInfoton = handleFailure(currentInfoton,maybeErrMsg)
              val serializedInfoton = JsonSerializer.encodeInfoton(modifiedCurrentInfoton, true, true)

              val ret = (Requests.indexRequest("cmwell_current_latest").`type`("infoclone").id(infoton.uuid).create(true).source(serializedInfoton), Some(current -> infoton.uuid), modifiedCurrentInfoton)

              ret
            }
            actions.append(fun)
          }
        }
      }
    } else {
      logger error "got 0 infotons back from irwService. this should not have happened"
      !!!
    }

    actions
  }


}


object Worker {
  def props(ftsService:FTSServiceES, stateManager:ActorRef, workProducer:ActorRef, irwService: IRWService): Props = Props(new Worker(ftsService, stateManager, workProducer, irwService))
}

class Worker(ftsService:FTSServiceES, stateManager:ActorRef, workProducer:ActorRef, irwService: IRWService) extends Actor with LazyLogging with WithMetrics {

  import context._

  val numOfBulks = system.settings.config.getInt("indexer.numOfBulks")
  // The minumum level of bulks waiting to index before requesting more work
  val numOfBulksThreshold = system.settings.config.getInt("indexer.numOfBulksThreshold")
  val numOfRetriesToGiveUp = system.settings.config.getInt("indexer.numOfRetriesToGiveUp")
  val bulkSize = system.settings.config.getInt("indexer.bulkSize")
  val recommendedDocsPerShard = system.settings.config.getLong("indexer.recommendedDocsPerShard")
  val numOfBulksToMaintainIndices = recommendedDocsPerShard * getNumOfShards /bulkSize
  var bulkCounterForMaintenance = 0
  val isMaster = system.settings.config.getBoolean("indexer.isMaster")
  val pollUnavailableFTSServiceInterval = system.settings.config.getInt("indexer.pollUnavailableFTSServiceInterval")

  @volatile var bulkCounter:Int = 0

  // true means indexing, false means idle
  @volatile var indexing:Boolean = false

  /* Metrics */
  // In order to avoid collisions during actor restart, we'll try to unregister old metrics before creating new ones
  metrics.registry.remove(metrics.baseName.append("Total es actions indexed").name)
  val totalIndexedMeter = metrics.meter("Total es actions indexed")
  metrics.registry.remove(metrics.baseName.append("Total recoverable failures").name)
  val totalRecoverableFailuresCounter = metrics.counter("Total recoverable failures")
  metrics.registry.remove(metrics.baseName.append("Total none recoverable failures").name)
  val totalNonRecoverableFailuresCounter = metrics.counter("Total none recoverable failures")
  metrics.registry.remove(metrics.baseName.append("Indexing time").name)
  val indexingTimer = metrics.timer("Indexing time")

  metrics.registry.remove( metrics.baseName.append("Bulk Inventory").name )
  metrics.gauge("Bulk Inventory"){
    bulkCounter
  }
  metrics.registry.remove(metrics.baseName.append("Indexing").name)
  metrics.gauge("Indexing"){
    indexing
  }

  var paused = !system.settings.config.getBoolean("indexer.auto-start")

  if(!paused) {
    setReceiveTimeout(FiniteDuration(1, SECONDS))
    workProducer ! ProduceWork(numOfBulks, bulkSize)
    bulkCounter = numOfBulks
  }

  def ftsServiceUnavailable:Receive = {

    case ReceiveTimeout =>
      logger error "Switching to running mode, maybe FTSService is back up"
      implicit val timeout:akka.util.Timeout = FiniteDuration(2, SECONDS)
      logger error "Requesting StateManager next timestamp to continue from"
      ask(stateManager, GetState).mapTo[Long].andThen{
        case Success(startTimeStamp) =>
          logger error s"Received next timestamp: $startTimeStamp"
          become(receive)
          workProducer ! ProduceFrom(startTimeStamp)
          setReceiveTimeout(FiniteDuration(1, SECONDS))
        case Failure(t) =>
          logger error s"Failed to get current state from state manager. staying in 'FTSService Unavailable' mode:\n ${getStackTrace(t)}"
      }
  }

  def receive:Receive = {

    case Bulk(bulkTimeStamp, esActions) =>
      indexing = true

      val tuples = esActions.map(baker => baker(None,None))

      /*index bulk */
      val responseTry = Try {
        indexingTimer.time {
          Await.result(cmwell.util.concurrent.retry(numOfRetriesToGiveUp, 500.millis) {
            ftsService.executeBulkActionRequests(tuples.map(_._1))
          }, FiniteDuration(10, SECONDS))
        }
      }

      responseTry match {
        case Success(res) =>
          var response = res
          if(!response.hasFailures) {
            logger debug s"successfully indexed bulk: $bulkTimeStamp, sending WorkDone message to StateManager"
            totalIndexedMeter.mark(response.getItems.length)
            Await.ready(
              cmwell.util.concurrent.travector(tuples) {
                case (_,Some((idxT,uuid)),_) => cmwell.util.concurrent.retry(3, 10.milliseconds)(irwService.addIndexTimeToUuid(uuid,idxT,QUORUM))
                case (_,None,_) => Future.successful(())
              }, FiniteDuration(10, SECONDS)
            )

          } else {
            var retryActions:Option[Seq[(ActionBaker,Option[Infoton],Option[String])]] = Some(esActions.map((_,None,None)))
            var retryTuples = tuples
            var michaelsMagicNumber = 51
            do {
              michaelsMagicNumber -= 1
              val successfulIndexes = response.getItems.collect{case action if !action.isFailed => action.getItemId}.toSet
              if(successfulIndexes.nonEmpty) {
                val successfulTuples = retryTuples.zipWithIndex.collect { case ((_,Some(t),_), i) if successfulIndexes(i) => t }
                val successfulFutues = Future.traverse(successfulTuples) {
                  case (idxT,uuid) =>
                    cmwell.util.concurrent.retry(3, 10.milliseconds)(irwService.addIndexTimeToUuid(uuid, idxT, QUORUM))
                }
                Await.ready(successfulFutues, FiniteDuration(10, SECONDS))
              }


//              val unsuccessfulIndexes = response.getItems.collect{case action if action.isFailed => action.getItemId}.toSet
//              val unsuccessfulTuples = retryTuples.zipWithIndex.collect { case ((_,_,inf), i) if unsuccessfulIndexes(i) => inf }


              retryActions = handleFailures(response, retryActions.get, retryTuples.map(_._3).toIndexedSeq)
              if(retryActions.isDefined){
                logger debug s"retrying ${retryActions.size} recoverable failed infotons"
                // TODO replace this delay with a more 'Akka' way. This is OK for now since we have only one actor that sends
                // TODO actions to Elasticsearch for indexing
                Thread sleep 1000

                retryTuples = retryActions.get.map { case (f,inopt,maybeErrMsg) => f(inopt,maybeErrMsg) }

                logger.info(s"[613] retryTuples is $retryTuples")

                //TODO: leave only system fields for the last retry
//                if(michaelsMagicNumber == 0) {
//                  logger.error("tried to \"peal off\"")
//                }

                response = indexingTimer.time{Await.result(ftsService.executeBulkActionRequests(retryTuples.map(_._1)), FiniteDuration(10, SECONDS))}
              }
              totalIndexedMeter.mark(response.getItems.size)
            } while(retryActions.isDefined && michaelsMagicNumber > 0)
          }

          /* bulk done */
          stateManager ! WorkDone(bulkTimeStamp)
          bulkCounter -= 1

          // check maintenance if required
          bulkCounterForMaintenance += 1
          if(isMaster && bulkCounterForMaintenance >= numOfBulksToMaintainIndices) {
            maintainIndices(recommendedDocsPerShard)
            bulkCounterForMaintenance = 0
          }

          // If got to minimum threshold,
          if (bulkCounter == numOfBulksThreshold) {
            // Request more work, if not paused
            if(!paused) {
              sender() ! ProduceWork(numOfBulks, bulkSize)
            }

            bulkCounter += numOfBulks
          }

        case Failure(e) =>
          logger error s"Failed to index $numOfRetriesToGiveUp continuous times. Switching to 'FTS Service Not Available' mode !!!!\n ${getStackTrace(e)}"
          context.setReceiveTimeout(FiniteDuration(pollUnavailableFTSServiceInterval, SECONDS))
          indexing = false
          become(ftsServiceUnavailable)
      }

    case Pause =>
      if(!paused) {
        paused = true
        setReceiveTimeout(Duration.Undefined)
      }

    case Resume =>
      if(paused) {
        paused = false
        workProducer ! ProduceWork(numOfBulks, bulkSize)
        setReceiveTimeout(FiniteDuration(1, SECONDS))
      }

    case GetState =>
      if(!paused) {
        sender() ! "Running"
      } else {
        sender() ! "Paused"
      }

    case ReceiveTimeout =>
      indexing = false
      logger debug "waited for 2 seconds and got no new work. requesting more"
      workProducer ! ProduceWork(numOfBulks, bulkSize)
      bulkCounter = numOfBulks

  }

  def getNumOfShards = {
    val currentAliasRes = ftsService.client.admin.indices().prepareGetAliases("cmwell_current_latest").execute().actionGet()
    val lastCurrentIndexName = currentAliasRes.getAliases.keysIt().next()
    val lastCurrentIndexRecovery = ftsService.client.admin().indices().prepareRecoveries("cmwell_current_latest").get()
    lastCurrentIndexRecovery.shardResponses().get(lastCurrentIndexName).asScala.filter(_.recoveryState().getPrimary).size
  }

  def maintainIndices(maxDocumentsPerShard:Long) = {

    val indicesStats = ftsService.client.admin().indices().prepareStats("cmwell_current", "cmwell_history").clear().setDocs(true).execute().actionGet()

    // find latest current index name
    val currentAliasRes = ftsService.client.admin.indices().prepareGetAliases("cmwell_current_latest").execute().actionGet()
    val lastCurrentIndexName = currentAliasRes.getAliases.keysIt().next()
    val numOfDocumentsCurrent = indicesStats.getIndex(lastCurrentIndexName).getTotal.getDocs.getCount
    val lastCurrentIndexRecovery = ftsService.client.admin().indices().prepareRecoveries("cmwell_current_latest").get()
    val numOfShardsCurrent = lastCurrentIndexRecovery.shardResponses().get(lastCurrentIndexName).asScala.filter(_.recoveryState().getPrimary).size
    // find latest history index name
    val historyAliasRes = ftsService.client.admin.indices().prepareGetAliases("cmwell_history_latest").execute().actionGet()
    val lastHistoryIndexName = historyAliasRes.getAliases.keysIt().next()
    val numOfDocumentsHistory = indicesStats.getIndex(lastHistoryIndexName).getTotal.getDocs.getCount

    val lastHistoryIndexRecovery = ftsService.client.admin().indices().prepareRecoveries("cmwell_history_latest").get()
    val numOfShardsHistory = lastHistoryIndexRecovery.shardResponses().get(lastHistoryIndexName).asScala.filter(_.recoveryState().getPrimary).size


    // If number of document per shard in cmwell_current_latest index is greater than threshold
    if( (numOfDocumentsCurrent/numOfShardsCurrent) > maxDocumentsPerShard) {
        logger info "switching current index"
      // create new index
      val lastCurrentIndexCounter = lastCurrentIndexName.substring (lastCurrentIndexName.lastIndexOf ('_') + 1, lastCurrentIndexName.length).toInt
      val nextCurrentIndexName = "cmwell_current_" + (lastCurrentIndexCounter + 1)

      // create new index while adding it to cmwell_current alias
      val createNextCurrentIndexResponse = ftsService.client.admin().indices().prepareCreate(nextCurrentIndexName).addAlias(new Alias("cmwell_current")).execute().actionGet()

      // wait 10 seconds before switching aliases
      system.scheduler.scheduleOnce(10.seconds) {
        // remove latest alias and add it again pointing to new index
        ftsService.client.admin().indices().prepareAliases().removeAlias(lastCurrentIndexName, "cmwell_current_latest").addAlias(nextCurrentIndexName, "cmwell_current_latest").execute().actionGet()
      }
    }

    // If number of document per shard in cmwell_history_latest index is greater than threshold
    if( (numOfDocumentsHistory/numOfShardsHistory) > maxDocumentsPerShard) {
        logger info "switching history index"
      /* switch to a new history index */
      val lastHistoryIndexCounter = lastHistoryIndexName.substring(lastHistoryIndexName.lastIndexOf('_')+1, lastHistoryIndexName.length).toInt
      val nextHistoryIndexName =  "cmwell_history_" + (lastHistoryIndexCounter + 1)

      // create new index while adding it to cmwell_history alias
      ftsService.client.admin().indices().prepareCreate(nextHistoryIndexName).addAlias(new Alias("cmwell_history")).execute().actionGet()

      // wait 10 seconds before switching aliases
      system.scheduler.scheduleOnce(10.seconds) {
        // remove latest alias and add it again pointing to new index
        ftsService.client.admin().indices().prepareAliases().removeAlias(lastHistoryIndexName, "cmwell_history_latest").addAlias(nextHistoryIndexName, "cmwell_history_latest").execute().actionGet()
      }
    }

  }

  def handleFailures(response:BulkResponse, esActions:Seq[(ActionBaker,Option[Infoton],Option[String])], infotons: IndexedSeq[Infoton]) :  Option[Seq[(ActionBaker,Option[Infoton],Option[String])]] = {

    var reply: Option[Seq[(ActionBaker,Option[Infoton],Option[String])]] = None

    val numOfFailed = response.getItems.count {
      _.isFailed
    }

    logger error s"Failure with ${numOfFailed} actions out of ${esActions.size}"

    // split failures to recoverables and non recoverables
    val failedRecoverable = ArrayBuffer[(ActionBaker,Option[Infoton],Option[String])]()
    val failedNonRecoverable = ArrayBuffer[EsAction]()

    val indexedActions = esActions.toIndexedSeq
    response.getItems.foreach{ ri =>
      if(ri.isFailed) {
        if(ri.getFailureMessage.contains("EsRejectedExecutionException")) {
          logger error s"RecoverableError: ${ri.getFailureMessage}"
          val indexedAction = indexedActions(ri.getItemId)
          failedRecoverable += ((indexedActions(ri.getItemId)._1,None,None))
        } else if(ri.getFailureMessage.contains("MapperParsingException")) {
          val ia = indexedActions(ri.getItemId)
          failedRecoverable += ((ia._1,Some(infotons(ri.getItemId)),Some(ri.getFailureMessage)))
        } else {
          logger debug s"NonRecoverableError: ${ri.getFailureMessage}"
          failedNonRecoverable += indexedActions(ri.getItemId)._1(None,None)._1
        }
      }
    }

    totalRecoverableFailuresCounter.inc(failedRecoverable.size)
    totalNonRecoverableFailuresCounter.inc(failedNonRecoverable.size)

    if (failedNonRecoverable.size > 0) {
      // log non recoverable failures
      logger.debug (s"Failed ${failedNonRecoverable.size} actions with non recoverable error:\n ${
        failedNonRecoverable.map{ a => val bso = new BytesStreamOutput(); a.writeTo(bso); bso.bytes().toUtf8}.mkString("\n")}")
    }

    if (failedRecoverable.size > 0) {
      reply = Some(failedRecoverable)
      logger.debug(s"Failed ${failedRecoverable.size} action with recoverable errors: ${
        failedRecoverable.map { a => val bso = new BytesStreamOutput(); a._1(None,a._3)._1.writeTo(bso); bso.bytes().toUtf8}.mkString("\n")
      }")
    }

    reply
  }
}


/**
 * Responsible for persisting the latest next timestamp of uuid tlog to process
 */
object StateManager {
  def props(tlogState:TLogState):Props = Props(new StateManager(tlogState))
}

class StateManager(tlogState:TLogState) extends Actor with LazyLogging {

  def receive = {
    case WorkDone(nextTimeStamp) =>
      logger debug s"saved state: $nextTimeStamp"
      tlogState.saveState(nextTimeStamp)
    case GetState =>
      logger debug s"recieved a request to get state"
      val state = tlogState.loadState
      logger debug s"loaded last state: $state"
      sender() ! tlogState.loadState.getOrElse(0L)
  }

}



/** Messages**/

case object Pause
case object Resume
case object GetState

case class WorkProduced(baseTimeStamp:Long)
case class WorkDone(baseTimeStamp:Long)

case class ProduceWork(numOfBulks:Int = 100, bulkSize:Int = 1000)

case class ProduceFrom(startTimeStamp:Long)

case class Bulk(bulkTimeStamp:Long, esActions:Seq[ActionBaker])




