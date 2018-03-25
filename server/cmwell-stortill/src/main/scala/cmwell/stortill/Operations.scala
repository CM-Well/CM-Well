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


package cmwell.stortill

import akka.NotUsed
import akka.stream.impl.fusing.MapAsyncUnordered
import akka.stream.{ClosedShape, Materializer, SourceShape}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import cmwell.common.formats.{JsonSerializer, JsonSerializerForES}
import cmwell.domain.{FileContent, FileInfoton, Infoton, autoFixDcAndIndexTime, _}
import cmwell.driver.Dao
import cmwell.formats.JsonFormatter
import cmwell.fts._
import cmwell.irw.{IRWService, IRWServiceNativeImpl2}
import cmwell.stortill.Strotill._
import cmwell.common.formats.JsonSerializer
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.Requests
import org.elasticsearch.index.VersionType
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import cmwell.syntaxutils._
import cmwell.util.BoxedFailure
import cmwell.util.stream.{MapInitAndLast, SortedStreamsMergeBy}
import org.elasticsearch.action.ActionRequest
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.util.Try

/**
 * Created by markz on 3/4/15.
 */
abstract class Operations ( irw : IRWService , ftsService : FTSServiceOps ) {
  def verify(path: String, limit: Int): Future[Boolean]
  def fix(path: String, retries: Int, limit: Int): Future[(Boolean,String)]
  def rFix(path: String, retries: Int, parallelism: Int = 1): Future[Source[(Boolean,String),NotUsed]]
  def info(path: String, limit: Int): Future[(CasInfo,EsExtendedInfo,ZStoreInfo)]
  def fixDc(path: String, dc: String, retries: Int = 1, indexTimeOpt: Option[Long] = None): Future[Boolean]
  def shutdown: Unit
}

object ProxyOperations {

  lazy val specialInconsistenciesLogger: Logger = Logger(LoggerFactory.getLogger("cmwell.xfix"))
  lazy val jsonFormatter = new JsonFormatter(identity)

  def apply(irw : IRWService , ftsService : FTSServiceOps) : ProxyOperations = {
    new ProxyOperations(irw ,ftsService)
  }

  //used when working with the REPL
  def apply(clusterName : String , hostname : String): ProxyOperations = {
    System.setProperty("ftsService.transportAddress" , hostname)
    System.setProperty("ftsService.clusterName" , clusterName)
    val dao = Dao("operation" , "data" , hostname ,10 )
    val irw = IRWService(dao)
    val fts = FTSServiceES.getOne("ftsService.yml")
    new ProxyOperations(irw ,fts)
  }
}

class ProxyOperations private(irw: IRWService, ftsService: FTSServiceOps) extends Operations(irw, ftsService) with LazyLogging {

  val isNewBg = {
    val nFts = ftsService.isInstanceOf[FTSServiceNew]
    val nIrw = irw.isInstanceOf[IRWServiceNativeImpl2]
    if((nFts && !nIrw) || (!nFts && nIrw)) throw new IllegalStateException(s"ProxyOperations must have IRW & FTS conforoming in terms of `nbg`, got: ($nFts,$nIrw)")
    else nFts && nIrw
  }
  val s = Strotill(irw , ftsService)
  import ProxyOperations.{specialInconsistenciesLogger => log}

  val maxRetries: Int = 3 //TODO Take from some config
  val retryWait: FiniteDuration = 613.millis //TODO Take from some config
  def retry[T](task: => Future[T]) = cmwell.util.concurrent.retry[T](maxRetries, retryWait)(task)(global)

  override def verify(path : String, limit: Int) = {

    val casInfoFut = s.extractHistoryCas(path, limit)
    s.extractHistoryEs(path, limit).flatMap{ v =>
      if(v.groupBy(_._1).exists(_._2.size != 1)) Future.successful(false)
      else {
        val esMapFut = cmwell.util.concurrent.travemp(v) {
          case (uuid, index) => {
            import cmwell.fts.EsSourceExtractor.Implicits.esMapSourceExtractor
            ftsService.extractSource(uuid, index).map {
              case (source, version) => {
                val system = source.get("system").asInstanceOf[java.util.HashMap[String, Object]]
                val current = system.get("current").asInstanceOf[Boolean]
                uuid -> (index, current)
              }
            }
          }
        }

        for {
          casInfo <- casInfoFut
          esInfo <- esMapFut
        } yield esInfo.size == casInfo.size &&
          esInfo.count(_._2._2) < 2         && //must be either 0 (latest is deleted) or 1. cannot have more than 1 current
          casInfo.forall { case (uuid, _) => esInfo.contains(uuid) }
      }
    }
  }

  type Timestamp = Long
  type Uuid = String
  type EsIndex = String

  // NOTE: in the old path, we must pull everything (without data!), sort in mem,
  // and only then fix it in small chunks.
  // in new data path, the stream from cassandra is truely reactive, and naturaly sorted.
  // so when we have the new data path inplace, if we decide we still need "x-fix",
  // we should also pull ES data reactively in a sorted fashion.
  override def rFix(path: String, retries: Int, parallelism: Int): Future[Source[(Boolean,String),NotUsed]] = {

    type MergedByTimestamp = (Timestamp, Vector[(Timestamp, Uuid)], Vector[(Timestamp, Uuid, EsIndex)])

    val cassandraPathsSource: Source[(Timestamp, Uuid), NotUsed] = irw.historyReactive(path)
    ftsService.rInfo(path, paginationParams = DefaultPaginationParams, withHistory = true).map { rEsInfo =>

      //TODO: we can have a sorted stream from ES, a la consume style.
      val elasticsearchPathInfo: Source[(Timestamp, Uuid, EsIndex), NotUsed] =  {
        rEsInfo
          .fold(Vector.empty[(Timestamp, Uuid, EsIndex)])(_ ++ _)
          .mapConcat(_.sortBy(_._1))
      }

      Source.fromGraph(GraphDSL.create() {
        implicit b => {
          import GraphDSL.Implicits._

          val caS = b.add(cassandraPathsSource)
          val esS = b.add(elasticsearchPathInfo)
          val smb = b.add(new SortedStreamsMergeBy[(Timestamp, Uuid), (Timestamp, Uuid, EsIndex), Timestamp](_._1, _._1))
          val ial = b.add(new MapInitAndLast[MergedByTimestamp, (MergedByTimestamp,Boolean)](_ -> false, _ -> true))
          val fix = b.add(Flow[(MergedByTimestamp,Boolean)].mapAsyncUnordered(parallelism){
            case ((t, v1, v2),isLast) =>
              fixWith(path, retries, Future.successful(v1), Future.successful(v2.map(tt => tt._2 -> tt._3)),isContainsCurrent = isLast).map {
                case (bool, "") => bool -> s"${cmwell.util.string.dateStringify(new DateTime(t))} [$t]"
                case (bool, m) => bool -> s"$m, ${cmwell.util.string.dateStringify(new DateTime(t))} [$t]"
              }
          })

          caS ~> smb.in0
          esS ~> smb.in1
          smb.out ~> ial ~> fix

          SourceShape(fix.out)
        }
      })
    }
  }

  override def fix(path: String, retries: Int, limit: Int): Future[(Boolean,String)] = {
    val cUuids: Future[Vector[(Timestamp, Uuid)]] = retry(irw.historyAsync(path,limit))
    val rawEsUuids: Future[Vector[(Uuid, EsIndex)]] = retry(ftsService.info(path, paginationParams = DefaultPaginationParams, withHistory = true))
    fixWith(path, retries, cUuids, rawEsUuids, isContainsCurrent = true)
  }

  private def fixWith(path: String, retries: Int, cUuids: Future[Vector[(Timestamp, Uuid)]], rawEsUuids: Future[Vector[(Uuid, EsIndex)]], isContainsCurrent: Boolean = false) = {

    val esUuids: Future[Vector[(Uuid, EsIndex)]] = rawEsUuids.flatMap {
      case v if v.isEmpty => Future.successful(Vector.empty)
      case esus => {
        val esusmap = esus.groupBy(_._1)
        cmwell.util.concurrent.travector(esusmap.toSeq) {
          case (uuid, Vector(uuidInSingleIndex)) => Future.successful(uuidInSingleIndex)
          case (uuid, vec) => {
            val oldestIngest = vec.minBy(_._2)
            ftsService.purgeByUuidsAndIndexes(vec.filterNot(oldestIngest.eq)).map { bulkRes =>
              if (bulkRes.hasFailures) {
                log.error(bulkRes.toString)
              }
              oldestIngest
            }
          }
        }
      }
    }

    cUuids.flatMap { usFromC =>
      esUuids.flatMap { usFromES =>

        val (onlyC, onlyES, both, all) = {
          val (pb, oc) = usFromC.partition(u => usFromES.exists(_._1 == u))
          val (b, onlyES) = usFromES.partition(u => pb.exists(_._2 == u))
          val onlyC = oc.map { case (ts, u) => u -> ts }.toMap
          val both = {
            val gByUuid = b.groupBy(_._1)
            gByUuid.map {
              case (u, esIdxs) =>
                u -> (pb.find(_._2 == u).get._1 -> esIdxs.map(_._2))
            }
          }
          val all = usFromC.map(_._2).toSet ++ usFromES.map(_._1)
          (onlyC, onlyES.groupBy(_._1).mapValues(_.map(_._2)), both, all)
        }

        val foundFut: Future[Set[Either[Uuid, Infoton]]] = Future.traverse(all) { uuid =>
                                                          // if o.isEmpty, o.get will throw,
                                                          // and the retry will kickoff again
          retry(irw.readUUIDAsync(uuid, cmwell.irw.QUORUM).map(o => Option(o.get))).recoverWith {
            case t: Throwable => {
              log.error(s"could not retreive uuid [$uuid] with QUORUM",t)
              irw.readUUIDAsync(uuid, cmwell.irw.ONE).map{
                case BoxedFailure(e) =>
                  log.error(s"could not retreive uuid [$uuid] with ONE",e)
                  None
                case box => box.toOption
              }
            }
          }.map(infopt => infopt.map {
            case i if i.uuid != uuid => i.overrideUuid(uuid)
            case i => i
          }.toEither(uuid))
        }
        foundFut.flatMap { findings =>

          val found = findings.collect { case Right(i) => i }
          val nFound = findings.collect { case Left(u) => u }

          val purgeNotFound = {

            val uuidFromEsButNotFound = usFromES.filter { case (u, _) => nFound(u) }
            val uuidFromCasButNotFound = usFromC.filter { case (_, u) => nFound(u) }
            val filteredOnlyES          = onlyES.filter { case (u, _) => nFound(u) }
            val filteredOnlyC            = onlyC.filter { case (u, _) => nFound(u) }
            val filteredBoth              = both.filter { case (u, _) => nFound(u) }
            purgeAndLog(path, uuidFromEsButNotFound, uuidFromCasButNotFound, filteredBoth, filteredOnlyES, filteredOnlyC)
          }

          val fixFoundFut: Future[(Boolean,String)] = {

            if (found.isEmpty) irw.purgePathOnly(path).map(_ => true -> "no UUIDs were found")
            else {
              //TODO 1: optimization: first find lastModified duplicates, and delete those, and only fix what is left
              //TODO 2: if there is no indexTime (infoton was only written in cassandra, but not in ES),
              //TODO:   then if it's the latest (current) version, set indexTime = System.currentTimeMillis,
              //TODO:   and if it's history, set as lastModified, unless lastModified is 0, in this case, we should discuss what to do...
              val foundAndFixedFut = Future.traverse(found)(fixAndUpdateInfotonInCas)
              foundAndFixedFut.flatMap { foundAndFixed =>

                //all infotons have valid indexTime since we already fixed it in `fixAndUpdateInfotonInCas`
                lazy val cur = foundAndFixed.maxBy(_.indexTime.get)

                Future.traverse(foundAndFixed.groupBy(_.lastModified.getMillis)) {
                  case (_, is) if is.size == 1 => Future.successful(is.head)
                  case (_, is) => {
                    val maxiton = is.maxBy(_.indexTime.getOrElse(0L))
                    Future.traverse(is.filterNot(_ == maxiton)) { i =>
                      log.debug(s"purging  an infoton only because lastModified collision: ${ProxyOperations.jsonFormatter.render(i)}")
                      // we are purging an infoton only because lastModified collision,
                      // but we should log the lost data (JsonFormatter which preserves the "last name" hash + quads data?)
                      val f1 = retry(ftsService.purgeByUuidsAndIndexes( onlyES(i.uuid).map(i.uuid -> _))).map[Infoton] { br =>
                        if (br.hasFailures) logEsBulkResponseFailures(br); i
                      }.recover {
                        case e: Throwable =>
                          log.info(s"purge from es failed for uuid=${i.uuid} of path=${i.path}", e); i
                      }
                      val f2 = retry(purgeFromCas(i.path, i.uuid, onlyC.getOrElse(i.uuid,i.lastModified.getMillis))).map(_ => i).recover {
                        case e: Throwable =>
                          log.info(s"purge from cas failed for uuid=${i.uuid} of path=${i.path}", e); i
                      }
                      f1.flatMap(_ => f2)
                    }.map(_ => maxiton)
                  }
                }.flatMap { noRepetitionsInLastModified =>

                  val writeToCasPathsFut = Future.traverse(noRepetitionsInLastModified.filter(i => onlyES.contains(i.uuid))) { onlyEsInfoton =>

                    val f = retry(irw.setPathHistory(onlyEsInfoton, cmwell.irw.QUORUM)).recover {
                      case e: Throwable =>
                        log.info(s"setPathHistory failed for uuid=${onlyEsInfoton.uuid} of path=${onlyEsInfoton.path}", e)
                        onlyEsInfoton
                    }
                    if (isContainsCurrent && onlyEsInfoton == cur) {
                      f.flatMap(i => retry(irw.setPathLast(i, cmwell.irw.QUORUM)).recover {
                        case e: Throwable =>
                          log.info(s"setPathLast failed for uuid=${onlyEsInfoton.uuid} of path=${onlyEsInfoton.path}", e)
                          onlyEsInfoton
                      })
                    } else f
                  }

                  writeToCasPathsFut.flatMap { writeToCasPaths =>
                    // purge from ES all the found uuids in order to re-write 'em correctly
                    // this must be only after succeeding to write the uuid to cas.path,
                    // or else we might lose the uuid "handle", if something is wrong during the fix
                    val uFromEsThatAreFound = usFromES.filterNot { case (u, _) => nFound(u) }
                    val purgeFromEsFut = Future.traverse(uFromEsThatAreFound) {
                      // todo .recover + check .hasFailures
                      case (uuid, index) =>
                        retry(ftsService.purgeByUuidsAndIndexes(Vector(uuid -> index)))
                    }

                    purgeFromEsFut.flatMap { _ =>
                      //lastly, write infocolones for the good infotons
                      if (isNewBg) {
                        //TODO: if indexed in cm_well_latest, should we also take care of the update in cassandra for indexName?
                        val actions = foundAndFixed.map(i => ESIndexRequest(createEsIndexActionForNewBG(i, if(i.indexName.isEmpty) "cm_well_latest" else i.indexName, i eq cur), None))
                        retry(ftsService.executeBulkIndexRequests(actions)).map(_ => (true, ""))
                      }
                      else {
                        val actions = foundAndFixed.map {
                          case i if isContainsCurrent && (i eq cur) => createEsIndexAction(i, "cmwell_current_latest")
                          case i => createEsIndexAction(i, "cmwell_history_latest")
                        }
                        // todo .recover + check .hasFailures
                        retry(ftsService.executeBulkActionRequests(actions)).map(_ => (true, ""))
                      }
                    }
                  }
                }
              }
            }
          }

          purgeNotFound.flatMap(_ => fixFoundFut).map(_ => true -> "")
        }
      }
    }
  }

  private def logEsBulkResponseFailures(br: BulkResponse) = {
    log.info(s"ES BulkResponse has failures: ${br.getItems.filter(_.isFailed).map(_.getFailureMessage).mkString(", ")}")
  }

  private def purgeAndLog(path: String, uuidsFromEs: Vector[(Uuid,EsIndex)],uuidsFromCas: Vector[(Timestamp,Uuid)], both: Map[Uuid,(Timestamp,Vector[EsIndex])], onlyES: Map[Uuid,Vector[EsIndex]], onlyC: Map[Uuid,Timestamp]) = {
    val logEsSourcesForMissingUuids = Future.traverse(uuidsFromEs) {
      case (uuidInEs, esIndex) =>
        val f = retry(ftsService.extractSource(uuidInEs, esIndex)).recover {
          case e: Throwable =>
            log.error(s"could not retrieve ES sources for uuid=[$uuidInEs] from index=[$esIndex]",e)
            "NO SOURCES AVAILABLE" -> -1L
        }
        f.map(uuidInEs -> _)
    }.map { uuidsToSourceTuplesVector =>
      val uuidsToSourceMap = uuidsToSourceTuplesVector.toMap
      both.foreach {
        case (uuid, (timestamp, esIndex)) =>
          log.info(s"$uuid for $path was not found in cas.infoton, although it was found in cas.path[$timestamp] and also in ES${esIndex.mkString("[",",","]")} with (source,version): ${uuidsToSourceMap(uuid)}")
      }
      onlyES.foreach { case (uuid, index) =>
        log.info(s"$uuid for $path was not found in cas, although it was found in ES[$index] with (source,version): ${uuidsToSourceMap(uuid)}")
      }
    }

    onlyC.foreach {
      case (timestamp, uuid) =>
        log.info(s"$uuid for $path was not found in cas.infoton, although it was found in cas.path[$timestamp]")
    }

    val p = Promise[Unit]()
    //we want to purge only after we are done with the logging of the bad data
    logEsSourcesForMissingUuids.onComplete { t =>
      t.failed.foreach(err => log.error("logEsSourcesForMissingUuids future failed", err))
      p.completeWith {
        val f1 = {
          if(uuidsFromEs.isEmpty) Future.successful(())
          else retry(ftsService.purgeByUuidsAndIndexes(uuidsFromEs)).recover {
            case e: Throwable => log.error(s"error occured while purging=${uuidsFromEs.mkString("[", ",", "]")} for path=[$path] from ES", e)
          }
        }

        val f2 = Future.traverse(uuidsFromCas) {
          case (timestamp, uuid) => retry(irw.purgeFromPathOnly(path, timestamp, cmwell.irw.QUORUM)).recover{
            case e: Throwable => log.error(s"could not purge uuid=[$uuid] for path=[$path] from CAS.path with timestamp=[$timestamp}]",e)
          }
        }
        f1.flatMap(_ => f2).map(_=>())
      }
    }
    p.future
  }

  private def createEsIndexActionForNewBG(infoton: Infoton, index: String, isCurrent: Boolean): ActionRequest[_ <: ActionRequest[_ <: AnyRef]] = {
    val infotonWithUpdatedIndexTime = infoton.indexTime.fold {
      infoton.replaceIndexTime(infoton.lastModified.getMillis)
    }(_ => infoton)
    val serializedInfoton = JsonSerializerForES.encodeInfoton(infotonWithUpdatedIndexTime, isCurrent)
    Requests.indexRequest(index).`type`("infoclone").id(infoton.uuid).create(true).source(serializedInfoton)
  }

  private def createEsIndexAction(infoton: Infoton, index: String) = Requests.
    indexRequest(index).`type`("infoclone").id(infoton.uuid).create(true).
    versionType(VersionType.FORCE).version(1).
    source(JsonSerializer.encodeInfoton(infoton, omitBinaryData = true, toEs = true))

  private def purgeFromCas(path: String, uuid: String, timestamp: Long) = irw.purgeUuid(path, uuid, timestamp, isOnlyVersion = false, cmwell.irw.QUORUM)

  // filling up `dc` and `indexTime`, since some old data do not have these fields
  private def fixAndUpdateInfotonInCas(i: Infoton): Future[Infoton] =
    autoFixDcAndIndexTime(i, Settings.dataCenter)
      .fold(Future.successful(i))(j => irw.writeAsyncDataOnly(j,cmwell.irw.QUORUM).recover{
        case e: Throwable =>
          log.error(s"could not write to cassandra the infoton with uuid=[${j.uuid}}] for path=[${j.path}}]",e)
          j
      })

  override def info(path : String, limit: Int) : Future[(CasInfo,EsExtendedInfo,ZStoreInfo)] = {

    val esinfo = s.extractHistoryEs(path,limit).flatMap{ uuidIndexVec =>
      cmwell.util.concurrent.travector(uuidIndexVec){
        case (uuid,index) => ftsService.extractSource(uuid,index).map{
          case (source,version) => (uuid,index,version,source)
        }
      }
    }

    s.extractHistoryCas(path,limit).flatMap { v =>
      val zsKeys = v.collect { case (_, Some(FileInfoton(_, _, _, _, _, Some(FileContent(_, _, _, Some(dp))), _))) => dp }.distinct
      esinfo.map((v, _, zsKeys))
    }
  }

  override def fixDc(path: String, dc: String, retries: Int, indexTimeOpt: Option[Long] = None): Future[Boolean] = {
    import cmwell.domain.{addDc, addIndexInfo}
    import com.datastax.driver.core.ConsistencyLevel._

    import scala.concurrent.duration._

    require(dc != "na", "fix-dc with \"na\"?")
    require(!isNewBg, "fixDc not implemented for nbg path")

    val task = cmwell.util.concurrent.retry(retries,1.seconds) {
      s.extractLastCas(path).flatMap { infoton =>

        val dummyFut = Future.successful(())

        val (infotonWithFixedIndexTime,addIdxTInCasFut) = {
          val iIdxTOpt = infoton.indexTime
          lazy val lmMillis = infoton.lastModified.getMillis
          indexTimeOpt match {
            case Some(t) if iIdxTOpt.isDefined && t == iIdxTOpt.get => infoton -> dummyFut
            case None if iIdxTOpt.isDefined => infoton -> dummyFut
            case Some(t) => addIndexTime(infoton, indexTimeOpt, force = true) ->
                            s.irwProxy.addIndexTimeToUuid(infoton.uuid, t, level = QUORUM)
            case None => addIndexTime(infoton, Some(lmMillis), force = true) ->
                         s.irwProxy.addIndexTimeToUuid(infoton.uuid, lmMillis, level = QUORUM)
          }
        }

        val (infotonWithFixedIndexTimeAndDc,addDcInCasFut) = infoton.dc match {
          case dc2 if dc2 == dc => infotonWithFixedIndexTime -> dummyFut
          case _ => addDc(infotonWithFixedIndexTime, dc, force = true) ->
                    s.irwProxy.addDcToUuid(infoton.uuid, dc, level = QUORUM)
        }

        Future.sequence(Seq(addIdxTInCasFut, addDcInCasFut)).map{_ =>
          () => {
            val esIndexAction = {
              Requests
                .indexRequest("cmwell_current_latest")
                .`type`("infoclone")
                .id(infotonWithFixedIndexTimeAndDc.uuid)
                .create(true)
                .versionType(VersionType.FORCE)
                .version(1)
                .source(JsonSerializer.encodeInfoton(infotonWithFixedIndexTimeAndDc, true, true))
            }
            s.ftsProxy.purgeByUuids(Seq(), Some(infotonWithFixedIndexTimeAndDc.uuid)).flatMap {
              case br if br.hasFailures => Future.failed(new Exception("purging indexes failed"))
              case _ => s.ftsProxy.executeBulkActionRequests(Seq(esIndexAction)).flatMap {
                case br if br.hasFailures => Future.failed(new Exception("re-indexing failed"))
                case _ => Future.successful(true)
              }
            }
          }
        }
      }
    }

    task.flatMap{ t =>
      cmwell.util.concurrent.retry(retries,1.seconds)(t())
    }
  }

  override def shutdown : Unit ={
    this.s.irwProxy.daoProxy.shutdown()
    this.s.ftsProxy.close()
  }
}
