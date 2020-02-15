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
package cmwell.stortill

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.scaladsl.{Flow, GraphDSL, Source}
import cmwell.common.formats.JsonSerializerForES
import cmwell.domain.{FileContent, FileInfoton, Infoton, autoFixDcAndIndexTime}
import cmwell.formats.JsonFormatter
import cmwell.fts._
import cmwell.irw.IRWService
import cmwell.stortill.Strotill._
import cmwell.syntaxutils._
import cmwell.util.BoxedFailure
import cmwell.util.stream.{MapInitAndLast, SortedStreamsMergeBy}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

/**
  * Created by markz on 3/4/15.
  */
abstract class Operations(irw: IRWService, ftsService: FTSService) {
  def verify(path: String, limit: Int): Future[Boolean]
  def fix(path: String, retries: Int, limit: Int): Future[(Boolean, String)]
  def rFix(path: String, retries: Int, parallelism: Int = 1): Future[Source[(Boolean, String), NotUsed]]
  def info(path: String, limit: Int): Future[(CasInfo, EsExtendedInfo, ZStoreInfo)]
  def shutdown: Unit
}

object ProxyOperations {

  lazy val specialInconsistenciesLogger: Logger = Logger(LoggerFactory.getLogger("cmwell.xfix"))
  lazy val jsonFormatter = new JsonFormatter(identity)

  def apply(irw: IRWService, ftsService: FTSService): ProxyOperations = {
    new ProxyOperations(irw, ftsService)
  }

}

class ProxyOperations private (irw: IRWService, ftsService: FTSService)
    extends Operations(irw, ftsService)
    with LazyLogging {

  val s = Strotill(irw, ftsService)
  import ProxyOperations.{specialInconsistenciesLogger => log}

  val maxRetries: Int = 3 //TODO Take from some config
  val retryWait: FiniteDuration = 613.millis //TODO Take from some config
  def retry[T](task: => Future[T]) = cmwell.util.concurrent.retry[T](maxRetries, retryWait)(task)(global)

  override def verify(path: String, limit: Int) = {

    val casInfoFut = s.extractHistoryCas(path, limit)
    s.extractHistoryEs(path, limit).flatMap { v =>
      if (v.groupBy(_._1).exists(_._2.size != 1)) Future.successful(false)
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
        } yield
          esInfo.size == casInfo.size &&
            esInfo.count(_._2._2) < 2 && //must be either 0 (latest is deleted) or 1. cannot have more than 1 current
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
  override def rFix(path: String, retries: Int, parallelism: Int): Future[Source[(Boolean, String), NotUsed]] = {

    type MergedByTimestamp = (Timestamp, Vector[(Timestamp, Uuid)], Vector[(Timestamp, Uuid, EsIndex)])

    val cassandraPathsSource: Source[(Timestamp, Uuid), NotUsed] = irw.historyReactive(path)
    ftsService.rInfo(path, scrollTTL = 300 , paginationParams = DefaultPaginationParams, withHistory = true).map { rEsInfo =>
      //TODO: we can have a sorted stream from ES, a la consume style.
      val elasticsearchPathInfo: Source[(Timestamp, Uuid, EsIndex), NotUsed] = {
        rEsInfo
          .fold(Vector.empty[(Timestamp, Uuid, EsIndex)])(_ ++ _)
          .mapConcat(_.sortBy(_._1))
      }

      Source.fromGraph(GraphDSL.create() { implicit b =>
        {
          import GraphDSL.Implicits._

          val caS = b.add(cassandraPathsSource)
          val esS = b.add(elasticsearchPathInfo)
          val smb =
            b.add(new SortedStreamsMergeBy[(Timestamp, Uuid), (Timestamp, Uuid, EsIndex), Timestamp](_._1, _._1))
          val ial = b.add(new MapInitAndLast[MergedByTimestamp, (MergedByTimestamp, Boolean)](_ -> false, _ -> true))
          val fix = b.add(Flow[(MergedByTimestamp, Boolean)].mapAsyncUnordered(parallelism) {
            case ((t, v1, v2), isLast) =>
              fixWith(path,
                      retries,
                      Future.successful(v1),
                      Future.successful(v2.map(tt => tt._2 -> tt._3)),
                      isContainsCurrent = isLast).map {
                case (bool, "") => bool -> s"${cmwell.util.string.dateStringify(new DateTime(t))} [$t]"
                case (bool, m)  => bool -> s"$m, ${cmwell.util.string.dateStringify(new DateTime(t))} [$t]"
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

  override def fix(path: String, retries: Int, limit: Int): Future[(Boolean, String)] = {
    val cUuids: Future[Vector[(Timestamp, Uuid)]] = retry(irw.historyAsync(path, limit))
    val rawEsUuids: Future[Vector[(Uuid, EsIndex)]] = retry(
      ftsService.info(path, paginationParams = DefaultPaginationParams, withHistory = true)
    )
    fixWith(path, retries, cUuids, rawEsUuids, isContainsCurrent = true)
  }

  private def fixWith(path: String,
                      retries: Int,
                      cUuids: Future[Vector[(Timestamp, Uuid)]],
                      rawEsUuids: Future[Vector[(Uuid, EsIndex)]],
                      isContainsCurrent: Boolean = false) = {

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
          (onlyC, onlyES.groupBy(_._1).view.mapValues(_.map(_._2)).toMap, both, all)
        }

        val foundFut: Future[Set[Either[Uuid, Infoton]]] = Future.traverse(all) { uuid =>
          // if o.isEmpty, o.get will throw,
          // and the retry will kickoff again
          retry(irw.readUUIDAsync(uuid, cmwell.irw.QUORUM).map(o => Option(o.get)))
            .recoverWith {
              case t: Throwable => {
                log.error(s"could not retreive uuid [$uuid] with QUORUM", t)
                irw.readUUIDAsync(uuid, cmwell.irw.ONE).map {
                  case BoxedFailure(e) =>
                    log.error(s"could not retreive uuid [$uuid] with ONE", e)
                    None
                  case box => box.toOption
                }
              }
            }
            .map(
              infopt =>
                infopt
                  .map {
                    case i if i.uuid != uuid => i.overrideUuid(uuid)
                    case i => i
                  }
                  .toEither(uuid)
            )
        }
        foundFut.flatMap { findings =>
          val found = findings.collect { case Right(i) => i }
          val nFound = findings.collect { case Left(u) => u }

          val purgeNotFound = {

            val uuidFromEsButNotFound = usFromES.filter { case (u, _) => nFound(u) }
            val uuidFromCasButNotFound = usFromC.filter { case (_, u) => nFound(u) }
            val filteredOnlyES = onlyES.filter { case (u, _) => nFound(u) }
            val filteredOnlyC = onlyC.filter { case (u, _) => nFound(u) }
            val filteredBoth = both.filter { case (u, _) => nFound(u) }
            purgeAndLog(path,
              uuidFromEsButNotFound,
              uuidFromCasButNotFound,
              filteredBoth,
              filteredOnlyES,
              filteredOnlyC)
          }

          val fixFoundFut: Future[(Boolean, String)] = {

            if (found.isEmpty) irw.purgePathOnly(path).map(_ => true -> "no UUIDs were found")
            else {
              //TODO 1: optimization: first find lastModified duplicates, and delete those, and only fix what is left
              //TODO 2: if there is no indexTime (infoton was only written in cassandra, but not in ES),
              //TODO:   then if it's the latest (current) version, set indexTime = System.currentTimeMillis,
              //TODO:   and if it's history, set as lastModified, unless lastModified is 0, in this case, we should discuss what to do...
              val foundAndFixedFut = Future.traverse(found)(fixAndUpdateInfotonInCas)
              foundAndFixedFut.flatMap { foundAndFixed =>
                //all infotons have valid indexTime since we already fixed it in `fixAndUpdateInfotonInCas`
                lazy val cur = foundAndFixed.maxBy(_.systemFields.indexTime.get)

                Future
                  .traverse(foundAndFixed.groupBy(_.systemFields.lastModified.getMillis).view.values) {
                    case is if is.size == 1 => Future.successful(is.head)
                    case is => {
                      val maxiton = is.maxBy(_.systemFields.indexTime.getOrElse(0L))
                      Future
                        .traverse(is.filterNot(_ == maxiton)) { i =>
                          log.debug(
                            s"purging  an infoton only because lastModified collision: ${ProxyOperations.jsonFormatter.render(i)}"
                          )
                          // we are purging an infoton only because lastModified collision,
                          // but we should log the lost data (JsonFormatter which preserves the "last name" hash + quads data?)
                          val f1 = retry(ftsService.purgeByUuidsAndIndexes(onlyES(i.uuid).map(i.uuid -> _)))
                            .map[Infoton] { br =>
                            if (br.hasFailures) logEsBulkResponseFailures(br); i
                          }
                            .recover {
                              case e: Throwable =>
                                log.info(s"purge from es failed for uuid=${i.uuid} of path=${i.systemFields.path}", e); i
                            }
                          val f2 = retry(
                            purgeFromCas(i.systemFields.path, i.uuid, onlyC.getOrElse(i.uuid, i.systemFields.lastModified.getMillis))
                          ).map(_ => i).recover {
                            case e: Throwable =>
                              log.info(s"purge from cas failed for uuid=${i.uuid} of path=${i.systemFields.path}", e); i
                          }
                          f1.flatMap(_ => f2)
                        }
                        .map(_ => maxiton)
                    }
                  }
                  .flatMap { noRepetitionsInLastModified =>
                    val writeToCasPathsFut =
                      Future.traverse(noRepetitionsInLastModified.filter(i => onlyES.contains(i.uuid))) {
                        onlyEsInfoton =>
                          val f = retry(irw.setPathHistory(onlyEsInfoton, cmwell.irw.QUORUM)).recover {
                            case e: Throwable =>
                              log.info(
                                s"setPathHistory failed for uuid=${onlyEsInfoton.uuid} of path=${onlyEsInfoton.systemFields.path}",
                                e
                              )
                              onlyEsInfoton
                          }
                          if (isContainsCurrent && onlyEsInfoton == cur) {
                            f.flatMap(
                              i =>
                                retry(irw.setPathLast(i, cmwell.irw.QUORUM)).recover {
                                  case e: Throwable =>
                                    log.info(
                                      s"setPathLast failed for uuid=${onlyEsInfoton.uuid} of path=${onlyEsInfoton.systemFields.path}",
                                      e
                                    )
                                    onlyEsInfoton
                                }
                            )
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
                        val actions = foundAndFixed.map(i => ESIndexRequest(createEsIndexAction(i, i.systemFields.indexName, i eq cur), None))
                        retry(ftsService.executeBulkIndexRequests(actions)).map(_ => (true, ""))
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

  private def purgeAndLog(path: String,
                          uuidsFromEs: Vector[(Uuid, EsIndex)],
                          uuidsFromCas: Vector[(Timestamp, Uuid)],
                          both: Map[Uuid, (Timestamp, Vector[EsIndex])],
                          onlyES: Map[Uuid, Vector[EsIndex]],
                          onlyC: Map[Uuid, Timestamp]) = {
    val logEsSourcesForMissingUuids = Future
      .traverse(uuidsFromEs) {
        case (uuidInEs, esIndex) =>
          val f = retry(ftsService.extractSource(uuidInEs, esIndex)).recover {
            case e: Throwable =>
              log.error(s"could not retrieve ES sources for uuid=[$uuidInEs] from index=[$esIndex]", e)
              "NO SOURCES AVAILABLE" -> -1L
          }
          f.map(uuidInEs -> _)
      }
      .map { uuidsToSourceTuplesVector =>
        val uuidsToSourceMap = uuidsToSourceTuplesVector.toMap
        both.foreach {
          case (uuid, (timestamp, esIndex)) =>
            log.info(
              s"$uuid for $path was not found in cas.infoton, although it was found in cas.path[$timestamp] and also in ES${esIndex
                .mkString("[", ",", "]")} with (source,version): ${uuidsToSourceMap(uuid)}"
            )
        }
        onlyES.foreach {
          case (uuid, index) =>
            log.info(
              s"$uuid for $path was not found in cas, although it was found in ES[$index] with (source,version): ${uuidsToSourceMap(uuid)}"
            )
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
          if (uuidsFromEs.isEmpty) Future.successful(())
          else
            retry(ftsService.purgeByUuidsAndIndexes(uuidsFromEs)).recover {
              case e: Throwable =>
                log.error(
                  s"error occured while purging=${uuidsFromEs.mkString("[", ",", "]")} for path=[$path] from ES",
                  e
                )
            }
        }

        val f2 = Future.traverse(uuidsFromCas) {
          case (timestamp, uuid) =>
            retry(irw.purgeFromPathOnly(path, timestamp, cmwell.irw.QUORUM)).recover {
              case e: Throwable =>
                log.error(s"could not purge uuid=[$uuid] for path=[$path] from CAS.path with timestamp=[$timestamp}]",
                          e)
            }
        }
        f1.flatMap(_ => f2).map(_ => ())
      }
    }
    p.future
  }

  private def createEsIndexAction(infoton: Infoton,
                                          index: String,
                                          isCurrent: Boolean): DocWriteRequest[_] = {
    val infotonWithUpdatedIndexTime = infoton.systemFields.indexTime.fold {
      infoton.replaceIndexTime(Some(infoton.systemFields.lastModified.getMillis))
    }(_ => infoton)
    val serializedInfoton = JsonSerializerForES.encodeInfoton(infotonWithUpdatedIndexTime, isCurrent)
    Requests.indexRequest(index).id(infoton.uuid).create(true).source(serializedInfoton, XContentType.JSON)
  }

  private def purgeFromCas(path: String, uuid: String, timestamp: Long) =
    irw.purgeUuid(path, uuid, timestamp, isOnlyVersion = false, cmwell.irw.QUORUM)

  // filling up `dc` and `indexTime`, since some old data do not have these fields
  private def fixAndUpdateInfotonInCas(i: Infoton): Future[Infoton] =
    autoFixDcAndIndexTime(i, ftsService.dataCenter)
      .fold(Future.successful(i))(
        j =>
          irw.writeAsyncDataOnly(j, cmwell.irw.QUORUM).recover {
            case e: Throwable =>
              log.error(s"could not write to cassandra the infoton with uuid=[${j.uuid}}] for path=[${j.systemFields.path}}]", e)
              j
        }
      )

  override def info(path: String, limit: Int): Future[(CasInfo, EsExtendedInfo, ZStoreInfo)] = {

    val esinfo = s.extractHistoryEs(path, limit).flatMap { uuidIndexVec =>
      cmwell.util.concurrent.travector(uuidIndexVec) {
        case (uuid, index) =>
          ftsService.extractSource(uuid, index).map {
            case (source, version) => (uuid, index, version, source)
          }
      }
    }

    s.extractHistoryCas(path, limit).flatMap { v =>
      val zsKeys = v.collect {
        case (_, Some(FileInfoton(_, _, Some(FileContent(_, _, _, Some(dp)))))) => dp
      }.distinct
      esinfo.map((v, _, zsKeys))
    }
  }

  override def shutdown: Unit = {
    this.s.irwProxy.daoProxy.shutdown()
    this.s.ftsProxy.shutdown()
  }
}
