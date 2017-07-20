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


package logic

import java.util.Properties

import akka.NotUsed
import akka.actor.{Actor, ActorSystem}
import akka.actor.Actor.Receive
import akka.stream.scaladsl.Source
import cmwell.domain._
import cmwell.driver.Dao
import cmwell.formats.{NquadsFlavor, RdfType}
import cmwell.fts.{FTSServiceNew, Settings => _, _}
import cmwell.irw._
import cmwell.stortill.Strotill.{CasInfo, EsInfo, ZStoreInfo}
import cmwell.stortill.{Operations, ProxyOperations}
import cmwell.tlog.{TLog, TLogState}
import cmwell.util.{Box, BoxedFailure, EmptyBox, FullBox}
import cmwell.util.concurrent.SingleElementLazyAsyncCache
import cmwell.common.{BulkCommand, DeleteAttributesCommand, DeletePathCommand, WriteCommand, _}
import cmwell.ws.Settings
import cmwell.zcache.{L1Cache, ZCache}
import cmwell.zstore.ZStore
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.elasticsearch.action.bulk.BulkResponse
import org.joda.time.{DateTime, DateTimeZone}
import wsutil.FormatterManager

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import javax.inject._

import controllers.NbgToggler
import ld.cmw.{NbgPassiveFieldTypesCache, ObgPassiveFieldTypesCache}

@Singleton
class CRUDServiceFS @Inject()(tbg: NbgToggler)(implicit ec: ExecutionContext, sys: ActorSystem) extends LazyLogging {

  import cmwell.ws.Settings._

  def newBG = tbg.get

  lazy val nbgPassiveFieldTypesCache = new NbgPassiveFieldTypesCache(this,ec,sys)
  lazy val obgPassiveFieldTypesCache = new ObgPassiveFieldTypesCache(this,ec,sys)
  val level: ConsistencyLevel = ONE
  // create state object in read only
  val impState = TLogState("imp", updatesTLogName, updatesTLogPartition, true)
  val indexerState = TLogState("indexer", uuidsTLogName, updatesTLogPartition, true)

  val updatesTlog = TLog(updatesTLogName, updatesTLogPartition)
  updatesTlog.init()

  val uuidsTlog = TLog(uuidsTLogName, uuidsTLogPartition, readOnly = true)
  uuidsTlog.init()

  lazy val defaultParallelism = cmwell.util.os.Props.os.getAvailableProcessors
  lazy val zStore = ZStore(Dao(irwServiceDaoClusterName, irwServiceDaoKeySpace2, irwServiceDaoHostName))
  lazy val zCache = new ZCache(zStore)

  lazy val _irwService = IRWService(Dao(irwServiceDaoClusterName, irwServiceDaoKeySpace, irwServiceDaoHostName), disableReadCache = !Settings.irwReadCacheEnabled)
  lazy val _irwService2 = IRWService.newIRW(Dao(irwServiceDaoClusterName, irwServiceDaoKeySpace2, irwServiceDaoHostName), disableReadCache = !Settings.irwReadCacheEnabled)
  def irwService(nbg: Boolean = newBG): IRWService = {
    if(nbg || newBG) _irwService2
    else _irwService
  }

  lazy val ftsServiceOld = FTSServiceES.getOne("ws.es.yml", false)
  lazy val ftsServiceNew = FTSServiceNew("ws.es.yml")
  def ftsService(nbg: Boolean = newBG): FTSServiceOps = {
    if(nbg || newBG) ftsServiceNew
    else ftsServiceOld
  }

  lazy val oldServices: (IRWService,FTSServiceOps) = _irwService -> ftsServiceOld
  lazy val newServices: (IRWService,FTSServiceOps) = _irwService2 -> ftsServiceNew

  val producerProperties = new Properties
  producerProperties.put("bootstrap.servers", kafkaURL)
  producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  val kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](producerProperties)

  val proxyOpsOld: Operations = ProxyOperations(_irwService, ftsServiceOld)
  val proxyOpsNew: Operations = ProxyOperations(_irwService2, ftsServiceNew)
  def proxyOps(nbg: Boolean = newBG): Operations  = {
    if(nbg || newBG) proxyOpsNew
    else proxyOpsOld
  }

  val ESMappingsCacheOld = new SingleElementLazyAsyncCache[Set[String]](Settings.fieldsNamesCacheTimeout.toMillis,Set.empty)(ftsServiceOld.getMappings(withHistory = true))
  val ESMappingsCacheNew = new SingleElementLazyAsyncCache[Set[String]](Settings.fieldsNamesCacheTimeout.toMillis,Set.empty)(ftsServiceNew.getMappings(withHistory = true))
  def ESMappingsCache(nbg: Boolean = newBG): SingleElementLazyAsyncCache[Set[String]] = {
    if(nbg || newBG) ESMappingsCacheNew
    else ESMappingsCacheOld
  }

  val MetaNsCacheOld =
    new SingleElementLazyAsyncCache[Set[String]](Settings.fieldsNamesCacheTimeout.toMillis,Set.empty)(fetchEntireMetaNsAsPredicates(false))
  val MetaNsCacheNew =
    new SingleElementLazyAsyncCache[Set[String]](Settings.fieldsNamesCacheTimeout.toMillis,Set.empty)(fetchEntireMetaNsAsPredicates(true))

  def metaNsCache(nbg: Boolean = newBG) = if(nbg || newBG) MetaNsCacheNew else MetaNsCacheOld

  private val fieldsSetBreakout = scala.collection.breakOut[Seq[Option[String]],String,Set[String]]

  private def fetchEntireMetaNsAsPredicates(nbg: Boolean = newBG) = {
    val chunkSize = 512
    def fetchFields(offset: Int = 0): Future[Seq[Infoton]] = {
      val fieldsFut = search(Some(PathFilter("/meta/ns", descendants = true)), paginationParams = PaginationParams(offset, chunkSize), withData = true, nbg = nbg)
      fieldsFut.flatMap{ fields =>
        if(fields.length==chunkSize) fetchFields(offset+chunkSize).map(_ ++ fields.infotons) else Future.successful(fields.infotons)
      }
    }

    fetchFields().map { f =>
      val (fieldsInfotons,namespacesInfotons) = f.partition(_.path.count('/'.==)>3)
      val prefixToUrl = (for {
        i <- namespacesInfotons
        f <- i.fields
        u <- f.get("url")
        v <- u.collect{case FString(w,_,_) => w}.headOption
      } yield i.name -> v).toMap

      val fieldsSet = fieldsInfotons.map{ infoton =>
        val lastTwoPathParts = infoton.path.split('/').reverseIterator.take(2).toArray.reverse
        val (prefix,localName) = lastTwoPathParts(0) -> lastTwoPathParts(1)
        prefixToUrl.get(prefix).map(_ + localName)
      }.collect{
        case Some(x) => x
      }(fieldsSetBreakout)
      fieldsSet
    }
  }


  def countSearchOpenContexts(nbg: Boolean = this.newBG): Array[(String,Long)] =
    ftsService(nbg).countSearchOpenContexts()

  def getInfotonByPathAsync(path: String,nbg: Boolean = this.newBG): Future[Box[Infoton]] =
    irwService(nbg).readPathAsync(path, level)

  def getInfoton(path: String, offset: Option[Int], length: Option[Int], nbg: Boolean = this.newBG): Future[Option[ContentPortion]] = {

    def listChildrenBoundedTime(path: String, offset: Option[Int], length: Option[Int]): Future[Option[FTSSearchResponse]] = {
      val fut =
          ftsService(nbg).listChildren(path, offset.getOrElse(0), length.get)
      val dur = cmwell.ws.Settings.esGracfulDegradationTimeout.seconds
      cmwell.util.concurrent.timeoutOptionFuture(fut, dur)
    }

    val infotonFuture = irwService(nbg).readPathAsync(path, level)

    val reply = if (length.getOrElse(0) > 0) {
      val searchResponseFuture = listChildrenBoundedTime(path, offset, length)
      // if children requested
      for {
        infotonBox <- infotonFuture
        searchResponseOpt <- searchResponseFuture
      } yield {
        infotonBox match {
          case FullBox(i) => {
            searchResponseOpt match {
              case None => Some(UnknownNestedContent(i))
              case Some(searchResponse) => {
                if (searchResponse.infotons.nonEmpty) {
                  Some(Everything(CompoundInfoton(i.path, i.dc, i.indexTime, i.lastModified, i.fields, searchResponse.infotons, searchResponse.offset, searchResponse.length, searchResponse.total)))
                } else Some(Everything(i))
              }
            }
          }
          case EmptyBox => Option.empty[ContentPortion]
          case BoxedFailure(e) =>
            logger.error(s"boxed failure for readPathAsync [$path], nbg=$nbg",e)
            Option.empty[ContentPortion]
        }
      }
    } else {
      // no children requested, just return infoton from IRW service
      infotonFuture.map{
        case BoxedFailure(e) =>
          logger.error(s"boxed failure for readPathAsync [$path], nbg=$nbg",e)
          None
        case box => box.toOption.map(Everything.apply)
      }
    }
    reply
  }

  def getInfotonHistory(path: String, limit: Int, nbg: Boolean = newBG): Future[InfotonHistoryVersions] = {
    val (_, uuidVec) = irwService(nbg).history(path,limit).sortBy(_._1).unzip
    if (uuidVec.isEmpty) Future.successful(InfotonHistoryVersions(Vector.empty[Infoton]))
    else irwService(nbg).readUUIDSAsync(uuidVec, level).map(seq => InfotonHistoryVersions(seq.collect{case FullBox(i) => i}))
  }

  def getInfotonHistoryReactive(path: String, nbg: Boolean = newBG): Source[Infoton,NotUsed] = {
    irwService(nbg)
      .historyReactive(path)
      .mapAsync(defaultParallelism) {
        case (_, uuid) => irwService(nbg).readUUIDAsync(uuid).andThen {
          case Failure(fail) => logger.error(s"uuid [$uuid] could not be fetched from cassandra", fail)
          case Success(EmptyBox) => logger.error(s"uuid [$uuid] could not be fetched from cassandra: got EmptyBox")
          case Success(BoxedFailure(e)) => logger.error(s"uuid [$uuid] could not be fetched from cassandra: got BoxedFailure",e)
        }
      }
      .collect { case FullBox(i) => i }
  }

  def getInfotons(paths: Seq[String], nbg: Boolean = newBG): Future[BagOfInfotons] =
    irwService(nbg).readPathsAsync(paths, level).map{ infopts =>
      BagOfInfotons(infopts.collect{
        case FullBox(infoton) => infoton
      })
    }

  def getInfotonsByPathOrUuid(paths: Vector[String] = Vector.empty[String], uuids: Vector[String] = Vector.empty[String], nbg: Boolean = newBG): Future[BagOfInfotons] = {
    val futureInfotonsList: Future[List[Infoton]] = (paths, uuids) match {
      case (ps, us) if ps.isEmpty && us.isEmpty => Future.successful(Nil)
      case (ps, us) if us.isEmpty => irwService(nbg).readPathsAsync(ps, level).map(_.collect { case FullBox(i) => i }.toList)
      case (ps, us) if ps.isEmpty => irwService(nbg).readUUIDSAsync(us, level).map(_.collect { case FullBox(i) => i }.toList)
      case (ps, us) => {
        val f1 = irwService(nbg).readPathsAsync(ps, level).map(_.collect { case FullBox(i) => i })
        val f2 = irwService(nbg).readUUIDSAsync(us, level).map(_.collect { case FullBox(i) => i })
        Future.sequence(List(f1, f2)).map(_.flatten.distinct)
      }
    }

    futureInfotonsList.map(BagOfInfotons.apply)
  }

  def getInfotonByUuidAsync(uuid: String, nbg: Boolean = newBG): Future[Box[Infoton]] = {
    irwService(nbg).readUUIDAsync(uuid, level)
  }

  def getInfotonsByUuidAsync(uuidVec: Seq[String], nbg: Boolean = newBG): Future[Seq[Infoton]] = {
    irwService(nbg).readUUIDSAsync(uuidVec, level).map(_.collect{
      case FullBox(i) => i
    })
  }

  def putInfoton(infoton: Infoton): Future[Boolean] = {
    // build a command with infoton
    val cmdWrite = WriteCommand(infoton)
    // convert the command to Array[Byte] payload
    lazy val payload: Array[Byte] = CommandSerializer.encode(cmdWrite)

    if (infoton.fields.map(_.map(_._2.size).sum).getOrElse(0) > 10000) {
      Future.failed(new IllegalArgumentException("too many fields"))
    }
    else {
      // write payload to tlog
      val payloadForIndirectLargeInfoton: Future[(Array[Byte],Array[Byte])] = infoton match {
        case i@FileInfoton(_, _, _, _, _, Some(FileContent(Some(data), _, _, _)),_) if data.length >= thresholdToUseZStore => {
          val fi = i.withoutData
          zStore.put(fi.content.flatMap(_.dataPointer).get, data).map { _ =>
            val withPossibleEpoch = CommandSerializer.encode(WriteCommand(fi))
            if(i.lastModified.getMillis == 0L) withPossibleEpoch -> CommandSerializer.encode(WriteCommand(fi.copy(lastModified = new DateTime())))
            else withPossibleEpoch -> withPossibleEpoch
          }
        }
        case i => {
          val t =
            if(i.lastModified.getMillis == 0L) payload -> CommandSerializer.encode(WriteCommand(i.copyInfoton(lastModified = new DateTime())))
            else payload -> payload
          Future.successful(t)
        }
      }
      val tlogWriteRes = if (oldBGFlag) payloadForIndirectLargeInfoton.flatMap(t => updatesTlog.write(t._1)) else Future.successful(true)
      payloadForIndirectLargeInfoton.flatMap { payload =>
        val kafkaWriteRes = if (newBGFlag || newBG) sendToKafka(infoton.path, payload._2).map(_ => true)
                            else Future.successful(true)
        tlogWriteRes.zip(kafkaWriteRes).map(t => t._2 && t._2)
      }
    }
  }

  def putOverwrites(infotons: Vector[Infoton]): Future[Boolean] = {
    val cmds = infotons.map(OverwriteCommand(_))
    val bulks = cmds.grouped(maxBulkSize)
    val tLogWrites = if(oldBGFlag) {
      Future.traverse(bulks) { vec =>
        // build a command with vec of commands
        val cmdBulk = BulkCommand(vec.toList)
        // convert bulk command to Array[Byte] payload
        val payload: Array[Byte] = CommandSerializer.encode(cmdBulk)
        // write payload to tlog
        updatesTlog.write(payload)
      }.map(_ => true)
    } else Future.successful(true)

    val kafkaWritesRes = if(newBGFlag || newBG) {
      Future.traverse(cmds)(sendToKafka).map{_ => true}
    } else Future.successful(true)

    tLogWrites.zip(kafkaWritesRes).map{_ => true}

  }

  def putInfotons(infotons: Vector[Infoton], tid: Option[String] = None, atomicUpdates: Map[String,String] = Map.empty) = {
    val tLogWriteRes = if(oldBGFlag) {
      Future.sequence(
          infotons.map(WriteCommand(_)).grouped(maxBulkSize).map { vec =>
          // build a command with vec of commands
          val cmdBulk = BulkCommand(vec.toList)
          // convert bulk command to Array[Byte] payload
          val payload: Array[Byte] = CommandSerializer.encode(cmdBulk)
          // write payload to tlog
          updatesTlog.write(payload)
        }
      ).map{ _ => true}
    } else Future.successful(true)

    val kafkaWritesRes = if(newBGFlag || newBG) {
      Future.traverse(infotons){
        case infoton if infoton.lastModified.getMillis == 0L =>
          sendToKafka(
            WriteCommand(
              infoton.copyInfoton(lastModified = DateTime.now(DateTimeZone.UTC)),
              validTid(infoton.path,tid),
              prevUUID = atomicUpdates.get(infoton.path)))
        case infoton => sendToKafka(WriteCommand(infoton,validTid(infoton.path,tid),atomicUpdates.get(infoton.path)))
      }.map(_ => true)
    } else Future.successful(true)

    tLogWriteRes.zip(kafkaWritesRes).map{t => t._1 && t._2}
  }

  def deleteInfotons(deletes: List[(String, Option[Map[String, Set[FieldValue]]])], tidOpt: Option[String] = None, atomicUpdates: Map[String,String] = Map.empty) = {
    val dt = new DateTime()
    val commands: List[SingleCommand] = deletes.map {
      case (path, Some(fields)) => DeleteAttributesCommand(path, fields, dt, validTid(path,tidOpt), atomicUpdates.get(path))
      case (path, None) => DeletePathCommand(path, dt, validTid(path,tidOpt), atomicUpdates.get(path))
    }

    val tLogWritesRes = if(oldBGFlag) {
      Future.sequence(
        commands.grouped(maxBulkSize).map { cmds =>
          updatesTlog.write(CommandSerializer.encode(BulkCommand(cmds)))
        }
      ).map{ _ => true}
    } else Future.successful(true)

    val kafkaWritesRes = if(newBGFlag || newBG) {
      Future.traverse(commands)(sendToKafka).map(_ => true)
    } else Future.successful(true)

    tLogWritesRes.zip(kafkaWritesRes).map(_ => true)

  }

  def deleteInfoton(path: String, data: Option[Map[String, Set[FieldValue]]]) = {

    val delCommand = data match {
      case None => DeletePathCommand(path, new DateTime())
      case Some(fields) => DeleteAttributesCommand(path, fields, new DateTime())
    }

    val payload = CommandSerializer.encode(delCommand)
    val tLogWriteRes = if(oldBGFlag) updatesTlog.write(payload).map { _ => true } else Future.successful(true)

    val kafkaWriteRes = if(newBGFlag || newBG) sendToKafka(delCommand.path, payload)
                        else Future.successful(true)

    tLogWriteRes.zip(kafkaWriteRes).map{_ => true}
  }

  /**
   * upsert == update & insert
   * will delete ALL (!!!) values for a given field!
   * to backup values to preserve, you must add it to the inserts vector!
   */
  def upsertInfotons(inserts: List[Infoton], deletes: Map[String, Map[String, Option[Set[FieldValue]]]], tid: Option[String] = None, atomicUpdates: Map[String,String] = Map.empty): Future[Boolean] = {
    //require(!inserts.isEmpty,"if you only have DELETEs, use delete. not upsert!")
    require(inserts.forall(i => deletes.keySet(i.path)),
      "you can't use upsert for entirely new infotons! split your request into upsertInfotons and putInfotons!\n" +
        s"deletes: ${deletes}\ninserts: ${inserts}")
    type FMap = Map[String, Map[String, Option[Set[FieldValue]]]]
    val eSet = Set.empty[FieldValue]
    val eMap = Map.empty[String, Set[FieldValue]]

    if (inserts.isEmpty && deletes.isEmpty) Future.successful(true)
    else {
      val dt = new DateTime()
      val (mixedDeletes, pureDeletes): Tuple2[FMap, FMap] = deletes.partition { case (k, _) => inserts.exists(_.path == k) }
      val dels = pureDeletes.map {
        case (path, fieldSet) => {
          val m = fieldSet.map {
            case (f, None) => f -> eSet
            case (f, Some(s)) => f -> s
          }
          UpdatePathCommand(path, m, eMap, dt, validTid(path,tid), atomicUpdates.get(path))
        }
      }.toList

      val ups = inserts.map {
        i => {
          val del = mixedDeletes(i.path).map {
            case (f, None) => f -> eSet
            case (f, Some(s)) => f -> s
          }
          val ins = i.fields match {
            case Some(fields) => fields
            case None => eMap //TODO: should we block this option? regular DELETE could have been used instead...
          }
          UpdatePathCommand(i.path, del, ins, i.lastModified, validTid(i.path, tid), atomicUpdates.get(i.path))
        }
      }

      val commands:List[SingleCommand] = dels ::: ups

      val tLogWritesRes = if(oldBGFlag) {
        Future.sequence(
          commands.grouped(maxBulkSize).map { cmds =>
            updatesTlog.write(CommandSerializer.encode(BulkCommand(cmds)))
          }
        ).map{ _ => true}
      } else Future.successful(true)

      val kafkaWritesRes = if(newBGFlag || newBG) {
        Future.traverse(commands){
          case cmd@UpdatePathCommand(_, _, _, lastModified, _, _) if lastModified.getMillis == 0L => sendToKafka(cmd.copy(lastModified = DateTime.now(DateTimeZone.UTC)))
          case cmd => sendToKafka(cmd)
        }.map{_ => true}
      } else Future.successful(true)

      tLogWritesRes.zip(kafkaWritesRes).map{_ => true}
    }
  }



  // todo move this logic to InputHandler!
  private def validTid(path: String, tid: Option[String]): Option[String] =
  tid.fold(Option.empty[String]){ t =>
    if(path.matches("/meta/n(n|s)/.+")) None
    else tid
  }

  private def sendToKafka(command: SingleCommand): Future[Unit] =
    sendToKafka(command.path, CommandSerializer.encode(command))

  private def sendToKafka(path: String, payload: Array[Byte]): Future[Unit] = {
    val payloadForKafkaFut = if (payload.length > thresholdToUseZStore) {
      val key = cmwell.util.string.Hash.md5(payload)
      zStore.put(key, payload, secondsToLive = 7.days.toSeconds.toInt, false).
        map(_ => CommandSerializer.encode(CommandRef(key)))
    } else Future.successful(payload)

    payloadForKafkaFut.flatMap { payloadForKafka =>
      val pRecord = new ProducerRecord[Array[Byte], Array[Byte]](persistTopicName, path.getBytes("UTF-8"), payloadForKafka)
      injectFuture(kafkaProducer.send(pRecord, _))
    }

    payloadForKafkaFut.map(_ => ())
  }

  //TODO: add with-deleted to aggregations
  def aggregate(pathFilter: Option[PathFilter] = None,
                fieldsFilters: Option[FieldFilter] = None,
                datesFilter: Option[DatesFilter] = None,
                paginationParams: PaginationParams = DefaultPaginationParams,
                withHistory: Boolean = false,
                aggregationFilters: Seq[AggregationFilter],
                debugInfo: Boolean = false, nbg: Boolean = newBG): Future[AggregationsResponse] = {
      ftsService(nbg).aggregate(pathFilter, fieldsFilters, datesFilter, paginationParams, aggregationFilters, debugInfo = debugInfo)
  }

  def thinSearch(pathFilter: Option[PathFilter] = None,
                 fieldFilters: Option[FieldFilter] = None,
                 datesFilter: Option[DatesFilter] = None,
                 paginationParams: PaginationParams = DefaultPaginationParams,
                 withHistory: Boolean = false,
                 fieldSortParams: SortParam = SortParam.empty,
                 debugInfo: Boolean = false,
                 withDeleted: Boolean = false,
                 nbg:Boolean = newBG)(implicit searchTimeout : Option[Duration] = None): Future[SearchThinResults] = {

    val searchResultsFuture = {
      //withDeleted is only available in new FTS, and using it forces nbg
      ftsService(nbg || withDeleted).thinSearch(pathFilter, fieldFilters, datesFilter, paginationParams,
        fieldSortParams, withHistory, withDeleted = withDeleted, debugInfo = debugInfo, timeout = searchTimeout)
    }

    searchResultsFuture.map { ftr =>
      SearchThinResults(ftr.total, ftr.offset, ftr.length, ftr.thinInfotons.map{ ti =>
        SearchThinResult(ti.path, ti.uuid, ti.lastModified, ti.indexTime, ti.score)
      }, debugInfo = ftr.searchQueryStr)
    }
  }


//  object SearchCacheHelpers {
//
//    private val nquadsFormatter = FormatterManager.getFormatter(RdfType(NquadsFlavor))
//
//    case class SearchRequest(pathFilter: Option[PathFilter] = None,
//                             fieldFilters: Option[FieldFilter] = None,
//                             datesFilter: Option[DatesFilter] = None,
//                             paginationParams: PaginationParams = DefaultPaginationParams,
//                             withHistory: Boolean = false,
//                             withData: Boolean = false,
//                             fieldSortParams: SortParam = SortParam.empty,
//                             debugInfo: Boolean = false,
//                             withDeleted: Boolean = false) {
//      def getDigest = cmwell.util.string.Hash.md5(this.toString)
//    }
//
//
//    def wSearch(searchRequest: SearchRequest, nbg: Boolean = newBG): Future[SearchResults] = {
//      search(searchRequest.pathFilter, searchRequest.fieldFilters, searchRequest.datesFilter,
//        searchRequest.paginationParams, searchRequest.withHistory, searchRequest.withData, searchRequest.fieldSortParams,
//        searchRequest.debugInfo, searchRequest.withDeleted,nbg)
//    }
//
//    def serializer(searchResults: SearchResults): Array[Byte] =
//      nquadsFormatter.render(searchResults).getBytes("UTF-8")
//
//    def deserializer(payload: Array[Byte]): SearchResults = {
//      ???
//    }
//
//    def searchViaCache(nbg: Boolean = newBG) = cmwell.zcache.l1l2[SearchRequest,SearchResults](wSearch(_,nbg))(_.getDigest, deserializer, serializer)()(zCache)
//  }

  def search(pathFilter: Option[PathFilter] = None,
             fieldFilters: Option[FieldFilter] = None,
             datesFilter: Option[DatesFilter] = None,
             paginationParams: PaginationParams = DefaultPaginationParams,
             withHistory: Boolean = false,
             withData: Boolean = false,
             fieldSortParams: SortParam = SortParam.empty,
             debugInfo: Boolean = false,
             withDeleted: Boolean = false,
             nbg:Boolean = newBG)(implicit searchTimeout : Option[Duration] = None): Future[SearchResults] = {

    val searchResultsFuture = {
      //withDeleted is only available in new FTS, and using it forces nbg
      ftsService(nbg || withDeleted).search(pathFilter, fieldFilters, datesFilter, paginationParams,
        fieldSortParams, withHistory, debugInfo = debugInfo, timeout = searchTimeout, withDeleted = withDeleted)
    }
    def ftsResults2Dates(ftsResults: FTSSearchResponse): (Option[DateTime], Option[DateTime]) = {
      if (ftsResults.length > 0) {
        val to = ftsResults.infotons.maxBy(_.lastModified.getMillis)
        val from = ftsResults.infotons.minBy(_.lastModified.getMillis)
        Some(from.lastModified) -> Some(to.lastModified)
      } else (None, None)
    }

    val results = withData match {
      case true => searchResultsFuture.flatMap { ftsResults =>
        val (fromDate, toDate) = ftsResults2Dates(ftsResults)
        val xs = cmwell.util.concurrent.travector(ftsResults.infotons) { i =>
          irwService(nbg || withDeleted).readUUIDAsync(i.uuid,level).map(_ -> i.fields)
        }

        xs
        /*
        irwService.readUUIDSAsync(ftsResults.infotons.map { i =>
          i.uuid
        }.toVector, level) */ .map { infotonsSeq =>
          if(infotonsSeq.exists(_._1.isEmpty)) {
            val esUuidsSet: Set[String] = ftsResults.infotons.map(_.uuid)(scala.collection.breakOut[Seq[Infoton],String,Set[String]])
            val casUuidsSet: Set[String] = infotonsSeq.collect{case (FullBox(i),_) => i.uuid}(scala.collection.breakOut[Vector[(Box[Infoton],Option[Map[String,Set[FieldValue]]])],String,Set[String]])
            logger.error("some uuids retrieved from ES, could not be retrieved from cassandra: " + esUuidsSet.diff(casUuidsSet).mkString("[",",","]"))
          }
          val infotons = {
            if(fieldSortParams eq NullSortParam) infotonsSeq.collect { case (FullBox(i), _) => i }
            else infotonsSeq.collect { case (FullBox(i), e) => addExtras(i,e) }
          }
          SearchResults(fromDate, toDate, ftsResults.total, ftsResults.offset, ftsResults.length, infotons, ftsResults.searchQueryStr)
        }
      }
      case false => searchResultsFuture.map { ftsResults =>
        val (fromDate, toDate) = ftsResults2Dates(ftsResults)
        SearchResults(fromDate, toDate, ftsResults.total, ftsResults.offset, ftsResults.length, ftsResults.infotons, ftsResults.searchQueryStr)
      }
    }
    results
  }

  //FIXME: extra should not contain same keys as fields (all keys should start with '$'), so another ugly hack...
  private def addExtras(infoton: Infoton, extra: Option[Map[String,Set[FieldValue]]]): Infoton = infoton match {
    case i: ObjectInfoton => new ObjectInfoton(i.path,i.dc,i.indexTime,i.lastModified,i.fields.fold(extra)(f => extra.fold(i.fields)(e => Some(f ++ e)))) {
      override def uuid = i.uuid
      override def kind = i.kind
    }
    case i: FileInfoton => new FileInfoton(i.path,i.dc,i.indexTime,i.lastModified,i.fields.fold(extra)(f => extra.fold(i.fields)(e => Some(f ++ e))),i.content) {
      override def uuid = i.uuid
      override def kind = i.kind
    }
    case i: LinkInfoton => new LinkInfoton(i.path,i.dc,i.indexTime,i.lastModified,i.fields.fold(extra)(f => extra.fold(i.fields)(e => Some(f ++ e))),i.linkTo,i.linkType) {
      override def uuid = i.uuid
      override def kind = i.kind
    }
    case _ => infoton
  }

  private def addIndexTime(fromCassandra: Seq[Infoton], fromES: Seq[Infoton]): Seq[Infoton] = {
    val m = fromES.collect { case i if i.indexTime.isDefined => i.uuid -> i.indexTime.get }.toMap
    fromCassandra.map {
      case i: ObjectInfoton if m.isDefinedAt(i.uuid) && i.indexTime.isEmpty => i.copy(indexTime = m.get(i.uuid))
      case i: FileInfoton if m.isDefinedAt(i.uuid) && i.indexTime.isEmpty => i.copy(indexTime = m.get(i.uuid))
      case i: LinkInfoton if m.isDefinedAt(i.uuid) && i.indexTime.isEmpty => i.copy(indexTime = m.get(i.uuid))
      case i: DeletedInfoton if m.isDefinedAt(i.uuid) && i.indexTime.isEmpty => i.copy(indexTime = m.get(i.uuid))
      case i => i
    }
  }

  def getListOfDC(nbg: Boolean = newBG): Future[Seq[String]] = {
    ftsService(nbg).listChildren("/meta/sys/dc",0,20).map { sr =>
      Settings.dataCenter +: sr.infotons.map(_.path.drop("/meta/sys/dc/".length))
    }
  }

  def getLastIndexTimeFor(dc: String = Settings.dataCenter, nbg: Boolean = newBG): Future[Option[VirtualInfoton]] = {

    def mkVirtualInfoton(indexTime: Long): VirtualInfoton =
      VirtualInfoton(ObjectInfoton(s"/proc/dc/$dc", Settings.dataCenter, None,
        Map("lastIdxT" -> Set[FieldValue](FLong(indexTime)),
          "dc" -> Set[FieldValue](FString(dc)))))
    ftsService(nbg).getLastIndexTimeFor(dc).map(lOpt => Some(mkVirtualInfoton(lOpt.getOrElse(0L))))
  }

  def getESFieldsVInfoton(nbg: Boolean = newBG): Future[VirtualInfoton] = {
    val fields = ESMappingsCache(nbg).getAndUpdateIfNeeded.map(toFieldValues)

    fields.flatMap { f =>
      val predicates = metaNsCache(nbg).getAndUpdateIfNeeded.map(toFieldValues)
      predicates.map { p =>
        VirtualInfoton(ObjectInfoton(s"/proc/fields", Settings.dataCenter, None, Map("fields" -> f, "predicates" -> p)))
      }
    }
  }

  private def toFieldValues(ss: Set[String]): Set[FieldValue] = ss.map(FString.apply)

  def startScroll(pathFilter: Option[PathFilter] = None,
                  fieldsFilters: Option[FieldFilter] = None,
                  datesFilter: Option[DatesFilter] = None,
                  paginationParams: PaginationParams = DefaultPaginationParams,
                  scrollTTL: Long,
                  withHistory: Boolean = false,
                  withDeleted: Boolean = false,
                  debugInfo: Boolean = false,
                  nbg:Boolean = false): Future[IterationResults] = {
    //withDeleted is only available in new FTS, and using it forces nbg
    ftsService(nbg || withDeleted).startScroll(pathFilter, fieldsFilters, datesFilter, paginationParams, scrollTTL, withHistory, withDeleted, debugInfo = debugInfo).map { ftsResults =>
      IterationResults(ftsResults.scrollId, ftsResults.total, debugInfo = ftsResults.searchQueryStr)
    }
  }

  def startSuperScroll(pathFilter: Option[PathFilter] = None,
                       fieldFilters: Option[FieldFilter] = None,
                       datesFilter: Option[DatesFilter] = None,
                       paginationParams: PaginationParams = DefaultPaginationParams,
                       scrollTTL: Long,
                       withHistory: Boolean = false,
                       withDeleted: Boolean = false,
                       nbg: Boolean = false): Seq[Future[IterationResults]] = {
    //withDeleted is only available in new FTS, and using it forces nbg
    ftsService(nbg || withDeleted).startSuperScroll(pathFilter, fieldFilters, datesFilter, paginationParams, scrollTTL, withHistory, withDeleted).map(_.map { ftsResults =>
      IterationResults(ftsResults.scrollId, ftsResults.total)
    })
  }

  def startSuperMultiScroll(pathFilter: Option[PathFilter] = None,
                            fieldFilters: Option[FieldFilter] = None,
                            datesFilter: Option[DatesFilter] = None,
                            paginationParams: PaginationParams = DefaultPaginationParams,
                            scrollTTL: Long,
                            withHistory: Boolean = false,
                            withDeleted: Boolean = false,
                            nbg: Boolean = false): Seq[Future[IterationResults]] = {
    //withDeleted is only available in new FTS, and using it forces nbg
    ftsService(nbg || withDeleted).startSuperMultiScroll(pathFilter, fieldFilters, datesFilter, paginationParams, scrollTTL, withHistory, withDeleted).map(_.map { ftsResults =>
      IterationResults(ftsResults.scrollId, ftsResults.total)
    })
  }

  def startMultiScroll(pathFilter: Option[PathFilter] = None,
                       fieldFilters: Option[FieldFilter] = None,
                       datesFilter: Option[DatesFilter] = None,
                       paginationParams: PaginationParams = DefaultPaginationParams,
                       scrollTTL: Long,
                       withHistory: Boolean = false,
                       withDeleted: Boolean = false,
                       nbg: Boolean = false): Seq[Future[IterationResults]] = {
    //withDeleted is only available in new FTS, and using it forces nbg
    ftsService(nbg || withDeleted).startMultiScroll(pathFilter, fieldFilters, datesFilter, paginationParams, scrollTTL, withHistory, withDeleted).map(_.map { ftsResults =>
      IterationResults(ftsResults.scrollId, ftsResults.total)
    })
  }

  def scroll(scrollId: String, scrollTTL: Int, withData: Boolean, nbg: Boolean = false): Future[IterationResults] = {

    val searchResultFuture = ftsService(nbg).scroll(scrollId, scrollTTL)
    val results = withData match {
      case false => searchResultFuture.map { ftsResults =>
        IterationResults(ftsResults.scrollId, ftsResults.total, Some(ftsResults.infotons))
      }
      case true => searchResultFuture.flatMap { ftsResults =>
        irwService(nbg).readUUIDSAsync(ftsResults.infotons.map {
          _.uuid
        }.toVector, level).map { infotonsSeq =>
          if(infotonsSeq.exists(_.isEmpty)) {
            val esUuidsSet: Set[String] = ftsResults.infotons.map(_.uuid)(scala.collection.breakOut[Seq[Infoton],String,Set[String]])
            val casUuidsSet: Set[String] = infotonsSeq.collect{case FullBox(i) => i.uuid}(scala.collection.breakOut[Seq[Box[Infoton]],String,Set[String]])
            logger.error("some uuids retrieved from ES, could not be retrieved from cassandra: " + esUuidsSet.diff(casUuidsSet).mkString("[",",","]"))
          }
          val infotons = addIndexTime(infotonsSeq.collect{case FullBox(i) => i}, ftsResults.infotons)
          IterationResults(ftsResults.scrollId, ftsResults.total, Some(infotons))
        }
      }
    }
    results
  }

  def verify(path: String, limit: Int, nbg: Boolean = newBG): Future[Boolean] = proxyOps(nbg).verify(path,limit)

  def fix(path: String, limit: Int, nbg: Boolean = newBG): Future[(Boolean, String)] = {
    logger.debug(s"x-fix invoked for path $path")
    proxyOps(nbg).fix(path, cmwell.ws.Settings.xFixNumRetries,limit)
  }

  def rFix(path: String, parallelism: Int = 1, nbg: Boolean = newBG): Future[Source[(Boolean,String),NotUsed]] = {
    logger.debug(s"x-fix&reactive invoked for path $path")
    proxyOps(nbg).rFix(path, cmwell.ws.Settings.xFixNumRetries, parallelism)
  }

  def info(path: String, limit: Int, nbg: Boolean = newBG): Future[(CasInfo, EsInfo, ZStoreInfo)] = proxyOps(nbg).info(path,limit)

  def fixDc(path: String, actualDc: String, nbg: Boolean = newBG): Future[Boolean] = {
    proxyOps(nbg).fixDc(path, actualDc, cmwell.ws.Settings.xFixNumRetries)
  }

  def getRawCassandra(uuid: String, nbg: Boolean = newBG): Future[(String,String)] = {
    val (irw,mime) = irwService(nbg) match {
      case irw@`_irwService2` => irw -> "text/csv;charset=UTF-8"
      case irw@`_irwService` => irw -> "application/json;charset=UTF-8"
    }
    irw.getRawRow(uuid).map(_ -> mime)
  }

  def reactiveRawCassandra(uuid: String): Source[String,NotUsed] = _irwService2.getReactiveRawRow(uuid,QUORUM)

  // assuming not the only version of the infoton!
  def purgeUuid(infoton: Infoton): Future[Unit] = {
    cmwell.util.concurrent.travector(Vector(oldServices,newServices)){ case (irwService, ftsService) =>
      irwService.purgeHistorical(infoton, isOnlyVersion = false, QUORUM).flatMap { _ =>
        ftsService.purge(infoton.uuid).map(_ => Unit)
      }
    }.map(_=> Unit)
  }

  def purgeUuidFromIndex(uuid: String, index: String, nbg: Boolean = newBG): Future[Unit] = {
    ftsService(nbg).purgeByUuidsAndIndexes(Vector(uuid->index)).map(_ => ()) //TODO also purge from ftsServiceNew
  }

  def purgePath(path: String, includeLast: Boolean, limit: Int, nbg: Boolean = newBG): Future[Unit] = {

    import scala.language.postfixOps

    cmwell.util.concurrent.travector(Vector(oldServices, newServices)) { case (irwService, ftsService) =>

      val casHistory = irwService.history(path, limit)
      val lastOpt = if (casHistory.nonEmpty) Some(casHistory.maxBy(_._1)) else None

      // union uuids from es and cas (and keeping indexes, if known):
      val allPossibleUuidsFut = ftsService.info(path, DefaultPaginationParams, withHistory = true).map { esInfo =>
        val allUuids = casHistory.map(_._2).toSet ++ esInfo.map(_._1).toSet
        if (includeLast || lastOpt.isEmpty)
          esInfo -> allUuids
        else
          esInfo.filterNot(_._1 == lastOpt.get._2) -> (allUuids - lastOpt.get._2)
      }

      cmwell.util.concurrent.retry(3, 1.seconds) {

        allPossibleUuidsFut.flatMap { case allUuids =>
          val (uuidsWithIndexes, justUuids) = allUuids

          val purgeJustByUuids = {
            if (justUuids.nonEmpty)
              ftsService.purgeByUuidsFromAllIndexes(justUuids.toVector)
            else
              Future.successful(new BulkResponse(Array(), 0))
          }

          purgeJustByUuids.flatMap { bulkResponse =>
            if (bulkResponse.hasFailures) {
              throw new Exception("purge from es by uuids from all Indexes failed: " + bulkResponse.buildFailureMessage())
            } else {
              if (uuidsWithIndexes.nonEmpty)
                ftsService.purgeByUuidsAndIndexes(uuidsWithIndexes.toVector)
              else
                Future.successful(new BulkResponse(Array(), 0))
            }.flatMap { bulkResponse =>
              if (bulkResponse.hasFailures) {
                throw new Exception("purge from es by uuids on specific Indexes failed: " + bulkResponse.buildFailureMessage())
              } else {
                val purgeHistoryFut =
                  if (includeLast || lastOpt.isEmpty) {
                    // no need to delete from Paths one by one, will delete entire row when purgeHistorical below will be invoked with isOnlyVersion=true
                    Future.traverse(casHistory.map(_._2))(irwService.purgeFromInfotonsOnly(_))
                  } else {
                    Future.traverse(casHistory.filter(lastOpt.get !=)) {
                      h => irwService.purgeHistorical(path, h._2, h._1, isOnlyVersion = false, level = ConsistencyLevel.QUORUM)
                    }
                  }

                val purgeHistoryForDanglingInfotonsFut = {
                  val danglingUuids = justUuids -- casHistory.map(_._2).toSet
                  Future.traverse(danglingUuids)(irwService.purgeFromInfotonsOnly(_))
                }

                if (includeLast && lastOpt.isDefined)
                  purgeHistoryFut.
                    flatMap(_ => purgeHistoryForDanglingInfotonsFut).
                    flatMap(_ => irwService.purgeHistorical(path, lastOpt.get._2, lastOpt.get._1, isOnlyVersion = true, level = ConsistencyLevel.QUORUM))
                else purgeHistoryFut.
                  flatMap(_ => purgeHistoryForDanglingInfotonsFut).
                  map(_ => ())
              }
            }
          }
        }
      }
    }.map(_ => ())
  }

  /**
    * Rollback an Infoton means purging last version of it, and, if there exists one or more history versions, make the
    * one with largest lastModified the current version.
    */
  def rollback(path: String, limit: Int, nbg: Boolean = newBG): Future[Unit] = {
    case class Version(lastModified: Long, uuid: String)

    cmwell.util.concurrent.travector(Vector(oldServices, newServices)) { case (irwService, ftsService) =>

      irwService.historyAsync(path, limit).map { casHistory =>
        if (casHistory.isEmpty) Future.successful(())
        else {
          val sortedCasHistory = casHistory.sortBy(_._1).map { case (lm, uuid) => Version(lm, uuid) }
          val last = sortedCasHistory.last
          val prev = sortedCasHistory.init.lastOption

          def purgeLast(isTherePrev: Boolean) = cmwell.util.concurrent.retry(3, 1.seconds) {
            irwService.purgeHistorical(path, last.uuid, last.lastModified, isOnlyVersion = !isTherePrev, ConsistencyLevel.QUORUM).flatMap { _ =>
              ftsService.purgeByUuidsFromAllIndexes(Vector(last.uuid))
            }
          }

          def setPrevAsLast(pv: Version) = cmwell.util.concurrent.retry(3, 1.seconds) {
            irwService.setPathLast(path, new java.util.Date(pv.lastModified), pv.uuid, ConsistencyLevel.QUORUM).flatMap { _ =>
              irwService.readUUIDAsync(pv.uuid).flatMap { infpot =>
                val prevInfoton = infpot.getOrElse(throw new RuntimeException(s"Previous infoton for path $path was not found under uuid ${pv.uuid}"))
                ftsService.purgeByUuidsFromAllIndexes(Vector(pv.uuid)).flatMap { _ =>
                  ftsService.index(prevInfoton, None)
                }
              }
            }
          }

          purgeLast(prev.isDefined).flatMap { _ => prev.map(setPrevAsLast).getOrElse(Future.successful(())) }
        }
      }
    }.map(_ => ())
  }

  def purgePath2(path: String, limit: Int, nbg: Boolean = newBG): Future[Unit] = {

    import cmwell.util.concurrent.retry

    import scala.language.postfixOps

    irwService(nbg).historyAsync(path,limit).map { casHistory =>

      val uuids = casHistory.map(_._2)

      retry(3, 1.seconds) {
        val purgeEsByUuids = ftsService(nbg).purgeByUuidsFromAllIndexes(uuids)
        purgeEsByUuids.flatMap { bulkResponse =>
          if (bulkResponse.hasFailures) {
            throw new Exception("purge from es by uuids from all Indexes failed: " + bulkResponse.buildFailureMessage())
          } else {
            val purgeFromInfoton = Future.traverse(uuids)(irwService(nbg).purgeFromInfotonsOnly(_))
            purgeFromInfoton.flatMap(_ => irwService(nbg).purgePathOnly(path))
          }
        }
      }
    }
  }

  //var persistTopicOffset = new AtomicLong()

  /**
    * Converts Kafka Async call to Scala's Future
    */
  private def injectFuture(f: Callback => java.util.concurrent.Future[RecordMetadata], timeout : Duration = FiniteDuration(9, SECONDS)) = {
    val p = Promise[RecordMetadata]()
    f(new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if(exception != null) {
          p.failure(exception)
        } else {
          //persistTopicOffset.set(metadata.offset()) // This is ugly but temporary

//          val topic = metadata.topic
//          val partition = metadata.partition
//          BgStateReporter.report(topic, partition, metadata.offset())
          p.success(metadata)
        }
      }
    }
    )
    TimeoutFuture.withTimeout(p.future, timeout)
  }
}


