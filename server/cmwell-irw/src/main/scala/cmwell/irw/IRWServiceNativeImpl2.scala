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
package cmwell.irw

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.alpakka.cassandra.scaladsl.CassandraSource
import akka.stream.scaladsl.Source
import cmwell.common.metrics.WithMetrics
import cmwell.domain._
import cmwell.driver.{Dao, DaoExecution}
import cmwell.util.collections.partitionWith
import cmwell.util.concurrent.{FutureTimeout, travector}
import cmwell.util.jmx._
import cmwell.util.{Box, BoxedFailure, FullBox}
import cmwell.zstore.ZStore
import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes
import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration.{Duration => SDuration, _}
import scala.util.{Failure, Success}

class IRWServiceNativeImpl2(
  storageDao: Dao,
  maxReadSize: Int = 25,
  disableReadCache: Boolean = false,
  readCacheDuration: FiniteDuration = 120.seconds
)(implicit val defaultCasTimeout: SDuration = 10.seconds)
    extends IRWService
    with LazyLogging
    with WithMetrics
    with DaoExecution {

  val delayOnError = 40.millis
  val cacheSize: Long = 50000L
  val sysQuad = InfotonSerializer.sysQuad
  // TODO: make both configurable
  val dataCahce: Cache[String, Infoton] = CacheBuilder
    .newBuilder()
    .maximumSize(cacheSize)
    .expireAfterWrite(readCacheDuration.toMillis, TimeUnit.MILLISECONDS)
    .recordStats()
    .build[String, Infoton]()
  val dataCacheMBean = new GuavaCacheJMX[String, Infoton](dataCahce)
  jmxRegister(dataCacheMBean, "cmwell.irw:type=IRWServiceImpl2")
  val getFromCacheFunc: String => Option[Infoton] = { uuid =>
    Option(dataCahce.getIfPresent(uuid))
  }

  val zStore = ZStore(storageDao)

  val infotonsIRWReadDataTimer = metrics.meter("IRW Read Data Time")
  val infotonsIRWWriteDataTimer = metrics.meter("IRW Write Data Time")

  val infotonsIRWReadPathTimer = metrics.meter("IRW Read Path Time")
  val infotonsIRWWritePathTimer = metrics.meter("IRW Write Path Time")

  // here we are prepering all statments to use

  val getInfotonUUID: PreparedStatement = storageDao.getSession.prepare("SELECT * FROM infoton WHERE uuid = ?")
  val getSystemFields: PreparedStatement = storageDao.getSession.prepare(s"SELECT quad,field,value FROM infoton WHERE uuid = ? AND quad = '$sysQuad'")
  val getLastInPath: PreparedStatement = storageDao.getSession.prepare("SELECT uuid, last_modified FROM path WHERE path = ? LIMIT 1")
  val getAllHistory: PreparedStatement =
    storageDao.getSession.prepare("SELECT last_modified,uuid FROM path WHERE path = ?")
  val getHistory = storageDao.getSession.prepare("SELECT last_modified,uuid FROM path WHERE path = ? limit ?")
  val getHistoryNeighbourhoodAsc = storageDao.getSession.prepare("SELECT last_modified,uuid FROM path WHERE path = ? AND last_modified >= ? LIMIT ?")
  val getHistoryNeighbourhoodDesc = storageDao.getSession.prepare("SELECT last_modified,uuid FROM path WHERE path = ? AND last_modified <= ? LIMIT ?")
  val getIndexTime: PreparedStatement = storageDao.getSession.prepare(
    s"SELECT uuid,quad,field,value FROM infoton WHERE uuid = ? AND quad = '$sysQuad' AND field = 'indexTime'"
  )

  val setInfotonData: PreparedStatement =
    storageDao.getSession.prepare("INSERT INTO infoton (uuid,quad,field,value) VALUES (?,?,?,?)")
  val setInfotonFileData: PreparedStatement =
    storageDao.getSession.prepare("INSERT INTO infoton (uuid,quad,field,value,data) VALUES (?,?,?,?,?)")
  val setDc: PreparedStatement =
    storageDao.getSession.prepare(s"INSERT INTO infoton (uuid,quad,field,value) VALUES (?,'$sysQuad','dc',?)")
  val setIndexTime: PreparedStatement =
    storageDao.getSession.prepare(s"INSERT INTO infoton (uuid,quad,field,value) VALUES (?,'$sysQuad','indexTime',?)")
  val setPathLast: PreparedStatement =
    storageDao.getSession.prepare("INSERT INTO path (path,last_modified,uuid) VALUES (?,?,?)")

  val delIndexTime: PreparedStatement =
    storageDao.getSession.prepare(s"DELETE FROM infoton WHERE uuid = ? AND quad = '$sysQuad' AND field = 'indexTime'")
  val purgeInfotonByUuid: PreparedStatement = storageDao.getSession.prepare("DELETE FROM infoton WHERE uuid = ?")
  val purgeHistoryEntry: PreparedStatement =
    storageDao.getSession.prepare("DELETE FROM path WHERE path = ? AND last_modified = ?")
  val purgeAllHistory: PreparedStatement = storageDao.getSession.prepare("DELETE FROM path WHERE path = ?")

  implicit val daoProxy: Dao = storageDao

  case class UnmatchingReadUUID(requestedUuid: String, receivedInfoton: Infoton)
      extends IllegalStateException(
        s"received infoton with uuid=[${receivedInfoton.uuid}] for requested uuid=[$requestedUuid]."
      )

  private def convert(uuid: String)(result: ResultSet): Box[Infoton] = {

    if (result.isExhausted()) Box.empty[Infoton]
    else
      try {
        val i = InfotonSerializer.deserialize2(
          uuid,
          new Iterator[(String, String, (String, Array[Byte]))] {
            override def hasNext: Boolean = !result.isExhausted()

            override def next(): (String, String, (String, Array[Byte])) = {
              val r: Row = result.one()
              val q = r.getString("quad")
              val f = r.getString("field")
              val v = r.getString("value")
              val d = {
                if (f != "data") null
                else Bytes.getArray(r.getBytes("data"))
              }
              (q, f, v -> d)
            }
          }
        )
        if (i.uuid != uuid) BoxedFailure(UnmatchingReadUUID(uuid, i))
        else FullBox(i)
      } catch {
        case t: Throwable => BoxedFailure(t)
      }
  }

  def rawReadSystemFields(uuid: String, lvl: ConsistencyLevel): Future[Seq[(String,String,String)]] = {
    // TODO this was copied from convert method, we need to stay DRY

    import scala.concurrent.ExecutionContext.Implicits.global

    executeAsyncInternal(getSystemFields.bind(uuid).setConsistencyLevel(lvl)).map { result =>
      new Iterator[(String, String, String)] {
        override def hasNext: Boolean = !result.isExhausted
        override def next(): (String, String, String) = {
          val r: Row = result.one()
          val q = r.getString("quad")
          val f = r.getString("field")
          val v = r.getString("value")
          (q, f, v)
        }
      }.toSeq
    }
  }

  def rawReadUuidAsyc(uuid: String, lvl: ConsistencyLevel): Future[Seq[(String,String,(String,Array[Byte]))]] = {
    // TODO this was copied from convert method, we need to stay DRY

    import scala.concurrent.ExecutionContext.Implicits.global

    executeAsyncInternal(getInfotonUUID.bind(uuid).setConsistencyLevel(lvl)).map { result =>
      new Iterator[(String, String, (String, Array[Byte]))] {
        override def hasNext: Boolean = !result.isExhausted
        override def next(): (String, String, (String, Array[Byte])) = {
          val r: Row = result.one()
          val q = r.getString("quad")
          val f = r.getString("field")
          val v = r.getString("value")
          val d = {
            if (f != "data") null
            else Bytes.getArray(r.getBytes("data"))
          }
          (q, f, v -> d)
        }
      }.toSeq
    }
  }

  def readUUIDSAsync(uuids: Seq[String],
                     level: ConsistencyLevel = ONE)(implicit ec: ExecutionContext): Future[Seq[Box[Infoton]]] = {

    def getUuidsFromCas(us: Seq[String]) = travector(us) { uuid =>
      readUUIDAsync(uuid, level)(ec).recover {
        case err => {
          logger.error(s"could not retrieve UUID [$uuid] from cassandra")
          BoxedFailure(err)
        }
      }
    }

    if (!disableReadCache) {
      val all = uuids.map(uuid => uuid -> getFromCacheFunc(uuid))
      if (all.forall(_._2.isDefined)) Future.successful(all.map { case (_, i) => Box.wrap(i.get) })
      else {
        val (unCachedUuids, cachedInfotons) = partitionWith(all) {
          case (_, Some(i)) => Right(Box.wrap(i))
          case (uuid, None) => Left(uuid)
        }
        getUuidsFromCas(unCachedUuids).map(_ ++ cachedInfotons)
      }
    } else getUuidsFromCas(uuids)
  }

  def readUUIDAsync(uuid: String, level: ConsistencyLevel = ONE, dontFetchPayload: Boolean = false)(
    implicit ec: ExecutionContext
  ): Future[Box[Infoton]] = {

    def getFromCas(lvl: ConsistencyLevel, isARetry: Boolean = false): Future[Box[Infoton]] = {
      val stmt = getInfotonUUID.bind(uuid).setConsistencyLevel(lvl)
      val resultedInfotonFuture =
        cmwell.util.concurrent.retry(5, delayOnError, 2)(executeAsyncInternal(stmt).map(convert(uuid)))(ec)
      resultedInfotonFuture
        .recover {
          case UnmatchingReadUUID(_, i) => FullBox(i)
          case err: Throwable => {
            logger.error(s"could not read/convert uuid [$uuid] to infoton. returning None", err)
            BoxedFailure(err)
          }
        }
        .flatMap {
          case b: Box[Infoton] if b.isEmpty =>
            if (isARetry) Future.successful(b)
            else cmwell.util.concurrent.SimpleScheduler.scheduleFuture(delayOnError)(getFromCas(QUORUM, true))
          case io @ FullBox(i) =>
            if (isARetry) {
              logger.warn(s"The uuid $uuid is only available in QUORUM")
              if (i.uuid != uuid)
                logger.error(
                  s"The infoton [${i.systemFields.path}] retrieved with different uuid [${i.uuid}] from requested uuid [$uuid]"
                )
            }
            if (i.uuid != uuid && !isARetry) getFromCas(QUORUM, true)
            else Future.successful(io)
        }(ec)
    }

    def populateDataIfNeeded(infotonBoxFut: Future[Box[Infoton]]): Future[Box[Infoton]] = {

      def populateDataIfNeededInternal(infoton: Infoton): Future[Infoton] = infoton match {
        case fi: FileInfoton if fi.hasDataPointer && !dontFetchPayload => fi.populateDataFromPointerBy(zStore.get)(ec)
        case i                                                         => Future.successful(i)
      }

      infotonBoxFut.flatMap {
        case FullBox(i) => populateDataIfNeededInternal(i).map(Box.wrap(_))
        case box        => Future.successful(box)
      }
    }

    if (disableReadCache) populateDataIfNeeded(getFromCas(level))
    else
      getFromCacheFunc(uuid).fold(populateDataIfNeeded(getFromCas(level)).andThen {
        case Success(FullBox(i)) if i.uuid == uuid => dataCahce.put(uuid, i)
      })(futureBox)
  }

  def executeAsync(statmentToExec: Statement,
                   identifierForFailures: => String,
                   retries: Int = 5,
                   delay: FiniteDuration = delayOnError,
                   casTimeout: SDuration = defaultCasTimeout)(implicit ec: ExecutionContext): Future[ResultSet] =
    cmwell.util.concurrent.retryWithDelays(Vector.iterate(delayOnError, retries)(_ * 2): _*) {
      if (casTimeout.isFinite)
        cmwell.util.concurrent
          .timeoutFuture(executeAsyncInternal(statmentToExec), casTimeout.asInstanceOf[FiniteDuration])
          .andThen {
            case Failure(FutureTimeout(f)) => {
              val givingUpTimestamp = System.currentTimeMillis()
              val msg = identifierForFailures
              val id = cmwell.util.numeric.Radix64.encodeUnsigned(givingUpTimestamp) + "_" + cmwell.util.numeric.Radix64
                .encodeUnsigned(msg.hashCode())
              logger.error(s"[$id] FutureTimeout for [ $msg ], and givingUpTimestamp = [ $givingUpTimestamp ]")
              f.onComplete {
                case Success(_) =>
                  logger.error(
                    s"executeAsync for [$id] returned successfully ${System.currentTimeMillis() - givingUpTimestamp}ms after giving up"
                  )
                case Failure(t) =>
                  logger.error(
                    s"executeAsync for [$id] returned with failure ${System.currentTimeMillis() - givingUpTimestamp}ms after giving up",
                    t
                  )
              }
            }
          } else
        executeAsyncInternal(statmentToExec)
    }(ec)

  @deprecated("No need in IRW2, use `setPathLast` instead", "1.5.x")
  def setPathHistory(infoton: Infoton, level: ConsistencyLevel = QUORUM): Future[Infoton] =
    setPathLast(infoton, level).map(_ => infoton)(scala.concurrent.ExecutionContext.Implicits.global)

  def setPathLast(infoton: Infoton, level: ConsistencyLevel = QUORUM): Future[Unit] =
    setPathLast(infoton.systemFields.path, infoton.systemFields.lastModified.toDate, infoton.uuid, level)

  def setPathLast(path: String, lastModified: java.util.Date, uuid: String, level: ConsistencyLevel): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val stmt = setPathLast.bind(path, lastModified, uuid).setConsistencyLevel(level)
    executeAsync(stmt, s"'${setPathLast.getQueryString}'.bind($path, $lastModified, $uuid)").map { rs =>
      logger.trace(s"resultSet from setPathLast: $rs")
    }
  }

  def writeAsyncDataOnly(infoton: Infoton,
                         level: ConsistencyLevel = QUORUM)(implicit ec: ExecutionContext): Future[Infoton] = {

    val p = Promise[Infoton]()

    val (uuid, rows) = InfotonSerializer.serialize2(infoton)

    // Write the changes for one infoton as an un-logged batch so that the changes are written atomically.

    val statements: Seq[(Statement, () => String)] = rows.flatMap {
      case (quad, fields) =>
        fields.flatMap {
          case (field, values) =>
            values.map {
              case (value, data) =>
                if (field == "data")
                  (setInfotonFileData.bind(uuid, quad, field, value, ByteBuffer.wrap(data)).setConsistencyLevel(level),
                   () => s"'${setInfotonFileData.getQueryString}'.bind($uuid, $quad, $field, $value, ${data.take(8)})")
                else
                  (setInfotonData.bind(uuid, quad, field, value).setConsistencyLevel(level),
                   () => s"'${setInfotonData.getQueryString}'.bind($uuid, $quad, $field, $value)")
            }
        }
    }

    // This un-logged batch runs against a single partition, so it does not have the performance issue that
    // multi-partition unlogged batches do.
    // Cassandra limits the number of statements in a batch to 0xFFFF.

    val futureResults = statements.grouped(0xFFFF).map { zipped =>
      val (stmts, ids) = zipped.unzip(identity)
      val failStringFunc = () => {
        if (ids.length <= 10)
          ids
            .foldLeft(new StringBuilder) {
              case (sb, f) =>
                sb += '\n'
                sb ++= f()
            }
            .result()
        else {
          val h = ids.take(8).foldLeft(new StringBuilder) {
            case (sb, f) =>
              sb += '\n'
              sb ++= f()
          }
          h ++= "\n…\n"
          h ++= ids.last()
          h.result
        }
      }
      executeAsync(new BatchStatement(BatchStatement.Type.UNLOGGED).addAll(stmts.asJava), failStringFunc())
    }

    Future.sequence(futureResults).onComplete {
      case Failure(t) => p.failure(t)
      case Success(l) if l.exists(!_.wasApplied()) =>
        p.failure(new RuntimeException(s"some statements was not applied: ${l
          .collect { case r if !r.wasApplied() => r.getExecutionInfo.getQueryTrace.toString }
          .mkString("\n\t", "\n\t", "\n")}"))
      case Success(_) => p.success(infoton)
    }

    p.future.andThen {
      case Success(i) if !disableReadCache => dataCahce.put(i.uuid, i)
    }

  }

  def writeSeqAsync(infoton: Seq[Infoton], level: ConsistencyLevel = QUORUM, skipSetPathLast: Boolean = false)(
    implicit ec: ExecutionContext
  ): Future[Seq[Infoton]] =
      travector(infoton)(i => writeAsync(i, level, skipSetPathLast))

  def writeAsync(infoton: Infoton, level: ConsistencyLevel = QUORUM, skipSetPathLast: Boolean = false)(
    implicit ec: ExecutionContext
  ): Future[Infoton] =
    if (skipSetPathLast) writeAsyncDataOnly(infoton, level)
    else
      writeAsyncDataOnly(infoton, level).flatMap { i =>
        setPathLast(infoton, level).map(_ => i)
      }

  def addIndexTimeToUuid(uuid: String, indexTime: Long, level: ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    if (!disableReadCache) {
      Option(dataCahce.getIfPresent(uuid)).foreach { i =>
        dataCahce.put(uuid, addIndexTime(i, Some(indexTime)))
      }
    }

    readIndexTimeRowsForUuid(uuid, level).flatMap { indexTimes =>
      if (indexTimes.isEmpty) {
        val indexTimeToWrite =
          if (!disableReadCache) Option(dataCahce.getIfPresent(uuid)).flatMap { infoton =>
            if (infoton.systemFields.indexTime.fold(false)(_ != indexTime)) {
              logger.warn(s"was asked to `addIndexTimeToUuid` with indexTime=$indexTime for uuid [$uuid], " +
                s"but index time [${infoton.systemFields.indexTime}] was already in cache. Writing again the indexTime that was in the cache.")
            }
            infoton.systemFields.indexTime
          }.getOrElse(indexTime)
          else indexTime
        writeIndexTimeToUuid(uuid, indexTimeToWrite, level)
      }
      else if (indexTimes.head == indexTime && indexTimes.tail.isEmpty) {
        logger.info(
          s"was asked to `addIndexTimeToUuid` for uuid [$uuid], but index time already written [$indexTime]: taking no action and returning Future.successful"
        )
        Future.successful(())
      } else {
        logger.info(
          s"was asked to `addIndexTimeToUuid` for uuid [$uuid], but different indexTime(s) is already present ${indexTimes
            .mkString("[", ",", "]")}: will delete these, and write the new indexTime [$indexTime]"
        )
        deleteIndexTimeFromUuid(uuid, level).flatMap { _ =>
          writeIndexTimeToUuid(uuid, indexTime, level)
        }
      }
    }
  }

  private def deleteIndexTimeFromUuid(uuid: String, level: ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val stmt = delIndexTime.bind(uuid).setConsistencyLevel(level)
    executeAsync(stmt, s"'${delIndexTime.getQueryString}'.bind($uuid)").map(rs => if (!rs.wasApplied()) ???)
  }

  private def writeIndexTimeToUuid(uuid: String, indexTime: Long, level: ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val stmt = setIndexTime.bind(uuid, indexTime.toString).setConsistencyLevel(level)
    executeAsync(stmt, s"'${setIndexTime.getQueryString}'.bind($uuid, $indexTime)").map(rs => if (!rs.wasApplied()) ???)
  }

  def readIndexTimeRowsForUuid(uuid: String, level: ConsistencyLevel = QUORUM): Future[Seq[Long]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.collection.JavaConverters._
    val stmt = getIndexTime.bind(uuid).setConsistencyLevel(level)
    executeAsync(stmt, s"'${getIndexTime.getQueryString}'.bind($uuid)").map { rs =>
      if (!rs.wasApplied()) ???
      else
        rs.all().asScala.map { row =>
          row.getString("value").toLong
        }.toSeq
    }
  }

  def addDcToUuid(uuid: String, dc: String, level: ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    if (!disableReadCache) {
      Option(dataCahce.getIfPresent(uuid)).foreach { i =>
        dataCahce.put(uuid, addDc(i, dc))
      }
    }
    val stmt = setDc.bind(uuid, dc).setConsistencyLevel(level)
    executeAsync(stmt, s"'${setDc.getQueryString}'.bind($uuid, $dc)").map(rs => if (!rs.wasApplied()) ???)
  }

  //FIXME: arghhhhh
  private def extractLast(result: ResultSet): Option[String] = {
    var s: Option[String] = None
    val it = result.iterator()
    while (it.hasNext) {
      val r: Row = it.next()
      val last = r.getString("uuid")
      s = Some(last)
    }
    s
  }

  private def readPathAndLast(path: String, level: ConsistencyLevel = ONE, retry: Boolean = false)(
    implicit ec: ExecutionContext
  ): Future[Box[(String, String)]] = {
    readPathUUIDA(path, level, retry).map(_.map(path -> _))
  }

  def readPathsAsync(paths: Seq[String],
                     level: ConsistencyLevel = ONE)(implicit ec: ExecutionContext): Future[Seq[Box[Infoton]]] = {
    travector(paths) { p =>
      readPathAsync(p, level).recover(recoverAsBoxedFailure)
    }
  }

  /**
    * Reads Async infoton according to path
    *
    * @param path  The path of the infoton
    * @param level The level of consistency
    * @return a future with option of infoton if the infoton does not exists return Future with Nobe else Future with Infoton
    */
  def readPathAsync(path: String,
                    level: ConsistencyLevel = ONE)(implicit ec: ExecutionContext): Future[Box[Infoton]] = {

    // first lets try to read path the get uuid
    val uuidFuture = readPathUUIDA(path, level)

    uuidFuture
      .flatMap {
        case b: Box[String] if b.isEmpty => Future.successful(b.asInstanceOf[Box[Infoton]])
        case FullBox(uuid)               => readUUIDAsync(uuid, level)
      }
      .recover(recoverAsBoxedFailure)
  }

  def readPathUUIDA(path: String, level: ConsistencyLevel = ONE, retry: Boolean = false)(
    implicit ec: ExecutionContext
  ): Future[Box[String]] = {
    val stmt = getLastInPath.bind(path).setConsistencyLevel(level)

    executeAsync(stmt, s"'${getLastInPath.getQueryString}'.bind($path)")
      .flatMap((extractLast _).andThen {
        case None if level == ONE => readPathUUIDA(path, QUORUM, retry = true)
        case os =>
          if (os.isDefined && retry) logger.warn(s"The path $path is only available in QUORUM")
          Future.successful(Box(os))
      })
      .recover(recoverAsBoxedFailure)
  }

  /** ********************************************************************************/
  //TODO: remove this method, only use the async version of it. BTW it is truly async, not a bluff.
  @deprecated("use `historyAsync` instead", "1.4.x")
  def history(path: String, limit: Int): Vector[(Long, String)] = {

    def executeRetry(statement: Statement, retry: Int = 4, timeToWait: Long = 1000): ResultSet = {
      var res: ResultSet = null
      var count: Int = 0
      while (count != retry) {
        try {
          res = storageDao.getSession.execute(statement)
          // if did not got exception stop while
          count = retry
        } catch {
          case e: Throwable =>
            count += 1
            logger.error(s"in executeRetry $statement retry $count", e)
            Thread.sleep(timeToWait * (1 >> (count + 1)))
            if (count == retry)
              throw e
        }
      }
      res
    }

    val q = s"select last_modified,uuid  from path where path = '${path.replaceAll("'", "''")}' LIMIT $limit"
    val res: ResultSet = executeRetry(new SimpleStatement(q))
    val it = res.iterator()
    val b = Vector.newBuilder[(Long, String)]

    while (it.hasNext) {
      val r: Row = it.next()
      if (r ne null) {
        val d = r.getTimestamp("last_modified")
        val u = r.getString("uuid")
        b += (d.getTime -> u)
      }
    }

    b.result()
  }

  def historyReactive(path: String, level: ConsistencyLevel): Source[(Long, String), NotUsed] = {
    val stmt = getAllHistory.bind(path).setConsistencyLevel(level)
    CassandraSource(stmt)(storageDao.getSession).map { r: Row =>
      r.getTimestamp("last_modified").getTime -> r.getString("uuid")
    }
  }

  def lastVersion(path: String, level: ConsistencyLevel): Future[Option[(Long, String)]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val stmt = getLastInPath.bind(path.replaceAll("'", "''")).setConsistencyLevel(level)
    executeAsync(stmt, s"'${getLastInPath.getQueryString}'.bind($path)").map { res =>
      val it = res.iterator()
      val b = Vector.newBuilder[(Long, String)]
      while (it.hasNext) {
        val r: Row = it.next()
        if (r ne null) {
          val d = r.getTimestamp("last_modified") // TODO this Vector.newBuilder should be a private def!!!
          val u = r.getString("uuid")
          b += (d.getTime -> u)
        }
      }

      b.result().headOption
    }
  }

  override def historyNeighbourhood(path: String, timestamp: Long, desc: Boolean, limit: Int, level: ConsistencyLevel): Future[Vector[(Long, String)]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val getHistoryNeighbourhood = if(desc) getHistoryNeighbourhoodDesc else getHistoryNeighbourhoodAsc
    val stmt = getHistoryNeighbourhood.bind(path.replaceAll("'", "''"), new java.util.Date(timestamp), Int.box(limit)).setConsistencyLevel(level)
    executeAsync(stmt, s"'${getHistoryNeighbourhood.getQueryString}'.bind($path,$timestamp,$limit)").map { res =>
      val it = res.iterator()
      val b = Vector.newBuilder[(Long, String)]
      while (it.hasNext) {
        val r: Row = it.next()
        if (r ne null) {
          val d = r.getTimestamp("last_modified")
          val u = r.getString("uuid")
          b += (d.getTime -> u)
        }
      }

      b.result()
    }
  }

  def historyAsync(path: String, limit: Int): Future[Vector[(Long, String)]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val stmt = getHistory.bind(path.replaceAll("'", "''"), Int.box(limit)).setConsistencyLevel(ConsistencyLevel.QUORUM)
    executeAsync(stmt, s"'${getHistory.getQueryString}'.bind($path)").map { res =>
      val it = res.iterator()
      val b = Vector.newBuilder[(Long, String)]
      while (it.hasNext) {
        val r: Row = it.next()
        if (r ne null) {
          val d = r.getTimestamp("last_modified")
          val u = r.getString("uuid")
          b += (d.getTime -> u)
        }
      }

      b.result()
    }
  }

  def exists(paths: Set[String], level: ConsistencyLevel = ONE): Map[String, Option[String]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    if (paths.isEmpty)
      Map.empty
    else {
      val res = travector(paths) { path =>
        readPathAndLast(path, level)
      }
      val r = Await.result(res, 10000.millis)
      val data = r.collect { case FullBox(tup) => tup }.toMap
      val v2 = paths.map { p =>
        p -> data.get(p)
      }
      v2.toMap
    }
  }

  def purgeHistorical(infoton: Infoton,
                      isOnlyVersion: Boolean = false,
                      level: ConsistencyLevel = QUORUM): Future[Unit] = {
    purgeHistorical(infoton.systemFields.path, infoton.uuid, infoton.systemFields.lastModified.getMillis, isOnlyVersion, level)
  }

  def purgeHistorical(path: String,
                      uuid: String,
                      lastModified: Long,
                      isOnlyVersion: Boolean,
                      level: ConsistencyLevel): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val pHistoryEntry = {
      val stmt = purgeHistoryEntry.bind(path, new java.util.Date(lastModified)).setConsistencyLevel(level)
      executeAsync(stmt, s"'${purgeHistoryEntry.getQueryString}'.bind($path, $lastModified)")
        .map(rs => if (!rs.wasApplied()) ???)
    }

    def pAllHistory = {
      val stmt = purgeAllHistory.bind(path).setConsistencyLevel(level)
      executeAsync(stmt, s"'${purgeAllHistory.getQueryString}'.bind($path)").map(rs => if (!rs.wasApplied()) ???)
    }

    def pInfoton = {
      val stmt = purgeInfotonByUuid.bind(uuid).setConsistencyLevel(level)
      executeAsync(stmt, s"'${purgeInfotonByUuid.getQueryString}'.bind($uuid)").map { rs =>
        dataCahce.invalidate(uuid)
        if (!rs.wasApplied()) ???
      }
    }

    (if (isOnlyVersion) pAllHistory else pHistoryEntry).flatMap(_ => pInfoton)
  }

  def purgeUuid(path: String,
                uuid: String,
                lastModified: Long,
                isOnlyVersion: Boolean,
                level: ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    def pAllHistory = {
      val stmt = purgeAllHistory.bind(path).setConsistencyLevel(level)
      executeAsync(stmt, s"'${purgeAllHistory.getQueryString}'.bind($path)").map(rs => if (!rs.wasApplied()) ???)
    }

    def pHistoryEntry = {
      val stmt = purgeHistoryEntry.bind(new java.util.Date(lastModified), path).setConsistencyLevel(level)
      executeAsync(stmt, s"'${purgeHistoryEntry.getQueryString}'.bind($lastModified, $path)")
        .map(rs => if (!rs.wasApplied()) ???)
    }

    purgeFromInfotonsOnly(uuid, level).flatMap { _ =>
      if (isOnlyVersion) pAllHistory else pHistoryEntry
    }
  }

  def purgeFromInfotonsOnly(uuid: String, level: ConsistencyLevel = QUORUM) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val stmt = purgeInfotonByUuid.bind(uuid).setConsistencyLevel(level)
    executeAsync(stmt, s"'${purgeInfotonByUuid.getQueryString}'.bind($uuid)").map { rs =>
      dataCahce.invalidate(uuid)
      if (!rs.wasApplied()) ???
    }
  }

  def purgeFromPathOnly(path: String, lastModified: Long, level: ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val stmt = purgeHistoryEntry.bind(new java.util.Date(lastModified), path).setConsistencyLevel(level)
    executeAsync(stmt, s"'${purgeHistoryEntry.getQueryString}'.bind($lastModified, $path)")
      .map(rs => if (!rs.wasApplied()) ???)
  }

  def purgePathOnly(path: String, level: ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val stmt = purgeAllHistory.bind(path).setConsistencyLevel(level)
    executeAsync(stmt, s"'${purgeAllHistory.getQueryString}'.bind($path)").map(rs => if (!rs.wasApplied()) ???)
  }

  override def purgeAll(): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    Future
      .sequence(Seq("path", "infoton", "zstore").map { table =>
        val truncate = s"TRUNCATE TABLE data2.$table"
        executeAsync(new SimpleStatement(truncate), truncate)
      })
      .map(_ => ())
  }

  def fixPath(path: String,
              last: (DateTime, String),
              history: Seq[(DateTime, String)],
              level: ConsistencyLevel = QUORUM): Future[Seq[Infoton]] = ???

  /*{
    val uuids = history.map(_._2).toVector
    val existingInfotonsFut = this.readUUIDSAsync(uuids)

    existingInfotonsFut.flatMap { existingInfopts =>
      val existingInfotons = existingInfopts.collect{ case Some(i) => i}
      if (existingInfotons.length < history.length) {
        logger.warn(s"FixCAS for $path: There are only ${existingInfotons.length} in `infoton`, but ES found ${history.length} versions. Cannot fix!")
        Future.failed(new Exception("missing data"))
      } else {
        import scala.collection.JavaConverters._

        val historyMap = history.map(e => new java.util.Date(e._1.getMillis) -> e._2).toMap.asJava
        val lastUuid = last._2
        val bsSetPathLastAndHistory = new BoundStatement(setPathLastAndHistory)
        bsSetPathLastAndHistory.setConsistencyLevel(level)
        bsSetPathLastAndHistory.bind(path, historyMap, lastUuid)
        executeAsync(bsSetPathLastAndHistory).map { _ => existingInfotons }
      }
    }
  }*/

  def rowToRecord(r: Row): String = {
    val u = r.getString("uuid")
    val q = r.getString("quad")
    val f = r.getString("field")
    val v = org.apache.commons.lang3.StringEscapeUtils.escapeCsv(r.getString("value"))
    val d =
      Option(r.getBytes("data")).fold("null")(x => cmwell.util.string.Base64.encodeBase64String(Bytes.getArray(x)))
    s"$u,$q,$f,$v,$d"
  }

  def getRawRow(uuid: String, level: ConsistencyLevel)(implicit ec: ExecutionContext): Future[String] = {
    val stmt = getInfotonUUID.bind(uuid).setConsistencyLevel(level)
    cmwell.util.concurrent.retry(5, delayOnError, 2)(executeAsyncInternal(stmt))(ec).map { result =>
      val sb = StringBuilder.newBuilder
      sb.append("uuid,quad,field,value,data\n")
      while (!result.isExhausted()) {
        val r: Row = result.one()
        sb.append(s"${rowToRecord(r)}\n")
      }
      sb.result()
    }
  }

  def getReactiveRawRow(uuid: String, level: ConsistencyLevel): Source[String, NotUsed] = {
    val stmt = getInfotonUUID.bind(uuid).setConsistencyLevel(level)
    val s1 = Source.single("uuid,quad,field,value,data")
    val s2 = CassandraSource(stmt)(storageDao.getSession).map(rowToRecord)
    s1.concat(s2)
  }
}
