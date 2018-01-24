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


package cmwell.irw

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.Source
import cmwell.domain._
import cmwell.driver.Dao
import cmwell.util.{Box,FullBox,EmptyBox,BoxedFailure}
import cmwell.util.collections.partitionWith
import cmwell.util.concurrent.travector
import com.datastax.driver.core._
import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.{FutureCallback, Futures, MoreExecutors}
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}
import cmwell.common.metrics.WithMetrics
import cmwell.zstore.ZStore

import scala.concurrent._
import duration._


/**
 * IRWService (infoton read write service) provide a service for read and writing infotons
 */
object IRWService {

  def apply(storageDao : Dao) = new IRWServiceNativeImpl(storageDao)
  def apply(storageDao : Dao, disableReadCache: Boolean) = new  IRWServiceNativeImpl(storageDao, disableReadCache = disableReadCache)
  def apply(storageDao : Dao, maxReadSize : Int,disableReadCache : Boolean, readCacheDuration: FiniteDuration) = new IRWServiceNativeImpl(storageDao, maxReadSize , disableReadCache, readCacheDuration)

  def newIRW(storageDao: Dao) = new IRWServiceNativeImpl2(storageDao)
  def newIRW(storageDao: Dao, disableReadCache: Boolean) = new  IRWServiceNativeImpl2(storageDao, disableReadCache = disableReadCache)
  def newIRW(storageDao: Dao, disableReadCache: Boolean, casTimeout: FiniteDuration) = new  IRWServiceNativeImpl2(storageDao, disableReadCache = disableReadCache)(casTimeout)
  def newIRW(storageDao: Dao, maxReadSize: Int, disableReadCache: Boolean, readCacheDuration: FiniteDuration) = new IRWServiceNativeImpl2(storageDao, maxReadSize , disableReadCache, readCacheDuration)
}

trait IRWService {
  /*********************************** New API ************************************************************************/

  /**
   *
   * @param path
   * @param level
   * @return
   */
  def readPathAsync (path : String , level : ConsistencyLevel = ONE )(implicit ec: ExecutionContext) : Future[Box[Infoton]]

  /**
   *
   * @param path
   * @param level
   * @return
   */
  def readPathsAsync (path : Seq[String] , level : ConsistencyLevel = ONE )(implicit ec: ExecutionContext) : Future[Seq[Box[Infoton]]]

  /**
   *
   * @param path
   * @param level
   * @return
   */
  def readPathUUIDA (path : String , level : ConsistencyLevel = ONE, retry : Boolean = false )(implicit ec: ExecutionContext) : Future[Box[String]]

  /**
   *
   * @param uuid
   * @param level
   * @return
   */
  def readUUIDAsync (uuid : String , level : ConsistencyLevel = ONE, dontFetchPayload: Boolean = false )(implicit ec: ExecutionContext) : Future[Box[Infoton]]

  /**
   *
   * @param uuid
   * @param level
   * @return
   */
  def readUUIDSAsync( uuid : Seq[String], level : ConsistencyLevel = ONE )(implicit ec: ExecutionContext) : Future[Seq[Box[Infoton]]]


  /**
   *
   * @param infoton
   * @param level
   * @return
   */
  def writeAsyncDataOnly(infoton: Infoton, level: ConsistencyLevel = QUORUM)(implicit ec: ExecutionContext): Future[Infoton]

  /**
   *
   * @param infoton
   * @param level
   * @return
   */
  def writeAsync(infoton : Infoton, level: ConsistencyLevel = QUORUM, skipSetPathLast: Boolean = false)(implicit ec: ExecutionContext): Future[Infoton]

  /**
   *
   * @param infoton
   * @param level
   * @return
   */
  def writeSeqAsync(infoton : Seq[Infoton], level: ConsistencyLevel = QUORUM, skipSetPathLast: Boolean = false)(implicit ec: ExecutionContext): Future[Seq[Infoton]]

  /**
   *
   * @param uuid
   * @param indexTime
   * @return
   */
  def addIndexTimeToUuid(uuid: String, indexTime: Long, level : ConsistencyLevel = QUORUM ): Future[Unit]

  /**
   *
   * @param uuid
   * @param dc
   * @return
   */
  def addDcToUuid(uuid: String, dc: String, level : ConsistencyLevel = QUORUM ): Future[Unit]

  /**
   *
   * @param infoton
   * @param level
   * @return
   */
  def setPathHistory(infoton : Infoton, level : ConsistencyLevel = QUORUM): Future[Infoton]

  /**
   *
   * @param infoton
   * @param level
   * @return
   */
  def setPathLast(infoton : Infoton, level : ConsistencyLevel = QUORUM): Future[Unit]

  def setPathLast(path: String, lastModified: java.util.Date, uuid: String, level: ConsistencyLevel): Future[Unit]

  /********************************************************************************************************************/

  /**
   * Retrieve list of infoton uuid's for a given path
   *
   * @param path
   * @return list of uuid's
   */
  @deprecated("use `historyAsync` instead","1.4.x")
  def history(path: String, limit: Int) : Vector[(Long , String )]

  def historyReactive(path: String, level : ConsistencyLevel = ONE): Source[(Long, String),NotUsed]

  def historyAsync(path: String, limit: Int): Future[Vector[(Long, String)]]

  /**
   * Check if given paths exists
    *
    * @param paths
   * @return a vector of all paths
   */
  def exists(paths : Set[String] , level : ConsistencyLevel = ONE) : Map[String , Option[String]]

  // will purge a specific historical version
  def purgeHistorical(infoton: Infoton, isOnlyVersion: Boolean, level: ConsistencyLevel): Future[Unit]

  def purgeHistorical(path: String, uuid: String, lastModified: Long, isOnlyVersion: Boolean, level: ConsistencyLevel): Future[Unit]

  def purgeUuid(path: String, uuid: String, lastModified: Long, isOnlyVersion: Boolean, level: ConsistencyLevel = QUORUM): Future[Unit]

  def purgeFromInfotonsOnly(uuid: String, level: ConsistencyLevel = QUORUM): Future[Unit]

  def purgeFromPathOnly(path: String, lastModified: Long, level: ConsistencyLevel = QUORUM): Future[Unit]

  def purgePathOnly(path: String, level: ConsistencyLevel = QUORUM): Future[Unit]

  // Used by tests only!!! will delete everything
  def purgeAll():Future[Unit]

  // adds missing row in `path` column-family, in case all uuids are present in `infoton` column-family.
  @deprecated("wrong usage in irw2, and seems unused anyway...","1.5.x")
  def fixPath(path: String, last: (DateTime, String), history: Seq[(DateTime, String)], level: ConsistencyLevel = QUORUM): Future[Seq[Infoton]]

  def daoProxy : Dao

  def getRawRow(uuid: String, level: ConsistencyLevel = QUORUM)(implicit ec: ExecutionContext): Future[String]

  protected def futureBox[A](a: A) = Future.successful(Box.wrap(a))

  val recoverAsBoxedFailure: PartialFunction[Throwable,Box[Nothing]] = { case e: Throwable => BoxedFailure(e) }
  val futureEmptyBox: Future[Box[Infoton]] = Future.successful(Box.empty[Infoton])
}

class IRWServiceNativeImpl(storageDao: Dao, maxReadSize: Int = 25, disableReadCache: Boolean = false, readCacheDuration: FiniteDuration = 120.seconds) extends IRWService with LazyLogging with WithMetrics {

  val delayOnError = 40.millis
  val cacheSize = 50000L

  val dataCahce : Cache[String, Infoton] = CacheBuilder.newBuilder().
    maximumSize(cacheSize).
    expireAfterWrite(readCacheDuration.toMillis, TimeUnit.MILLISECONDS).
    build[String , Infoton]()
  val getFromCacheFunc: String => Option[Infoton] = { uuid =>
    Option(dataCahce.getIfPresent(uuid))
  }

  val zStore = ZStore(storageDao)

  val infotonsIRWReadDataTimer  = metrics.meter("IRW Read Data Time")
  val infotonsIRWWriteDataTimer = metrics.meter("IRW Write Data Time")

  val infotonsIRWReadPathTimer  = metrics.meter("IRW Read Path Time")
  val infotonsIRWWritePathTimer = metrics.meter("IRW Write Path Time")

  // here we are prepering all statments to use

  lazy val getInfotonUUID : PreparedStatement = storageDao.getSession.prepare("SELECT * FROM infoton WHERE uuid = ?")
  lazy val getLastInPath : PreparedStatement = storageDao.getSession.prepare ("SELECT last FROM path WHERE path = ?")

  lazy val setInfotonData : PreparedStatement = storageDao.getSession.prepare("INSERT INTO infoton (uuid,fields) VALUES (?,?)")
  lazy val setDc : PreparedStatement = storageDao.getSession.prepare("UPDATE infoton SET fields['$dc'] = ? where uuid = ?")
  lazy val setIndexTime : PreparedStatement = storageDao.getSession.prepare("UPDATE infoton SET fields['$indexTime'] = ? where uuid = ?")
  lazy val setPathLast : PreparedStatement = storageDao.getSession.prepare("INSERT INTO path (path,last) VALUES (?,?)")
  lazy val setHistory : PreparedStatement = storageDao.getSession.prepare("UPDATE path SET history[?] = ? where path = ?")
  lazy val setPathLastAndHistory : PreparedStatement = storageDao.getSession.prepare("INSERT INTO path (path,history,last) VALUES (?,?,?)")

  lazy val getLastAndPath : PreparedStatement = storageDao.getSession.prepare("SELECT path , last FROM path WHERE path = ?")

  lazy val purgeInfotonByUuid: PreparedStatement = storageDao.getSession.prepare("DELETE FROM infoton WHERE uuid = ?")
  lazy val purgeHistoryEntry: PreparedStatement = storageDao.getSession.prepare("DELETE history[?] FROM path WHERE path = ?")
  lazy val purgeAllHistory: PreparedStatement = storageDao.getSession.prepare("DELETE FROM path WHERE path = ?")

//  @deprecated("use `executeRetryAsync` instead","1.4.x")
  private def executeRetry(statement : Statement , retry : Int = 4 , timeToWait : Long = 1000) : ResultSet = {
    var res: ResultSet = null
    var count : Int = 0
    while ( count != retry ) {
      try {
        res = storageDao.getSession.execute(statement)
        // if did not got exception stop while
        count = retry
      } catch {
        case e: Throwable =>
          count += 1
          logger.error (s"in executeRetry ${statement} retry ${count}" , e)
          Thread.sleep(timeToWait * (1>> (count + 1)) )
          if ( count == retry)
            throw e
      }
    }
    res
  }

  private def executeRetryAsync(statement: Statement, retries: Int = 4, timeToWaitMs: Long = 1000): Future[ResultSet] = {
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    require(retries > 0, "must be positive")
    require(retries < 10,"exponential growth... you don't really want to go there...")
    cmwell.util.concurrent.retryWithDelays(Vector.iterate(1.seconds,retries)(_*2):_*){
      executeAsyncInternal(statement)
    }
  }

  def daoProxy : Dao = storageDao

  private def convert(result : ResultSet) : Box[Infoton] = {
    val it = result.iterator()
    var oi: Box[Infoton] = EmptyBox
    while (it.hasNext) {
      val r: Row = it.next()
      val jm = r.getMap("fields", classOf[String], classOf[String])
      import scala.collection.JavaConversions._
      val m = Map.empty[String, String] ++ jm
      val buf = new mutable.ListBuffer[(String, Array[Byte])]()
      m map {
        case (k, v) => buf += ((k, v.getBytes("UTF-8")))
      }
      try {
        val i = InfotonSerializer.deserialize(buf)
        oi = FullBox(i)
      }
      catch {
        case t: Throwable => {
          val msg = s"failed deserializing row: [$r]"/*, timestamp: Long*/
          logger.error(msg,t)
          BoxedFailure(new RuntimeException(msg,t))
        }
      }
    }
    oi
  }

  def readUUIDSAsync( uuids : Seq[String], level : ConsistencyLevel = ONE )(implicit ec: ExecutionContext) : Future[Seq[Box[Infoton]]] = {

    def getUuidsFromCas(us: Seq[String]) = {
      travector(us) {
        u => readUUIDAsync(u, level)(ec).recover(recoverAsBoxedFailure)
      }
    }

    if(!disableReadCache) {
      val all = uuids.map(uuid => uuid -> getFromCacheFunc(uuid))
      if(all.forall(_._2.isDefined)) Future.successful(all.map{ case (_,i) => Box.wrap(i.get)})
      else {
        val (unCachedUuids,cachedInfotons) = partitionWith(all){
          case (_,Some(i)) => Right(Box.wrap(i))
          case (uuid,None) => Left(uuid)
        }
        getUuidsFromCas(unCachedUuids).map(_ ++ cachedInfotons)
      }
    }
    else getUuidsFromCas(uuids)
  }

  def readUUIDAsync (uuid : String, level : ConsistencyLevel = ONE, dontFetchPayload: Boolean = false)(implicit ec: ExecutionContext) : Future[Box[Infoton]] = {

    def getFromCas(lvl: ConsistencyLevel, retry: Boolean = false) : Future[Box[Infoton]] = {
      val boundStatement = getInfotonUUID.bind(uuid)
      boundStatement.setConsistencyLevel(level)

      executeAsync(boundStatement).flatMap(convert _ andThen {
        case b@(EmptyBox | BoxedFailure(_)) if !retry => {
          val msg = s"retrying getting [$uuid] after failure"
          if(b.isFailure) logger.warn(msg,b.getFailure)
          else logger.warn(msg)
          cmwell.util.concurrent.SimpleScheduler.scheduleFuture(delayOnError)(getFromCas(QUORUM, retry = true))
        }
        case oi => {
          if (oi.isDefined) {
            if (level == QUORUM && retry) {
              logger.warn(s"The uuid $uuid is only available in QUORUM")
            }
            if(oi.get.uuid != uuid) {
              logger.error(s"The infoton [${oi.get.path}] retrieved with different uuid [${oi.get.uuid}] from requested uuid [${uuid}]")
            }
          }
          else if(oi.isFailure) {
            logger.error(s"failed to retrieve [$uuid]",oi.getFailure)
          }

          val fullOiFut: Future[Box[Infoton]] = oi match {
            case FullBox(infoton) => (infoton match {
              case fi: FileInfoton if fi.hasDataPointer => fi.populateDataFromPointerBy(zStore.get)(ec)
              case i => Future.successful(i)
            }).map(i => Box.wrap(i)).recover {
              case e: Throwable => BoxedFailure(e)
            }
            case z => Future.successful(z)
          }

          fullOiFut
        }
      })
    }

    if (disableReadCache) getFromCas(level)
    else getFromCacheFunc(uuid).fold(getFromCas(level).andThen {
      case Success(FullBox(i)) if i.uuid == uuid => dataCahce.put(uuid,i)
    })(futureBox)
  }

  private def executeAsyncInternal(statmentToExec : Statement) : Future[ResultSet] = {
    val f : ResultSetFuture = storageDao.getSession.executeAsync(statmentToExec)
    val p = Promise[ResultSet]()
    Futures.addCallback(f,
      new FutureCallback[ResultSet]() {
        def onSuccess(result : ResultSet ) : Unit = {
          p.success(result)
        }
        def onFailure(t : Throwable ) : Unit = {
          p.failure(t)
        }
      },
      MoreExecutors.directExecutor()
    )
    p.future
  }

  def executeAsync(statmentToExec: Statement, retries: Int = 5, delay: FiniteDuration = delayOnError)(implicit ec: ExecutionContext): Future[ResultSet] =
    cmwell.util.concurrent.retryWithDelays(Vector.iterate(delayOnError,retries)(_*2):_*){
      executeAsyncInternal(statmentToExec)
    }(ec)

  def setPathHistory(infoton : Infoton, level : ConsistencyLevel = QUORUM): Future[Infoton] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val boundStatementSetHistory = new BoundStatement(setHistory)
    boundStatementSetHistory.setConsistencyLevel(level)
    val d = new java.util.Date(infoton.lastModified.getMillis)
    boundStatementSetHistory.bind(d,infoton.uuid,infoton.path)
    executeAsync(boundStatementSetHistory).map{ rs =>
      logger.trace(s"resultSet from setHisory: $rs")
      infoton
    }
  }

  def setPathLast(infoton : Infoton, level : ConsistencyLevel = QUORUM): Future[Unit] =
    setPathLast(infoton.path, null, infoton.uuid, level)

  def setPathLast(path: String, lastModified: java.util.Date, uuid: String, level : ConsistencyLevel): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val boundStatementSetHistory = new BoundStatement(setPathLast)
    boundStatementSetHistory.setConsistencyLevel(level)
    boundStatementSetHistory.bind(path , uuid)
    executeAsync(boundStatementSetHistory).map{ rs =>
      logger.trace(s"resultSet from setPathLast: $rs")
    }
  }

  def writeAsyncDataOnly(infoton: Infoton, level: ConsistencyLevel = QUORUM)(implicit ec: ExecutionContext): Future[Infoton] = {
    val fields = Map.empty ++ InfotonSerializer.serialize(infoton) map { case (k,v) => (k , new String(v,"UTF-8"))}
    val boundStatementSetInfotonData = new BoundStatement(setInfotonData)
    boundStatementSetInfotonData.setConsistencyLevel(level)
    import collection.JavaConversions._
    boundStatementSetInfotonData.bind(infoton.uuid , mapAsJavaMap(fields) )
    executeAsync(boundStatementSetInfotonData).map { rs =>
      if(!disableReadCache) {
        dataCahce.put(infoton.uuid, infoton)
      }
      infoton
    }
  }

  def writeSeqAsync(infoton: Seq[Infoton], level: ConsistencyLevel = QUORUM, skipSetPathLast: Boolean = false)(implicit ec: ExecutionContext): Future[Seq[Infoton]] =
    travector(infoton)(i => writeAsync(i,level,skipSetPathLast))

  def writeAsync(infoton: Infoton, level: ConsistencyLevel = QUORUM, skipSetPathLast: Boolean = false)(implicit ec: ExecutionContext): Future[Infoton] = {
    val fields = Map.empty ++ InfotonSerializer.serialize(infoton) map { case (k,v) => (k , new String(v,"UTF-8"))}
    val boundStatementSetInfotonData = new BoundStatement(setInfotonData)
    boundStatementSetInfotonData.setConsistencyLevel(level)
    import collection.JavaConversions._
    boundStatementSetInfotonData.bind(infoton.uuid , mapAsJavaMap(fields) )
    executeAsync(boundStatementSetInfotonData).flatMap{ rs =>
      logger.trace(s"resultSet from writeAsync: $rs")
      if(!disableReadCache) {
        dataCahce.put(infoton.uuid, infoton)
      }
      val f = setPathHistory(infoton,level)
      if(skipSetPathLast) f
      else setPathLast(infoton,level).flatMap(_ => f)
    }
  }

  def addIndexTimeToUuid(uuid: String, indexTime: Long, level : ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    if(!disableReadCache) {
      Option(dataCahce.getIfPresent(uuid)).foreach { i =>
        dataCahce.put(uuid, addIndexTime(i, Some(indexTime)))
      }
    }
    val boundStatementSetInfotonData = new BoundStatement(setIndexTime)
    boundStatementSetInfotonData.setConsistencyLevel(level)
    boundStatementSetInfotonData.bind( indexTime.toString , uuid)
    executeAsync(boundStatementSetInfotonData).map(rs => if(!rs.wasApplied()) ???)
  }

  def addDcToUuid(uuid: String, dc: String, level : ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    if(!disableReadCache) {
      Option(dataCahce.getIfPresent(uuid)).foreach { i =>
        dataCahce.put(uuid, addDc(i, dc))
      }
    }
    val boundStatementSetInfotonData = new BoundStatement(setDc)
    boundStatementSetInfotonData.setConsistencyLevel(level)
    boundStatementSetInfotonData.bind(dc, uuid)
    executeAsync(boundStatementSetInfotonData).map(rs => if(!rs.wasApplied()) ???)
  }

  private def extractLast(result : ResultSet) : Option[String] = {
    var s : Option[String] = None
    val it = result.iterator()
    while (it.hasNext) {
      val r: Row = it.next()
      val last = r.getString("last")
      s = Some(last)
    }
    s
  }

  private def extractLastAndPath(result : ResultSet) : Option[(String,String)] = {
    var s : Option[(String,String)] = None
    val it = result.iterator()
    while (it.hasNext) {
      val r: Row = it.next()
      val last = r.getString("last")
      val path = r.getString("path")
      s = Some( (path, last) )
    }
    s
  }

  private def readPathAndLast (path: String, level: ConsistencyLevel = ONE, retry: Boolean = false)(implicit ec: ExecutionContext) : Future[Option[(String,String)]] = {
    val p = Promise[Option[(String,String)]]()
    val boundStatement = new BoundStatement(getLastAndPath)
    boundStatement.bind(path)
    boundStatement.setConsistencyLevel(level)
    executeAsync(boundStatement).onComplete{
      case Success(result) =>
        val convertResponseFuture = Future {
          extractLastAndPath(result)
        }.map {
          case None if level == ONE => readPathAndLast(path, QUORUM, retry = true)
          case oss =>
            if(oss.isDefined && level == QUORUM && retry) logger.warn(s"The path $path is only available with QUORUM")
            Future.successful(oss)
        }.flatMap(identity)
        convertResponseFuture.andThen {
          case Success(res) =>
            p.success(res)
          case Failure(t) =>
            p.failure(t)
        }

      case Failure(t) =>
        p.failure(t)
    }
    p.future
  }

  def readPathsAsync (paths : Seq[String] , level : ConsistencyLevel = ONE )(implicit ec: ExecutionContext) : Future[Seq[Box[Infoton]]] = {
    travector(paths) {
      p => readPathAsync(p, level).recover(recoverAsBoxedFailure)
    }
  }

  /**
   * Reads Async infoton according to path
    *
    * @param path The path of the infoton
   * @param level The level of consistency
   * @return a future with option of infoton if the infoton does not exists return Future with Nobe else Future with Infoton
   */
  def readPathAsync (path : String , level : ConsistencyLevel = ONE)(implicit ec: ExecutionContext) : Future[Box[Infoton]] = {

    // first lets try to read path the get uuid
    val uuidFuture = readPathUUIDA(path,level)

    uuidFuture.flatMap{
      case b: Box[String] if b.isEmpty => Future.successful(b.asInstanceOf[Box[Infoton]])
      case FullBox(uuid) => readUUIDAsync(uuid, level)
    }.recover(recoverAsBoxedFailure)
  }

  def readPathUUIDA (path: String, level: ConsistencyLevel = ONE, retry: Boolean = false)(implicit ec: ExecutionContext) : Future[Box[String]] = {
    val boundStatement = getLastInPath.bind(path)
    boundStatement.setConsistencyLevel(level)

    executeAsync(boundStatement).map(extractLast).flatMap {
      case None if level == ONE => readPathUUIDA(path, QUORUM, retry = true)
      case os =>
        if(os.isDefined && level == QUORUM && retry) logger.warn(s"The path $path is only available in QUORUM")
        Future.successful(Box(os))
    }.recover(recoverAsBoxedFailure)
  }

  //TODO: remove this method, only use the async version of it. BTW it is truly async, not a bluff.
  @deprecated("use `historyAsync` instead","1.4.x")
  def history(path : String, limit: Int) : Vector[(Long , String )] = {
    val q = "select history from path where path = '%s'".format(path.replaceAll("'", "''"))
    val res : ResultSet = executeRetry(new SimpleStatement(q))
    val it = res.iterator()
    //    while (it.hasNext ) {
    val r : Row = it.next()
    if ( r != null ) {
      val jm = r.getMap("history", classOf[java.util.Date], classOf[String])
      import scala.collection.JavaConversions._
      val m = Map.empty[java.util.Date, String] ++ jm
      val d = m map { case (k, v) => (k.getTime, v)}
      //    }
      Vector.empty ++ d.toVector
    } else {
      Vector.empty
    }
  }

  //FIXME: Thats an ugly bluff...
  //due to old schema being stupid, i.e:
  //everything in a single row with a map collection,
  //we need to resort to this ugly patch.
  //we need to dump the old IRW & FTS ASAP!
  def historyReactive(path: String, level: ConsistencyLevel): Source[(Long, String),NotUsed] = {
    Source
      .fromFuture(historyAsync(path,Int.MaxValue))
      .mapConcat(_.sortBy(_._1))
  }

  def historyAsync(path: String, limit: Int): Future[Vector[(Long, String)]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val q = "select history from path where path = '%s' limit %d".format(path.replaceAll("'", "''"),limit)
    executeAsync(new BoundStatement(storageDao.getSession.prepare(q))).map { res =>
      val it = res.iterator()
      val r: Row = it.next()
      if (r == null) Vector.empty
      else {
        val jm = r.getMap("history", classOf[java.util.Date], classOf[String])
        import scala.collection.JavaConversions._
        val m = Map.empty[java.util.Date, String] ++ jm
        val d = m map { case (k, v) => (k.getTime, v) }
        Vector.empty ++ d.toVector
      }
    }
  }

  def exists(paths : Set[String], level : ConsistencyLevel = ONE) : Map[String , Option[String]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    if ( paths.isEmpty )
      Map.empty
    else {
      val res = travector(paths) { path =>
        readPathAndLast(path,level)
      }
      val r = Await.result(res,10000.millis)
      val data = r.collect{ case Some(tup) => tup}.toMap
      val v2 = paths map {
        p => p -> data.get(p)
      }
      v2.toMap
    }
  }

  def purgeHistorical(infoton: Infoton, isOnlyVersion: Boolean, level: ConsistencyLevel): Future[Unit] = {
    purgeHistorical(infoton.path, infoton.uuid, infoton.lastModified.getMillis, isOnlyVersion, level)
  }

  def purgeHistorical(path: String, uuid: String, lastModified: Long, isOnlyVersion: Boolean, level: ConsistencyLevel): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val pHistoryEntry = {
      val bs = new BoundStatement(purgeHistoryEntry)
      bs.setConsistencyLevel(level)
      bs.bind(new java.util.Date(lastModified), path)
      executeAsync(bs).map(rs => if(!rs.wasApplied()) ???)
    }

    def pAllHistory = {
      val bs = new BoundStatement(purgeAllHistory)
      bs.setConsistencyLevel(level)
      bs.bind(path)
      executeAsync(bs).map(rs => if(!rs.wasApplied()) ???)
    }

    def pInfoton = {
      val bs = new BoundStatement(purgeInfotonByUuid)
      bs.setConsistencyLevel(level)
      bs.bind(uuid)
      executeAsync(bs).map{ rs =>
        dataCahce.invalidate(uuid)
        if(!rs.wasApplied()) ???
      }
    }

    (if(isOnlyVersion) pAllHistory else pHistoryEntry).flatMap(_=>pInfoton)
  }

  def purgeUuid(path: String, uuid: String, lastModified: Long, isOnlyVersion: Boolean, level: ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    def pAllHistory = {
      val bs = new BoundStatement(purgeAllHistory)
      bs.setConsistencyLevel(level)
      bs.bind(path)
      executeAsync(bs).map(rs => if(!rs.wasApplied()) ???)
    }

    def pHistoryEntry = {
      val bs = new BoundStatement(purgeHistoryEntry)
      bs.setConsistencyLevel(level)
      bs.bind(new java.util.Date(lastModified), path)
      executeAsync(bs).map(rs => if(!rs.wasApplied()) ???)
    }

    purgeFromInfotonsOnly(uuid, level).flatMap {_ => if(isOnlyVersion) pAllHistory else pHistoryEntry }
  }

  def purgeFromInfotonsOnly(uuid: String, level: ConsistencyLevel = QUORUM) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val bs = new BoundStatement(purgeInfotonByUuid)
    bs.setConsistencyLevel(level)
    bs.bind(uuid)
    executeAsync(bs).map { rs =>
      dataCahce.invalidate(uuid)
      if (!rs.wasApplied()) ???
    }
  }

  def purgeFromPathOnly(path: String, lastModified: Long, level: ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val bs = new BoundStatement(purgeHistoryEntry)
    bs.setConsistencyLevel(level)
    bs.bind(new java.util.Date(lastModified), path)
    executeAsync(bs).map(rs => if(!rs.wasApplied()) ???)
  }

  override def purgeAll(): Future[Unit] = ???

  def purgePathOnly(path: String, level: ConsistencyLevel = QUORUM): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val bs = new BoundStatement(purgeAllHistory)
    bs.setConsistencyLevel(level)
    bs.bind(path)
    executeAsync(bs).map(rs => if(!rs.wasApplied()) ???)
  }


  def fixPath(path: String, last: (DateTime, String), history: Seq[(DateTime, String)], level: ConsistencyLevel = QUORUM): Future[Seq[Infoton]] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val uuids = history.map(_._2).toVector
    val existingInfotonsFut = this.readUUIDSAsync(uuids)

    existingInfotonsFut.flatMap { existingInfopts =>
      val existingInfotons = existingInfopts.collect{ case FullBox(i) => i}
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
  }

   def getRawRow(uuid: String, level: ConsistencyLevel)(implicit ec: ExecutionContext): Future[String] = {
     val boundStatement = new BoundStatement(getInfotonUUID)
     boundStatement.bind(uuid)
     boundStatement.setConsistencyLevel(level)

     executeAsync(boundStatement).map { result =>
       val it = result.iterator()
       if (it.hasNext) {
         val r: Row = it.next()
         val jm = r.getMap("fields", classOf[String], classOf[String])
         import scala.collection.JavaConversions._
         val m = jm.map {
           case (k, v) => s""""$k":"$v""""
         }
         val storedUuid = r.getString("uuid")
         s"""{"uuid":"$storedUuid","fields":${m.mkString("{", ",", "}")}"""
       }
       else s"empty result set was returned! [$result]"
     }
   }
}

