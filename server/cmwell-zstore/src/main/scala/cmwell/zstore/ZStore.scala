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
package cmwell.zstore

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import cmwell.driver.{Dao, DaoExecution}
import cmwell.util.concurrent._
import cmwell.util.numeric._
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object ZStore {
  def apply(dao: Dao): ZStore = new ZStoreImpl(dao)
}

// Design choice: using (seconds: Int) TTL, and not Duration. There are reasons...
trait ZStore {
  def put(uzid: String, value: Array[Byte], batched: Boolean = false): Future[Unit]
  // not using FiniteDuration, since CAS only supports INT values for TTL
  def put(uzid: String, value: Array[Byte], secondsToLive: Int, batched: Boolean): Future[Unit]
  def get(uzid: String): Future[Array[Byte]]
  def get(uzid: String, dontRetry: Boolean): Future[Array[Byte]]
  def getOpt(uzid: String, dontRetry: Boolean = false): Future[Option[Array[Byte]]]
  def putString(uzid: String, value: String, batched: Boolean = false): Future[Unit] = {
    put(uzid, value.getBytes(StandardCharsets.UTF_8), batched)
  }
  def putLong(uzid: String, value: Long, batched: Boolean = false): Future[Unit] =
    put(uzid, ByteBuffer.allocate(8).putLong(value).array(), batched)

  def putInt(uzid: String, value: Int, batched: Boolean = false): Future[Unit] =
    put(uzid, ByteBuffer.allocate(4).putInt(value).array(), batched)

  def putBoolean(uzid: String, value: Boolean, batched: Boolean = false): Future[Unit] = {
    val i: Int = if (value) 1 else 0
    put(uzid, ByteBuffer.allocate(1).put(i.toByte).array(), batched)
  }

  def getString(uzid: String): Future[String] = get(uzid).map { new String(_, StandardCharsets.UTF_8) }

  def getStringOpt(uzid: String): Future[Option[String]] = getOpt(uzid, true).map { _.map { new String(_, StandardCharsets.UTF_8) } }

  def getLong(uzid: String): Future[Long] = get(uzid).map { bytes =>
    ByteBuffer.wrap(bytes).getLong
  }

  def getInt(uzid: String): Future[Int] = get(uzid).map { bytes =>
    ByteBuffer.wrap(bytes).getInt
  }

  def getBoolean(uzid: String): Future[Boolean] = get(uzid).map(_(0).toInt == 1)

  def getLongOpt(uzid: String): Future[Option[Long]] = getOpt(uzid, true).map { bytesOpt =>
    bytesOpt.map { ByteBuffer.wrap(_).getLong }
  }

  def getIntOpt(uzid: String): Future[Option[Int]] = getOpt(uzid, true).map { bytesOpt =>
    bytesOpt.map { ByteBuffer.wrap(_).getInt }
  }

  def getBooleanOpt(uzid: String): Future[Option[Boolean]] = getOpt(uzid, true).map { bytesOpt =>
    bytesOpt.map(_(0) == 1)
  }

  def remove(uzid: String): Future[Unit]
}

class ZStoreImpl(dao: Dao) extends ZStore with DaoExecution with LazyLogging {
  implicit val daoProxy = dao

  override def put(uzid: String, value: Array[Byte], batched: Boolean): Future[Unit] =
    put(uzid, value, neverExpire, batched)

  override def put(uzid: String, value: Array[Byte], secondsToLive: Int, batched: Boolean): Future[Unit] = {

    def serialize(obj: ZStoreObj): Vector[ZStoreRow] = {
      // NOTE: "adler" < "bytes" < "data"
      val adlerRow = ZStoreRow(obj.uzid, "adler", toIntegerBytes(obj.adler32))
      val bytesRow = ZStoreRow(obj.uzid, "bytes", toLongBytes(obj.value.length.toLong))
      val indexLength = (((obj.value.length - 1) / chunkSize) + 1).toString.length
      val dataRows = obj.value.grouped(chunkSize).zipWithIndex.map {
        case (chunk, index) => ZStoreRow(obj.uzid, s"data%0${indexLength}d".format(index), chunk)
      }
      Vector(adlerRow, bytesRow) ++ dataRows
    }

    val ttl = new java.lang.Integer(secondsToLive)
    val adler32 = cmwell.util.string.Hash.adler32int(value)
    val rows = serialize(ZStoreObj(uzid, adler32, value))

    def rowToStatement(row: ZStoreRow) =
      putPStmt.bind(row.uzid, row.field, ByteBuffer.wrap(row.chunk), ttl).setConsistencyLevel(ConsistencyLevel.QUORUM)

    if (batched) {
      val batch = new BatchStatement()
      rows.foreach { row =>
        batch.add(putPStmt.bind(row.uzid, row.field, ByteBuffer.wrap(row.chunk), ttl))
      }
      execWithRetry(batch.setConsistencyLevel(ConsistencyLevel.QUORUM)).map(_ => ())
    }
    else {
      travector(rows)((rowToStatement _).andThen(execWithRetry)).map(_ => ())
    }
  }

  override def get(uzid: String): Future[Array[Byte]] = {
    val getStmtOne = getPStmt.bind(uzid).setConsistencyLevel(ConsistencyLevel.ONE)
    val getStmtQuorum = getPStmt.bind(uzid).setConsistencyLevel(ConsistencyLevel.QUORUM)

    def fetchAndDeserialize(stmt: Statement) =
      retry(exec(stmt).map((deserialize(uzid) _).andThen(_.fold(throw new NoSuchElementException)(_.value))))

    fetchAndDeserialize(getStmtOne).andThen {
      case Success(value) => value
      case Failure(e) =>
        logger.warn(s"[zStore] Reading uzid $uzid with cl=ONE threw $e. Trying cl=QUORUM...")
        fetchAndDeserialize(getStmtQuorum).andThen {
          case Success(value) => value
          case Failure(e1) =>
            logger.warn(s"[zStore] Reading uzid $uzid with cl=QUORUM threw $e1. Trying last time with cl=ONE...")
            fetchAndDeserialize(getStmtOne)
        }
    }
  }

  override def get(uzid: String, dontRetry: Boolean): Future[Array[Byte]] = {
    if (!dontRetry)
      get(uzid)
    else {
      val stmt = getPStmt.bind(uzid).setConsistencyLevel(ConsistencyLevel.QUORUM)
      exec(stmt).map((deserialize(uzid) _).andThen(_.fold(throw new NoSuchElementException)(_.value)))
    }
  }

  override def getOpt(uzid: String, dontRetry: Boolean): Future[Option[Array[Byte]]] = {
    require(dontRetry, "getOpt only works in dontRetry mode") // might be implemented, but YAGNI
    val stmt = getPStmt.bind(uzid).setConsistencyLevel(ConsistencyLevel.QUORUM)
    exec(stmt).map(deserialize(uzid)(_)).map(_.map(_.value)) // not throwing an exception, but returning an Option
  }

  override def remove(uzid: String): Future[Unit] = {
    val delStmt = delPStmt.bind(uzid).setConsistencyLevel(ConsistencyLevel.QUORUM)
    execWithRetry(delStmt).map(_ => ())
  }

  //TODO: 8 retries + delay factor of 2, means you could retry for 50+100+200+400+800+1600+3200+6400 = 12750ms
  //TODO: this change is mandatory because new driver backpressures with reject exceptions.
  //TODO: we need to propogate back preasure instead of retrying for so long...
  private def execWithRetry(stmt: Statement) = cmwell.util.concurrent.retry(8, 50.millis, 2)(exec(stmt))

  private def exec(stmt: Statement) = executeAsyncInternal(stmt)

  private def retry[T](task: Future[T]) = cmwell.util.concurrent.retry(3, 50.millis, 2)(task)

  private val neverExpire = 0
  private val chunkSize = ConfigFactory.load().getInt("cmwell.zstore.chunkSize")

  // zStore deliberately contains its setup and not in a resource cql file.
  private val createTableCqlStmt = {
    // assuming data2 already exists from cmwell install
    val createTableCql =
      """|
       |CREATE TABLE IF NOT EXISTS data2.zstore (
       |   uzid text,
       |   field text,
       |   value blob,
       |   PRIMARY KEY (uzid, field)
       |) WITH CLUSTERING ORDER BY (field ASC)
       |AND compression = {'class': 'LZ4Compressor'}
       |AND caching = {'keys':'ALL', 'rows_per_partition':'ALL'} ;
     """.stripMargin

    prepare(createTableCql).bind
  }
  dao.getSession.execute(createTableCqlStmt) // deliberately not using execAsync

  private val putPStmt = prepare("INSERT   INTO data2.zstore (uzid,field,value) VALUES(?,?,?) USING TTL ?") // TTL 0 == persist forever
  private val getPStmt = prepare("SELECT * FROM data2.zstore WHERE uzid = ?")
  private val delPStmt = prepare("DELETE   FROM data2.zstore WHERE uzid = ?")

  private def deserialize(uzid: String)(result: ResultSet): Option[ZStoreObj] = {
    if (result.isExhausted) None
    else {
      val adlerRow = result.one()
      require(!result.isExhausted, "cassandra ResultSet exhausted after adler row")
      val adler = ByteBuffer.wrap(Bytes.getArray(adlerRow.getBytes("value"))).getInt
      val bytesRow = result.one()
      val bytes = ByteBuffer.wrap(Bytes.getArray(bytesRow.getBytes("value"))).getLong
      if (result.isExhausted) {
        require(bytes == 0 && adler == ZStoreImpl.zeroAdler,
                s"expected empty content for uzid [$uzid] with bytes [$bytes] and adler [$adler]")
        Some(ZStoreObj(uzid, adler, Array.emptyByteArray))
      } else {
        val it = new Iterator[Array[Byte]] {
          override def hasNext: Boolean = !result.isExhausted
          override def next(): Array[Byte] = {
            val row: Row = result.one()
            Bytes.getArray(row.getBytes("value"))
          }
        }
        val valueBuilder = Array.newBuilder[Byte]
        valueBuilder.sizeHint(longToIntOrMaxInt(bytes))
        it.foreach(valueBuilder ++= _)
        val obj = ZStoreObj(uzid, adler, valueBuilder.result())
        if (!obj.isCorrect) {
          val a = obj.adlerizedValue
          val l = obj.value.length
          // scalastyle:off
          logger.error(s"[zStore] Reading uzid [$uzid] failed because data corruption! (stored adler[$adler] != computed adler[$a], stored bytes[$bytes], actual value size[$l])")
//          throw new RuntimeException("Corrupted data!")
          // scalastyle:on
        }
        Some(obj)
      }
    }
  }

  private def longToIntOrMaxInt(long: Long) = math.min(Int.MaxValue, long).toInt
}

object ZStoreImpl {
  val zeroAdler = cmwell.util.string.Hash.adler32int(Array.emptyByteArray)
}

case class ZStoreRow(uzid: String, field: String, chunk: Array[Byte])

case class ZStoreObj(uzid: String, adler32: Int, value: Array[Byte]) {
  lazy val adlerizedValue = cmwell.util.string.Hash.adler32int(value)
  lazy val isCorrect: Boolean = adler32 == adlerizedValue
}

/**
  * an In Memory implementation of the ZStore trait
  */
class ZStoreMem extends ZStore {
  private var store = Map.empty[String, Array[Byte]]

  override def put(uzid: String, value: Array[Byte], batched: Boolean): Future[Unit] =
    Future.successful(store += uzid -> value)

  override def put(uzid: String, value: Array[Byte], secondsToLive: Int, batched: Boolean): Future[Unit] =
    Future.successful {
      store += uzid -> value
      delayedTask(secondsToLive.seconds) { store -= uzid }
    }

  override def get(uzid: String): Future[Array[Byte]] =
    Future(store(uzid))

  override def get(uzid: String, dontRetry: Boolean): Future[Array[Byte]] = get(uzid)

  override def getOpt(uzid: String, dontRetry: Boolean): Future[Option[Array[Byte]]] =
    Future.successful(store.get(uzid))

  override def remove(uzid: String): Future[Unit] =
    Future.successful(store -= uzid)

  def keySet = store.keySet
}
