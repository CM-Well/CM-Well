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

import cmwell.driver.{Dao, DaoExecution}
import cmwell.util.concurrent._
import cmwell.util.exceptions._
import cmwell.util.testSuitHelpers.test.CassandraDockerSuite
import com.typesafe.config.ConfigFactory
import org.scalatest.{AsyncFunSpec, BeforeAndAfterAll, Matchers, Succeeded}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

/**
  * Created by yaakov on 9/19/16.
  */
class ZStoreSpec extends AsyncFunSpec with Matchers with BeforeAndAfterAll with CassandraDockerSuite {
  override def cassandraVersion: String = "3.11.4" //cmwell.util.build.BuildInfo.cassandraVersion - should be used but it had circular dependency!!!
  val utf8 = "UTF-8"
  val indexingDuration = 1.second
  var _dao : Dao = _
  var zStore: ZStore = _

  override protected def beforeAll() {
    super.beforeAll()
    // scalastyle:off
    val initCommands = Some(List(
      "CREATE KEYSPACE IF NOT EXISTS data2 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};"
    ))
    // scalastyle:on
    _dao = Dao("Test", "data2", container.containerIpAddress, container.mappedPort(9042), initCommands = initCommands)
    zStore = ZStore(_dao)
  }

  override protected def afterAll() {
    super.afterAll()
    _dao.shutdown()
  }

  it("should get a NoSuchElementException when getting a non existing key") {
    mapToFailure(zStore.get("no-such-key")).map(assertIsNoSuchElementException)
  }

  it("should put key with small value, and get it") {
    val (k,v) = ("key1", "value1".getBytes(utf8))
    zStore.put(k, v).flatMap(_ => zStore.get(k).map(_ should be(v)))
  }

  it("should put key with large value, and get it") {
    val (k,v) = ("key2", scala.util.Random.alphanumeric.take(math.round(7.5 * chunkSize).toInt).mkString.getBytes(utf8))
    zStore.put(k, v).flatMap(_ => withClue("large value was not as expected") {
      zStore.get(k).map(_ should be(v))
    })
  }

  it("should put key with value and TTL, get it, wait, and verify it's gone") {
    val (k,v) = ("key3", "who let the dogs out? who? who? who?".getBytes(utf8))
    val ttl = 3 // seconds
    def f1 = zStore.put(k, v, ttl, false).flatMap(_ => zStore.get(k).map(_ should be(v)))
    def f2 = mapToFailure(zStore.get(k)).map(assertIsNoSuchElementException)

    f1.flatMap(_ => cmwell.util.concurrent.delayedTask(4.seconds)(f2).flatMap(identity))
  }

  //Not failing any more due to corrupted data. just emitting an error message in logs.
  ignore("should corrupt data and get corresponding exception when getting it") {
    val (k,v) = ("key4", scala.util.Random.alphanumeric.take(math.round(3.14*chunkSize).toInt).mkString.getBytes(utf8))

    def deleteOneChunk() = {
      implicit val daoProxy = _dao
      new DaoExecution {
        def execDeleteOneChunk = {
          val stmt = _dao.getSession.prepare(s"DELETE FROM data2.zstore WHERE uzid='$k' AND field='data2';").bind
          executeAsyncInternal(stmt)
        }
      }.execDeleteOneChunk
    }

    zStore.put(k, v).flatMap{ _ =>
      deleteOneChunk().flatMap{ _ =>
        mapToFailure(zStore.get(k)).flatMap { e =>
          e.getMessage should be("Corrupted data!")
        }
      }
    }
  }

  it("should put and get a very large random content (64MB)") {
    fillArrayAndTest(64*1024*1000, getRandomByte)
  }

  it("should put and get a very large constant content (64MB)") {
    fillArrayAndTest(64*1024*1000, 1.toByte)
  }

  it("should put and get a very large constant content (128MB)") {
    fillArrayAndTest(128*1024*1000, 1.toByte)
  }

  private def mapToFailure[T](f: Future[T]) = {
    val p = Promise[Throwable]()
    f.onComplete {
      case Success(v) => p.failure(new RuntimeException(s"Success($v) was not expected"))
      case Failure(e) => p.success(e)
    }
    p.future
  }

  private def assertIsNoSuchElementException(t: Throwable) = t match {
    case _: NoSuchElementException => Succeeded
    case e => fail(s"$e is not NoSuchElementException")
  }

  private def getRandomByte = (scala.util.Random.nextInt(256) - 128).toByte

  private def fillArrayAndTest(size: Int, elementValue: => Byte) = {
    import cmwell.util.string.Hash.sha1

    val value = Array.fill(size)(elementValue)
    val checksum = sha1(value)
    Try {
      zStore.put(checksum, value).flatMap { _ =>
        SimpleScheduler.scheduleFuture(indexingDuration) {
          zStore.get(checksum).map(sha1)
        }
      }
    } match {
      case Failure(e) => fail(stackTraceToString(e))
      case Success(v) => v.map(_ should be(checksum)).recover {
        case t: Throwable => fail(stackTraceToString(t))
      }
    }
  }

  private val chunkSize = ConfigFactory.load().getInt("cmwell.zstore.chunkSize")
}
