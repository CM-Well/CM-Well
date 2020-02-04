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


package cmwell.zcache

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import cmwell.util.concurrent.delayedTask
import cmwell.zstore.ZStoreMem
import org.scalatest.{AsyncFunSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by yaakov on 12/12/16.
  */
class ZCacheSpec extends AsyncFunSpec with Matchers {

  val zCacheMem = new ZCache(new ZStoreMem)

  val ttl = 3
  val pollingMaxRetries = 2
  val pollingInterval = 1

  val stob = (s:String) => s.getBytes
  val btos = (ba:Array[Byte]) => new String(ba)

  // counters to validate amount of "actually performing heavy task"
  val c1: AtomicInteger = new AtomicInteger(0)
  val c2: AtomicInteger = new AtomicInteger(0)
  val c3: AtomicInteger = new AtomicInteger(0)

  it("should not found a non existing value") {
    zCacheMem.get("foo")(btos, pollingMaxRetries, pollingInterval).map(_ should be(None))
  }

  it("should put a new value") {
    zCacheMem.put("foo", "bar")(stob, ttl).map(_ should be("bar"))
  }

  it("should get an existing value") {
    zCacheMem.get("foo")(btos, pollingMaxRetries, pollingInterval).map(_ should be(Some("bar")))
  }

  it("should wait and see that item was gone") {
    delayedTask((ttl+1).seconds)(()).flatMap { _ =>
      zCacheMem.get("foo")(btos, pollingMaxRetries, pollingInterval).map(_ should be(None))
    }
  }

  // the "heavy" task
  def fetchData(dataId: String) = delayedTask((ttl/2).seconds) {
    dataId match {
      case "1" => c1.incrementAndGet()
      case "2" => c2.incrementAndGet()
      case _ =>   c3.incrementAndGet()
    }
    s"Data[$dataId]"
  }

  val fetchViaZCache = zCacheMem.memoize(
    fetchData)(
    digest = identity, deserializer = btos, serializer = stob)(
    ttl, pollingMaxRetries, pollingInterval)(
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(10)))

  val fetchViaL1L2 = l1l2(fetchData)(identity, btos, stob)(ttl, pollingMaxRetries, pollingInterval)(zCacheMem)

  it("should wrap data fetching (CORE FUNC. STEP 1/2)") {
    fetchViaZCache("1").map { res =>
      val c = c1.get
      if(c > 1) throw new TooManyFetchesException(c.toString)
      res should be("Data[1]")
    }
  }

  it("should wrap data fetching again, making sure no actual IO has been done (CORE FUNC. STEP 2/2)") {
    fetchViaZCache("1").map { res =>
      val c = c1.get
      if(c > 1) throw new TooManyFetchesException(c.toString)
      res should be("Data[1]")
    }
  }

  it("should test the PENDING logic (slashdot)") {
    val amount = 613
    Future.traverse(Seq.fill(amount)("2"))(fetchViaZCache).map { res =>
      val c = c2.get
      c should be < amount //) succeed // fail(new TooManyFetchesException(c.toString))
      res should be(Seq.fill(amount)("Data[2]"))
    }
  }

  it("should test the L1 L2 logic") {
    val (numOfUniqRequests, amountOfEach) = (1500, 20)
    val requests: Seq[String] = scala.util.Random.shuffle(Seq.fill(amountOfEach)((3 to numOfUniqRequests + 3).map(_.toString)).flatten)
    Future.sequence(requests.map(fetchViaL1L2)).map { res =>
      val c = c3.get
      if (c >= numOfUniqRequests*amountOfEach) throw new TooManyFetchesException(c.toString)
      (res == requests.map(i => s"Data[$i]")) should be(true)
    }
  }

  class TooManyFetchesException(msg: String) extends Exception(msg)
}
