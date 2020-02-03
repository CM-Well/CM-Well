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
package cmwell.util.algorithms.test

import cmwell.util.algorithms._
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.compatible.Assertion
import org.scalatest._

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.DurationInt

class BinRangeSearchTests extends AsyncFunSpec with Matchers with OptionValues with EitherValues with TryValues with LazyLogging {

  @inline def r(id: String, l: Long): Future[Long] = {
    logger.info(s"[$id]: $l")
    Future.successful(l)
  }

  val basePF: String =>  PartialFunction[Long, Future[Long]] = id => {
    case ts@1425817765688L => r(s"$id($ts)", 1017L)
    case ts@1508943374048L => r(s"$id($ts)", 5000000L)
    case ts@1467380569868L => r(s"$id($ts)", 4000000L)
    case ts@1446599167778L => r(s"$id($ts)", 3999999L)
    case ts@1436208466733L => r(s"$id($ts)", 3999990L)
    case ts@1431013116211L => r(s"$id($ts)", 3999900L)
    case ts@1428415440950L => r(s"$id($ts)", 3999000L)
    case ts@1427116603320L => r(s"$id($ts)", 3990000L)
    case ts@1426467184505L => r(s"$id($ts)", 3900000L)
    case ts@1426142475098L => r(s"$id($ts)", 3888888L)
    case ts@1425980120395L => r(s"$id($ts)", 3888800L)
    case ts@1425898943044L => r(s"$id($ts)", 3880000L)
    case ts@1425858354369L => r(s"$id($ts)", 1000000L)
  }

  describe("binRangeSearch should") {
    val bigGap: Future[Assertion] = {
      val pf = basePF("bigGap")
      binRangeSearch[Long](83929378534L, 1425817765688L, 1508943374048L, 1000000L, 0.5, 6.seconds)(pf).map {
        case (from, to, nextTo) =>
          from should be(83929378534L)
          to should be(1425858354369L)
          nextTo should be(empty)
      }
    }

    val bigGapNextTo: Future[Assertion] = {
      val pf = basePF("bigGapNextTo")
      binRangeSearch[Long](83929378534L, 1425817765688L, 1508943374048L, 1000000L, 0.5, 6.seconds) {
        case ts@1425898943044L => r(s"bigGapNextTo($ts)", 2880000L)
        case ts => pf(ts)
      }.map {
        case (from, to, nextTo) =>
          from should be(83929378534L)
          to should be(1425858354369L)
          nextTo.value should be(1425898943044L)
      }
    }

    val eventHorizon: Future[Assertion] = {
      val pf = basePF("eventHorizon")
      binRangeSearch[Long](83929378534L, 1425817765688L, 1508943374048L, 1000000L, 0.5, 6.seconds) {
        case ts@1508943374048L => r(s"eventHorizon($ts)", 1729L)
        case ts => pf(ts)
      }.map {
        case (from, to, nextTo) =>
          from should be(83929378534L)
          to should be(1508943374048L)
          nextTo should be(empty)
      }
    }

    val earlyCutOffWithTimeToShrink: (Future[Assertion],Future[Assertion]) = {
      val pf = basePF("earlyCutOff")
      val timeout = Promise[Unit]
      val searchFunction: Long => Future[Long] = { l =>
        if (l == 1431013116211L) {
          timeout.success(())
          r(s"earlyCutOff($l)", 3999900L)
        }
        else pf(l)
      }
      val f0 = expandRange[Long](83929378534L, 1425817765688L, 1508943374048L, 500000, 1500000, timeout.future)(searchFunction)

      val f1 = f0.map { either =>
        either should be a 'left
        val (timePosition, step, nextTo) = either.left.value
        timePosition should be(1467380569868L)
        step should be        (20781402090L)
        nextTo should be(empty)
      }

      val f2 = f0.flatMap {
        case Right(_) => Future.successful(fail("Future result should be Left (was Right)"))
        case Left((timePosition, step, nextToOptimization)) => shrinkingStepBinarySearch(
          83929378534L,
          timePosition,
          step,
          nextToOptimization,
          500000,
          1500000,
          timeout.future)(searchFunction).map {
          case (from, to, nextTo) =>
            from should be(83929378534L)
            to should be(1425817765690L)
            nextTo should be(empty)
        }
      }
      f1 -> f2
    }

    val earlyCutOffWhileExpanding: Future[Assertion] = {
      val pf = basePF("earlyCutOff")
      val timeout = Future.successful(())
      val searchFunction: Long => Future[Long] = { l =>
        logger.info(s"earlyCutOff with [$l]")
        if (l == 1508943374048L) {
          r(s"earlyCutOff($l)", 5000000L)
        }
        else pf(l)
      }
      expandRange[Long](83929378534L, 1425817765688L, 1508943374048L, 500000, 1500000, timeout)(searchFunction).map { either =>
        either should be a 'right
        val (from, to, nextTo) = either.right.value
        from should be(83929378534L)
        to should be(1425817765688L)
        nextTo should be(empty)
      }
    }

    it("handle initial big gaps properly (adapted from actual recorded case)")(bigGap)
    it("handle initial big gaps properly with nextTo optimization")(bigGapNextTo)
    it("handle event horizon")(eventHorizon)
    describe("handle early cut-off with time to shrink") {
      it("in expandRange + checkRangeUpToNow")(earlyCutOffWithTimeToShrink._1)
      it("in shrinkingStepBinarySearch")(earlyCutOffWithTimeToShrink._2)
    }
    it("handle early cut-off while expanding")(earlyCutOffWhileExpanding)
  }
}
