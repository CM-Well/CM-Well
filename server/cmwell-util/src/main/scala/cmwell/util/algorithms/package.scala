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
package cmwell.util

import cmwell.util.concurrent.SimpleScheduler.schedule
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

package object algorithms extends LazyLogging {
  /*
   * Union Find Algorithm
   *
   * Inspired by blog.xebia.com/the-union-find-algorithm-in-scala-a-purely-functional-implementation
   *
   * IMPORTANT: THIS IS THE NAÏVE IMPLEMENTATION AND CAN BE OPTIMIZED IN FEW WAYS (extra pointer to head, by rank, paths compression, and more).
   * NOT TO BE USED AS IS ON LARGE DATASETS!!!
   */
  class UnionFind[T](nodes: Vector[UFNode[T]]) {

    /**
      * @param t1 data of element to be connected
      * @param t2 data of element to be connected
      *
      *           Note: t2 will be parent of t1.
      *           So if you want to represent X-->Y-->Z, use .union(Y,X).union(Z,Y)
      */
    def union(t1: T, t2: T): UnionFind[T] = union(indexOf(t1), indexOf(t2))

    /**
      * @param index1 index of element to be connected
      * @param index2 index of element to be connected
      *
      *               Note: index2 will be parent of index1.
      *               So if you want to represent 1-->2-->3, use .union(2,1).union(3,2)
      */
    def union(index1: Int, index2: Int): UnionFind[T] = {
      val (root1, root2) = (find(index1), find(index2))
      if (index1 == index2 || root1 == root2) this
      else {
        val newNode1 = nodes(root1).copy(parent = Some(index2))
        new UnionFind(nodes.updated(root1, newNode1))
      }
    }

    // can add @tailrec if instead of fold we will match { case None => t case Some(p) => find(p) }
    /**
      * @param idx index of element to find its root
      * @return the index of the disjoint set representative, i.e. its root
      */
    def find(idx: Int): Int = nodes(idx).parent.fold(idx)(find)

    /**
      * @param t data of element to find its root
      * @return the data of the Disjoint Set Representative, i.e. its root
      */
    def find(t: T): T = nodes(find(indexOf(t))).data

    def isConnected(t1: Int, t2: Int): Boolean = t1 == t2 || find(t1) == find(t2)

    override def toString: String = s"UnionFind(${nodes.length})"

    private def indexOf(t: T) = nodes.zipWithIndex.find(_._1.data == t).map(_._2).getOrElse(-1)
  }

  case class UFNode[T](data: T, parent: Option[Int] = None)

  trait IntegralConsts[N] {
    val tc: Integral[N]
    lazy val two = tc.plus(tc.one, tc.one)
    lazy val four = tc.plus(two, two)
  }

  object IntegralConsts {
    implicit def consts[N: Integral] = new IntegralConsts[N] {
      override val tc = implicitly[Integral[N]]
    }
  }

  /**
    * Binary search for a range on a timeline
    * with optimizations for consecutive searches (the nextTo in the response).
    * @param from
    * @param toSeed
    * @param upperBound
    * @param threshold
    * @param thresholdFactor
    * @param timeout
    * @param searchFunction
    * @param ec
    * @tparam N
    * @return (from, to , nextTo)
    */
  def binRangeSearch[N: IntegralConsts](
    from: N,
    toSeed: N,
    upperBound: N,
    threshold: Long,
    thresholdFactor: Double,
    timeout: FiniteDuration
  )(searchFunction: N => Future[Long])(implicit ec: ExecutionContext): Future[(N, N, Option[N])] = {
    val consts = implicitly[IntegralConsts[N]]
    val math = consts.tc
    require(math.compare(from, math.zero) >= 0, "from must be positive or zero")
    require(math.compare(from, toSeed) < 0, "from must be smaller than toSeed")
    require(math.compare(toSeed, upperBound) <= 0, "toSeed must be smaller or equal to upperBound")
    require(thresholdFactor < 1.0 && thresholdFactor > 0.0, "thresholdFactor must be greater than 0, but less than 1")
    val notEnough = (threshold * (1 - thresholdFactor)).toLong
    val tooMany = (threshold * (1 + thresholdFactor)).toLong
    val timeoutMarker = schedule(timeout)(())
    logger.trace(s"expandRange($from, $toSeed, $upperBound, $notEnough, $tooMany, ...)")
    expandRange(from, toSeed, upperBound, notEnough, tooMany, timeoutMarker)(searchFunction).flatMap {
      case Right(result) => Future.successful(result)
      case r @ Left((timePosition, step, nextToOptimization)) =>
        logger.trace(s"expandRange returned $r moving on to the shrinking binary search")
        shrinkingStepBinarySearch(from, timePosition, step, nextToOptimization, notEnough, tooMany, timeoutMarker)(
          searchFunction
        )
    }
  }

  /**
    *
    * @param from
    * @param toSeed
    * @param upperBound
    * @param notEnough
    * @param tooMany
    * @param timeoutMarker
    * @param searchFunction
    * @param ec
    * @tparam N
    * Options to "Right" (don't proceed to shrinking step):
    *   1. timeout during expand.
    *   2. not enough until upperBound.
    *   3. found suitable range during the expand phase
    * @return either Right(from, to, nextTo) or Left(position, step, nextTo)
    */
  def expandRange[N: IntegralConsts](from: N,
                                     toSeed: N,
                                     upperBound: N,
                                     notEnough: Long,
                                     tooMany: Long,
                                     timeoutMarker: Future[Unit])(
    searchFunction: N => Future[Long]
  )(implicit ec: ExecutionContext): Future[Either[(N, N, Option[N]), (N, N, Option[N])]] = {
    val consts = implicitly[IntegralConsts[N]]
    val math, ord = consts.tc

    def inner(to: N): Future[Either[(N, N, Option[N]), (N, N, Option[N])]] = {
      //stop conditions: 1. in range. 2. out of range 3. next to > now 4. early cut off (return the last known position to be "not enough" - the previous to)
      logger.trace(s"expandTimeRange: from[$from], to[$to]")
      if (timeoutMarker.isCompleted) {
        val resultingTo =
          //in case that the time was finished before doing any inner iteration - return the toSeed we started with.
        // If not it will return the middle between from and toSeed to potentially don't have any data in it.
          if (ord.compare(to, toSeed) == 0) toSeed
          else math.minus(to, math.quot(math.minus(to, from), consts.two))
        Future.successful(Right((from, resultingTo, None)))
      }
      //if to>=now then 1. the binary search should be between the previous position and now or 2.
      // the now position itself. Both cases will be check in the below function
      else if (ord.compare(to, upperBound) >= 0)
        checkRangeUpToUpperBound(from,
                                 math.minus(to, math.quot(math.minus(to, from), consts.two)),
                                 upperBound,
                                 notEnough,
                                 tooMany)(searchFunction)
      else
        searchFunction(to).flatMap { total =>
          //not enough results - keep expanding
          if (total < notEnough) inner(math.plus(to, math.minus(to, from)))
          //in range - return final result
          else if (total < tooMany) Future.successful(Right(from, to, None))
          //too many results - return the position to start the binary search from
          else {
            val nextToOptimizedForTheNextToken = if (total < tooMany * 2) Some(to) else None
            //The last step got us to this position
            val lastStep = math.quot(math.minus(to, from), consts.two)
            val toToStartSearchFrom = math.minus(to, math.quot(lastStep, consts.two))
            Future.successful(
              Left(toToStartSearchFrom, math.quot(lastStep, consts.four), nextToOptimizedForTheNextToken)
            )
          }
        }
    }

    inner(toSeed)
  }

  def checkRangeUpToUpperBound[N: IntegralConsts](from: N,
                                                  rangeStart: N,
                                                  upperBound: N,
                                                  notEnough: Long,
                                                  tooMany: Long)(
    searchFunction: N => Future[Long]
  )(implicit ec: ExecutionContext): Future[Either[(N, N, Option[N]), (N, N, Option[N])]] = {
    logger.trace(s"checkRangeUpToNow: from[$from], rangeStart[$rangeStart]")
    val consts = implicitly[IntegralConsts[N]]
    val math = consts.tc
    //This function will be called ONLY when the previous step didn't have enough results
    searchFunction(upperBound).map { total =>
      if (total <= tooMany) Right((from, upperBound, None))
      else {
        val nextToOptimizedForTheNextToken = if (total < tooMany * 2) Some(upperBound) else None
        //rangeStart is the last known position to be with not enough results. This is the lower bound for the binary search
        //range is the range of the binary search. The whole search will be between rangeStart and now
        val range = math.minus(upperBound, rangeStart)
        //The next position to be checked using the binary search
        val middle = math.plus(rangeStart, math.quot(range, consts.two))
        //In case the next iteration won't finish, this is the step to be taken. The step is half of the step that was taken to get to the middle point
        val step = math.quot(range, consts.four)
        Left(middle, step, nextToOptimizedForTheNextToken)
      }
    }
  }

  def shrinkingStepBinarySearch[N: IntegralConsts](
    from: N,
    timePosition: N,
    step: N,
    nextTo: Option[N],
    notEnough: Long,
    tooMany: Long,
    timeoutMarker: Future[Unit]
  )(searchFunction: N => Future[Long])(implicit ec: ExecutionContext): Future[(N, N, Option[N])] = {
    logger.trace(s"shrinkingStepBinarySearch: from[$from], timePosition[$timePosition], step[$step], nextTo[$nextTo]")
    val consts = implicitly[IntegralConsts[N]]
    val math = consts.tc
    //stop conditions: 1. in range. 2. early cut off
    //In case of an early cut off we have 2 options:
    //1. the previous didn't have enough results - we can use it
    //2. the previous had too many results - we cannot use it but we can use the position before it which is our position minus twice the given step
    //Also note: even with the case we moved back several times the logic is correct.
    //           The next step to reach the last position that we jump ahead from is exactly the previous step done
    //           which is twice the step we would do in case that we had continue the search
    if (timeoutMarker.isCompleted)
      Future.successful((from, math.minus(timePosition, math.times(step, consts.two)), nextTo))
    else
      searchFunction(timePosition).flatMap { total =>
        //not enough results - keep the search up in the timeline
        if (total < notEnough)
          shrinkingStepBinarySearch(from,
                                    math.plus(timePosition, step),
                                    math.quot(step, consts.two),
                                    nextTo,
                                    notEnough,
                                    tooMany,
                                    timeoutMarker)(searchFunction)
        //in range - return final result
        else if (total < tooMany) Future.successful((from, timePosition, nextTo))
        //too many results - keep the search down in the timeline
        else
          shrinkingStepBinarySearch(
            from,
            math.minus(timePosition, step),
            math.quot(step, consts.two),
            nextTo.orElse(if (total < tooMany * 2) Some(timePosition) else None),
            notEnough,
            tooMany,
            timeoutMarker
          )(searchFunction)
      }
  }
}
