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
package cmwell.util.stream

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FanInShape2, Inlet, Outlet}

class SortedStreamsMergeBy[L, R, K: Ordering](lKey: L => K, rKey: R => K)
    extends GraphStage[FanInShape2[L, R, (K, Vector[L], Vector[R])]] {

  val in0: Inlet[L] = Inlet[L]("SortedStreamsMergeByLeft.in")
  val in1: Inlet[R] = Inlet[R]("SortedStreamsMergeByRight.in")
  val out: Outlet[(K, Vector[L], Vector[R])] = Outlet[(K, Vector[L], Vector[R])]("SortedStreamsMergeBy.out")

  override val shape = new FanInShape2(in0, in1, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private val ord = implicitly[Ordering[K]]

    private var currentLK = null.asInstanceOf[K]
    private var currentRK = null.asInstanceOf[K]
    private var lPending = null.asInstanceOf[L]
    private var rPending = null.asInstanceOf[R]
    private var lAcc = Vector.newBuilder[L]
    private var rAcc = Vector.newBuilder[R]
    private var ready = null.asInstanceOf[(K, Vector[L], Vector[R])]

    override def preStart(): Unit = {
      tryPull(in0)
      tryPull(in1)
    }

    setHandler(
      out,
      new OutHandler {
        override def onPull(): Unit = {
          if (ready ne null) {
            push(out, ready)
            ready = null
            if (currentLK == null && currentRK == null && isClosed(in0) && isClosed(in1)) completeStage()
            else {
              startAccumulatingAndPull()
              if (isClosed(in0) && isClosed(in1)) {
                val c = ord.compare(currentLK, currentRK)
                val k = {
                  if (c < 0) {
                    val tmp = currentLK
                    currentLK = null.asInstanceOf[K]
                    tmp
                  } else if (c > 0) {
                    val tmp = currentRK
                    currentRK = null.asInstanceOf[K]
                    tmp
                  } else {
                    val tmp = currentLK
                    currentLK = null.asInstanceOf[K]
                    currentRK = null.asInstanceOf[K]
                    tmp
                  }
                }
                ready = (k, lAcc.result(), rAcc.result())
                lAcc = Vector.newBuilder[L]
                rAcc = Vector.newBuilder[R]
              }
            }
          }
        }
      }
    )

    def startAccumulatingAndPull(): Unit = {

      def accAndPullLeft(): Unit = {
        lAcc += lPending
        lPending = null.asInstanceOf[L]
        if (!isClosed(in0)) pull(in0)
        else if (!isClosed(in1)) currentLK = null.asInstanceOf[K]
      }

      def accAndPullRight(): Unit = {
        rAcc += rPending
        rPending = null.asInstanceOf[R]
        if (!isClosed(in1)) pull(in1)
        else if (!isClosed(in0)) currentRK = null.asInstanceOf[K]
      }

      //i.e: in0 is already closed & no pending
      if (currentLK == null) accAndPullRight()
      //i.e: in1 is already closed & no pending
      else if (currentRK == null) accAndPullLeft()
      else {
        val c = ord.compare(currentLK, currentRK)
        if (c < 0) accAndPullLeft()
        else if (c > 0) accAndPullRight()
        else {
          accAndPullLeft()
          accAndPullRight()
        }
      }
    }

    def prepareAccumulatedAndTryPush(k: K): Unit = {
      require(ready eq null)
      ready = (k, lAcc.result(), rAcc.result())
      lAcc = Vector.newBuilder[L]
      rAcc = Vector.newBuilder[R]

      if (isAvailable(out)) {
        push(out, ready)
        ready = null
        if (currentLK == null && currentRK == null) completeStage()
        else startAccumulatingAndPull()
      }
    }

    setHandler(
      in0,
      new InHandler {
        override def onPush(): Unit = {
          val l = grab(in0)
          val k = lKey(l)
          //first left element in stream
          if (currentLK == null) {
            require(lPending == null)
            currentLK = k
            lPending = l
            if (currentRK != null || isClosed(in1)) startAccumulatingAndPull()
          }
          //continue to pull
          else if (currentLK == k) {
            lAcc += l
            pull(in0)
          }
          //halt and wait for right to finish accumulate
          else if (rPending == null && !isClosed(in1)) {
            require(lPending == null)
            currentLK = k
            lPending = l
          }
          //right waited up until now (or it is closed) for left to finish accumulate
          else {
            val key = currentLK
            currentLK = k
            lPending = l
            prepareAccumulatedAndTryPush(key)
          }
        }

        override def onUpstreamFinish(): Unit = {
          //current streak of same K elements (might be empty) ended
          if (lPending == null) {
            if (rPending != null || isClosed(in1)) {
              if (currentRK == null && currentLK == null) completeStage()
              else {
                val key = currentLK
                currentLK = null.asInstanceOf[K]
                prepareAccumulatedAndTryPush(key)
              }
            } else currentLK = null.asInstanceOf[K]
            //else: halt and wait for right to finish accumulating
          }
          //else: we were waiting for right side since the last element we got couldn't be accumulated
        }
      }
    )

    setHandler(
      in1,
      new InHandler {
        override def onPush(): Unit = {
          val r = grab(in1)
          val k = rKey(r)
          //first right element in stream
          if (currentRK == null) {
            require(rPending == null)
            currentRK = k
            rPending = r
            if (currentLK != null || isClosed(in0)) startAccumulatingAndPull()
          }
          //continue to pull
          else if (currentRK == k) {
            rAcc += r
            pull(in1)
          }
          //halt and wait for left to finish accumulate
          else if (lPending == null && !isClosed(in0)) {
            require(rPending == null)
            currentRK = k
            rPending = r
          }
          //left waited up until now (or it is closed) for right to finish accumulate
          else {
            val key = currentRK
            currentRK = k
            rPending = r
            prepareAccumulatedAndTryPush(key)
          }
        }

        override def onUpstreamFinish(): Unit = {
          //current streak of same K elements (might be empty) ended
          if (rPending == null) {
            if (lPending != null || isClosed(in0)) {
              if (currentRK == null && currentLK == null) completeStage()
              else {
                val key = currentRK
                currentRK = null.asInstanceOf[K]
                prepareAccumulatedAndTryPush(key)
              }
            } else currentRK = null.asInstanceOf[K]
            //else: halt and wait for left to finish accumulating
          }
          //else: we were waiting for left side since the last element we got couldn't be accumulated
        }
      }
    )
  }
}
