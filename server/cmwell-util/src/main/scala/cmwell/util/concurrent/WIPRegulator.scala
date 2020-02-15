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
package cmwell.util.concurrent

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import com.typesafe.scalalogging.Logger
import scala.concurrent.ExecutionContext.Implicits.global
import cmwell.util.jmx._

/**
  * Created with IntelliJ IDEA.
  * User: israel
  * Date: 12/3/13
  * Time: 1:35 PM
  * To change this template use File | Settings | File Templates.
  */
case class WIPRegulator(private var numOfWorkers: Int, noWorkerAlertInterval: Long = 30000) extends WIPRegulatorMBean {

  jmxRegister(this, "cmwell.indexer:type=WIPRegulator")

  private val wipQueue = new ArrayBlockingQueue[String](50)

  // Set intitial number of concurrent requests
  for (i <- 1 to numOfWorkers)(wipQueue.add("WIP Worker " + i))

  def doWithWorkerAsync[T](f: => Future[T])(implicit logger: Logger): Future[T] = {
    var notFinished = true
    var reply: Future[T] = Future.failed(FailedToExecuteException())

    while (notFinished) {
      Try {
        wipQueue.poll(noWorkerAlertInterval, TimeUnit.MILLISECONDS)
      } match {
        case Success(null) =>
          logger.error(s"waited for $noWorkerAlertInterval miliseconds and did not get worker, something is wrong")
        case Success(worker) => reply = f; reply.onComplete(_ => wipQueue.add(worker)); notFinished = false
        case Failure(execption) =>
          logger.error("InteruptedException while trying to poll a worker from queue"); reply = Future.failed(execption);
          notFinished = false
      }
    }

    reply
  }

  def getNumOfWorkers(): Int = wipQueue.size()

  def addWorker(): Unit = this.synchronized { numOfWorkers += 1; wipQueue.add(s"WIP Worker $numOfWorkers") }

  def removeWorker(): Unit = this.synchronized { wipQueue.remove(s"WIP Worker $numOfWorkers"); numOfWorkers -= 1 }

}

trait WIPRegulatorMBean {
  def getNumOfWorkers(): Int
  def addWorker()
  def removeWorker()
}

case class FailedToExecuteException(msg: String = "Undifiened reason") extends Exception(msg)
