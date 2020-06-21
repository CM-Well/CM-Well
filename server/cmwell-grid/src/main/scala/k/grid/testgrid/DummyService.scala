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
package k.grid.testgrid

import akka.actor.Actor
import akka.actor.Actor.Receive
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid
import k.grid.dmap.api.MapData
import k.grid.dmap.impl.persistent.PersistentDMap
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 4/20/16.
  */
case class DummyMessage(str: String)
case class WriteToPersistentDMap(md: MapData, delay: FiniteDuration = 1.seconds)

class DummyService extends Actor with LazyLogging {

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    logger.info(" *** Starting DummyService")
  }

  override def receive: Receive = {
    case msg @ DummyMessage(str) => {
      logger.info(s" *** DummyService Received $msg")
      sender ! str
    }

    case msg @ WriteToPersistentDMap(md, delay) => {
      logger.info(s" *** DummyService Received $msg")
      Grid.system.scheduler.scheduleOnce(delay) {
        md.m.foreach { tuple =>
          PersistentDMap.set(tuple._1, tuple._2)
        }
      }
    }
  }
}
