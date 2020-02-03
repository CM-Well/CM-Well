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
package cmwell.rts

import akka.actor.{Actor, ActorRef, ActorSelection}
import cmwell.domain.Infoton
import k.grid.Grid
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by markz on 7/9/14.
  */
case class AddSubscriber(subscriber: String, rule: Rule)
case class RemoveSubscriber(subscriber: String)

case class Publish(i: Vector[Infoton])
case class PublishOne(uuid: String)
//case class Publish(s : String)

class PublishAgent extends Actor with LazyLogging {
  // need a mapping rule (q) -> actor (label)
  var rules: Map[String, Rule] = Map.empty[String, Rule]
  var subMap: Map[String, ActorSelection] = Map.empty[String, ActorSelection]

  def publish_data(subscriber: String, i: Infoton) {
    // send
    val a = subMap(subscriber)
    logger.debug(s"Send data to $subscriber [$a]")
    a ! PublishOne(i.uuid)
  }

  def receive = {
    // add a rule to the internal map
    case AddSubscriber(subscriber: String, rule: Rule) =>
      val addr = sender().path.address
      val path = s"akka.tcp://${addr.system}@${addr.host.getOrElse("")}:${addr.port.getOrElse(0)}/user/$subscriber"
      rules += (subscriber -> rule)
      subMap += (subscriber -> context.actorSelection(path))
      logger.debug(s"AddRule rules [${rules}] sub map [${subMap}]")
    // remove the rule from the internal map
    case RemoveSubscriber(subscriber: String) =>
      rules -= (subscriber)
      subMap -= (subscriber)
      logger.debug(s"RemoveRule ${subscriber} rules [${rules}] sub map [${subMap}]")

    // this publish the infoton according the rule
    case Publish(infotonVec: Vector[Infoton]) => {
      logger.debug(s"in actor $infotonVec")
      // first lets calc
      infotonVec.foreach { i =>
        rules.foreach {
          case (subscriber, rule) =>
            rule match {
              case NoFilter =>
                publish_data(subscriber, i)
              case PathFilter(path) =>
                if (path.check(i.systemFields.path))
                  publish_data(subscriber, i)
              case MatchFilter(f) =>
                if (i.fields.isDefined && f.check(i.fields.get))
                  publish_data(subscriber, i)
              case PMFilter(p, m) =>
                if (p.check(i.systemFields.path) && i.fields.isDefined && m.check(i.fields.get))
                  publish_data(subscriber, i)
            }
        }
      }
    }
    case _ =>
      logger.debug("Error")
  }

}

object Publisher {
  val publishAgentActor: ActorRef = Grid.create(classOf[PublishAgent], "publisher")
  val p: Publisher = new Publisher(publishAgentActor)
  def init: Unit = {}
  def publish(i: Vector[Infoton]): Unit = p.publish(i)
}

class Publisher(val publishAgentActor: ActorRef) {

  def publish(i: Vector[Infoton]): Unit = {
    // no block call here
    publishAgentActor ! Publish(i)
  }

}
