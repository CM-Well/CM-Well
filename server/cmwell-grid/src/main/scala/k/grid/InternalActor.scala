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
package k.grid

import akka.actor.{Actor, ActorRef, Props}

/**
  * Created by markz on 6/30/14.
  */
case class WatchMe(actorRef: ActorRef)
case class UnWatchMe(actorRef: ActorRef)
case class Create(props: Props, name: String)

case object GetGridHosts
case class GridHosts(hosts: Set[String])

class InternalActor extends Actor {

  var actors = Set.empty[ActorRef]

  def receive = {
    case Create(p, name) =>
      // lets create first the actor
      val newActorRef = context.system.actorOf(p, name)
      // send to all nodes a register watch operation for a specific actor
      Grid.members.foreach { m =>
        val actorFullPath = m.toString + "/user/" + name
        val actorRef = context.system.actorSelection(actorFullPath)
        actorRef ! WatchMe(newActorRef)
      }

    case WatchMe(actorRef: ActorRef) =>
      actors += actorRef
      context.watch(actorRef)
    case UnWatchMe(actorRef: ActorRef) =>
      actors -= actorRef
      context.unwatch(actorRef)

    case GetGridHosts => sender ! GridHosts(Grid.members.map(_.host.getOrElse("")))

  }
}
