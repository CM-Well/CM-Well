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

import akka.cluster.Member
import k.grid.monitoring.{ActiveActors, SingletonData}

/**
  * Created by michael on 7/1/15.
  */
object Formatters {
  val pad = 60
  def taple(fields: String*): String = {
    fields.map(f => f.padTo(pad, " ").mkString).mkString(" | ") + "\n"
  }

  def memberFormatter(member: Member, isLeader: Boolean): String = {
    taple(member.address.toString + (if (isLeader) "*" else ""), member.roles.mkString(", "), member.status.toString)
  }

  def membersFormatter(members: Map[GridJvm, JvmInfo]): String = {
    taple("Address") +
      members.toSeq.sortBy(m => m._1.hostname).map(m => taple(m._1.hostname)).mkString +
      s"Total: ${members.size}"
  }

  def singletonsFormatter(singletons: Set[SingletonData]): String = {
    taple("Singleton", "Role", "Location") + singletons.map(s => taple(s.name, s.role, s.location)).mkString
  }

  def activeActorsFormatter(actors: Set[ActiveActors]): String = {
    taple("Member", "Actor", "Latency") +
      actors.map(a => a.actors.map(aa => taple(a.host, aa.name, aa.latency.toString)).mkString).mkString
  }
}
