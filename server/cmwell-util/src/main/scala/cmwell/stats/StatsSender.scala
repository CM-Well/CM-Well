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
package cmwell.stats

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.util.Calendar
import java.text.SimpleDateFormat
import akka.actor.{Actor, ActorSystem, Props}
import akka.actor.Actor.Receive

/**
  * Created with IntelliJ IDEA.
  * User: Michael
  * Date: 4/3/14
  * Time: 12:26 PM
  * To change this template use File | Settings | File Templates.
  */
case class Message(msg: String, host: String, port: Int)

class SenderActor extends Actor {
  private val dsocket = new DatagramSocket()

  sys.addShutdownHook {
    dsocket.close()
  }

  override def receive: Receive = {
    case Message(msg, host, port) =>
      val address = InetAddress.getByName(host)
      val packet = new DatagramPacket(msg.getBytes(), msg.length, address, port)
      dsocket.send(packet)
  }
}

class StatsSender(path: String, host: String = "localhost", port: Int = 8125) {

  object Sender {
    val system = ActorSystem("mySystem")
    val actor = system.actorOf(Props[SenderActor], "SenderActor")
    def send(message: String) {
      actor ! Message(message, host, port)
    }

  }

  private def getCurrentTimeStr: String = {
    val now = Calendar.getInstance().getTime()
    val dateFormat = new SimpleDateFormat("ddMMyyyy_hhmm")
    dateFormat.format(now)
  }

  private def getMachineName: String = {
    java.net.InetAddress.getLocalHost().getHostName().split('.')(0)
  }

  private def getName(p: String, action: String): String = {
    p.replace("{MachineName}", getMachineName).replace("{DateTime}", getCurrentTimeStr) + "." + action
      .replace(".", "-")
      .replace(" ", "_")
  }

  def sendCounts(action: String, num: Int) {
    val message = getName(path, action) + ":" + num + "|c"
    Sender.send(message)
  }

  def sendTimings(action: String, num: Int) {
    val message = getName(path, action) + ":" + num + "|ms"
    Sender.send(message)
  }

  def sendGauges(action: String, num: Int) {
    val message = getName(path, action) + ":" + num + "|g"
    Sender.send(message)
  }

  def sendSets(action: String) {
    val message = getName(path, action) + "|s"
    Sender.send(message)
  }
}
