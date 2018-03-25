/**
  * Copyright 2015 Thomson Reuters
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
import cmwell.rts._
import k.grid.{Grid, GridConnection}
import scala.io.StdIn

/**
  * Created by markz on 7/13/14.
  */
object RTSSub extends App {

  val ip: String = "127.0.0.1"

//  Grid.roles = Set("subscriber")

  Grid.setGridConnection(GridConnection(memberName = "rts", hostName = ip, seeds = Set("127.0.0.1:2551"), port = 0))
  Grid.joinClient
  //Grid.join(Set("127.0.0.1"),0,Set("subscriber"))
  Subscriber.init
  Thread.sleep(5000)
  println("-----------------------------------------")
  // now lets subscribe
  println("Press enter to subscriber.")
  StdIn.readLine()

  //val key = Subscriber.subscribe("sub01", Rule("/cmt/cm/command-test", true), Push("http://www.cool.com/"))
  //println(s"the key to use $key")
//  Subscriber.subscribe("sub02", Rule("/cmt/cm/command-test", false), Pull)
//  Subscriber.subscribe("sub03", Rule())
  StdIn.readLine()
  println("Press enter to unsubscriber.")
//  Subscriber.unsubscribe(key)
//  Subscriber.unsubscribe("sub03")
  println("wait.")
  StdIn.readLine()
  Grid.shutdown
}
