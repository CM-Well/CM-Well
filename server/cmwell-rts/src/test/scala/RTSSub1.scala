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


import cmwell.rts.{Rule, PathFilter, Subscriber}
import k.grid.{GridConnection, Grid}
import scala.io.StdIn

/**
 * Created by markz on 7/13/14.
 */
object RTSSub1 extends App {


//  Grid.roles = Set("subscriber")
  Grid.setGridConnection(GridConnection(memberName = "rts", hostName = "127.0.0.1", seeds = Set("127.0.0.1:2551"), port = 0))
  Grid.joinClient
  Subscriber.init
  Thread.sleep(5000)
  println("-----------------------------------------")
  // now lets subscribe
  println("Press enter subscriber.")
  StdIn.readLine()
//  Subscriber.subscribe("sub02", Rule("/cmt/cm/command-test", true))
  //Subscriber.subscribe("sub02", Rule())
  println("wait.")
  StdIn.readLine()
}
