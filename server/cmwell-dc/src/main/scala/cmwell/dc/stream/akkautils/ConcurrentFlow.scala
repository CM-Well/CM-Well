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
package cmwell.dc.stream.akkautils

import akka.NotUsed
import akka.stream.{FlowShape, Graph}
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Merge}

/**
  * Created by eli on 25/07/16.
  */
object ConcurrentFlow {

  def apply[I, O](
    parallelism: Int
  )(flow: Graph[FlowShape[I, O], NotUsed]): Graph[FlowShape[I, O], NotUsed] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val balancer = builder.add(Balance[I](parallelism))
      val merger = builder.add(Merge[O](parallelism))
      for (i <- 0 until parallelism) {
        balancer.out(i) ~> flow.async ~> merger.in(i)
      }
      FlowShape(balancer.in, merger.out)
    }
}
