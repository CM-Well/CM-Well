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
package cmwell.util.algorithms.test

import cmwell.util.algorithms.{UFNode, UnionFind}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

/**
  * Created by yaakov on 3/2/17.
  */
// format: off
class AlgorithmsSpec extends PropSpec with PropertyChecks with Matchers {
  /*
   * Test case:
   *
   *  0  1--2
   *  |  |
   *  3  4  5
   *
   *  Using Strings ("one","two",etc.) as data of nodes.
   */
  val uf: UnionFind[String] = new UnionFind(Vector(UFNode("one"), UFNode("two"), UFNode("three"), UFNode("four"), UFNode("five"), UFNode("six"))).
    union(3, 0).
    union(4, 1).
    union("three", "two")

  property("UnionFind.isConnected") {
    val cases = Table(
      ("node1", "node2", "connectivity"),
      (0      , 1      , false         ),
      (0      , 2      , false         ),
      (0      , 3      , true          ),
      (1      , 2      , true          ),
      (1      , 4      , true          ),
      (1      , 5      , false         ),
      (5      , 0      , false         ),
      (2      , 4      , true          )
    )
    forAll(cases) { (node1: Int, node2: Int, expectedConnectivity: Boolean) =>
      uf.isConnected(node1, node2) should be(expectedConnectivity)
    }
  }

  property("UnionFind.find") {
    val cases = Table(
      ("node", "root"),
      (0     , 0     ),
      (1     , 1     ),
      (2     , 1     ),
      (3     , 0     ),
      (4     , 1     ),
      (5     , 5     )
    )
    forAll(cases) { (node: Int, expectedRoot: Int) => uf.find(node) should be(expectedRoot) }
  }

  property("UnionFind.find (by content)") {
    uf.find("four") should be("one")
  }
}
// format: on
