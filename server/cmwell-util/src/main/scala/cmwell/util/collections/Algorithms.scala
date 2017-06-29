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


package cmwell.util.collections

/**
  * Created by yaakov on 3/2/17.
  */
object Algorithms {
  /*
   * Union Find Algorithm
   *
   * Inspired by blog.xebia.com/the-union-find-algorithm-in-scala-a-purely-functional-implementation
   *
   * IMPORTANT: THIS IS THE NAÏVE IMPLEMENTATION AND CAN BE OPTIMIZED IN FEW WAYS (extra pointer to head, by rank, paths compression, and more).
   * NOT TO BE USED AS IS ON LARGE DATASETS!!!
   */
  class UnionFind[T](nodes: Vector[UFNode[T]]) {

    /**
      * @param t1 data of element to be connected
      * @param t2 data of element to be connected
      *
      *           Note: t2 will be parent of t1.
      *           So if you want to represent X-->Y-->Z, use .union(Y,X).union(Z,Y)
      */
    def union(t1: T, t2: T): UnionFind[T] = union(indexOf(t1), indexOf(t2))

    /**
      * @param index1 index of element to be connected
      * @param index2 index of element to be connected
      *
      *               Note: index2 will be parent of index1.
      *               So if you want to represent 1-->2-->3, use .union(2,1).union(3,2)
      */
    def union(index1: Int, index2: Int): UnionFind[T] = {
      val (root1, root2) = (find(index1), find(index2))
      if (index1 == index2 || root1 == root2) this
      else {
        val newNode1 = nodes(root1).copy(parent = Some(index2))
        new UnionFind(nodes.updated(root1, newNode1))
      }
    }

    // can add @tailrec if instead of fold we will match { case None => t case Some(p) => find(p) }
    /**
      * @param idx index of element to find its root
      * @return the index of the disjoint set representative, i.e. its root
      */
    def find(idx: Int): Int = nodes(idx).parent.fold(idx)(find)

    /**
      * @param t data of element to find its root
      * @return the data of the Disjoint Set Representative, i.e. its root
      */
    def find(t: T): T = nodes(find(indexOf(t))).data

    def isConnected(t1: Int, t2: Int): Boolean = t1 == t2 || find(t1) == find(t2)

    override def toString: String = s"UnionFind(${nodes.length})"

    private def indexOf(t: T) = nodes.zipWithIndex.find(_._1.data == t).map(_._2).getOrElse(-1)
  }

  case class UFNode[T](data: T, parent: Option[Int] = None)
}
