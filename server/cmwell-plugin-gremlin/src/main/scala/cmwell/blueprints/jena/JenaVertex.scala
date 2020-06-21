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
package cmwell.blueprints.jena

import java.lang.Iterable

import org.apache.jena.rdf.model._
import com.tinkerpop.blueprints.util.{DefaultVertexQuery, StringFactory}
import com.tinkerpop.blueprints.{Direction, Edge, Vertex, VertexQuery}

import scala.collection.JavaConverters._
import scala.collection.mutable
import Extensions._

class JenaVertex(model1: Model, rdfNode1: RDFNode) extends JenaElement(model1, rdfNode1) with Vertex {

  override def getId = rdfNode.id

  override def getEdges(direction: Direction, labels: String*): Iterable[Edge] = {
    val labelsSet = labels.toSet
    val edges = mutable.Set[Edge]()

    val statements = model.listStatements(getSelector(direction))
    while (statements.hasNext) {
      val statement = statements.next
      if (!statement.getObject.isLiteral) {
        val edge = new JenaEdge(model, statement.getPredicate, statement.getObject, statement.getSubject)
        if (labelsSet.isEmpty || labelsSet(edge.getLabel))
          edges += edge
      }
    }
    edges.asJava
  }

  override def getVertices(direction: Direction, labels: String*): Iterable[Vertex] = {
    val labelsSet = labels.toSet
    val vertices = mutable.Set[Vertex]()

    val statements = model.listStatements(getSelector(direction))
    while (statements.hasNext) {
      val statement = statements.next
      if (labelsSet.isEmpty || labelsSet(statement.getPredicate.id.toString)) {
        val s = statement.getSubject
        val o = statement.getObject
        vertices += new JenaVertex(model, if (rdfNode == s) o else s)
      }
    }
    vertices.asJava
  }

  override def query(): VertexQuery = new DefaultVertexQuery(this)
  override def toString = StringFactory.vertexString(this)

  override def addEdge(s: String, vertex: Vertex): Edge =
    throw new UnsupportedOperationException("Current implementation is for a ReadOnly graph")
  override def remove(): Unit =
    throw new UnsupportedOperationException("Current implementation is for a ReadOnly graph")
  override def removeProperty[T](key: String): T =
    throw new UnsupportedOperationException("Current implementation is for a ReadOnly graph")

  private def getSelector(dir: Direction): Selector = dir match {
    case Direction.IN =>
      new SimpleSelector {
        override def selects(s: Statement): Boolean = s.getObject.isSameAs(rdfNode) && !s.getObject.isLiteral
      }
    case Direction.OUT =>
      new SimpleSelector {
        override def selects(s: Statement): Boolean = s.getSubject.isSameAs(rdfNode) && !s.getObject.isLiteral
      }
    case Direction.BOTH =>
      new SimpleSelector {
        override def selects(s: Statement): Boolean =
          (s.getSubject.isSameAs(rdfNode) || s.getObject.isSameAs(rdfNode)) && !s.getObject.isLiteral
      }
    case _ => throw new IllegalArgumentException("Direction")
  }
}
