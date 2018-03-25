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


package cmwell.blueprints.jena

import org.apache.jena.rdf.model.{Model, RDFNode}
import com.tinkerpop.blueprints.util.StringFactory
import com.tinkerpop.blueprints.{Direction, Edge}
import Extensions._

class JenaEdge(model1: Model, rdfNode1: RDFNode, inVertex1: RDFNode, outVertex1: RDFNode) extends JenaElement(model1, rdfNode1) with Edge {

  val inVertex = inVertex1
  val outVertex = outVertex1

  override def getId = s"${outVertex.id}-${rdfNode.id}->${inVertex.id}"

  override def getVertex(dir: Direction) = dir match {
    case Direction.IN => new JenaVertex(model, inVertex)
    case Direction.OUT => new JenaVertex(model, outVertex)
    case _ => throw new IllegalArgumentException("Edge only have IN and OUT vertices")
  }

  override def getLabel = rdfNode.asResource.getURI
  override def toString = StringFactory.edgeString(this)

  override def equals(obj: Any): Boolean = {
    if (obj == null) {
      return false
    }
    if (getClass ne obj.getClass) {
      return false
    }
    val other: JenaVertex = obj.asInstanceOf[JenaVertex]

    this.getId == other.getId
  }

  override def hashCode: Int = {
    val prime: Int = 31
    var result: Int = 1
    result = prime * result + (if ((rdfNode == null)) 0 else rdfNode.hashCode)
    return result
  }

  override def getProperty[T](key: String): T = throw new UnsupportedOperationException("RDF Edge has no Props")
  override def removeProperty[T](key: String): T = throw new UnsupportedOperationException("Current implementation is for a ReadOnly graph")
  override def setProperty(key: String, value: Object) = throw new UnsupportedOperationException("Current implementation is for a ReadOnly graph")
  override def remove(): Unit = throw new UnsupportedOperationException("Current implementation is for a ReadOnly graph")
}
