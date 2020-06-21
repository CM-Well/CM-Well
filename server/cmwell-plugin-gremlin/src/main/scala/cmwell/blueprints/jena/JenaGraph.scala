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

import java.io.{OutputStream, StringReader, StringWriter}
import java.lang.Iterable

import org.apache.jena.rdf.model.{Model, ModelFactory, SimpleSelector, Statement}
import com.tinkerpop.blueprints.util.DefaultGraphQuery
import com.tinkerpop.blueprints._

import scala.collection.JavaConverters._
import scala.collection.mutable
import Extensions._

//todo Literals
//todo ========
//todo if there's only one L s.t. (S,P,L) than L would be a property of S: S[P]==L
//todo if there's more than one L s.t. (S,P,L1),(S,P,L2) than S[P] would be null, and there will be two Vertices: (S)-[P]->(L1) and (S)-[P]->(L2)
//todo that way, the user will be able to achieve both: v("URI").companyName
//todo                                                  v("URI").out("category")

//todo but... what if we support groovy arrays?

class JenaGraph(model: Model = ModelFactory.createDefaultModel) extends Graph {

  def this(rdfXml: String) = {
    this()
    this.model.read(new StringReader(rdfXml), null)
  }

  override def getEdge(id: Object): Edge = {
    val statements = model.listStatements(new SimpleSelector {
      override def selects(s: Statement): Boolean =
        !s.getObject.isLiteral && s"${s.getSubject.id}-${s.getPredicate.id}->${s.getObject.id}" == id.toString
    })
    if (statements.hasNext) {
      val statement = statements.next
      new JenaEdge(model, statement.getPredicate, statement.getObject, statement.getSubject)
    } else {
      throw new QueryException(s"Edge [$id] not present in Graph")
    }
  }

  override def getVertex(id: Object) = {
    Seq(model.listSubjects.asScala.filter(_.id == id.toString),
        model.listObjects.asScala.filter(o => o.id == id.toString && !o.isLiteral)).flatMap(identity).headOption match {
      case Some(rdfNode) => new JenaVertex(model, rdfNode)
      case None          => throw new QueryException(s"Vertex [$id] not present in Graph")
    }
  }

  override def getEdges = {
    val edges = mutable.ListBuffer[Edge]()
    val statements = model.listStatements
    while (statements.hasNext) {
      val statement = statements.next
      val predicate = statement.getPredicate
      if (!statement.getObject.isLiteral)
        edges += new JenaEdge(model, predicate, statement.getObject, statement.getSubject)
    }
    edges.asJava
  }

  override def getVertices = {
    val vertices = mutable.Set[Vertex]()
    val statements = model.listStatements
    while (statements.hasNext) {
      val statement = statements.next

      val sub = statement.getSubject
      vertices.add(new JenaVertex(model, sub))

      val obj = statement.getObject
      if (!obj.isLiteral)
        vertices.add(new JenaVertex(model, obj))
    }
    vertices.asJava
  }

  override def removeEdge(edge: Edge) =
    throw new UnsupportedOperationException("Current implementation is for a ReadOnly graph")
  override def removeVertex(vertex: Vertex) =
    throw new UnsupportedOperationException("Current implementation is for a ReadOnly graph")

  def write(out: OutputStream) = model.write(out)

  def getRDFXML = {
    val writer = new StringWriter
    model.write(writer)
    writer.getBuffer.toString
  }

  override def query(): GraphQuery = new DefaultGraphQuery(this)
  override def shutdown = {}

  override def getEdges(key: String, value: Object): Iterable[Edge] =
    throw new UnsupportedOperationException("getEdges by key,value is not supported")
  override def getVertices(key: String, value: Object): Iterable[Vertex] =
    throw new UnsupportedOperationException("getVertices by key,value is not supported")

  override def addEdge(id: Object, outVertex: Vertex, inVertex: Vertex, label: String) =
    throw new UnsupportedOperationException("Current implementation is for a ReadOnly graph")
  override def addVertex(id: Object) =
    throw new UnsupportedOperationException("Current implementation is for a ReadOnly graph")

  override def getFeatures = features
  private val features = new Features
  features.supportsDuplicateEdges = false
  features.supportsSelfLoops = true
  features.supportsSerializableObjectProperty = true
  features.supportsBooleanProperty = true
  features.supportsDoubleProperty = true
  features.supportsFloatProperty = true
  features.supportsIntegerProperty = true
  features.supportsPrimitiveArrayProperty = true
  features.supportsUniformListProperty = false
  features.supportsMixedListProperty = false
  features.supportsLongProperty = true
  features.supportsMapProperty = false
  features.supportsStringProperty = true
  features.ignoresSuppliedIds = false
  features.isPersistent = false
  features.isWrapper = true
  features.supportsIndices = false
  features.supportsVertexIndex = false
  features.supportsEdgeIndex = false
  features.supportsKeyIndices = false
  features.supportsVertexKeyIndex = false
  features.supportsEdgeKeyIndex = false
  features.supportsEdgeIteration = true
  features.supportsVertexIteration = true
  features.supportsEdgeRetrieval = true
  features.supportsVertexProperties = true
  features.supportsEdgeProperties = false
  features.supportsTransactions = false
  features.supportsThreadedTransactions = false
}

class QueryException(msg: String) extends Exception(msg) {}
