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

import org.apache.jena.rdf.model._
import com.tinkerpop.blueprints.Element

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import Extensions._

abstract class JenaElement(val model: Model, val rdfNode: RDFNode) extends Element {

  private val propertiesSelector = new SimpleSelector {
    override def selects(s: Statement) = s.getSubject.id == rdfNode.id && s.getObject.isLiteral
  }

  override def getProperty[T](key: String): T = {
    val it = model.listStatements(propertiesSelector)
    val values = ListBuffer[Any]()
    while(it.hasNext) {
      val next = it.next
      if(next.getPredicate.id==key)
        values += next.getObject.asLiteral.getValue
    }

    (values.size match {
      case 0 => null
      case 1 => values.head
      case _ => values.toArray
    }).asInstanceOf[T]
  }

  override def getPropertyKeys: java.util.Set[String] = model.listStatements(propertiesSelector).map(_.getPredicate.id.toString).toSet[String]

  override def equals(obj: Any): Boolean = {
    if (obj == null) {
      return false
    }
    if (getClass ne obj.getClass) {
      return false
    }
    val other: JenaVertex = obj.asInstanceOf[JenaVertex]
    if (rdfNode == null) {
      if (other.rdfNode != null) {
        return false
      }
    }
    else if (!(rdfNode == other.rdfNode)) {
      return false
    }
    return true
  }

  override def hashCode: Int = {
    val prime: Int = 31
    var result: Int = 1
    result = prime * result + (if ((rdfNode == null)) 0 else rdfNode.hashCode)
    return result
  }

  override def removeProperty[T](key: String): T = throw new UnsupportedOperationException("RDF Edge has no Props")
  override def setProperty(key: String, value: Object) = throw new UnsupportedOperationException("RDF Edge has no Props")
}
