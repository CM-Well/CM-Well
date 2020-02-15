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
package cmwell.util

import com.typesafe.scalalogging.LazyLogging

import scala.xml._
import scala.xml.transform._

/**
  * Created by gilad on 5/12/14.
  */
package object xml extends LazyLogging {
  private def addChild(n: Node, newChild: Node) = n match {
    case Elem(prefix, label, attribs, scope, child @ _*) =>
      Elem(prefix, label, attribs, scope, true, child ++ newChild: _*)
    case _ => {
      logger.error("Can only add children to elements!")
      n
    }
  }

  private class AddChildrenTo(label: String, newChild: Node) extends RewriteRule {
    override def transform(n: Node) = n match {
      case n @ Elem(_, `label`, _, _, _*) => addChild(n, newChild)
      case other                          => other
    }
  }

  def addChildInXml(xmlDoc: Node, parentNameInXmlDoc: String, childToAddUnderParent: Node): Node =
    new RuleTransformer(new AddChildrenTo(parentNameInXmlDoc, childToAddUnderParent)).transform(xmlDoc).head
}
