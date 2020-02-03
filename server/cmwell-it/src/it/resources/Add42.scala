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


import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.expr.{ExprList, NodeValue}
import org.apache.jena.sparql.expr.nodevalue.NodeValueString
import org.apache.jena.sparql.function.FunctionEnv

import scala.collection.JavaConversions._

class Add42 extends org.apache.jena.sparql.function.Function {

  override def build(uri: String, args: ExprList): Unit = { }

  override def exec(binding: Binding, args: ExprList, uri: String, env: FunctionEnv): NodeValue = {
    val `var` = args.getList.headOption.getOrElse(new NodeValueString("arg0")).asVar()
    val res: String = `var`.asNode() match {
      case n if n.isURI => n.getURI
      case n if n.isLiteral => n.getLiteral.toString()
      case n if n.isVariable => Option(binding.get(`var`)).fold("Could not bind")(_.getLiteral.toString())
      case _ => "???"
    }
    new NodeValueString("42_" + res)
  }
}
