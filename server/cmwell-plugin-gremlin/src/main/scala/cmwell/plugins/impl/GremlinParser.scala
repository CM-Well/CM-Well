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
package cmwell.plugins.impl

import javax.script.{ScriptEngine, ScriptException}

import cmwell.blueprints.jena.{JenaGraph, QueryException}
import cmwell.plugins.spi.SgEngineClient
import org.apache.jena.query.Dataset
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import com.tinkerpop.pipes.Pipe
import com.tinkerpop.pipes.util.iterators.SingleIterator

import scala.util.{Failure, Success, Try}

class GremlinParser extends SgEngineClient {

  import scala.collection.JavaConverters._

  override def eval(ds: Dataset, query: String): String = {
    val graph: com.tinkerpop.blueprints.Graph = new JenaGraph(ds.getDefaultModel) // todo figure out if Quads cannot be supported on Gremlin!!!
    val engine: ScriptEngine = new GremlinGroovyScriptEngine()
    val bindings = engine.createBindings
    bindings.put("g", graph)

    def eval = engine.eval(query, bindings)

    // evil hack:
    def extractStartElementFromQuery = {
      // should match the first v("URI") OR e("URI") and extract the URI out of it:
      """(?:[v|e]\(")(.+?)(?:"\))""".r.findFirstMatchIn(query).map(_.group(1))

      // how does it work? glad you asked:
      // (?:[v|e]\(") --> non-capturing group of v or e with a (" afterwards
      // (.+?) --> capturing group of non-greedy whatever with one or more chars
      // (?:"\)) --> non-capturing group of ")
      // get the first occurrence of that if exists, or return None

      //todo one possible improvement is to have [v|e] captured and returned along with the URI,
      //todo so the code invoking will know whether to getVertex(URI) or getEdge(URI)

      //todo 2 - does g.e("... even legal?!
    }

    def makeTypedPipe[T](starts: T) = {
      val pipe = eval // must re-eval per type, otherwise setStarts overrides itself and the universe collapses.
      val typedPipe = pipe.asInstanceOf[Pipe[T, String]]
      typedPipe.setStarts(new SingleIterator[T](starts))
      typedPipe
    }

    def read(p: Pipe[_, _]) = p.iterator().asScala.mkString("\n")

    val firstNode = extractStartElementFromQuery.map(e => Try(graph.getVertex(e)).getOrElse(graph.getEdge(e)))

    Try(eval) match {
      case Failure(e) =>
        e match {
          case e: QueryException  => s"[ Error: ${e.getMessage} ]"
          case e: ScriptException => "[ Gremlin Syntax Error ]"
        }
      case Success(r) =>
        r match {
          case p: Pipe[_, _] => {
            Seq(Some(graph), firstNode)
              .collect { case Some(x) => x }
              .map(makeTypedPipe)
              .map(
                p =>
                  Try(read(p)) match {
                    case Success(r) => Some(r)
                    case Failure(e) =>
                      e match {
                        case e: ClassCastException => None
                        case _                     => Some("[ Unknown Error ]")
                      }
                }
              )
              .collect { case Some(s) => s }
              .mkString
          }
          case null => "[ Requested element not present in Graph! ]"
          case v    => v.toString
        }
    }
  }
}
