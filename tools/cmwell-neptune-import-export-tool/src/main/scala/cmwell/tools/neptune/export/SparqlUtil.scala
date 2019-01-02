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
package cmwell.tools.neptune.export

import java.io.ByteArrayOutputStream
import java.net.URLEncoder

import org.apache.jena.graph.Graph
import org.apache.jena.riot.{Lang, RDFDataMgr}

object SparqlUtil {



   def extractSubjectFromTriple(triple: String):String = {
    triple.split(" ")(0)
  }

   def getTriplesOfSubGraph(subGraph:Graph):String  = {
    val tempOs = new ByteArrayOutputStream
    RDFDataMgr.write(tempOs, subGraph, Lang.NTRIPLES)
    new String(tempOs.toByteArray, "UTF-8")
  }

  def buildGroupedSparqlCmd(subjects: Iterable[String], allSubjGraphTriples: Iterable[List[SubjectGraphTriple]], updateMode: Boolean): String = {
    var sparqlCmd = "update="
    val deleteSubj = if (updateMode) Some(subjects.map(subject => "delete where { " + encode(subject) + " ?anyPred ?anyObj};").mkString) else None
    val insertcmd = allSubjGraphTriples.flatten.filterNot(trio => predicateContainsMeta(trio)).map(trio => "INSERT DATA" + trio.graph.fold("{" + encode(trio.triple) + "};")(graph => "{" + "GRAPH <" + encode(graph) + ">{" + encode(trio.triple) + "}};"))
    sparqlCmd + deleteSubj.getOrElse("") + insertcmd.mkString
  }

   def encode(str: String):String = {
    URLEncoder.encode(str, "UTF-8")
  }

   def predicateContainsMeta(trio: SubjectGraphTriple): Boolean = {
    trio.triple.split(" ")(1).contains("meta/sys")
  }

}
