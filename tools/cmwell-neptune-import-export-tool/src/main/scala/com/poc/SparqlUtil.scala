package com.poc

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

  def buildGroupedSparqlCmd(subjects: Iterable[String], allSubjGraphTriples: Iterable[List[SubjectGraphTriple]], updateMode: Boolean, withDeleted:Boolean): String = {
    var sparqlCmd = "update="
    val deleteSubj = if (updateMode) Some(subjects.map(subject => "delete where { " + encode(subject) + " ?anyPred ?anyObj};").mkString) else None
    val insertcmd = allSubjGraphTriples.flatten.filterNot(trio => predicateContainsMeta(trio, withDeleted)).map(trio => "INSERT DATA" + trio.graph.fold("{" + encode(trio.triple) + "};")(graph => "{" + "GRAPH <" + encode(graph) + ">{" + encode(trio.triple) + "}};"))
    sparqlCmd + deleteSubj.getOrElse("") + insertcmd.mkString
  }

   def encode(str: String):String = {
    URLEncoder.encode(str, "UTF-8")
  }

   def predicateContainsMeta(trio: SubjectGraphTriple, withDeleted:Boolean): Boolean = {
    if(!withDeleted) false
    else trio.triple.split(" ")(1).contains("meta/sys")
  }




}
