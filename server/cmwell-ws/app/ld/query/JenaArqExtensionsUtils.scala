package ld.query

import cmwell.fts._
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.query._
import com.typesafe.scalalogging.LazyLogging
import ld.cmw.PassiveFieldTypesCache
import ld.query.JenaArqExtensionsUtils._
import logic.CRUDServiceFS
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.query.{Query, QueryExecution, QueryExecutionFactory}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.core.Quad
import wsutil.{HashedFieldKey, RawFieldFilter, RawMultiFieldFilter, RawSingleFieldFilter}

import scala.annotation.switch
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.DurationInt
import scala.util.Try


object JenaArqExtensionsUtils {

  case class BakedSparqlQuery(qe: QueryExecution, driver: DatasetGraphCmWell)


  val nbgSymbol = org.apache.jena.sparql.util.Symbol.create("nbg")
  val emptyLtrl = NodeFactory.createLiteral("")

  def buildCmWellQueryExecution(query: Query,
                                host: String,
                                config: Config = Config.defaultConfig,
                                nbg: Boolean,
                                crudServiceFS: CRUDServiceFS,
                                arqCache: ArqCache,
                                jenaArqExtensionsUtils: JenaArqExtensionsUtils,
                                dataFetcher: DataFetcher)(implicit ec: ExecutionContext) = {
    val driver = new DatasetGraphCmWell(host, config.copy(deadline = Some(config.finiteDuarationForDeadLine.fromNow)), nbg, crudServiceFS, arqCache, jenaArqExtensionsUtils, dataFetcher)
    val model = ModelFactory.createModelForGraph(driver.getDefaultGraph)
    val qe = QueryExecutionFactory.create(query, model) // todo quads

    qe.getContext.set(nbgSymbol,nbg)
    BakedSparqlQuery(qe, driver)
  }

  def mangleSubjectVariableNameIntoPredicate(t: Triple): Triple =
    new Triple(t.getSubject, mangleSubjectVariableNameIntoPredicate(t.getSubject, t.getPredicate), t.getObject)

  def mangleSubjectVariableNameIntoPredicate(key: String, t: Triple): Triple =
    new Triple(t.getSubject, mangleSubjectVariableNameIntoPredicate(NodeFactory.createLiteral(key), t.getPredicate), t.getObject)

  def mangleSubjectVariableNameIntoPredicate(s: Node, p: Node): Node = {
    val varName = if(s.isVariable) s.getName else ""
    NodeFactory.createURI(varName + manglingSeparator + p.getURI)
  }

  def unmanglePredicate(p: Node): (String,Node) = {
    if(!p.isURI || !p.getURI.contains(manglingSeparator)) "" -> p else {
//      val idxOfSep = p.getURI.indexOf(JenaArqExtensionsUtils.manglingSeparator)
//      val (subVarName, sepAndPred) = p.getURI.splitAt(idxOfSep)
      val (subVarName, sepAndPred) = p.getURI.span(manglingSeparator.!=)
      subVarName -> NodeFactory.createURI(sepAndPred.substring(1))
    }
  }

  def squashBySubject(triples: Seq[Triple]): Triple = {
    (triples.length: @switch) match {
      case 0 =>
        throw new IllegalArgumentException("squash of empty list")
      case 1 =>
        triples.head // not squashing
      case _ =>
        val containerPredicate: Node = NodeFactory.createURI(engineInternalUriPrefix + triples.map { t =>
          t.getPredicate.getURI.replace(cmwellInternalUriPrefix, "") + ":" + objectToQpValue(t.getObject).getOrElse("")
        }.mkString("|"))

        val emptyLiteral = NodeFactory.createLiteral("")
        val mangledContainerPredicate = JenaArqExtensionsUtils.mangleSubjectVariableNameIntoPredicate(triples.head.getSubject, containerPredicate)

        new Triple(emptyLiteral, mangledContainerPredicate, emptyLiteral)
    }
  }

  def objectToQpValue(obj: Node) = {
    if(obj.isURI)
      Some(obj.getURI)
    else if(obj.isLiteral)
      Some(obj.getLiteral.getValue.toString)
    else
      None // making qp just `field:`
  }

  def explodeContainerPredicate(pred: Node): Seq[(String,String)] = pred
    .getURI
    .replace(engineInternalUriPrefix,"")
    .split('|')
    .map(cmwell.util.string.splitAtNoSep(_, ':'))

  val cmwellInternalUriPrefix = "cmwell://meta/internal/"
  val engineInternalUriPrefix = "engine://"

  val manglingSeparator = '$'

  val fakeQuad: Quad = {
    // by having an empty subject and an empty object, this triple will never be visible to user as it won't be bind to anything. even for `select *` it's hidden
    new Quad(emptyLtrl, emptyLtrl, NodeFactory.createURI(engineInternalUriPrefix + "FAKE"), emptyLtrl)
  }

  def isConst(node: Node) = node != Node.ANY

  def queryToSseString(query: Query): String = Algebra.compile(query).toString(query.getPrefixMapping)
}

class JenaArqExtensionsUtils(arqCache: ArqCache, nbg: Boolean, typesCache: PassiveFieldTypesCache, cmwellRDFHelper: CMWellRDFHelper, dataFetcher: DataFetcher) extends LazyLogging {

  def predicateToInnerRepr(predicate: Node): Node = {
    if(!predicate.isURI) predicate else {
      val nsIdentifier = arqCache.namespaceToHash(predicate.getNameSpace)
      val localNameDotHash = predicate.getLocalName + "." + nsIdentifier // no dollar sign!
      NodeFactory.createURI(cmwellInternalUriPrefix + localNameDotHash)
    }
  }

  def innerReprToPredicate(innerReprPred: Node): Node = {
    if(innerReprPred.getURI.contains("/meta/sys") || innerReprPred.getURI.contains("msg://") || innerReprPred.getURI.contains(engineInternalUriPrefix+"FAKE")) innerReprPred else {
      val (hash, localName) = {
        val splt = innerReprPred.getURI.replace(cmwellInternalUriPrefix, "").split('#')
        splt(0) -> splt(1)
      }
      val nsUri = arqCache.hashToNamespace(hash)
      NodeFactory.createURI(nsUri + localName)
    }
  }

  def normalizeAsOutput(q: Quad): Quad =
    new Quad(q.getGraph, q.getSubject, innerReprToPredicate(q.getPredicate), q.getObject)


  private def defaultAmountCounter(t: Triple, graph: Option[CmWellGraph] = None) = {
    val ff = predicateToFieldFilter(t.getPredicate)(scala.concurrent.ExecutionContext.global) //TODO: don't use global, pass ec properly
    val amount = dataFetcher.count(ff)

    graph.collect { case g if g.dsg.config.explainOnly => g.dsg }.foreach { dsg =>
      val pred = Try(unmanglePredicate(t.getPredicate)._2).toOption.getOrElse(t.getPredicate).getURI.replace(cmwellInternalUriPrefix, "")
      dsg.logMsgOnce("Expl", s"Objects count for $pred: $amount")
    }

    amount
  }

  // todo add DocTest :)
  def sortTriplePatternsByAmount(triplePatterns: Iterable[Triple], amountCounter: (Triple, Option[CmWellGraph]) => Long = defaultAmountCounter)
                                (graph: CmWellGraph): Seq[Triple] = {
    def isVar(t: Triple) =
      t.getSubject.isVariable || (t.getSubject.isLiteral && t.getSubject.getLiteralValue == "")

    def isEngine(t: Triple) = t.getPredicate.getURI.contains(engineInternalUriPrefix)

    val allSorted = Vector.newBuilder[Triple]
    val actualSubBuilder = Vector.newBuilder[Triple]
    val engineSubBuilder = Vector.newBuilder[Triple]
    allSorted.sizeHint(triplePatterns.size)

    triplePatterns.foreach { triple =>
      if(!isVar(triple)) allSorted += triple // const subjects come first.
      else if(isEngine(triple)) engineSubBuilder += triple
      else actualSubBuilder += triple
    }

    val engineTriples = engineSubBuilder.result()
    val actualTriples = actualSubBuilder.result()

    val squashedSubjects = engineTriples.map(t => unmanglePredicate(t.getPredicate)._1).toSet
    def isSquashed(t: Triple): Boolean = squashedSubjects(t.getSubject.getName)

    val (squashedActualTriples, nonSquashedActualTriples) = actualTriples.partition(isSquashed)
    val groupedByEngine = squashedActualTriples.groupBy(_.getSubject.getName)

    (engineTriples ++ nonSquashedActualTriples).sortBy(amountCounter(_,Option(graph))).foreach { triple =>
      allSorted += triple
      if(isEngine(triple)) {
        val tripleSubName = unmanglePredicate(triple.getPredicate)._1
        allSorted ++= groupedByEngine(tripleSubName)
      }
    }

    allSorted.result()
  }

  def predicateToFieldFilter(p: Node, obj: Node = emptyLtrl)(implicit ec: scala.concurrent.ExecutionContext): FieldFilter = {

    val unmangled@(subVarName,pred) = unmanglePredicate(p)

    logger.trace(s"unmangled = $unmangled")

    def toExplodedMangledFieldFilter(qp: String, value: Option[String]) = {
      val (localName, hash) = { val splt = qp.split('.'); splt(0) -> splt(1) }
      RawSingleFieldFilter(Must, Equals, Right(HashedFieldKey(localName, hash)), value)
    }

    def evalAndAwait(rff: RawFieldFilter) = {
      //      def filterNot(ff: FieldFilter)(pred: SingleFieldFilter => Boolean): FieldFilter = ???
      //
      //      def isTypeIncompatible(sff: SingleFieldFilter) =
      //        sff.name.startsWith("d$") && sff.value.fold(false)(FDate.isDate)

      /*val evalRes =*/ Await.result(RawFieldFilter.eval(rff,typesCache,cmwellRDFHelper,nbg), 9.seconds)
      //      filterNot(evalRes)(isTypeIncompatible)
    }

    def noneIfEmpty(s: String): Option[String] = if(s.isEmpty) None else Some(s)

    if(pred.getURI.contains(JenaArqExtensionsUtils.engineInternalUriPrefix)) {
      val fieldFilters = JenaArqExtensionsUtils.explodeContainerPredicate(pred).map { case t@(name,value) =>
        logger.trace(s"explodeContainerPredicate result = $t")
        toExplodedMangledFieldFilter(name, noneIfEmpty(value))
      }
      evalAndAwait(RawMultiFieldFilter(Must, fieldFilters))
    } else {
      val value = JenaArqExtensionsUtils.objectToQpValue(obj)
      if (!isConst(pred)) {
        SingleFieldFilter(Must, Contains, "_all", value)
      } else {
        val qp = pred.getURI.replace(JenaArqExtensionsUtils.cmwellInternalUriPrefix,"")
        val rff = toExplodedMangledFieldFilter(qp, value.flatMap(noneIfEmpty))
        logger.trace(s"toExplodedMangledFieldFilter result = $rff")
        evalAndAwait(rff)
      }
    }
  }
}
