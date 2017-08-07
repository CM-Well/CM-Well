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


package cmwell.web.ld.query

import java.util
import java.util.concurrent.TimeUnit
import java.util.function.{Function, Predicate}
import javax.inject.Inject

import cmwell.crashableworker.WorkerMain
import cmwell.domain._
import cmwell.fts._
import cmwell.syntaxutils._
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import controllers.{JenaUtils, NbgToggler, SpHandler}
import ld.query.{ArqCache, JenaArqExtensionsUtils}
import logic.CRUDServiceFS
import org.apache.jena.graph.impl.GraphBase
import org.apache.jena.graph.{Graph, Node, NodeFactory, Triple}
import org.apache.jena.query._
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.sparql.ARQInternalErrorException
import org.apache.jena.sparql.algebra.{Algebra, Op}
import org.apache.jena.sparql.algebra.op.OpSlice
import org.apache.jena.sparql.core._
import org.apache.jena.sparql.engine._
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.engine.iterator.{QueryIterRoot, QueryIterTriplePattern}
import org.apache.jena.sparql.engine.main.{QueryEngineMain, StageBuilder, StageGenerator}
import org.apache.jena.sparql.util.Context
import org.apache.jena.util.iterator.ExtendedIterator
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.apache.jena.sparql.engine.{ExecutionContext => JenaExecutionContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

//TODO     Don't mess with low level strings. Just like mangle and unmangle methods,
//TODO     have more util methods such as isEngine or isInternal etc.

/**
  * Created by yaakov on 11/30/15.
  */
object JenaArqExtensions {
  def get(n: JenaArqExtensionsUtils,o: JenaArqExtensionsUtils) = new JenaArqExtensions(n,o)
}

class JenaArqExtensions private(nJenaArqExtensionsUtils: JenaArqExtensionsUtils,oJenaArqExtensionsUtils: JenaArqExtensionsUtils) extends LazyLogging {
  // init extension1
  val globalArqContext = ARQ.getContext
  val originalStageGenerator = globalArqContext.get(ARQ.stageGenerator).asInstanceOf[StageGenerator]
  StageBuilder.setGenerator(globalArqContext, new SortingAndMappingStageGenerator(nJenaArqExtensionsUtils, oJenaArqExtensionsUtils, Option(originalStageGenerator)))

  // init extension2
  val factory = new EmbedLimitQueryEngineFactory()
  QueryEngineRegistry.addFactory(factory)

  logger.info("JenaArqExtensions were applied.")
}

/**
  * Sorting the basicPattern (list of triples) and also convert all predicates to server-repr (i.e. localName.hash)
  */
class SortingAndMappingStageGenerator(nJenaArqExtensionsUtils: JenaArqExtensionsUtils, oJenaArqExtensionsUtils: JenaArqExtensionsUtils, original: Option[StageGenerator] = None) extends StageGenerator with LazyLogging {
  override def execute(basicPattern: BasicPattern, queryIterator: QueryIterator, ec: JenaExecutionContext): QueryIterator = {
    val nbg = ec.getContext.get[Boolean](JenaArqExtensionsUtils.nbgSymbol)
    val jenaArqExtensionsUtils = {
      if (nbg) nJenaArqExtensionsUtils
      else oJenaArqExtensionsUtils
    }
    ec.getActiveGraph match {
      case graph: CmWellGraph =>
        logger.info(s"[arq][FLOW] execute was invoked with ${basicPattern.getList.length} triplePatterns")

        val needToOptimize = basicPattern.getList.length > 1 && !graph.dsg.config.doNotOptimize

        if(needToOptimize)
          graph.dsg.logVerboseMsg("Plan", s"Optimizing ${basicPattern.getList.length} statements...")

        val mappedTriplePatterns = Try(basicPattern.getList.map { trpl =>
          val internalReprPredicate = jenaArqExtensionsUtils.predicateToInnerRepr(trpl.getPredicate)
          new Triple(trpl.getSubject, internalReprPredicate, trpl.getObject)
        }) match {
          case Success(triples) => triples
          case Failure(e) => e match {
            case err: NamespaceException => graph.dsg.msgs += "Error" -> err.toString; List[Triple]()
            case _ => graph.dsg.msgs += "Error" -> e.getMessage; List[Triple]()
          }
        }

        val all = if(needToOptimize) {
          val (varSubAndConstObj, others) = mappedTriplePatterns.partition(stmt => stmt.getSubject.isVariable && stmt.getObject.isConcrete)
          val bySubject = varSubAndConstObj.groupBy(_.getSubject.getName)
          val squashed = bySubject.map { case (_, gTriples) => JenaArqExtensionsUtils.squashBySubject(gTriples) }
          val markedOriginal = bySubject.flatMap {
            case (_, trpls) if trpls.length == 1 => Seq.empty[Triple] // no need to cache non-squashed triple - no mangling needed
            case (subName, trpls) => trpls.map(JenaArqExtensionsUtils.mangleSubjectVariableNameIntoPredicate)
          }
          val markedConstSub = others.map(JenaArqExtensionsUtils.mangleSubjectVariableNameIntoPredicate("", _))
          (squashed ++ markedConstSub ++ markedOriginal).toSeq
        } else mappedTriplePatterns

        val sorted = if(needToOptimize) jenaArqExtensionsUtils.sortTriplePatternsByAmount(all)(graph) else all

        if(graph.dsg.config.explainOnly)
          graph.dsg.logMsg("Expl", sorted.map { t =>
            Try {
              val noarmalizedPred = NodeFactory.createURI(JenaArqExtensionsUtils.unmanglePredicate(t.getPredicate)._2.getURI.
                replace(JenaArqExtensionsUtils.cmwellInternalUriPrefix, "").
                replace(JenaArqExtensionsUtils.engineInternalUriPrefix, "Cache += "))
              new Triple(t.getSubject, noarmalizedPred, t.getObject)
            }.getOrElse(t)
          }.mkString("Sorted Triple Patterns:\n\t","\n\t",""))

        val modifiedQueryIterator = if(graph.dsg.config.explainOnly) QueryIterRoot.create(ec) // empty iterator
                                    else sorted.foldLeft(queryIterator) { (qI, p) => new QueryIterTriplePattern(qI, p, ec) }

        if(!graph.dsg.msgs.exists { case(_,v) => v.contains("Planning completed.") })
          graph.dsg.logVerboseMsg("Plan", "Planning completed.")

        if(!graph.dsg.msgs.exists { case(_,v) => v.contains("Executing started.") })
          graph.dsg.logVerboseMsg("Exec", "Executing started.")

        modifiedQueryIterator

      case _ if original.isDefined =>
        original.get.execute(basicPattern, queryIterator, ec)

      case _ =>
        logger.error("Jena ARQ has no registered StageGenerator. That should never happen.")
        !!!
    }
  }
}


object DataFetcher {
  case class Chunk(scrollId: String, data: Seq[Infoton], state: State)

  case class State(intermediateLimit: Long, currentCount: Long, total: Long) {
    def isExausted = currentCount >= Math.min(total, intermediateLimit)
  }

  case class ScrollResults(total: Long, results: Stream[Chunk])
}

trait DataFetcher {
  def nbg: Boolean
  def crudServiceFS: CRUDServiceFS
  def config: Config
  def chunkSize: Int
  def singleGetThreshold: Int

  import DataFetcher._

  private def intermediateLimit = config.intermediateLimit

  def count(ff: FieldFilter): Long =
    Await.result(crudServiceFS.thinSearch(Some(PathFilter("/", descendants = true)), Some(ff), Some(DatesFilter(None, None)), PaginationParams(0, 1), withHistory = false), 9.seconds).total

  /**
    * fetch returns value is (Long,Seq[Infoton]) which is the amount of total results,
    * and a Sequence which might be a List or a lazy evaluated Stream
    */
  def fetch(ff: FieldFilter): (Long,Seq[Infoton])  = {
    val amount = count(ff)
    if(amount < singleGetThreshold) {
      amount -> get(ff)
    } else {
      val scrl = scroll(ff)
      scrl.total -> scrl.results.flatMap(_.data)
    }
  }

  private def get(ff: FieldFilter): Seq[Infoton] =
    Await.result(crudServiceFS.search(Some(PathFilter("/", descendants = true)), Some(ff), Some(DatesFilter(None, None)), PaginationParams(0, singleGetThreshold), withHistory = false, withData = true), 9.seconds).infotons

  private def scroll(ff: FieldFilter) = {
    val first = Await.result(crudServiceFS.startScroll(Some(PathFilter("/", descendants = true)), Some(ff), Some(DatesFilter(None, None)), PaginationParams(0, chunkSize), 60L, withHistory = false), 9.seconds)
    val firstData = first.infotons.getOrElse(Seq.empty[Infoton])

    ScrollResults(first.totalHits,
      Stream.iterate(Chunk(first.iteratorId, firstData, State(intermediateLimit, first.infotons.getOrElse(Seq.empty[Infoton]).length, first.totalHits))) {
        case Chunk(iid, data, State(intermediateLimit, c, t)) =>
          val ir = Await.result(crudServiceFS.scroll(iid, 60, withData = true), 9.seconds)
          val data = ir.infotons.getOrElse(Seq.empty[Infoton])
          Chunk(ir.iteratorId, data, State(config.intermediateLimit, c + data.length, t))
      }.
        takeWhile(!_.state.isExausted) // this line makes it finite
    )
  }


}

class DataFetcherImpl(val config: Config, val crudServiceFS: CRUDServiceFS, val nbg: Boolean) extends DataFetcher {
  val chunkSize = 100
  val singleGetThreshold = 512 // must be under 1000 (because "even google...")
}

//object DefaultDataFetcher extends DataFetcher {
//  def nbg: Boolean = WorkerMain.nbgToggler.get
//  def crudServiceFS: CRUDServiceFS = Option(WorkerMain.crudServiceFS) match {
//    case Some(crud) => crud
//    case None => throw new IllegalStateException("CRUDServiceFS defined in main(App) was null (lazy init crap...)")
//  }
//  def config: Config =  Config.defaultConfig
//  val chunkSize = 100
//  val singleGetThreshold = 512 // must be under 1000 (because "even google...")
//}

class NamespaceException(msg: String) extends RuntimeException(msg: String) { override def toString = msg }

class CmWellGraph(val dsg: DatasetGraphCmWell) extends GraphBase with LazyLogging {

  logger.info("[arq][FLOW] CmWellGraph was instansiated")

  override def graphBaseFind(triple: Triple): ExtendedIterator[Triple] = {
    val data = dsg.findInDftGraph(triple.getSubject, triple.getPredicate, triple.getObject).map(_.asTriple)
    new ExtendedIterator[Triple] {
      override def filterKeep(f: Predicate[Triple]): ExtendedIterator[Triple] = ???
      override def filterDrop(f: Predicate[Triple]): ExtendedIterator[Triple] = ???
      override def removeNext(): Triple = ???
      override def andThen[X <: Triple](iterator: util.Iterator[X]): ExtendedIterator[Triple] = ???
      override def remove(): Unit = ???
      override def toSet: util.Set[Triple] = ???
      override def toList: util.List[Triple] = ???

      override def close(): Unit = { }
      override def mapWith[U](map1: Function[Triple, U]): ExtendedIterator[U] = ???

      override def next(): Triple = data.next()
      override def hasNext: Boolean = data.hasNext
    }
  }
}

case class Config(doNotOptimize: Boolean, intermediateLimit: Long, resultsLimit: Long, verbose: Boolean, finiteDuarationForDeadLine: FiniteDuration, deadline: Option[Deadline], explainOnly: Boolean)
object Config {
  lazy val defaultConfig = new Config(
    doNotOptimize = false,
    intermediateLimit = 10000,
    resultsLimit = 10000,
    verbose = false,
    finiteDuarationForDeadLine = SpHandler.queryTimeout,
    deadline = None,
    explainOnly = false)
}

class DatasetGraphCmWell(val host: String,
                         val config: Config,
                         nbg: Boolean,
                         crudServiceFS: CRUDServiceFS,
                         arqCache: ArqCache,
                         jenaArqExtensionsUtils: JenaArqExtensionsUtils,
                         dataFetcher: DataFetcher)
                        (implicit ec: scala.concurrent.ExecutionContext) extends DatasetGraphTriplesQuads with LazyLogging { self =>

  import JenaArqExtensionsUtils.isConst

  /**
    * Query Id is a string based on each data-provider instance's hash code.
    * It is used for caching intermediate results in memory,
    * and is needed to distinguish between cached values of different queries.
    */
  val queryUuid = cmwell.util.numeric.Radix64.encodeUnsigned(this.##)

  // todo keep DRY! reuse TimedRequest from SpHandler. need to refactor to combine _sp features into _sparql
  protected val relativeEpochTime = System.currentTimeMillis()
  private val fmt : DateTimeFormatter = ISODateTimeFormat.hourMinuteSecondMillis

  logger.info("[arq][FLOW] DatasetGraphCmWell was instansiated")

  val msgs: ArrayBuffer[(String,String)] = ArrayBuffer()
  def logMsg(category: String, msg: String) = {
    val time = new DateTime(System.currentTimeMillis()-relativeEpochTime).toString(fmt)
    msgs += category -> s"$time $msg"
  }
  def logMsgOnce(category: String, msg: String) = {
    if (!msgs.exists { case (c, m) => c == category && m.contains(msg) })
      logMsg(category, msg)
  }

  def logVerboseMsg(category: String, msg: String) = if(config.verbose && !config.explainOnly) logMsg(s"$category ${Thread.currentThread.getId}", msg)

  override def findInSpecificNamedGraph(node: Node, node1: Node, node2: Node, node3: Node): util.Iterator[Quad] =
    findInAnyNamedGraphs(node, node1, node2) // todo quads

  override def findInAnyNamedGraphs(node: Node, node1: Node, node2: Node): util.Iterator[Quad] =
    findInDftGraph(node, node1, node2) // todo quads

  override def findInDftGraph(s: Node, p: Node, o: Node): util.Iterator[Quad] = {
    if(config.deadline.exists(_.isOverdue)) {
      logMsgOnce("Warning", "Query was timed out")
      Iterator[Quad]()
    } else {
      doFindInDftGraph(s,p,o)
    }
  }

  private def doFindInDftGraph(s: Node, pred: Node, o: Node): util.Iterator[Quad] = {

    def printNode(n: Node): String = if(n.isURI) n.getLocalName else if(n.isLiteral) n.getLiteral.toString() else n.toString()

    logger.debug(s"\n[arq] Searching for ${printNode(s)} <${printNode(pred)}> ${printNode(o)}")

    val subject = if (s != Node.ANY && s.isURI) s.getURI.replace("http:/", "") else "*"
    val (subVarName,p) = JenaArqExtensionsUtils.unmanglePredicate(pred)

    val cacheKey = s"$queryUuid.$subVarName"

    def matches(node1: Node, node2: Node) = !isConst(node1) || !isConst(node2) || node1 == node2

    def predicateMatches(predicate: Node, field: Node) = {
      !isConst(predicate) || {
        val predRepr = predicate.getURI.replace(JenaArqExtensionsUtils.cmwellInternalUriPrefix, "")
        val fieldRepr = field.getURI.replace(JenaArqExtensionsUtils.cmwellInternalUriPrefix, "").split('#').reverse.mkString(".")
        predRepr == fieldRepr
      }
    }

    def containerPredicateMatches(containerPredicate: Node, field: Node, value: Node): Boolean = {
      JenaArqExtensionsUtils.explodeContainerPredicate(containerPredicate).exists { case (name, eVal) =>
        val innerReprAsUri = NodeFactory.createURI(JenaArqExtensionsUtils.cmwellInternalUriPrefix + name)
        val objAsNode1 = if (eVal.isEmpty) Node.ANY else NodeFactory.createURI(eVal)
        val objAsNode2 = if (eVal.isEmpty) Node.ANY else NodeFactory.createLiteral(eVal)

        predicateMatches(innerReprAsUri, field) && (matches(value, objAsNode1)|| matches(value, objAsNode2))
      }
    }

    def getInfotonAndFilterFields: Iterator[Quad] = {
      arqCache.getInfoton(subject) match {
        case Some(i) => {
          val allFieldsAsQuads: Iterator[Quad] = infotonToQuadIterator(i)
          allFieldsAsQuads.filter(q => if(p.isURI && p.getURI.startsWith(JenaArqExtensionsUtils.engineInternalUriPrefix)) containerPredicateMatches(p, q.getPredicate, q.getObject) else predicateMatches(p, q.getPredicate) && matches(o, q.getObject))
        }
        case None => logger.info(s"[arq] could not retrieve infoton: $subject"); Iterator[Quad]()
      }
    }

    val quadFilter = (q: Quad) => if(p.getURI.startsWith(JenaArqExtensionsUtils.engineInternalUriPrefix)) containerPredicateMatches(p, q.getPredicate, q.getObject) else predicateMatches(p, q.getPredicate) && matches(o, q.getObject)

    def doSearchAndFilterFields: Iterator[Quad] = {
      val fieldFilter = jenaArqExtensionsUtils.predicateToFieldFilter(p, o)

      val cachedResults = arqCache.getSearchResults(cacheKey)

      if(cachedResults.nonEmpty) {
        logger.info(s"Reusing caching results (amount = ${cachedResults.length}, key = $subVarName)")
        cachedResults.filter(quadFilter).toIterator
      } else new Iterator[Quad] {
        var count = 0

        case class Chunk(iteratorId: String, quads: Iterator[Quad])

        var currentChunk: Chunk = startScroll
        var nextChunk: Chunk = scroll()

        def startScroll = {
          logger.info("[arq] Scrolling ")

//          logVerboseMsg("Fetch", fieldFilter)

          val startScrollRes = Await.result(crudServiceFS.startScroll(Some(PathFilter("/", descendants = true)), Some(fieldFilter), Some(DatesFilter(None, None)), PaginationParams(0, 10), 60L, withHistory = false), 9.seconds)
          scroll(Some(startScrollRes.iteratorId))
        }

        def scroll(iteratorId: Option[String] = None) = {
          val scrollRes = Await.result(crudServiceFS.scroll(iteratorId.getOrElse(currentChunk.iteratorId), 60, withData = true), 9.seconds)
          val quads = infotonsToQuadIterator(scrollRes.infotons.getOrElse(Seq.empty[Infoton]))
          val filteredQuads = quads.filter(quadFilter)
          Chunk(scrollRes.iteratorId, filteredQuads)
        }

        override def hasNext: Boolean = count < config.intermediateLimit && (currentChunk.quads.hasNext || nextChunk.quads.hasNext)

        override def next(): Quad = {
          count += 1

          if(count == config.intermediateLimit)
            msgs += "Warning" -> "a query search was exhausted; results below may be partial! Please narrow your query to have complete results."

          if(count > config.intermediateLimit)
            throw new java.util.NoSuchElementException("next on empty iterator")

          if (!currentChunk.quads.hasNext) {
            currentChunk = nextChunk
            nextChunk = scroll()
          }
          currentChunk.quads.next
        }
      }
    }

    val results = if(p.isURI && p.getURI.startsWith(JenaArqExtensionsUtils.engineInternalUriPrefix)) {

      val fieldFilter = jenaArqExtensionsUtils.predicateToFieldFilter(p, o)
      val infotonResults = dataFetcher.fetch(fieldFilter)._2
      val filteredQuads = infotonResults.flatMap(infotonToQuadIterator).filter(quadFilter)
      arqCache.putSearchResults(cacheKey, filteredQuads)

      Iterator(JenaArqExtensionsUtils.fakeQuad) // returning a Seq in length 1 that its content won't be visible to user.
                                                // queryIterator will continue as if it's the cartesian product `1 × <rest of results>`
    } else (isConst(s), isConst(p), isConst(o)) match {
      case (true, _, _) => getInfotonAndFilterFields
      case (_, true, _) => doSearchAndFilterFields
      case (_, _, true) => doSearchAndFilterFields
      case _ => {
        logger.info(s"unexpected STREAM From [ $s $p $o ]")
        val errMsg = "Each triple-matching must have binding of a subject, a predicate or an object. If you'd like to download entire CM-Well's content, please use the Stream API"
        msgs += "Error" -> errMsg
        Iterator()
      }
    }

    results map jenaArqExtensionsUtils.normalizeAsOutput
  }

  override def listGraphNodes(): util.Iterator[Node] = Iterator[Node]() // this is a hack

  override def toString = "< CmWell DatasetGraph (AKA JenaDriver) >"

  // write - unimplemented
  override def addToNamedGraph(node: Node, node1: Node, node2: Node, node3: Node): Unit = { }
  override def addToDftGraph(node: Node, node1: Node, node2: Node): Unit = { }
  override def deleteFromNamedGraph(node: Node, node1: Node, node2: Node, node3: Node): Unit = { }
  override def deleteFromDftGraph(node: Node, node1: Node, node2: Node): Unit = { }
  override def supportsTransactions(): Boolean = false
  override def begin(readWrite: ReadWrite): Unit = { }
  override def abort(): Unit = { }
  override def isInTransaction: Boolean = false
  override def end(): Unit = { }
  override def commit(): Unit = { }
  private val nullFormatter = new cmwell.formats.RDFFormatter(host, hash=>Some(JenaArqExtensionsUtils.cmwellInternalUriPrefix + hash + "#" -> None), false, false, false) {
    override def format: cmwell.formats.FormatType = ???
    override def render(formattable: Formattable): String = ???
  }

  private def asListOfNodes(triple: Triple) = List(triple.getSubject, triple.getPredicate, triple.getObject)

  private def matchesWithWildCards(triple: Triple, s: Node, p: Node, o: Node): Boolean = {
    def matches(node1: Node, node2: Node) = node1 == Node.ANY || node2 == Node.ANY || node1 == node2
    matches(triple.getSubject, s) && matches(triple.getPredicate, p) && matches(triple.getObject, o)
  }

  private def infotonsToQuadIterator(infotons: Seq[Infoton]): Iterator[Quad] = {
    infotons.flatMap(infotonToQuadIterator).toIterator
  }

  private def infotonToQuadIterator(infoton: Infoton): Iterator[Quad] = {
    val ds = nullFormatter.formattableToDataset(infoton)

    //todo support quads
    val g = NodeFactory.createBlankNode()
    JenaUtils.discardQuadsAndFlattenAsTriples(ds).listStatements.map(stmt => new Quad(g, stmt.getSubject.asNode, stmt.getPredicate.asNode, stmt.getObject.asNode))
  }

  override def getDefaultGraph: Graph = new CmWellGraph(this)

  override def getGraph(graphNode: Node): Graph = getDefaultGraph // todo quads
}

class EmbedLimitQueryEngineFactory extends QueryEngineFactory with LazyLogging {
  override def accept(query: Query, dataset: DatasetGraph, context: Context): Boolean = true

  override def create(query: Query, dataset: DatasetGraph, initial: Binding, context: Context): Plan = {
    val engine = new EmbedLimitQueryEngine(query, dataset, initial, context)
    engine.getPlan
  }

  // Refuse to accept algebra expressions directly.
  override def accept(op: Op, dataset: DatasetGraph, context: Context): Boolean = false

  // Should not be called because acceept/Op is false
  override def create(op: Op, dataset: DatasetGraph, inputBinding: Binding, context: Context): Plan =
    throw new ARQInternalErrorException("EmbedLimitQueryEngine: factory called directly with an algebra expression")
}

class EmbedLimitQueryEngine(query: Query, dataset: DatasetGraph, initial: Binding = null, context: Context = null) extends QueryEngineMain(query, dataset, initial, context) {
  override def eval(op: Op, dsg: DatasetGraph, input: Binding, context: Context) = dsg.getDefaultGraph match {
    case cmwg: CmWellGraph =>
      val opSlice = new OpSlice(op, Long.MinValue, cmwg.dsg.config.resultsLimit)
      super.eval(opSlice, dsg, input, context)
    case _ =>
      super.eval(op, dsg, input, context)
  }
}

