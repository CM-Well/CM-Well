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

import cmwell.domain._
import cmwell.fts._
import cmwell.syntaxutils._
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import controllers.{JenaUtils, SpHandler}
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
import wsutil.{HashedFieldKey, RawFieldFilter, RawMultiFieldFilter, RawSingleFieldFilter}

import org.apache.jena.sparql.engine.{ ExecutionContext => JenaExecutionContext }

import scala.annotation.switch
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import cmwell.crashableworker.WorkerMain.{cwLogger => logger}

//TODO     Don't mess with low level strings. Just like mangle and unmangle methods,
//TODO     have more util methods such as isEngine or isInternal etc.

/**
  * Created by yaakov on 11/30/15.
  */
object JenaArqExtensions {
  lazy val init = {
    // init extension1
    val globalArqContext = ARQ.getContext
    val originalStageGenerator = globalArqContext.get(ARQ.stageGenerator).asInstanceOf[StageGenerator]
    StageBuilder.setGenerator(globalArqContext, new SortingAndMappingStageGenerator(Option(originalStageGenerator)))

    // init extension2
    val factory = new EmbedLimitQueryEngineFactory()
    QueryEngineRegistry.addFactory(factory)

    logger.info("JenaArqExtensions were applied.")
  }
}

object JenaArqExtensionsUtils {
  case class BakedSparqlQuery(qe: QueryExecution, driver: DatasetGraphCmWell)

  val emptyLtrl = NodeFactory.createLiteral("")

  def buildCmWellQueryExecution(query: Query, host: String, config: Config = Config.defaultConfig) = {
    val driver = new DatasetGraphCmWell(host, config)
    val model = ModelFactory.createModelForGraph(driver.getDefaultGraph)
    val qe = QueryExecutionFactory.create(query, model) // todo quads

    BakedSparqlQuery(qe, driver)
  }

  def predicateToInnerRepr(predicate: Node): Node = {
    if(!predicate.isURI) predicate else {
      val nsIdentifier = ArqCache.namespaceToHash(predicate.getNameSpace)
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
      val nsUri = ArqCache.hashToNamespace(hash)
      NodeFactory.createURI(nsUri + localName)
    }
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
      val idxOfSep = p.getURI.indexOf(JenaArqExtensionsUtils.manglingSeparator)
      val (subVarName, sepAndPred) = p.getURI.splitAt(idxOfSep)
      subVarName -> NodeFactory.createURI(sepAndPred.substring(1))
    }
  }

  def normalizeAsOutput(q: Quad): Quad =
    new Quad(q.getGraph, q.getSubject, innerReprToPredicate(q.getPredicate), q.getObject)

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

  def explodeContainerPredicate(pred: Node): Seq[(String,String)] = {
    pred.getURI.replace(engineInternalUriPrefix,"").split('|').map(cmwell.util.string.spanNoSepBy(_, ':'))
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

  private def defaultAmountCounter(t: Triple, graph: Option[CmWellGraph] = None) = {
    val ff = JenaArqExtensionsUtils.predicateToFieldFilter(t.getPredicate)
    val amount = DefaultDataFetcher.count(ff)

    graph.collect { case g if g.dsg.config.explainOnly => g.dsg }.foreach { dsg =>
      val pred = Try(unmanglePredicate(t.getPredicate)._2).toOption.getOrElse(t.getPredicate).getURI.replace(cmwellInternalUriPrefix, "")
      dsg.logMsgOnce("Expl", s"Objects count for $pred: $amount")
    }

    amount
  }

  val cmwellInternalUriPrefix = "cmwell://meta/internal/"
  val engineInternalUriPrefix = "engine://"

  val manglingSeparator = "$"

  val fakeQuad: Quad = {
    // by having an empty subject and an empty object, this triple will never be visible to user as it won't be bind to anything. even for `select *` it's hidden
    new Quad(emptyLtrl, emptyLtrl, NodeFactory.createURI(engineInternalUriPrefix + "FAKE"), emptyLtrl)
  }

  def isConst(node: Node) = node != Node.ANY

  def predicateToFieldFilter(p: Node, obj: Node = emptyLtrl): FieldFilter = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val pred = unmanglePredicate(p)._2

    def toExplodedMangledFieldFilter(qp: String, value: Option[String]) = {
      val (localName, hash) = { val splt = qp.split('.'); splt(0) -> splt(1) }
      RawSingleFieldFilter(Must, Equals, HashedFieldKey(localName, hash), value)
    }

    def evalAndAwait(rff: RawFieldFilter) = {
      //      def filterNot(ff: FieldFilter)(pred: SingleFieldFilter => Boolean): FieldFilter = ???
      //
      //      def isTypeIncompatible(sff: SingleFieldFilter) =
      //        sff.name.startsWith("d$") && sff.value.fold(false)(FDate.isDate)

      /*val evalRes =*/ Await.result(RawFieldFilter.eval(rff), 9.seconds)
      //      filterNot(evalRes)(isTypeIncompatible)
    }

    def noneIfEmpty(s: String): Option[String] = if(s.isEmpty) None else Some(s)

    if(pred.getURI.contains(JenaArqExtensionsUtils.engineInternalUriPrefix)) {
      val fieldFilters = JenaArqExtensionsUtils.explodeContainerPredicate(pred).map { case (name,value) =>
        toExplodedMangledFieldFilter(name, noneIfEmpty(value))
      }
      evalAndAwait(RawMultiFieldFilter(Must, fieldFilters))
    } else {
      val value = JenaArqExtensionsUtils.objectToQpValue(obj)
      if (!isConst(pred)) {
        SingleFieldFilter(Must, Contains, "_all", value)
      } else {
        val qp = pred.getURI.replace(JenaArqExtensionsUtils.cmwellInternalUriPrefix,"")
        evalAndAwait(toExplodedMangledFieldFilter(qp, value.flatMap(noneIfEmpty)))
      }
    }
  }

  def queryToSseString(query: Query): String = Algebra.compile(query).toString(query.getPrefixMapping)
}

/**
  * Sorting the basicPattern (list of triples) and also convert all predicates to server-repr (i.e. localName.hash)
  */
class SortingAndMappingStageGenerator(original: Option[StageGenerator] = None) extends StageGenerator with LazyLogging {
  override def execute(basicPattern: BasicPattern, queryIterator: QueryIterator, ec: JenaExecutionContext): QueryIterator = {
    ec.getActiveGraph match {
      case graph: CmWellGraph =>
        logger.info(s"[arq][FLOW] execute was invoked with ${basicPattern.getList.length} triplePatterns")

        val needToOptimize = basicPattern.getList.length > 1 && !graph.dsg.config.doNotOptimize

        if(needToOptimize)
          graph.dsg.logVerboseMsg("Plan", s"Optimizing ${basicPattern.getList.length} statements...")

        val mappedTriplePatterns = Try(basicPattern.getList.map { trpl =>
          val internalReprPredicate = JenaArqExtensionsUtils.predicateToInnerRepr(trpl.getPredicate)
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

        val sorted = if(needToOptimize) JenaArqExtensionsUtils.sortTriplePatternsByAmount(all)(graph) else all

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

class DataFetcher(config: Config) {

  val chunkSize = 100
  val singleGetThreshold = 512 // must be under 1000 (because "even google...")

  def count(ff: FieldFilter) =
    Await.result(CRUDServiceFS.thinSearch(Some(PathFilter("/", descendants = true)), Some(ff), Some(DatesFilter(None, None)), PaginationParams(0, 1), withHistory = false), 9.seconds).total

  /**
    * fetch returns value is (Long,Seq[Infoton]) which is the amount of total results,
    * and a Sequence which might be a List or a lazy evaluated Stream
    */
  def fetch(ff: FieldFilter) = {
    val amount = count(ff)
    if(count(ff) < singleGetThreshold) {
      amount -> get(ff)
    } else {
      val scrl = scroll(ff)
      scrl.total -> scrl.results.flatMap(_.data)
    }
  }

  private def get(ff: FieldFilter) =
    Await.result(CRUDServiceFS.search(Some(PathFilter("/", descendants = true)), Some(ff), Some(DatesFilter(None, None)), PaginationParams(0, singleGetThreshold), withHistory = false, withData = true), 9.seconds).infotons

  private def scroll(ff: FieldFilter) = {
    val first = Await.result(CRUDServiceFS.startScroll(Some(PathFilter("/", descendants = true)), Some(ff), Some(DatesFilter(None, None)), PaginationParams(0, chunkSize), 60L, withHistory = false), 9.seconds)
    val firstData = first.infotons.getOrElse(Seq.empty[Infoton])

    ScrollResults(first.totalHits,
      Stream.iterate(Chunk(first.iteratorId, firstData, State(first.infotons.getOrElse(Seq.empty[Infoton]).length, first.totalHits))) {
        case Chunk(iid, data, State(c, t)) =>
          val ir = Await.result(CRUDServiceFS.scroll(iid, 60, withData = true), 9.seconds)
          val data = ir.infotons.getOrElse(Seq.empty[Infoton])
          Chunk(ir.iteratorId, data, State(c + data.length, t))
      }.
        takeWhile(!_.state.isExausted) // this line makes it finite
    )
  }

  case class Chunk(scrollId: String, data: Seq[Infoton], state: State)

  case class State(currentCount: Long, total: Long) {
    def isExausted = currentCount >= Math.min(total, config.intermediateLimit)
  }

  case class ScrollResults(total: Long, results: Stream[Chunk])
}

object DefaultDataFetcher extends DataFetcher(Config.defaultConfig)

// TODO find out a way to use Futures within ARQ
object ArqCache { // one day this will be a distributed cache ... // todo zStore all the caches! Actually, think twice, because serialization. Thrift...?
  def getInfoton(path: String): Option[Infoton] = infotonsCache.get(path)
  def namespaceToHash(ns: String): String = identifiersCache.get(ns)
  def hashToNamespace(hash: String): String = namespacesCache.get(hash).uri
  def hashToPrefix(hash: String): String = namespacesCache.get(hash).prefix

  def putSearchResults(key: String, value: Seq[Quad]): Unit = searchesCache.put(key, value)
  def getSearchResults(key: String): Seq[Quad] = Try(searchesCache.get(key)).toOption.getOrElse(Seq())

  private sealed trait Input
  private case class Uri(value: String) extends Input
  private case class Hash(value: String) extends Input
  private case class NsResult(uri: String, hash: String, prefix: String)

  private lazy val infotonsCache: LoadingCache[String, Option[Infoton]] =
    CacheBuilder.newBuilder().maximumSize(100000).expireAfterAccess(5, TimeUnit.MINUTES).
      build(new CacheLoader[String, Option[Infoton]] {
        override def load(key: String) = Await.result(CRUDServiceFS.getInfoton(key, None, None), 9.seconds) match { case Some(Everything(i)) => Option(i) case _ => None }
      })

  private lazy val searchesCache: LoadingCache[String, Seq[Quad]] =
    CacheBuilder.newBuilder().maximumSize(100000).expireAfterWrite(100, TimeUnit.SECONDS).
      build(new CacheLoader[String, Seq[Quad]] { override def load(key: String) = !!! })

  private lazy val identifiersCache: LoadingCache[String, String] =
    CacheBuilder.newBuilder().maximumSize(1024).expireAfterAccess(5, TimeUnit.MINUTES).
      build(new CacheLoader[String, String] { override def load(key: String) = translateAndFetch(Uri(key)).hash })

  private lazy val namespacesCache: LoadingCache[String, NsResult] =
    CacheBuilder.newBuilder().maximumSize(1024).expireAfterAccess(5, TimeUnit.MINUTES).
      build(new CacheLoader[String, NsResult] { override def load(key: String) = translateAndFetch(Hash(key)) })

  private def translateAndFetch(input: Input): NsResult = {
    val (hash,knownUri) = input match {
      case Uri(uri) => cmwell.util.string.Hash.crc32base64(uri) -> Some(uri)
      case Hash(h) => h -> None
    }

    def extractFieldValue(i: Infoton, fieldName: String) = i.fields.getOrElse(Map()).getOrElse(fieldName,Set()).mkString(",")

    // /meta/ns/{hash} if fields.url == pred.getURI return hash. else search over meta/ns?qp=url::pred.getURI -> get identifier from path of infoton
    Await.result(CRUDServiceFS.getInfoton(s"/meta/ns/$hash", None, None), 9.seconds) match {
      case Some(Everything(i)) if knownUri.isEmpty => {
        NsResult(extractFieldValue(i, "url"), hash, extractFieldValue(i, "prefix"))
      }
      case Some(Everything(i)) if extractFieldValue(i, "url") == knownUri.get => {
        logger.info(s"[arq] $hash was a good guess")
        NsResult(knownUri.get, hash, extractFieldValue(i, "prefix"))
      }
      case _ if knownUri.isDefined => {
        val infotonsWithThatUrl = Await.result(CRUDServiceFS.search(Some(PathFilter("/", descendants = true)), Some(SingleFieldFilter(Must, Equals, "url", knownUri)), Some(DatesFilter(None, None)), PaginationParams(0, 10)), 9.seconds).infotons
        (infotonsWithThatUrl.length: @switch) match {
          case 0 =>
            logger.info(s"[arq] ns for $hash was not found")
            throw new NamespaceException(s"Namespace ${knownUri.get} does not exist")
          case 1 =>
            logger.info(s"[arq] Fetched proper namespace for $hash")
            val infoton = infotonsWithThatUrl.head
            val path = infoton.path
            val actualHash = path.substring(path.lastIndexOf('/') + 1)
            val uri = knownUri.getOrElse(extractFieldValue(infoton, "url"))
            NsResult(uri, actualHash, extractFieldValue(infoton, "prefix"))
          case _ =>
            logger.info("[arq] this should never happen: same URL cannot be more than once in meta/ns")
            !!!
        }
      }
      case _ =>
        logger.info("[arq] this should never happen: given a hash, when the URI is unknown, and there's no meta/ns/hash - it means currupted data")
        !!!
    }
  }
}

class NamespaceException(msg: String) extends RuntimeException(msg: String) { override def toString = msg }

class CmWellGraph(val dsg: DatasetGraphCmWell) extends GraphBase {

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

case class Config(doNotOptimize: Boolean, intermediateLimit: Long, resultsLimit: Long, verbose: Boolean, deadline: Deadline, explainOnly: Boolean)
object Config { val defaultConfig = Config(doNotOptimize = false, 10000, 10000, verbose = false, deadline = SpHandler.queryTimeout.fromNow, explainOnly = false) }

class DatasetGraphCmWell(val host: String, val config: Config) extends DatasetGraphTriplesQuads {
  import JenaArqExtensionsUtils.isConst

  /**
    * Query Id is a string based on each data-provider instance's hash code.
    * It is used for caching intermediate results in memory,
    * and is needed to distinguish between cached values of different queries.
    */
  val queryUuid = cmwell.util.numeric.Radix64.encodeUnsigned(this.##)

  val dataFetcher = new DataFetcher(config)

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
    if(config.deadline.isOverdue) {
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
      ArqCache.getInfoton(subject) match {
        case Some(i) => {
          val allFieldsAsQuads: Iterator[Quad] = infotonToQuadIterator(i)
          allFieldsAsQuads.filter(q => if(p.isURI && p.getURI.startsWith(JenaArqExtensionsUtils.engineInternalUriPrefix)) containerPredicateMatches(p, q.getPredicate, q.getObject) else predicateMatches(p, q.getPredicate) && matches(o, q.getObject))
        }
        case None => logger.info(s"[arq] could not retrieve infoton: $subject"); Iterator[Quad]()
      }
    }

    val quadFilter = (q: Quad) => if(p.getURI.startsWith(JenaArqExtensionsUtils.engineInternalUriPrefix)) containerPredicateMatches(p, q.getPredicate, q.getObject) else predicateMatches(p, q.getPredicate) && matches(o, q.getObject)

    def doSearchAndFilterFields: Iterator[Quad] = {
      val fieldFilter = JenaArqExtensionsUtils.predicateToFieldFilter(p, o)

      val cachedResults = ArqCache.getSearchResults(cacheKey)

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

          val startScrollRes = Await.result(CRUDServiceFS.startScroll(Some(PathFilter("/", descendants = true)), Some(fieldFilter), Some(DatesFilter(None, None)), PaginationParams(0, 10), 60L, withHistory = false), 9.seconds)
          scroll(Some(startScrollRes.iteratorId))
        }

        def scroll(iteratorId: Option[String] = None) = {
          val scrollRes = Await.result(CRUDServiceFS.scroll(iteratorId.getOrElse(currentChunk.iteratorId), 60, withData = true), 9.seconds)
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

      val fieldFilter = JenaArqExtensionsUtils.predicateToFieldFilter(p, o)
      val infotonResults = dataFetcher.fetch(fieldFilter)._2
      val filteredQuads = infotonResults.flatMap(infotonToQuadIterator).filter(quadFilter)
      ArqCache.putSearchResults(cacheKey, filteredQuads)

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

    results map JenaArqExtensionsUtils.normalizeAsOutput
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
  private val nullFormatter = new cmwell.formats.RDFFormatter(host, hash=>Some(JenaArqExtensionsUtils.cmwellInternalUriPrefix + hash + "#" -> ""), false, false, false) {
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

