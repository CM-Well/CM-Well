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


package controllers

import java.io.{ByteArrayOutputStream, File, OutputStream}
import java.util
import javax.inject.{Inject, Singleton}

import cmwell.syntaxutils._
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.query.{Config, DataFetcherImpl}
import cmwell.ws.AggregateBothOldAndNewTypesCaches
import com.typesafe.scalalogging.LazyLogging
import info.aduna.iteration.{CloseableIteration, CloseableIteratorIteration, EmptyIteration}
import ld.query.TripleStore.TriplePattern
import ld.query.{SesameExtensions, TripleStore}
import logic.CRUDServiceFS
import org.openrdf.model.impl.SimpleValueFactory
import org.openrdf.model.{IRI, Namespace, Resource, Statement, Value, ValueFactory}
import org.openrdf.query._
import org.openrdf.query.algebra.TupleExpr
import org.openrdf.query.algebra.evaluation.TripleSource
import org.openrdf.query.algebra.evaluation.impl.SimpleEvaluationStrategy
import org.openrdf.query.parser._
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONWriter
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter
import org.openrdf.query.resultio.text.csv.SPARQLResultsCSVWriter
import org.openrdf.query.resultio.text.tsv.SPARQLResultsTSVWriter
import org.openrdf.repository.sail.{SailRepository, SailRepositoryConnection}
import org.openrdf.rio.RDFWriter
import org.openrdf.rio.ntriples.NTriplesWriter
import org.openrdf.rio.trig.TriGWriter
import org.openrdf.rio.turtle.TurtleWriter
import org.openrdf.sail.memory.model._
import org.openrdf.sail.{Sail, SailConnection, SailException, UpdateContext}
import org.openrdf.{IsolationLevel, IsolationLevels, OpenRDFException}
import play.api.mvc.{Action, Controller, Result}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// TODO 1. Rename this handler. Place the heavy lifting within CW (This should replace OverallSparqlQuery)
// TODO 2. Remove JenaArqExtensions :( But keep its DataFetcher. Should be closer to TripleStore
// TODO 3. Propagate query parameters to the proper places. Implement Verbose and Explain.
// TODO 4. Add optimizations, support Range Queries, add special functions, run on SPARK...
// TODO 5. propagate nbg properly according to request, or leave as is, knowing it's kinda broken (as in inconsistent), but will be "fixed" when old data path is discontinued

/**
  * Created by yaakov on 5/24/17.
  */

@Singleton
class OpenRdfSpHandler @Inject()(tbg: NbgToggler, crudServiceFS: CRUDServiceFS, cmwellRDFHelper: CMWellRDFHelper)(implicit ec: ExecutionContext) extends Controller with LazyLogging {

  val config: Config = Config.defaultConfig
  val typesCache = new AggregateBothOldAndNewTypesCaches(crudServiceFS,tbg)
  val dataFetcher = new DataFetcherImpl(config,crudServiceFS,tbg.get)
  val tripleStore = new TripleStore(dataFetcher, cmwellRDFHelper, tbg)
  val cmWellTripleSource = new CmWellTripleSource(tripleStore)
  val cmWellReadOnlySailConnection = new CmWellReadOnlySailConnection(cmWellTripleSource)
  val cmWellReadOnlySail = new CmWellReadOnlySail(cmWellReadOnlySailConnection)
  val sailConn = new CmWellSailRepositoryConnection(cmWellReadOnlySailConnection, cmWellReadOnlySail)

  def handleSsparqlPost(): Action[String] = Action.async(parse.tolerantText) {
    implicit request =>
      Try(QueryParserUtil.parseOperation(QueryLanguage.SPARQL, request.body, null)) match {
        case Success(po) =>

          // todo case class RequestParams combined with Config or such
          val formatOpt = request.getQueryString("format")
          val withoutMeta = request.queryString.keySet("without-meta")

          evaluateQuery(po, formatOpt, withoutMeta).map[Result](Ok.apply).recover {
            case t: IllegalArgumentException => BadRequest(t.getMessage)
            case _ => InternalServerError("Unknown error occurred")
          }

        case Success(_) =>
          Future.successful(BadRequest("Unsupported query"))

        case Failure(t) if t.isInstanceOf[MalformedQueryException] =>
          Future.successful(BadRequest(t.getMessage))

        case Failure(t) =>
          logger.error("Unexpected error in parsing SPARQL", t)
          Future.successful(InternalServerError("Unknown error occurred"))
      }
  }

  private def evaluateQuery(operation: ParsedOperation, formatOpt: Option[String], withoutMeta: Boolean): Future[String] = operation match {
    case ptq: ParsedTupleQuery =>
      val format = formatOpt.getOrElse("tsv").toLowerCase()
      evaluateSelect(ptq, format, withoutMeta)

    case pgq: ParsedGraphQuery =>
      val format = formatOpt.getOrElse("ntriples").toLowerCase()
      evaluateConstruct(pgq, format, withoutMeta)

    case _ => Future.failed(new IllegalArgumentException("Unsupported query type"))
  }

  private def evaluateSelect(ptq: ParsedTupleQuery, format: String, withoutMeta: Boolean): Future[String] = {
    val query = sailConn.prepareTupleQuery(QueryLanguage.SPARQL, ptq.getSourceString)

    val os = new ByteArrayOutputStream()
    val writer = getSelectResultsHandler(format, os)

    query.evaluate(writer)

    Future.successful(new String(os.toByteArray, "UTF-8"))
  }

  private def evaluateConstruct(pgq: ParsedGraphQuery, format: String, withoutMeta: Boolean): Future[String] = {
    val query = sailConn.prepareGraphQuery(QueryLanguage.SPARQL, pgq.getSourceString)

    val os = new ByteArrayOutputStream()
    val writer = getConstructResultsHandler(format, os)

    query.evaluate(writer)

    Future.successful(new String(os.toByteArray, "UTF-8"))
  }

  private def getSelectResultsHandler(format: String, os: OutputStream): TupleQueryResultHandler = format match {
    case "tsv" => new SPARQLResultsTSVWriter(os)
    case "csv" => new SPARQLResultsCSVWriter(os)
    case "xml" => new SPARQLResultsXMLWriter(os)
    case "json" => new SPARQLResultsJSONWriter(os) // todo bypass JAR Hell to enable json: Sesame uses jackson-core 2.6.2 but we use 2.8.3
    case _ => !!! // todo throw illegalArgException
  }

  private def getConstructResultsHandler(format: String, os: OutputStream): RDFWriter = format match {
    case "nt"  | "ntriples" => new NTriplesWriter(os)
    case "ttl" | "turtle"   => new TurtleWriter(os)
    case "trig"             => new TriGWriter(os)
    case _ => !!! // todo throw illegalArgException
  }
}

// todo refactor - find a proper place for the Driver

class CmWellSailRepositoryConnection(cmWellReadOnlySailConnection: CmWellReadOnlySailConnection, cmWellReadOnlySail: CmWellReadOnlySail)(implicit ec: ExecutionContext) extends SailRepositoryConnection(new SailRepository(cmWellReadOnlySail), cmWellReadOnlySailConnection)

class CmWellReadOnlySail(cmWellReadOnlySailConnection: CmWellReadOnlySailConnection) extends Sail {
  import scala.collection.JavaConversions._

  override def isWritable: Boolean = false

  override def getConnection: SailConnection = cmWellReadOnlySailConnection

  override def getValueFactory: ValueFactory = new MemValueFactory

  override def initialize(): Unit = {
    // currently, nothing do to here
  }

  override def shutDown(): Unit = {
    // currently, nothing do to here
  }

  override def getDefaultIsolationLevel: IsolationLevel = IsolationLevels.NONE
  override def getSupportedIsolationLevels: util.List[IsolationLevel] = Seq(IsolationLevels.NONE)

  override def getDataDir: File = null
  override def setDataDir(dataDir: File): Unit = { }
}

class CmWellReadOnlySailConnection(cmWellTripleSource: CmWellTripleSource)(implicit ec: ExecutionContext) extends SailConnection {
  override def prepare(): Unit = { /* Not supporting Transactions */ }
  override def commit(): Unit = { /* Not supporting Transactions */ }
  override def rollback(): Unit = { /* Not supporting Transactions */ }
  override def begin(): Unit = { /* Not supporting Transactions */ }
  override def begin(level: IsolationLevel): Unit = { /* Not supporting Transactions */ }

  override def removeNamespace(prefix: String): Unit = ???

  override def isActive: Boolean = false // as we are not supporting Transactions, there will never be an in-flight transaction

  override def getContextIDs: CloseableIteration[_ <: Resource, SailException] = ???

  override def flush(): Unit = {
    // currently, nothing to do here
  }

  override def removeStatement(op: UpdateContext, subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Unit = ???

  override def close(): Unit = {
    // currently, nothing to do here
  }

  override def startUpdate(op: UpdateContext): Unit = ???
  override def endUpdate(op: UpdateContext): Unit = ???

  override def getNamespaces: CloseableIteration[_ <: Namespace, SailException] = new EmptyIteration // todo should we list namespaces?

  override def clear(contexts: Resource*): Unit = ???
  override def removeStatements(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Unit = ???
  override def addStatement(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Unit = ???
  override def addStatement(op: UpdateContext, subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Unit = ???

  override def isOpen: Boolean = true
  override def getStatements(subj: Resource, pred: IRI, obj: Value, includeInferred: Boolean, contexts: Resource*): CloseableIteration[_ <: Statement, SailException] =
    cmWellTripleSource.originalIterator(subj, pred, obj, contexts:_*).asInstanceOf[CloseableIteration[_ <: Statement, SailException]]

  override def evaluate(tupleExpr: TupleExpr, dataset: Dataset, bindings: BindingSet, includeInferred: Boolean): CloseableIteration[_ <: BindingSet, QueryEvaluationException] = {
    // overriding Strategy (e.g. sort by cardinality or JoinSelectivity etc.) can take place here :)

    SesameExtensions.SortByCardinalityQueryOptimizer.optimize(tupleExpr, dataset, bindings)

    new SimpleEvaluationStrategy(cmWellTripleSource, dataset, null).evaluate(tupleExpr, bindings)
    // todo what's the deal with the FederatedServiceResolver ?
  }

  override def size(contexts: Resource*): Long = -1

  override def getNamespace(prefix: String): String = ??? // should we connect it to meta/ns ???????

  override def clearNamespaces(): Unit = ???

  override def setNamespace(prefix: String, name: String): Unit = ???

}


class CmWellTripleSource(tripleStore: TripleStore)(implicit ec: ExecutionContext) extends TripleSource {

  private val factory = SimpleValueFactory.getInstance()

  override def getStatements(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): CloseableIteration[_ <: Statement, QueryEvaluationException] = {
    originalIterator(subj, pred, obj, contexts:_*).asInstanceOf[CloseableIteration[_ <: Statement, QueryEvaluationException]]
  }

  // FIXME: this is temp. hack to map CloseableIteration of different types of Exceptions.
  def originalIterator(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): CloseableIteration[_ <: Statement, OpenRDFException] = {
    import scala.collection.JavaConversions._

    // todo levarage metadata to use cache or such
    //    subj match {
    //      case CmWellValue(metadata, iri) => println(s"here is some metadata: $metadata for IRI($iri)")
    //      case _ =>
    //    }

    val tp = TriplePattern(Option(subj).map(_.stringValue), Option(pred).map(_.stringValue), Option(obj).map(SesameExtensions.sesameValueToValue))
    val statementsIterator = tripleStore.findTriplesByPattern(tp).map(q => factory.createStatement(factory.createIRI(q.subject), factory.createIRI(q.predicate), SesameExtensions.valueToSesameValue(q.value)))

    new CloseableIteratorIteration(statementsIterator)
  }

  override def getValueFactory: ValueFactory = new MemValueFactory
}