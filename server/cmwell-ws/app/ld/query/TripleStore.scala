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


package ld.query

import javax.inject.Inject
import javax.xml.datatype.XMLGregorianCalendar

import cmwell.domain.{Everything, Formattable, Infoton}
import cmwell.fts._
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.query.DataFetcherImpl
import cmwell.ws.AggregateBothOldAndNewTypesCaches
import com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl
import controllers.{JenaUtils, NbgToggler}
import info.aduna.iteration.CloseableIteration
import ld.cmw.PassiveFieldTypesCache
import ld.query.TripleStore._
import org.apache.jena.datatypes.xsd.XSDDateTime
import org.apache.jena.rdf.model.{RDFNode, Statement}
import org.joda.time.DateTime
import org.openrdf.model.impl.{SimpleLiteral, _}
import org.openrdf.model.{BNode, IRI, Value}
import org.openrdf.query.algebra.evaluation.federation.FederatedServiceResolver
import org.openrdf.query.algebra.evaluation.impl.SimpleEvaluationStrategy
import org.openrdf.query.algebra.evaluation.{QueryOptimizer, TripleSource}
import org.openrdf.query.algebra.helpers.AbstractQueryModelVisitor
import org.openrdf.query.algebra._
import org.openrdf.query.{BindingSet, Dataset, QueryEvaluationException}
import wsutil.{FormatterManager, RawFieldFilter, RawSingleFieldFilter, UnresolvedURIFieldKey}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class TripleStore(dataFetcher: DataFetcherImpl, cmwellRDFHelper: CMWellRDFHelper, tbg: NbgToggler) {

  //  private val dataFetcher = new DataFetcher(Config.defaultConfig.copy(intermediateLimit = 100000, resultsLimit = 100000))

  val crudServiceFS = cmwellRDFHelper.crudServiceFS
  val typesCache = new AggregateBothOldAndNewTypesCaches(crudServiceFS,tbg)

  // todo only use Await.result in findTriplesByPattern. All private methods should return Future[_]

  /**
    * Find statements by triple pattern. Can be used as the data layer of any SPARQL Engine
    */
  def findTriplesByPattern(triplePattern: TriplePattern)(implicit ec: ExecutionContext): Iterator[Quad] = {
    val infotons: Seq[Infoton] = triplePattern.subject match {
      case Some(s) =>
        fetchInfoton(uriToCmWellPath(s)).toList // Option.toList - zero or one Infotons
      case None =>
        val ff = predicateAndObjectToFieldFilter(triplePattern.predicate, triplePattern.value)
        dataFetcher.fetch(ff)._2
    }

    val quads = infotons.flatMap(infotonToQuads).toIterator
    filter(quads, triplePattern.predicate, triplePattern.value)
  }

  private def fetchInfoton(path: String): Option[Infoton] =
    Await.result(crudServiceFS.getInfoton(path, None, None), 10.seconds).collect { case Everything(i) => i }

  private def infotonToQuads(i: Infoton): Iterator[Quad] = {
    import scala.collection.JavaConversions._

    val ds = nullFormatter.formattableToDataset(i)

    def stmtToQuad(stmt: Statement, quad: Option[TripleStore.IRI]): Quad =
      Quad(stmt.getSubject.getURI, stmt.getPredicate.getURI, jenaNodeToValue(stmt.getObject), quad)

    val defaultModelQuads = {
      val it = ds.getDefaultModel.listStatements().map(stmtToQuad(_, None))
      if(it.hasNext) List(it) else Nil
    }

    val namedModelsQuadsIterators = JenaUtils.getNamedModels(ds).foldLeft(defaultModelQuads) {
      case (itList,(quad, model)) =>
        val it = model.listStatements().map(stmtToQuad(_, Some(quad)))
        if(it.hasNext) it :: itList else itList
    }

    new Iterator[Quad] {
      private[this] var iterators = namedModelsQuadsIterators
      override def hasNext: Boolean = iterators.headOption.exists(_.hasNext)
      override def next(): Quad = {
        val rv = iterators.head.next()
        if(!iterators.head.hasNext) iterators = iterators.tail
        rv
      }
    }
  }


  private def predicateAndObjectToFieldFilter(p: Option[TripleStore.IRI], o: Option[TripleStore.Value])(implicit ec: ExecutionContext): FieldFilter = {
    val value = o.map(_.asString)
    p match {
      case None => SingleFieldFilter(Must, Contains, "_all", value)
      case Some(pred) => Await.result(RawFieldFilter.eval(RawSingleFieldFilter(Must, Equals, Left(UnresolvedURIFieldKey(pred)), value),typesCache,cmwellRDFHelper,tbg.get), 10.seconds)
    }
  }

  // todo propagate withoutMeta boolean from request to here. If withoutMeta is set to false, we need to make sure it won't fail in URIFieldKey(...)
  val f: String => Option[(String,Option[String])] = {(o: Option[String]) => o.map(u => u -> Option.empty[String])} compose {s => cmwellRDFHelper.hashToUrl(s,tbg.get)}
  private val nullFormatter = new cmwell.formats.RDFFormatter("cmwell", f, withoutMeta = true, filterOutBlanks = false, forceUniqueness = false) {
    override def format: cmwell.formats.FormatType = ???
    override def render(formattable: Formattable): String = ???
  }

}

object TripleStore {

  type IRI = String // todo - this can be better than type. e.g. case class IRI(iri: String, ... )

  case class TriplePattern(subject: Option[IRI], predicate: Option[IRI], value: Option[Value])

  case class Triple(subject: IRI, predicate: IRI, value: Value)

  case class Quad(subject: IRI, predicate: IRI, value: Value, graph: Option[IRI])

  sealed trait Value { def asString: String }
  case class LiteralValue(value: Any, `type`: LitaralType) extends Value { override def asString: String = value.toString }
  case class IriValue(iri: IRI) extends Value { override def asString: String = iri }
  case object Anon extends Value { override def asString: String = "_B" }

  sealed trait LitaralType
  case object BooleanLtrl extends LitaralType
  case object StringLtrl extends LitaralType
  case class LangStringLtrl(lang: String) extends LitaralType
  case object IntLtrl extends LitaralType
  case object LongLtrl extends LitaralType
  case object FloatLtrl extends LitaralType
  case object DoubleLtrl extends LitaralType
  case object DateLtrl extends LitaralType
  case object UnknownLtrlType extends LitaralType

  private def uriToCmWellPath(uri: IRI) = uri.replace("https://", "/https.").replace("http:/", "").replace("cmwell:/", "")

  private def jenaNodeToValue(jNode: RDFNode): Value = {
    if (jNode.isAnon)
      Anon
    else if (jNode.isResource)
      IriValue(jNode.asResource().getURI)
    else {
      val l = jNode.asLiteral()

      l.getDatatype.getJavaClass match {
        case Types.boolean => LiteralValue(l.getBoolean, BooleanLtrl)
        case Types.string if l.getLanguage.isEmpty => LiteralValue(l.getString, StringLtrl)
        case Types.string => LiteralValue(l.getString, LangStringLtrl(l.getLanguage))
        case Types.date => LiteralValue(l.getValue, DateLtrl)
        case Types.float => LiteralValue(l.getFloat, FloatLtrl)
        case Types.double => LiteralValue(l.getDouble, DoubleLtrl)
        case Types.int => LiteralValue(l.getInt, IntLtrl)
        case Types.long => LiteralValue(l.getLong, LongLtrl)
        case _ => LiteralValue(l.getValue, UnknownLtrlType)
      }
    }
  }

  private object Types {
    val boolean = classOf[java.lang.Boolean]
    val string = classOf[java.lang.String]
    val date = classOf[XSDDateTime]
    val int = classOf[java.lang.Integer]
    val long = classOf[java.lang.Long]
    val float = classOf[java.lang.Float]
    val double = classOf[java.lang.Double]
  }

  // todo in future, it doesn't have to be ==, if filter is propagated as fieldOperator to support Range Queries
  private def filter(quads: Iterator[Quad], pred: Option[IRI], obj: Option[Value]): Iterator[Quad] =
    quads.filter(q => pred.fold(true)(_ == q.predicate) && obj.fold(true)(_ == q.value))
}

object SesameExtensions {

  class SortByCardinalityEvaluationStrategy(tripleSource: TripleSource, serviceResolver: FederatedServiceResolver)
    extends SimpleEvaluationStrategy(tripleSource: TripleSource, serviceResolver: FederatedServiceResolver) {

    override def evaluate(tuplExpr: TupleExpr, bindings: BindingSet): CloseableIteration[BindingSet, QueryEvaluationException] = {
      SortByCardinalityQueryOptimizer.optimize(tuplExpr, null, bindings)
      super.evaluate(tuplExpr, bindings)
    }

  }

  object SortByCardinalityQueryOptimizer extends QueryOptimizer {
    override def optimize(tupleExpr: TupleExpr, dataset: Dataset, bindings: BindingSet): Unit = {
      tupleExpr.visit(CardinalitySorterVisitor)
    }

    object CardinalitySorterVisitor extends AbstractQueryModelVisitor {

      /**
        * Metadata injection
        */
      // todo move to a different Optimizer, placing it here as a side effect is not POLA
//      override def meet(sp: StatementPattern): Unit = {
        // todo populate metadata with something valuable to further optimizations
//        val (cmWellSubVar, cmWellObjVar) = (CmWellValue("fooBarSubj", sp.getSubjectVar), CmWellValue("fooBarObj", sp.getObjectVar))
//        val updatedSp = new StatementPattern(cmWellSubVar, sp.getPredicateVar, cmWellObjVar, sp.getContextVar)
//        sp.replaceWith(updatedSp)
//        super.meet(sp)
//      }

//      override def meet(j: Join): Unit = {
//        val triplePatterns = joinNodeToList(j).collect{ case qmn: StatementPattern => qmn }
//        // todo reorder by "JoinSelectivity(cardinality)", andThen j.replaceWith(listToJoinNode(sortedTriplePatterns))
//        super.meet(j)
//      }

//      private def joinNodeToList(j: Join): List[QueryModelNode] = {
//        val (left, right) = (j.getLeftArg, j.getRightArg.asInstanceOf[QueryModelNode])
//        (left match {
//          case jLeft: Join => joinNodeToList(jLeft)
//          case _ => List(left)
//        }) :+ right
//      }
//
//      private def listToJoinNode(list: List[TupleExpr]) = list.reduce(new Join(_, _))
    }
  }


  def sesameValueToValue(sv: Value): TripleStore.Value = {
    sv match {
      case _: BNode => Anon
      case iri: SimpleIRI => IriValue(iri.getNamespace + iri.getLocalName)
      case sl: SimpleLiteral if sl.getLanguage.isPresent => LiteralValue(sl.stringValue(), LangStringLtrl(sl.getLanguage.get()))
      case sl: SimpleLiteral => sl.getDatatype.getLocalName.toLowerCase match {
        case "boolean" => LiteralValue(sl.booleanValue(), BooleanLtrl)
        case "datetime" => LiteralValue(new XSDDateTime(sl.calendarValue().toGregorianCalendar), DateLtrl)
        case "integer" => LiteralValue(sl.intValue(), IntLtrl)
        case "long" => LiteralValue(sl.longValue(), LongLtrl)
        case "float" => LiteralValue(sl.floatValue(), FloatLtrl)
        case "double" => LiteralValue(sl.doubleValue(), DoubleLtrl)
      }
      case other => LiteralValue(other.stringValue(), UnknownLtrlType)
    }
  }

  def valueToSesameValue(value: TripleStore.Value): Value = {
    def dateConvert(jenaDate: XSDDateTime): XMLGregorianCalendar = {
      val millis = jenaDate.asCalendar().getTimeInMillis
      new XMLGregorianCalendarImpl(new DateTime(millis).toGregorianCalendar)
    }
    value match {
      case Anon => factory.createBNode()
      case IriValue(uri) => factory.createIRI(uri)
      case LiteralValue(v, BooleanLtrl) => factory.createLiteral(v.asInstanceOf[Boolean])
      case LiteralValue(v, StringLtrl) => factory.createLiteral(v.asInstanceOf[String])
      case LiteralValue(v, LangStringLtrl(lng)) => factory.createLiteral(v.asInstanceOf[String]) // todo lang?
      case LiteralValue(v, IntLtrl) => factory.createLiteral(v.asInstanceOf[Int])
      case LiteralValue(v, LongLtrl) => factory.createLiteral(v.asInstanceOf[Long])
      case LiteralValue(v, FloatLtrl) => factory.createLiteral(v.asInstanceOf[Float])
      case LiteralValue(v, DoubleLtrl) => factory.createLiteral(v.asInstanceOf[Double])
      case LiteralValue(v, DateLtrl) => factory.createLiteral(dateConvert(v.asInstanceOf[XSDDateTime]))
      case LiteralValue(v, UnknownLtrlType) => factory.createLiteral(v.toString)
      case other => factory.createLiteral(other.toString)
    }
  }

  private val factory = SimpleValueFactory.getInstance()
}

case class CmWellValue(metadata: String, iri: String) extends SimpleIRI(iri)

object CmWellValue {
  def apply(metadata: String, `var`: Var): Var = {
    val value = apply(metadata, `var`.getValue)
    new Var(`var`.getName, value)
  }

  def apply(metadata: String, value: Value): Value = value match {
    case i: IRI => CmWellValue(metadata, i.getNamespace + i.getLocalName)
    case _ => value
  }
}
