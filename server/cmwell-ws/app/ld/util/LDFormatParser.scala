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


package cmwell.web.ld.util

import java.io.InputStream

import cmwell.domain._
import cmwell.fts._
import cmwell.util.string.Hash._
import cmwell.util.string._
import cmwell.web.ld.cmw.CMWellRDFHelper.{Create, Update}
import cmwell.web.ld.cmw._
import cmwell.web.ld.exceptions._
import ld.cmw.PassiveFieldTypesCache
import security.Token
import cmwell.ws.Settings
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.datatypes.xsd.impl.RDFLangString
import org.apache.jena.graph.BlankNodeId
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{Seq => _, _}
import com.typesafe.scalalogging.LazyLogging
import filters.Attrs
import logic.{CRUDServiceFS, InfotonValidator}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.xerces.util.XMLChar
import security.{AuthUtils, PermissionLevel}
import wsutil._

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.immutable.{Map => IMap, Set => ISet}
import scala.collection.mutable.{ListBuffer, Map => MMap, Set => MSet}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

/**
 * Created with IntelliJ IDEA.
 * User: gilad
 * Date: 7/10/13
 * Time: 12:16 PM
 * To change this template use File | Settings | File Templates.
 */
object LDFormatParser extends LazyLogging {

	private val http = "http://"
	private val https = "https://"
  private val cmwell = "cmwell://"
  private val blank = cmwell + "blank_node/"
  private lazy val cwd = new java.io.File("").toURI

  implicit class LegalChar(ch: Char) {
    def isLegal: Boolean = {
      val i = ch.toInt
      (45 to 57).contains(i)  ||     // (-, ., /, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
      (65 to 90).contains(i)  ||     // (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z)
      (97 to 122).contains(i) ||     // (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z)
      i == 95 || i == 35 || i > 126  // 95 = '_', 35 = '#', 123 to 126 are: ({, |, }, ~)
    }
  }

  def createFieldValue(l : Literal, q: Option[String]) : FieldValue = {
    val lang = {
      val lng = l.getLanguage
      if(lng.isEmpty) None
      else Some(lng)
    }
    val quad = q
    l.getDatatypeURI match {
      case null => FieldValue(l.getLexicalForm,lang,quad)
      case xsd: String if xsd.startsWith(XSDDatatype.XSD) => getXSDType(xsd.drop(XSDDatatype.XSD.length + 1), l,lang,quad) //TODO: no need for lang here, will be the next case
      case rls: String if rls == RDFLangString.rdfLangString.getURI => getXSDType("string",l,lang,quad)
      case fun: String if fun.startsWith("urn:x-hp-jena:Functor") => ???
      case str: String if str.contains('$') => throw new IllegalArgumentException(s"custom types cannot contain '$$' character. please encode as '%24' or use another type. type recieved: '$str'")
      case str: String => FExternal(l.getLexicalForm, str,quad)
    }
  }

  def getXSDType(xsdType: String, l: Literal,lang: Option[String],quad: Option[String]): FieldValue = xsdType match {
    case "float" => FFloat(l.getFloat,quad) //convertXSD(l.getFloat, XSDDatatype.XSDfloat, l.getLexicalForm,lang,quad)
    case "double" => FDouble(l.getDouble,quad) //convertXSD(l.getDouble, XSDDatatype.XSDdouble, l.getLexicalForm,lang,quad)
    case "int" => FInt(l.getInt,quad) //convertXSD(l.getInt ,XSDDatatype.XSDint, l.getLexicalForm,lang,quad)
    case "long" => FLong(l.getLong,quad) //convertXSD(l.getLong, XSDDatatype.XSDlong, l.getLexicalForm,lang,quad)
//    case "short" => convertXSD(l.getShort, XSDDatatype.XSDshort, l.getLexicalForm)
//    case "byte" => convertXSD(l.getByte, XSDDatatype.XSDbyte, l.getLexicalForm)
//    case "unsignedByte" => convertXSD(l.getLexicalForm, XSDDatatype.XSDunsignedByte, l.getLexicalForm)
//    case "unsignedShort" => convertXSD(l.getLexicalForm, XSDDatatype.XSDunsignedShort, l.getLexicalForm)
//    case "unsignedInt" => convertXSD(l.getLexicalForm, XSDDatatype.XSDunsignedInt, l.getLexicalForm)
//    case "unsignedLong" => convertXSD(l.getLexicalForm, XSDDatatype.XSDunsignedLong, l.getLexicalForm)
    case "decimal" => FBigDecimal(BigDecimal(l.getLexicalForm).underlying(),quad) //convertXSD(BigDecimal(l.getLexicalForm).underlying(), XSDDatatype.XSDdecimal, l.getLexicalForm,lang,quad)
    case "integer" => FBigInt(BigInt(l.getLexicalForm).underlying(),quad) //convertXSD(BigInt(l.getLexicalForm).underlying(), XSDDatatype.XSDinteger, l.getLexicalForm,lang,quad)
//    case "nonPositiveInteger" => convertXSD(l.getLexicalForm, XSDDatatype.XSDnonPositiveInteger, l.getLexicalForm)
//    case "nonNegativeInteger" => convertXSD(l.getLexicalForm, XSDDatatype.XSDnonNegativeInteger, l.getLexicalForm)
//    case "positiveInteger" => convertXSD(l.getLexicalForm, XSDDatatype.XSDpositiveInteger, l.getLexicalForm)
//    case "negativeInteger" => convertXSD(l.getLexicalForm, XSDDatatype.XSDnegativeInteger, l.getLexicalForm)
    case "boolean" => FBoolean(l.getBoolean,quad) //convertXSD(l.getBoolean, XSDDatatype.XSDboolean, l.getLexicalForm,lang,quad)
    case "string" => FString(l.getString,lang,quad) //convertXSD(l.getString, XSDDatatype.XSDstring, l.getLexicalForm,lang,quad)
//    case "normalizedString" => convertXSD(l.getLexicalForm, XSDDatatype.XSDnormalizedString, l.getLexicalForm)
//    case "anyURI" => convertXSD(l.getLexicalForm, XSDDatatype.XSDanyURI, l.getLexicalForm)
//    case "token" => convertXSD(l.getLexicalForm, XSDDatatype.XSDtoken, l.getLexicalForm)
//    case "Name" => convertXSD(l.getLexicalForm, XSDDatatype.XSDName, l.getLexicalForm)
//    case "QName" => convertXSD(l.getLexicalForm, XSDDatatype.XSDQName, l.getLexicalForm)
//    case "language" => convertXSD(l.getLexicalForm, XSDDatatype.XSDlanguage, l.getLexicalForm)
//    case "NMTOKEN" => convertXSD(l.getLexicalForm, XSDDatatype.XSDNMTOKEN, l.getLexicalForm)
//    case "ENTITY" => convertXSD(l.getLexicalForm, XSDDatatype.XSDENTITY, l.getLexicalForm)
//    case "ID" => convertXSD(l.getLexicalForm, XSDDatatype.XSDID, l.getLexicalForm)
//    case "NCName" => convertXSD(l.getLexicalForm, XSDDatatype.XSDNCName, l.getLexicalForm)
//    case "IDREF" => convertXSD(l.getLexicalForm, XSDDatatype.XSDIDREF, l.getLexicalForm)
//    case "NOTATION" => convertXSD(l.getLexicalForm, XSDDatatype.XSDNOTATION, l.getLexicalForm)
//    case "hexBinary" => convertXSD(l.getLexicalForm, XSDDatatype.XSDhexBinary, l.getLexicalForm)
//    case "base64Binary" => convertXSD(l.getLexicalForm, XSDDatatype.XSDbase64Binary, l.getLexicalForm)
//    case "date" => convertXSD(l.getLexicalForm, XSDDatatype.XSDdate, l.getLexicalForm)
//    case "time" => convertXSD(l.getLexicalForm, XSDDatatype.XSDtime, l.getLexicalForm)
    case "dateTime" => FDate(l.getLexicalForm,quad)//convertXSD(l.getLexicalForm, XSDDatatype.XSDdateTime, l.getLexicalForm,lang,quad)
//    case "duration" => convertXSD(l.getLexicalForm, XSDDatatype.XSDduration, l.getLexicalForm)
//    case "gDay" => convertXSD(l.getLexicalForm, XSDDatatype.XSDgDay, l.getLexicalForm)
//    case "gMonth" => convertXSD(l.getLexicalForm, XSDDatatype.XSDgMonth, l.getLexicalForm)
//    case "gYear" => convertXSD(l.getLexicalForm, XSDDatatype.XSDgYear, l.getLexicalForm)
//    case "gYearMonth" => convertXSD(l.getLexicalForm, XSDDatatype.XSDgYearMonth, l.getLexicalForm)
//    case "gMonthDay" => convertXSD(l.getLexicalForm, XSDDatatype.XSDgMonthDay, l.getLexicalForm)
    case s: String => FExternal(l.getLexicalForm, s"xsd#$s",quad)
  }

//  def convertXSD(default: Any, xsd: XSDDatatype, lexicalForm: String,lang: Option[String],quad: Option[String]): FieldValue = Try(FieldValue(default,lang,quad)) match {
//    case Success(obj) => obj
//    case Failure(_) => FieldValue(xsd.parse(lexicalForm),lang,quad)
//  }

  case class ParsingResponse(infotons: Map[String, Map[DirectFieldKey, Set[FieldValue]]],
                             metaData: Map[String,MetaData],
                             knownCmwellHosts: Set[String],
                             deleteMap: Map[String,Set[(String,Option[String])]],
                             deleteVal: Map[String, Map[String,Set[FieldValue]]],
                             deletePaths: List[String],
                             atomicUpdates: Map[String,String]) {

    def isEmpty: Boolean = {
      metaData.valuesIterator.forall(_.isEmpty) &&
      infotons.isEmpty &&
      deleteMap.isEmpty &&
      deleteVal.isEmpty &&
      deletePaths.isEmpty
    }

    private def merge[K](im1: Map[String, Map[K,Set[FieldValue]]], im2: Map[String, Map[K,Set[FieldValue]]]) = {
      val keys = im1.keySet ++ im2.keySet
      keys.map(k => {
        val s = Seq(im1.get(k), im2.get(k)).collect{ case Some(x) => x }
        val fieldKeys = s.map(_.keySet).reduce(_ ++ _)
        val values = fieldKeys.map(fk => fk -> s.map(_.get(fk)).collect{ case Some(x) => x }.reduce(_ ++ _)).toMap
        k -> values
      }).toMap
    }

    def ⑃(that: ParsingResponse): ParsingResponse = {
      val infotons = merge(this.infotons,that.infotons)
      val metaData = {
        val keys = this.metaData.keySet ++ that.metaData.keySet
        keys.map { k =>
          k -> {
            val metaDatas = Seq(this.metaData.get(k), that.metaData.get(k)).collect { case Some(x) => x}
            metaDatas.reduce[MetaData](_ merge _)
          }
        }.toMap
      }
      val knownCmwellHosts = this.knownCmwellHosts.union(that.knownCmwellHosts)
      val deleteMap = {
        val keys = this.deleteMap.keySet ++ that.deleteMap.keySet
        keys.map(k => k -> Set(this.deleteMap.get(k), that.deleteMap.get(k)).collect{ case Some(x) => x }.flatten).toMap
      }
      val deleteVal = merge(this.deleteVal,that.deleteVal)
      val deletePaths = this.deletePaths ::: that.deletePaths

      val atomicUpdatesBuilder = Map.newBuilder[String,String]
      this.atomicUpdates.foreach(atomicUpdatesBuilder.+=)
      that.atomicUpdates.foreach {
        case pu@(path,uuid) => this.atomicUpdates.get(path) match {
          case None => atomicUpdatesBuilder += pu
          case Some(thisUuid) => if (uuid != thisUuid)
            throw new IllegalArgumentException(s"Atomic versioning must be unique per path. Document contained multiple base uuids: [$uuid,$thisUuid] for path [$path]")
        }
      }

      ParsingResponse(infotons,metaData,knownCmwellHosts,deleteMap,deleteVal,deletePaths,atomicUpdatesBuilder.result())
    }
  }

  val dialectToLang: PartialFunction[String,Lang] = {
    case "RDF/XML" | "RDF/XML-ABBREV" => Lang.RDFXML
    case "N-TRIPLE" => Lang.NTRIPLES
    case "TURTLE" => Lang.TURTLE
    case "TTL" => Lang.TTL
    case "N3" => Lang.N3
    case "JSON-LD" => Lang.JSONLD
    case "NQ" => Lang.NQ
    case "NQUADS" => Lang.NQUADS
    case "TRIG" => Lang.TRIG
  }



  /**
   * {{{
   * # Scala REPL style
   * scala> val m = org.apache.jena.rdf.model.ModelFactory.createDefaultModel
   * model: org.apache.jena.rdf.model.Model = <ModelCom   {} | >
   *
   * scala> m.read(cmwell.util.string.stringToInputStream("""<> <cmwell://meta/sys#replaceGraph> <*> ."""),null,"N-TRIPLES")
   * res0: org.apache.jena.rdf.model.Model = <ModelCom   { @cmwell://meta/sys#replaceGraph *} |  [, cmwell://meta/sys#replaceGraph, *]>
   *
   * scala> {
   *      |   import cmwell.web.ld.util.LDFormatParser.isGlobalMetaOp
   *      |   val it = m.listStatements
   *      |   val s = it.next()
   *      |   isGlobalMetaOp(s)
   *      | }
   * res1: Boolean = true
   *
   * scala> val s = m.createStatement(m.createResource(""),m.createProperty("cmwell://meta/sys#replaceGraph"),m.createResource("http://example.org/graphs/superman"))
   * s: org.apache.jena.rdf.model.Statement = [, cmwell://meta/sys#replaceGraph, http://example.org/graphs/superman]
   *
   * isGlobalMetaOp(s)
   * res2: Boolean = true
   * }}}
   *
   * @param stmt
   * @return
   */
  def isGlobalMetaOp(stmt: Statement) = {

    val thisDocSub = {
      val subj = stmt.getSubject
      //meaning subject is: `<>`
      subj.isURIResource && (subj.getURI.isEmpty || new java.net.URI(subj.getURI) == cwd)
    }

    val isMetaOp = {
      val pred = stmt.getPredicate
      //must be inner cm-well meta operation
      pred.getNameSpace.matches(metaOpRegex("(sys|ns)"))
    }

    thisDocSub && isMetaOp
  }

  def parseGlobalExpressionsAsync(cmwellRDFHelper: CMWellRDFHelper,
                                  crudServiceFS: CRUDServiceFS)(
                                  models: Seq[(Option[String],Model)],
                                  urlToLast: Map[String,String],
                                  metaNsInfotonMap: InfotonRepr): Future[ParsingResponse] = {
    val quadsToDelete = MSet.empty[String]
    val fuzzyQuadsToDelete = MSet.empty[String]
    val metaInfotons: MInfotonRepr = MMap.empty
    val deleteFieldsMap = MMap.empty[String, Set[(String, Option[String])]]

    var replaceGraphStatementsCounter = 0

    models.foreach {
      case (g,m) => {
        val globalMetaOpsStatements = {
          val it = m.listStatements(new SimpleSelector() {
            override def selects(s: Statement): Boolean = isGlobalMetaOp(s)
          })
          var s = Set.empty[Statement]
          while (it.hasNext) {
            s += it.next()
          }
          s
        }
        globalMetaOpsStatements.foreach { stmt =>
          val (pLocalName, pNameSpace) = {
            val p = stmt.getPredicate
            p.getLocalName -> p.getNameSpace
          }
          val o = stmt.getObject

          pLocalName match {
            case "replaceGraph" if o.isURIResource && pNameSpace.matches(metaOpRegex("sys")) => {
              val uri = o.asResource().getURI
              require(uri != "*", "trying to delete all quads with <*> ? naughty...")
              require(uri.nonEmpty, "empty quad as value for `replaceGraph` is illegal...")
              quadsToDelete += uri
              replaceGraphStatementsCounter += 1
            }
            case "fuzzyReplaceGraph" if pNameSpace.matches(metaOpRegex("sys")) => {
              val fuzzyQuad = o.toString
              require(fuzzyQuad.nonEmpty, "empty quad as value for `fuzzyReplaceGraph` is illegal...")
              fuzzyQuadsToDelete += fuzzyQuad
              replaceGraphStatementsCounter += 1
            }
            case "graphAlias" if g.isDefined => {
              require(o.isLiteral)
              val url = g.get
              val path = s"/meta/quad/${Base64.encodeBase64URLSafeString(url)}"
              val alias = o.asLiteral.getString
              metaInfotons ++= Map[String, Map[DirectFieldKey, Set[FieldValue]]](path ->
                Map[DirectFieldKey, Set[FieldValue]](
                  NnFieldKey("alias") -> Set(FString(alias)),
                  NnFieldKey("graph") -> Set(FReference(url))
                )
              )
              updateDeleteMap(deleteFieldsMap, path, "alias", None)
            }
            case prefix if pNameSpace.matches(metaOpRegex("ns")) => {
              val url = o.toString
              val last = urlToLast.getOrElse(url, cmwellRDFHelper.nsUrlToHash(url)._1)
              val (path, nsInfotonRepr) = createMetaNsInfoton(last, url, prefix)
              metaInfotons.update(path, nsInfotonRepr)
              updateDeleteMap(deleteFieldsMap, path, "prefix", None)
            }
            case op => throw new IllegalArgumentException(s"operation [$op] on <> with value [$o] is not defined.")
          }
        }
        require(replaceGraphStatementsCounter <= Settings.graphReplaceMaxStatements, s"maximum allowed `replaceGraph` statements per request exceeded (${replaceGraphStatementsCounter} > ${Settings.graphReplaceMaxStatements}).")
      }
    }

    val globalQuadsDeletionFuture = {
      if (quadsToDelete.isEmpty && fuzzyQuadsToDelete.isEmpty) {
        Future.successful(IMap.empty[String, Set[(String, Option[String])]])
      }
      else {
        //TODO: handle Global Operations authorizations
        //        require({
        //          if(/*Props.productionMode ||*/ java.lang.Boolean.getBoolean("use.authorization")) AuthUtils.isOperationAllowedForUser(null,None)
        //          else true
        //        }, "Global Operations must be enabled with proper authorization.")

        //we will use it in another thread context, so we must convert to immutable
        val quadsToDeleteImmutable = ISet.empty[String] ++ quadsToDelete
        val fuzzyQuadsToDeleteImmutable = ISet.empty[String] ++ fuzzyQuadsToDelete

//        val fieldFiltersForQuadsSearch = quadsToDeleteImmutable.map { q =>
//          (Should: FieldOperator) -> List(FieldFilter(Must, Equals, "system.quad", Some(q)))
//        }.toList ::: fuzzyQuadsToDeleteImmutable.flatMap { q =>
//          List(
//            (Should: FieldOperator) -> List(FieldFilter(Must, Contains, "quad", Some(q))),
//            (Should: FieldOperator) -> List(FieldFilter(Must, Contains, "system.quad", Some(q)))
//          )
//        }.toList

        val fieldFiltersForQuadsSearch = quadsToDeleteImmutable.map { q =>
          FieldFilter(Should, Equals, "system.quad", q)
        }(scala.collection.breakOut[Set[String],FieldFilter,List[FieldFilter]]) ::: fuzzyQuadsToDeleteImmutable.flatMap { q =>
          List(FieldFilter(Should, Contains, "quad", q),FieldFilter(Should, Contains, "system.quad", q))
        }(scala.collection.breakOut[Set[String],FieldFilter,List[FieldFilter]])

        implicit val searchTimeout = Some(Settings.graphReplaceSearchTimeout.seconds)

        crudServiceFS.thinSearch(
          pathFilter = None,
          fieldFilters = Some(MultiFieldFilter(Must, fieldFiltersForQuadsSearch)),
          datesFilter = None,
          paginationParams = PaginationParams(0, Settings.maxSearchResultsForGlobalQuadOperations),
          withHistory = false,
          withDeleted = false,
          fieldSortParams = NullSortParam,
          debugInfo = true).map { searchThinResults =>
            require(searchThinResults.total <= Settings.maxSearchResultsForGlobalQuadOperations, "Request is too costly. " +
              s"You cannot use global quads deletion if you need to modify/delete more than ${Settings.maxSearchResultsForGlobalQuadOperations} infotons. " +
              "But not all is lost, you can still do it manually:\n" +
              "(1) grab the subjects you're interested in via search/stream/iterator/consumer by specifying `qp=system.quad::<QUAD_URI>`\n" +
              "(2) for each such subject, POST the statement: `<SUBJECT> <cmwell://meta/sys#markReplace> <*> <QUAD_URI>`")
            searchThinResults.thinResults.map {
              _.path -> quadsToDeleteImmutable.map(q => "*" -> (Some(q): Option[String]))
            }.toMap
        }
      }
    }

    globalQuadsDeletionFuture.map { qDelMap =>
      ParsingResponse(
        infotons = metaNsInfotonMap ++ metaInfotons,
        metaData = IMap.empty,
        knownCmwellHosts = ISet.empty,
        deleteMap = qDelMap ++ deleteFieldsMap,
        deleteVal = IMap.empty,
        deletePaths = List.empty,
        atomicUpdates = IMap.empty)
    }
  }

  def validateAuth(authUtils: AuthUtils)(model: Model, dels: Resource => Boolean, token: Option[Token]): Unit = {
    val paths = ListBuffer[String]()
    val it = model.listStatements()
    it.foreach { stmt =>
      val s = stmt.getSubject
      if (!((s.isAnon && dels(s))||isGlobalMetaOp(stmt))) {
        Try {
          val path = subjectFromStatement(stmt.getSubject)
          paths += path
        }.recover {
          case t: security.UnauthorizedException => throw t
          case t: Throwable => {
            logger.error(s"statement causes failure: $stmt",t)
            throw t
          }
        }.get
      }
    }
    val unauthorizedPaths = authUtils.filterNotAllowedPaths(paths, PermissionLevel.Write, token)
    if (unauthorizedPaths.nonEmpty) throw new security.UnauthorizedException(unauthorizedPaths.mkString("unauthorized paths:\n\t", "\n\t", "\n\n"))
  }

  case class DataSetConstructs(graphModelTuplesSeq: Seq[(Option[String], Model)], urlToLastMap: Map[String,String], metaNsInfotons: InfotonRepr)

  def modelsFromRdfDataInputStream(cmwellRDFHelper: CMWellRDFHelper)(rdfData: InputStream, dialect: String): DataSetConstructs = {
    val ds = DatasetFactory.createGeneral()
    RDFDataMgr.read(ds, rdfData, dialectToLang(dialect))

    val defaultModel = ds.getDefaultModel
    val metaNsInfotonsAcc = MMap.empty[String, Map[DirectFieldKey, Set[FieldValue]]]
    val urlToLastAcc = MMap.empty[String, String]
    val modelsSeqWithdefaultModel: Seq[(Option[String],Model)] = Seq(None -> defaultModel)
    val graphModelsTuplesSeq = (modelsSeqWithdefaultModel /: ds.listNames) {
      case (acc,name) => {
        val m = ds.getNamedModel(name)
        val (u2lMap,nsiMap) = convertMetaMapToInfotonFormatMap(cmwellRDFHelper,m)
        metaNsInfotonsAcc ++= nsiMap
        urlToLastAcc ++= u2lMap
        acc :+ (Some(name) -> m)
      }
    }

    val (urlToLastDefault,metaNsInfotonsDefault) = convertMetaMapToInfotonFormatMap(cmwellRDFHelper,defaultModel)

    val metaQuadsMap = (IMap.empty[String,Map[DirectFieldKey,Set[FieldValue]]] /: graphModelsTuplesSeq) {
      case (m,(subGraph, model)) => {
        val quadOpt = subGraph.flatMap(uri => Option(model.getNsURIPrefix(uri)).map(_ -> uri))
        quadOpt match {
          case Some((prefix,url)) if url != "*" && cmwellRDFHelper.getAliasForQuadUrl(url).isEmpty => {
            m ++ Map[String, Map[DirectFieldKey, Set[FieldValue]]](s"/meta/quad/${Base64.encodeBase64URLSafeString(url)}" ->
              Map[DirectFieldKey, Set[FieldValue]](
                NnFieldKey("alias") -> Set(FString(prefix)),
                NnFieldKey("graph") -> Set(FReference(url))
              )
            )
          }
          case _ => m
        }
      }
    }

    DataSetConstructs(
      graphModelsTuplesSeq,
      urlToLastDefault ++ urlToLastAcc,
      metaNsInfotonsDefault ++ metaNsInfotonsAcc ++ metaQuadsMap
    )
  }

  def isQuadsDialect(dialect: String): Boolean = dialect match {
    case "RDF/XML" | "RDF/XML-ABBREV" => false
    case "N-TRIPLE" => false
    case "TURTLE" => false
    case "TTL" => false
    case "N3" => false
    case "JSON-LD" => true
    case "NQ" => true
    case "NQUADS" => true
    case "TRIG" => true
    case "TRIX" => true
    case _ => throw new IllegalArgumentException(s"unknown RDF dialect: [$dialect]")
  }

  def changeQuadAccordingToDialect(quad: Option[String], dialect: String): Option[String] =  quad.orElse {
    if (isQuadsDialect(dialect)) None
    else Some("*")
  }

	def rdfToInfotonsMap(cmwellRDFHelper: CMWellRDFHelper,
                       crudServiceFS: CRUDServiceFS,
                       authUtils: AuthUtils,
                       rdfData: String,
                       dialect: String,
                       token: Option[Token],
                       skipValidation: Boolean,
                       isOverwrite: Boolean): Future[ParsingResponse] =
    rdfToInfotonsMap(cmwellRDFHelper,crudServiceFS,authUtils,stringToInputStream(rdfData),dialect,token,skipValidation,isOverwrite)

	def rdfToInfotonsMap(cmwellRDFHelper: CMWellRDFHelper,
                       crudServiceFS: CRUDServiceFS,
                       authUtils: AuthUtils,
                       rdfData: InputStream,
                       dialect: String,
                       token: Option[Token],
                       skipValidation: Boolean,
                       isOverwrite: Boolean): Future[ParsingResponse] = {

		require(dialectToLang.isDefinedAt(dialect), "the format " + dialect + " is unknown.")

    val DataSetConstructs(models,urlToLast,metaInfotonsMap) = modelsFromRdfDataInputStream(cmwellRDFHelper)(rdfData, dialect)

    val globalParsingResponse = parseGlobalExpressionsAsync(cmwellRDFHelper,crudServiceFS)(models,urlToLast,metaInfotonsMap)

    val parsingResults = models.map{ case (subGraph,model) =>

      val dels: Set[Resource] = {
        val p = model.createProperty("cmwell://meta/sys#", "markDelete")
        val it = model.listStatements(null, p, null)
        val all = for {
          stmt <- it
          o = stmt.getObject
        } yield o.asResource
        all.toSet
      }

      val (delSubs, regular) = {
        val (delSubs, regular) = model.listStatements().partition(s => dels(s.getSubject.asResource))
        val filteredRegular = regular.filterNot(stmt => {
          val sub = stmt.getSubject
          //i.e: this is `this document's subject`, which is used for global operations
          sub.isURIResource && sub.getURI.isEmpty
        })
        delSubs.toSet -> filteredRegular
      }

      validateAuth(authUtils)(model,dels,token)

      if(isOverwrite) {

        logger.debug(model.listStatements().toIterable.mkString("\n"))

        val metaNamespaces = model.listNameSpaces().count(_.contains("/meta/sys#"))
        if (metaNamespaces != 0 || subGraph.isEmpty) {
          require(metaNamespaces == 1, s"must use exactly one `/meta/sys#` namespace in document when forcing uniquness, namespaces found: ${model.listNameSpaces().mkString("[", ",", "]")}")
          val metaNs = model.listNameSpaces().find(_.contains("/meta/sys#")).get
          val props: Seq[Property] = Seq("type", "lastModified", "dataCenter", "indexTime").map(model.createProperty(metaNs, _))
          val subIt = model.listSubjects()
          var subPred: String = ""
          var l: java.util.List[RDFNode] = null
          require(subIt.forall { s =>
            props.forall { p =>
              l = model.listObjectsOfProperty(s, p).toList
              val rv = l.size == 1
              if (!rv) {
                subPred = l.map(o => s"<${s.toString}> <${p.toString}> <${o.toString}> .").mkString("[ "," , "," ]")
              }
              rv
            }
          },"every infoton must have exactly one of each `/meta/sys#` properties. " +
            "i.e. don't upload 2 (or more) versions of the same infoton in a single document. " +
            "in case you have multiple versions of the same infoton, " +
            "you must split your document into multiple documents, " +
            "each containing at most 1 version of the infoton. bad trples are: " + subPred)
        }
      }

      val infotonsMap = MMap[String, Map[DirectFieldKey, Set[FieldValue]]]()
      val cmwMetaDataMap = MMap[String, MetaData]()
      val cmwHosts = MSet[String]()
      val deleteFieldsMap = MMap[String, Set[(String,Option[String])]]() // {infoton path -> [field1, ..., fieldN]}
      val deleteValuesMap = MMap[String, Map[String, Set[FieldValue]]]()
      val deletePathsList = ListBuffer[String]()
      val atomicUpdatesBuilder = Map.newBuilder[String,String]

      val it = regular.filterNot(isGlobalMetaOp)
      while (it.hasNext) {
        val stmt: Statement = it.next()
        if (!dels(stmt.getSubject.asResource)) {
          val subject: String = subjectFromStatement(stmt.getSubject)

          val predicate: Either[MetaPredicate,DirectFieldKey] = {
            val p = stmt.getPredicate
            val url = p.getNameSpace
            val firstName = p.getLocalName

            require(XMLChar.isValidNCName(firstName), s"illegal predicate localName: $firstName, predicate localName must conform the XML Namespaces 1.0 Recommendation: http://www.w3.org/TR/1999/REC-xml-names-19990114/#NT-NCName")

            if (isCMWellSystemNS(url)) {
              if (!url.startsWith(cmwell)) {
                cmwHosts += {
                  if (url.startsWith(http)) url.drop(http.length).takeWhile(_ != '/')
                  else if (url.startsWith(https)) url.drop(https.length).takeWhile(_ != '/')
                  else throw new IllegalArgumentException(s"the url: $url is weird")
                }
              }
              if (url.endsWith("/meta/nn#")) Right(NnFieldKey(firstName))
              else if (url.endsWith("/meta/sys#") && firstName == "markReplace") Left(MarkReplace)
              else if (url.endsWith("/meta/sys#") && firstName == "markDelete") Left(MarkDelete)
              else if (url.endsWith("/meta/sys#") && firstName == "fullDelete") Left(FullDelete)
              else if (url.endsWith("/meta/sys#") && firstName == "prevUUID") Left(PrevUUID)
              else Left(SysField(firstName))
            }
            else {
              Right(HashedFieldKey(firstName,urlToLast(url)))
            }
          }

          val allowWriteSysFields = (fieldName: String) => fieldName match {
            case "uuid"|"path"|"parent"|"lastModified"|"modifiedDate"|"indexTime"|"dataCenter" => authUtils.isOperationAllowedForUser(security.Overwrite, token, evenForNonProdEnv = true)
            case _ => true
          }

          val obj = stmt.getObject

          predicate match {
            case Right(fieldName) => {
              val value = createValueFromObject(obj, subGraph)
              if (infotonsMap.contains(subject)) {
                val infoton = infotonsMap(subject)
                val newValueSet = infoton.getOrElse(fieldName, Set[FieldValue]()) + value
                infotonsMap.update(subject, infoton + (fieldName -> newValueSet))
              }
              else {
                infotonsMap.update(subject, Map[DirectFieldKey, Set[FieldValue]](fieldName -> Set(value)))
              }
            }
            case Left(SysField(sys)) if allowWriteSysFields(sys) => {
              val value = createValueFromObject(obj)
              updateMetaData(cmwMetaDataMap, subject, sys, value)
            }
            case Left(_:SysField) => {
              // do nothing (i.e. ignore system fields)
            }
            case Left(MarkDelete) if obj.isAnon => {
              //predicate-value iterator
              val pvit = for {
                delStmt <- model.listStatements(obj.asResource, null, null)
                p = delStmt.getPredicate
                v = delStmt.getObject
              } yield p -> v
              updateDeleteValuesMap(cmwellRDFHelper, deleteValuesMap, subject, pvit.toList, changeQuadAccordingToDialect(subGraph,dialect))
            }
            case Left(MarkDelete) => throw new IllegalArgumentException("when using 'markDelete' API, one must supply an anonymous node as value")
            case Left(FullDelete) => {
              val path = normalizePath(subject)

              val recursive: Boolean =
                if (obj.isLiteral) obj.asLiteral().getBoolean
                else throw new IllegalArgumentException(s"was expecting a boolean value stating if the delete is recursive or not for path: $subject")

              val fut = controllers.ApplicationUtils.infotonPathDeletionAllowed(path,recursive,crudServiceFS)

              //TODO: Await is ugly :( ... but imperative loop with mutable accumulators forces this kind of uglyness
              Await.result(fut, Duration.Inf) match {
                case Right(xs) => deletePathsList ++= xs
                case Left(err) => throw new IllegalArgumentException(err)
              }
            }
            case Left(MarkReplace) => {
              val cmwellFieldName = {
                if (!obj.isResource) getCmwellFieldNameForUrl(cmwellRDFHelper,obj.toString)
                else {
                  val r = obj.asResource()
                  getCmwellFieldNameForUrl(cmwellRDFHelper, r.getNameSpace, Some(r.getLocalName))
                }
              }
              if(cmwellFieldName.isEmpty) throw new IllegalArgumentException(s"CM-Well could not resolve predicate [${
                if (!obj.isResource) obj.toString
                else {
                  val r = obj.asResource()
                  r.getNameSpace + r.getLocalName
                }
              }] as a valid field to be markReplaced.")
              cmwellFieldName.foreach { fieldName =>
                updateDeleteMap(deleteFieldsMap, subject, fieldName, subGraph)
              }
            }
            case Left(PrevUUID) => {
              val uuid = obj.toString
              require(uuid.isEmpty || uuid.matches("^[a-f0-9]{32}$"),s"invalid uuid supplied for path: [$subject] and predicate prevUUID")
              require(!stmt.getSubject.isAnon,"prevUUID predicates can't operate on anonymous subjects.")
              atomicUpdatesBuilder += subject -> uuid
            }
          }
        }
      }

      val immutableInfotonsMap: IMap[String, IMap[DirectFieldKey, Set[FieldValue]]] = IMap[String, Map[DirectFieldKey, Set[FieldValue]]]() ++ infotonsMap

      //make sure all the infotons are valid!
      if (!skipValidation) {
        require(immutableInfotonsMap.keys.forall(k => InfotonValidator.isInfotonNameValid(normalizePath(k))), "one or more subjects in the document are not valid")
      }

      val metaData = IMap[String, MetaData]() ++ cmwMetaDataMap
      val knownCmwellHosts = ISet[String]() ++ cmwHosts
      val deleteMap = IMap[String, Set[(String,Option[String])]]() ++ deleteFieldsMap
      val deleteVal = IMap[String, Map[String, Set[FieldValue]]]() ++ deleteValuesMap
      val deletePaths = deletePathsList.toList

      ParsingResponse(immutableInfotonsMap, metaData, knownCmwellHosts, deleteMap, deleteVal, deletePaths, atomicUpdatesBuilder.result())
    }.reduce(_ ⑃ _)

    globalParsingResponse.map(_ ⑃ parsingResults)
	}

  sealed trait MetaPredicate
  final case object MarkReplace extends MetaPredicate
  final case object MarkDelete extends MetaPredicate
  final case object FullDelete extends MetaPredicate
  final case object PrevUUID extends MetaPredicate
  final case class SysField(field: String) extends MetaPredicate

  def createValueFromObject(obj: RDFNode, subGraph: Option[String] = None) = {
    if (obj.isAnon) {
      val node = obj.asNode
      if (node.isBlank) FReference(blank + anonNameMimic(node.getBlankNodeId), subGraph)
      else throw new RuntimeException("object: %s is anon but not blank?".format(obj.toString))
    }
    else fieldValueFromObject(obj, subGraph)
  }

  def fieldValueFromObject(obj: RDFNode, quad: Option[String]) =
    if (obj.isLiteral) createFieldValue(obj.asLiteral,quad)
    else FReference(obj.toString,quad) //(subjectFromStatement(obj.asResource,true),None,quad)

  def updateDeleteMap(deleteFieldsMap: MMap[String,Set[(String,Option[String])]], infotonPath: String, cmwellFieldName: String, quad: Option[String]): Unit = {
    val fToDel = cmwellFieldName -> quad
    deleteFieldsMap.get(prependSlash(infotonPath)) match {
      case Some(fieldSet) if !fieldSet(fToDel) => deleteFieldsMap.update(prependSlash(infotonPath),fieldSet + fToDel)
      case None => deleteFieldsMap.update(prependSlash(infotonPath), Set(fToDel))
      case _ => //DO NOTHING (the deleted field already exist in the deleteFieldsMap)
    }
  }

  def updateDeleteValuesMap(cmwellRDFHelper: CMWellRDFHelper,
                            deleteValuesMap: MMap[String,Map[String,Set[FieldValue]]],
                            infotonPath: String,
                            pvl: List[(Property,RDFNode)],
                            quad: Option[String]): Unit = {
    pvl.foreach{
      case (p,v) => {
        val predicateUrl = p.getNameSpace
        require(!predicateUrl.matches(metaOpRegex("(sys|ns)")), s"sys fields not allowed for markDelete failed for: [$predicateUrl${p.getLocalName}]")
        val cmwAttribute = getCmwellFieldNameForUrl(cmwellRDFHelper, predicateUrl, Some(p.getLocalName)).get //TODO: protect against empty Option
        val validValue: FieldValue = fieldValueFromObject(v,quad)
        deleteValuesMap.get(infotonPath) match {
          case None =>  deleteValuesMap.update(infotonPath, Map(cmwAttribute -> Set(validValue)))
          case Some(mss) => mss.get(cmwAttribute) match {
            case None => deleteValuesMap.update(infotonPath, mss.updated(cmwAttribute, Set(validValue)))
            case Some(sf) =>  deleteValuesMap.update(infotonPath, mss.updated(cmwAttribute, sf + validValue))
          }
        }
      }
    }
  }

  private def urlValidate(u: String): Boolean = Try(new java.net.URL(u)).isSuccess ||
    (u.startsWith(cmwell) && Try(new java.net.URL(http + u.drop(cmwell.length))).isSuccess)

  def getCmwellFieldNameForUrl(cmwellRDFHelper: CMWellRDFHelper, predicateUrl: String, localName: Option[String] = None): Option[String] = predicateUrl match {
    case "*" => Some("*")
    case _ => {
      require(urlValidate(predicateUrl), s"the url: ${predicateUrl} is not a valid predicate url for ingest.")
      val (url, firstName) = localName match {
        case None => {
          val fName = predicateUrl.reverse.takeWhile(!uriSeparator(_)).reverse
          val url = predicateUrl.dropRight(fName.length)
          (url, fName)
        }
        case Some(fName) => (predicateUrl, fName)
      }

      if (url.endsWith("/meta/nn#")) {
        if(firstName.contains(".")) throw new IllegalArgumentException(s"nn (No Namespace) fields aren't allowed to contain dots. [$url$firstName]")
        else Some(firstName)
      }
      else cmwellRDFHelper.urlToHash(url).map{
        hash =>  s"$firstName.$hash"
      }
    }
  }

  def updateMetaData(cmwMetaDataMap: MMap[String, MetaData], subject: String, predicate: String, value: FieldValue) {
    cmwMetaDataMap.get(subject) match {
      case Some(md) => cmwMetaDataMap.update(subject,generateMetaData(predicate, value, md))
      case None => cmwMetaDataMap.update(subject, generateMetaData(predicate, value))
    }
  }

  def generateMetaData(predicate: String, value: FieldValue, md: MetaData = MetaData.empty): MetaData = {

    predicate match {
      case "type" => value.toString match {
        case "OBJ"  | "ObjectInfoton" | "CompoundInfoton" => md.copy(mdType = Some(ObjectMetaData))
        case "FILE" | "FileInfoton" => md.copy(mdType = Some(FileMetaData))
        case "LINK" | "LinkInfoton" => md.copy(mdType = Some(LinkMetaData))
        case "DEL"  | "DeletedInfoton" =>  md.copy(mdType = Some(DeletedMetaData))
        case _ => logger.warn("type: " + value + ", is not yet treated in cmwell RDF imports."); md
      }
      case "modifiedDate"| "lastModified" => md.copy(date = Some(Try(value.asInstanceOf[FDate].getDate).getOrElse(dt(value.toString))))
      case "contentType" | "mimeType" =>  md.copy(mdType = Some(FileMetaData), mimeType = Some(value.toString))
      case "data" => md.copy(mdType = Some(FileMetaData), text = Some(value.toString))
      case "base64-data" => md.copy(mdType = Some(FileMetaData), data = Some(Base64.decodeBase64(value.toString)))
      case "linkTo" if value.isInstanceOf[FReference] => md.copy(mdType = Some(LinkMetaData), linkTo = Some(value.asInstanceOf[FReference].getCmwellPath))
      case "linkTo" => md.copy(mdType = Some(LinkMetaData), linkTo = Some(value.toString))
      case "linkType" => md.copy(mdType = Some(LinkMetaData), linkType = Some(Try(value.asInstanceOf[FInt].value).getOrElse(md.linkType.getOrElse(1))))
      case "dataCenter" => md.copy(dataCenter = Some(value.toString))
      case "indexTime" => md.copy(indexTime = Try(value.asInstanceOf[FLong].value).toOption)
      case "uuid" | "parent" | "path" => md // these have no affect. better ignore without polluting the logs
      case _ => logger.warn("attribute: " + predicate + ", is not treated in cmwell RDF imports."); md
    }
  }

  type InfotonRepr  =  Map[String, Map[DirectFieldKey, Set[FieldValue]]]
  type MInfotonRepr = MMap[String, Map[DirectFieldKey, Set[FieldValue]]]

  //TODO: a more sophisticated approach is needed - domain should belong to cmwell, cmwell-host inference logic could be done by saving under /meta/sys a field: known_cmwell_domains, and check if domain is known to belong to CMWell...
  def isCMWellModifiedDateNS(ns: String) = ns.endsWith("/meta/sys#modifiedDate")
  def isCMWellSystemNS(url: String) = url.endsWith("/meta/sys#") || url.endsWith("/meta/nn#") || url.endsWith("/meta/ns#")

  /**
   * @param model
   * @return tuples of:
   * _1 : map from ns url to inner identifier (hash or old repr)
   * _2 : map of new `/meta/ns` infotons to write into cm-well
   */
  def convertMetaMapToInfotonFormatMap(cmwellRDFHelper: CMWellRDFHelper, model: Model): (Map[String,String],InfotonRepr) = {

    val noJenaMap = model.getNsPrefixMap.toSeq.collect{
      case (shortName,uriValue) if !shortName.matches("""[Jj].\d+""") && shortName != "" =>
        uriValue -> shortName
    }.toMap

    val all = {

      val it = model
        .listStatements()
        .map(_.getPredicate.getNameSpace)
        .filterNot(_.matches(metaOpRegex("(sys|ns|nn)")))

//      val it = model.listNameSpaces.filterNot(_.matches(metaOpRegex("(sys|ns|nn)")))
      
      it.map {
        case url if url.contains('$') => throw new IllegalArgumentException(s"predicate namespace must not contain a dollar ('$$') sign: $url")
        case url =>
        //      TODO: commented out is more efficient, but won't update old style data.
        //      TODO: replace current code with commented out, once all the old data is updated.
        //      val t = CMWellRDFHelper
        //        .urlToHash(url)
        //        .map(_ -> Exists)
        //        .getOrElse(CMWellRDFHelper.nsUrlToHash(url))
        //      t -> url
        cmwellRDFHelper.nsUrlToHash(url) -> url
      }.toSeq
    }
    val m = all.map{
      case ((hash,_),url) => url -> hash
    }.toMap

    val n = all.collect {
      case ((hash,Create),url) => createMetaNsInfoton(hash, url, noJenaMap.getOrElse(url, inferShortNameFromUrl(url)))
      case ((hash,Update),url) => createMetaNsInfoton(hash, url, noJenaMap.getOrElse(url, hash))
    }.toMap
    m -> n
  }

  def createMetaNsInfoton(last: String, url: String, prefix: String): (String,Map[DirectFieldKey,Set[FieldValue]]) = {
    val path = s"/meta/ns/$last"
    path -> Map(
      NnFieldKey("url") -> Set(FieldValue(url)),
      NnFieldKey("prefix") -> Set[FieldValue](FString(prefix, None, None))
    )
  }

  /**
   * {{{
   * # Scala REPL style
   * scala> cmwell.web.ld.util.LDFormatParser.inferShortNameFromUrl("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
   * res0: String = wwo
   * }}}
 *
   * @param url
   * @return
   */
  @tailrec
  def inferShortNameFromUrl(url: String, pilledSuffix: String = ""): String = {

    /*
     *this is how jena checks the legality of NS prefixes:
     *
     * private void checkLegal( String prefix )
     * {
     *   if (prefix.length() > 0 && !XMLChar.isValidNCName( prefix ))
     *     throw new PrefixMapping.IllegalPrefixException( prefix );
     * }
     */

    //if recursion peeled all the URL down to the bare domain, which wasn't legal: use the modified domain:
    if(url.isEmpty) {
      val domain = pilledSuffix.split('.')
      val chosenPartOrInitials = domain.find(!blackList(_)).getOrElse(domain.flatMap(_.headOption).mkString)
      val (start,rest): Tuple2[String,String] = if(XMLChar.isNCNameStart(chosenPartOrInitials.head)) chosenPartOrInitials.splitAt(1) else ("ns-",chosenPartOrInitials)
      val end: String = rest.flatMap{
        ch: Char => ch match {
          case c: Char if XMLChar.isNCName(c) => s"$ch"
          case '@' => "-at-"
          case '&' => "-amp-"
          case '+' => "-plus-"
          case _ => "-"
        }//TODO: this is temporary escaping mechanism, and should be thought through.
      }
      start + end
    }
    else {
      val mUrl = {
        if (url.startsWith("http://")) url.drop("http://".length)
        else if (url.startsWith("https://")) url.drop("https://".length)
        else url
      }
      val candidate = mUrl.reverse.tail.takeWhile(XMLChar.isNCName(_)).reverse
      if (XMLChar.isValidNCName(candidate) && !candidate.contains('.')) candidate
      else inferShortNameFromUrl(mUrl.dropRight(candidate.length + 1), candidate)
    }
  }

//  /**
//   *
//   * @param url
//   * @return
//   */
//  @tailrec
//  def inferQuadNameFromUrl(url: String, pilledSuffix: String = ""): String = {
//    if(url.isEmpty) {
//      val domain = pilledSuffix.split('.')
//      val chosenPartOrInitials = domain.find(!blackList(_)).getOrElse(domain.flatMap(_.headOption).mkString)
//      val (start,rest): Tuple2[String,String] = if(chosenPartOrInitials.headOption.isDefined && XMLChar.isNCNameStart(chosenPartOrInitials.head)) chosenPartOrInitials.splitAt(1) else ("q-",chosenPartOrInitials)
//      start + rest
//    }
//    else {
//      val candidate = {
//        val it = url.reverseIterator
//        (it.next, it.takeWhile(XMLChar.isNCName(_)).mkString.reverse) match {
//          case (char,rest) if XMLChar.isNCName(char) => s"$rest$char"
//          case (_, name) => name
//        }
//      }
//      if (XMLChar.isValidNCName(candidate)) candidate.map{
//        case '.' => '-'
//        case ch => ch
//      }
//      else inferQuadNameFromUrl(url.dropRight(candidate.length + 1), candidate)
//    }
//  }

  def blackList(s: String): Boolean = s.length < 3 || s.matches("""(www\d*)|static|org|net|com|biz|gov|edu|https?""")

  /**
   * method copied from
 *
   * @see org.apache.jena.rdf.model.impl.NTripleWriter.anonName
   */
  def anonNameMimic(id: BlankNodeId): String = {
    val name: StringBuilder = new StringBuilder("A") //no need for "_:" prefix. and ':' is not URI friendly...
    val sid: String = id.toString
    for (c: Char <- sid) {
      c match {
        case 'X' => name.append("XX")
        case _: Char if (c.isLetterOrDigit) => name.append(c)
        case _ => name.append("X" + c.toInt.toHexString + "X")
      }
    }
    return name.mkString
  }

//  /**
//   *
//   * @param url
//   * @return
//   */
//	def getNsShortName(url: String): String = {
//    val noProtocol = if(url.startsWith(http)) url.drop(http.length)
//    else if(url.startsWith(https)) url.drop(https.length)
//    else if(url.matches("\\w+://.+")){ //some other weird protocol... regex matches <protocol>://whatever...
//      var drops = 0
//      while(url.drop(drops).matches("\\w+://.+"))
//        drops = drops + 1
//      url.drop(drops+3)
//    }
//    else url //is this even a valid url?
//
//      val short = inferShortNameFromUrl(noProtocol)
//      CMWellRDFHelper.nsUrlToHash(short, url)
//	}

  /**
   *
   * @param short
   * @param url
   * @return
   */
	def shortWithUrlHash(short: String, url: String): String = short + "-" + crc32(url)

  def subjectFromStatement(res: Resource): String = {
    val su = {
      if (res.isAnon) blank + anonNameMimic(res.getId.getBlankNodeId)
      else res.getURI
    }
    if(su.matches("""((http(s?))|(cmwell))://\S+""")){
      val domainLength = detectIfUrlBelongsToCmwellAndGetLength(su)
      val v =
        if(su.startsWith(http)) su.drop(http.length + domainLength)
        else if(su.startsWith(https)) "https." + su.drop(https.length + domainLength)
        else su.drop(cmwell.length)

      val u = v.takeWhile(_ != '/') match {
        case host9000 if host9000.endsWith(":9000") => v.drop(host9000.length + 1)
        case cmw if cmw.toLowerCase.matches(""".*cm(\-)?well.*""") => v.drop(cmw.length + 1)
        case connext if connext.toLowerCase.contains("connext") => v.drop(connext.length + 1)
        case domain => v
      }

      if(u.forall(_.isLegal)) normalizePath(u)
      else throw new UnsupportedURIException(s"illegal subject name: $u")
    }
    else throw new UnsupportedURIException(s"URI must be http://* or https://* or cmwell://*. other formats are not supported. got: '$su' (isURI=${res.isURIResource}, isEmpty=${Option(res.getURI).exists(_.isEmpty)})")
  }
}
