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

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.{ContextBase, OutputStreamAppender}
import cmwell.crashableworker.{Status, _}
import cmwell.ctrl.config.Jvms
import cmwell.domain.{Everything, FString, FieldValue, FileInfoton}
import cmwell.fts.PathFilter
import cmwell.plugins.spi.SgEngineClient
import cmwell.util.concurrent._
import cmwell.util.files
import cmwell.util.http.SimpleResponse
import cmwell.util.loading.ChildFirstURLClassLoader
import cmwell.web.ld.exceptions.ParsingException
import cmwell.ws.Settings
import com.google.common.cache._
import com.typesafe.scalalogging.LazyLogging
import k.grid.{Grid, GridJvm}
import logic.CRUDServiceFS
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory, Statement}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import org.apache.jena.sparql.function.{FunctionRegistry, Function => JenaFunction}
import org.apache.jena.sparql.mgt.Explain
import org.apache.jena.sparql.resultset.ResultsFormat
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import play.api.mvc._
import cmwell.syntaxutils._
import cmwell.util.os.Props
import cmwell.zcache.L1Cache
import javax.inject._

import akka.NotUsed
import cmwell.ws.util.TypeHelpers
import cmwell.ws.util.TypeHelpers.asBoolean
import controllers.SpHandler.logger
import wsutil.overrideMimetype

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.parsing.combinator.RegexParsers
import scala.util.{Failure, Random, Success, Try}

/**
* Created by yaakov on 5/12/15.
*/

@Singleton
class SpHandlerController @Inject()(crudServiceFS: CRUDServiceFS, nbgToggler: NbgToggler)(implicit ec: ExecutionContext) extends Controller with LazyLogging with TypeHelpers {

  val parser = new SPParser

  def handleSparqlPost(formatByRest: String = "") = Action.async(parse.tolerantText) {
    implicit req =>
      handlePost(req, "") { req => (rp: RequestParameters) => /* no need to parse, returning the case class right away */ OverallSparqlQuery(req.body,req.host,rp) }
  }

  def handleSpPost(formatByRest: String = "") = Action.async(parse.tolerantText) {
    implicit req =>
      handlePost(req, formatByRest) { req => parser.parseQuery(req.body.filterNot(_ == '\r')) }
  }

  private def handlePost[T](req: Request[String], formatByRest: String = "")(parse: Request[String]=>(RequestParameters=>T)) = {

    import SpHandler._

    Try(parse(req)) match {
      case Failure(err) => Future.successful(wsutil.exceptionToResponse(new IllegalArgumentException("Parsing error")))
      case Success(fun) => {
        val formatByRestOpt = if (formatByRest.nonEmpty) Some(formatByRest) else None
        val rp = RequestParameters(req, formatByRestOpt)
        Try(fun(rp)) match {
          case Failure(err) => Future.successful(wsutil.exceptionToResponse(err))
          case Success(paq) => {
            val resultPromise = Promise[Result]()

            //TODO: consider using `guardHangingFutureByExpandingToSource` instead all the bloat below
            val singleEndln = Source.single(cmwell.ws.Streams.endln)
            val nbg = req.getQueryString("nbg").flatMap(asBoolean).getOrElse(false)

            val futureThatMayHang = if(rp.bypassCache) task(nbg || nbgToggler.get)(paq) else viaCache(nbg || nbgToggler.get,crudServiceFS)(paq)
            val initialGraceTime = 7.seconds
            val injectInterval = 3.seconds
            val backOnTime: QueryResponse => Result = {
              case Plain(v) => Ok(v)
              case Filename(path) => Ok.sendPath(Paths.get(path),onClose = () => files.deleteFile(path))
              case ThroughPipe(pipe) => ???
              case RemoteFailure(e) => wsutil.exceptionToResponse(e)
              case ShortCircuitOverloaded(activeRequests) => ServiceUnavailable("Busy, try again later").withHeaders("Retry-After" -> "10", "X-CM-WELL-N-ACT" -> activeRequests.toString)
            }
            val prependInjections = () => singleEndln
            val injectOriginalFutureWith: QueryResponse => Source[ByteString,_] = {
              case Plain(v) => Source.single(ByteString(v,StandardCharsets.UTF_8))
              case Filename(path) => FileIO.fromPath(Paths.get(path)).mapMaterializedValue(_.onComplete(_ => files.deleteFile(path)))
              case ThroughPipe(pipe) => ???
              case RemoteFailure(e) => {
                logger.error("_sp failure",e)
                Source.single(ByteString("Could not process request",StandardCharsets.UTF_8))
              }
              case ShortCircuitOverloaded(activeRequests) => Source.single(ByteString("Busy, try again later",StandardCharsets.UTF_8))
            }
            val continueWithSource: Source[Source[ByteString, _],NotUsed] => Result = s => Ok.chunked(s.flatMapConcat(x => x))

            wsutil.guardHangingFutureByExpandingToSource[QueryResponse,Source[ByteString,_],Result](futureThatMayHang,initialGraceTime,injectInterval)(backOnTime,prependInjections,injectOriginalFutureWith,continueWithSource)
          }
        }
      }
    }
  }
}

object SpHandler extends LazyLogging {

  lazy val nActorSel = Grid.selectActor("NQueryEvaluatorActor", GridJvm(Jvms.CW))
  lazy val oActorSel = Grid.selectActor("OQueryEvaluatorActor", GridJvm(Jvms.CW))
  implicit lazy val timeout = akka.util.Timeout(100.seconds)
  lazy val queryTimeout = 90.seconds

  def task[T](nbg: Boolean)(paq: T) = {
    val actorSel = {
      if (nbg) nActorSel
      else oActorSel
    }
    (actorSel ? paq).mapTo[QueryResponse].andThen {
      case Failure(e) =>
        logger.error(s"ask to ${if(nbg)"n"else "o"}ActorSel failed",e)
    }
  }
  def digest[T](input: T): String = cmwell.util.string.Hash.md5(input.toString)
  def deserializer(payload: Array[Byte]): QueryResponse = Plain(new String(payload, "UTF-8"))
  def serializer(qr: QueryResponse): Array[Byte] = qr match { case Plain(s) => s.getBytes("UTF-8") case _ => !!! }
  def isCachable(qr: QueryResponse): Boolean = qr match { case Plain(_) => true case _ => false }

  def viaCache(nbg: Boolean, crudServiceFS: CRUDServiceFS) = cmwell.zcache.l1l2(task(nbg))(digest,deserializer,serializer,isCachable)(
    ttlSeconds = Settings.zCacheSecondsTTL,
    pollingMaxRetries = Settings.zCachePollingMaxRetries,
    pollingInterval = Settings.zCachePollingIntervalSeconds,
    l1Size = Settings.zCacheL1Size)(crudServiceFS.zCache)
}



class SPParser extends RegexParsers {

  val spacesOrTabs = "[\\t ]*".r
  val whiteSpaces = """\s*""".r
  val anything = ".+".r
  val anythingOrEmpty = ".*".r
  val newline = "\n"
  val minuses = "((-{3,}))"
  override val skipWhitespace = false

  val paths = "(?i)paths".r ^^ {_.toLowerCase}

  val sourcesLiteral: Parser[String] = paths // | uuids | etc' ...
  def sourcesHeadLine: Parser[String] = whiteSpaces ~> sourcesLiteral <~ newline

  def source: Parser[String] = "/.*".r ^^ {
    //make sure all query strings has an equals sign, to help FakeRequest handle it better
    case path => {
      val (p,q) = {
        val all = path.split('?')
        all.head -> all.tail.mkString("?")
      }
      p + "?" + q.split('&').map{
        case qs if qs.contains('=') => qs
        case qs => qs + "="
      }.mkString("&")
    }
  } | failure("encountered invalid path") //TODO: refine constraints
  def sourceRep: Parser[Seq[String]] = repsep(source,newline)
  def sources: Parser[Seq[String]] = sourcesHeadLine ~> sourceRep

  val importLiteral = "(?i)import".r ^^ {_.toLowerCase}
  def importHeadline: Parser[String] = whiteSpaces ~> importLiteral <~ newline
  def importRep: Parser[Seq[String]] = repsep(anything, newline)
  def imports: Parser[Seq[String]] = importHeadline ~> importRep

  val sparql = "(?i)sparql".r ^^ {_ => {
    (sources: Seq[String], imports: Seq[String], queries: Seq[String]) => (rp: RequestParameters) => SparqlQuery(sources, imports, queries, rp)
  }}

  val gremlin = "(?i)gremlin".r ^^ {_ => {
    (sources: Seq[String], imports: Seq[String], queries: Seq[String]) => (rp: RequestParameters) => GremlinQuery(sources, imports, queries, rp)
  }}

  val queryLiteral: Parser[(Seq[String],Seq[String],Seq[String]) => (RequestParameters) => PopulateAndQuery] = sparql | gremlin
  def queryHeadLine: Parser[(Seq[String],Seq[String],Seq[String]) => (RequestParameters) => PopulateAndQuery] = whiteSpaces ~> queryLiteral <~ newline

  def queries: Parser[(Seq[String],Seq[String]) => (RequestParameters) => PopulateAndQuery] = queryHeadLine ~ repsep(anythingOrEmpty, newline) <~ whiteSpaces ^^ {
    case func ~ queries => (sources: Seq[String], imports: Seq[String]) => (rp: RequestParameters) => func(sources, imports, queries.mkString("\n").split(minuses).map(_.trim))(rp)
  }

  def populateAndQuery: Parser[(RequestParameters) => PopulateAndQuery] = sources ~ repN(1,newline) ~ imports ~ repN(1,newline) ~ queries ^^ {
    case s ~ _ ~ i ~ _ ~ f => (rp: RequestParameters) => f(s,i)(rp)
  } | sources ~ repN(1,newline) ~ queries ^^ {
    case s ~ _ ~ f => (rp: RequestParameters) => f(s,Seq())(rp) // import is optional. when not provided, is an empty Seq
  }

  import scala.util.{Failure => UFailure, Success => USuccess}

  def parseQuery(input: String): (RequestParameters) => PopulateAndQuery = {
    def throwError(msg: String) = throw new ParsingException(s"error: $msg, accured while parsing: $input")

    Try(parseAll(populateAndQuery, input)) match {
      case USuccess(Success(paq, _)) => paq
      case USuccess(NoSuccess(msg, _)) => throwError(msg)
      case UFailure(err) => throwError(err.getMessage)
    }
  }
}

object PopulateAndQuery extends LazyLogging {
  //TODO: generalize (adjust to use cmwell.formats)
  def formatToMimetype(format: String): String = format.toLowerCase match {
    case "ascii" => "text/plain;charset=UTF8"
    case "tsv"   => "text/tab-separated-values;charset=UTF8"
    case "rdf"   => "application/rdf+xml;charset=UTF8"
    case "xml"   => "application/xml;charset=UTF8"
    case "json"  => "application/json;charset=UTF8"
    case _       => "text/plain;charset=UTF8"
  }

  implicit val system: ActorSystem = Grid.system
  implicit val materializer = ActorMaterializer()

  def httpRequest2(path: String): Future[Either[String,InputStream]] = {
    def nonError(httpCode: Int) = httpCode < 300
    val textPlainUtf8 = Seq("Content-Type"->"text/plain;charset=UTF-8")

    //this import is causing the response to contain an `InputStream` instead of the default `Array[Byte]`
    import cmwell.util.http.SimpleResponse.Implicits.InputStreamHandler

    def pathHasURLEncodedQps = path.contains("%")

    val (justPath,queryParams) = if(!path.contains('?')) path -> "?" else path.splitAt(path.indexOf('?'))
    val parsedQueryParams = queryParams.substring(1).split('&').map{kv => val s = kv.split('='); (if(s.length>0) s(0) else "") -> (if(s.length>1) s(1) else "")}.toSeq

    val pathToGet = "http://localhost:9000" + (if(pathHasURLEncodedQps) path else justPath)
    val qpsToGet = if(pathHasURLEncodedQps) Seq() else parsedQueryParams

    cmwell.util.http.SimpleHttpClient.get(pathToGet, qpsToGet, textPlainUtf8).map {
      case SimpleResponse(respCode,_,(_,inputStream)) if nonError(respCode) => Right(inputStream)
      case SimpleResponse(respCode,_,(_,inputStream)) => {
        val respBody = scala.io.Source.fromInputStream(inputStream,"UTF-8").mkString
        Left(s"HTTP $respCode: $respBody")
      }
    }
  }

  import cmwell.syntaxutils._

  import scala.collection.JavaConversions._

  def retryFetchingIfWasEmpty(path: String): Future[Either[String,Dataset]] = {
    lazy val errMsg = {
      val reason = if(path.toLowerCase.contains("with-data")) "this might be a data issue" else "try adding `with-data`"
      s"Fetching $path had no results, $reason"
    }

    cmwell.util.concurrent.retry(3, 500.millis) {
      def isEmpty(ds: Dataset): Boolean = {
        val statements = JenaUtils.discardQuadsAndFlattenAsTriples(ds).listStatements.toVector

        def getLongValueOfSystemField(field: String): Option[Long] =
          statements.find(_.getPredicate.getURI.contains(s"/meta/sys#$field")).map(_.getObject.asLiteral().getLong)

        val (total, length) = (getLongValueOfSystemField("total"), getLongValueOfSystemField("length"))
        val min = List(Option(1L),total,length).flatten.min
        val dataPredicates = statements.map(_.getPredicate.getURI).filterNot(_.contains("/meta/sys"))

        // if we have total or length and not having that amount of data OR in case total and length aren't present,
        // we expect to at least one data statement
        dataPredicates.length < min
      }

      httpRequest2(path).flatMap {
        case Left(s) => Future.successful(Left(s)) // not retrying when status code > 200
        case Right(is) => {
          val ds = loadRdfToDataset(is)
          if(isEmpty(ds)) Future.failed(new Exception(errMsg)) // to retry
          else Future.successful(Right(ds))
        }
        case _ => !!!
      }
    }
  }

  def loadRdfToDataset(ntriplesOrNquads: InputStream, ds: Dataset = DatasetFactory.createGeneral()): Dataset = {
    RDFDataMgr.read(ds, ntriplesOrNquads, Lang.NQUADS)
    ds
  }
}

import controllers.PopulateAndQuery.retryFetchingIfWasEmpty

trait TimedRequestKind
case object Subgraph extends TimedRequestKind { override def toString = "Subgraph" }
case object Sparql   extends TimedRequestKind { override def toString = "SPARQL  " }
case object Gremlin  extends TimedRequestKind { override def toString = "Gremlin " }
case object Import   extends TimedRequestKind { override def toString = "Import  " }

abstract class PopulateAndQuery {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit class StringUtils(s: String) {
    def padOrEllide(length: Int): String = {
      if(s.length<length-3)
        s.padTo(length, ' ')
      else
        s.substring(0,length-3) + "..."
    }
  }

  type T
  def sources: Seq[String]
  def queries: Seq[String]
  val rp: RequestParameters
  def evaluate(jarsImporter: JarsImporter,queriesImporter: QueriesImporter,sourcesImporter: SourcesImporter): Future[String]

  def isTiming = rp.verbose && rp.format == "ascii"
  def isDumping = rp.showGraph && rp.format == "ascii"

  val relativeEpochTime = System.currentTimeMillis

  val timedRequestTitle = {
    def pad(p: String, to: Int = 13) = p.padTo(to, ' ')
    pad("Start") + pad("End") + pad("Duration") + pad("Type", 9) + "Task" + " "*60 + " # lines"
  }

  case class TimedRequest(path: String, kind: TimedRequestKind, started: Long, ended: Long) {
    private val fmt : DateTimeFormatter = ISODateTimeFormat.hourMinuteSecondMillis
    private def formatTime(millis: Long) = new DateTime(millis-relativeEpochTime).toString(fmt)
    override def toString: String = s"${formatTime(started)} ${formatTime(ended)} $kind $path"
    def toString(manipulator: String=>String): String = s"${formatTime(started)} ${formatTime(ended)} ${formatTime(ended-started+relativeEpochTime)} $kind ${manipulator(path)}"
  }

  protected def start(id: String, kind: TimedRequestKind): TimedRequest = TimedRequest(id, kind, System.currentTimeMillis, 0)
  protected def end(tr: TimedRequest): TimedRequest = tr.copy(ended=System.currentTimeMillis)


  lazy val populateFormat = if(rp.quads) "nquads" else "ntriples"
//  lazy val populateLanguage = if(rp.quads) Lang.NQUADS else Lang.NTRIPLES

  def populate(handler: ((Option[TimedRequest],Either[String,Dataset])) => (Option[TimedRequest],T), host: String = System.getProperty("cmwell.base")): Seq[Future[(Option[TimedRequest],T)]] = {

    def addFormat(url: String) ={
      url + (if (url.contains('?')) "&" else "?") + s"format=$populateFormat"
    }


    def validateUrl(url: String) = {
      //TODO: add more constraints
      def validKeyValuePair(key: String, value: String): Boolean = key->value match {
        case (k, v) if k.toLowerCase == "op" => Set("search", "read")(v.toLowerCase)
        case (k, v) if k.toLowerCase == "format" => v.toLowerCase == populateFormat
        case _ => true
      }

      val queryString = url.dropWhile(_!='?').tail
      queryString.split('&').map(_.split("=",2)).foreach { kv => require(validKeyValuePair(kv(0),kv(1))) }
    }

    def timeWrapIfNeeded[T](id: String, f: => Future[T]): Future[(Option[TimedRequest],T)] = {
      if(isTiming) {
        val tr = start(id, Subgraph)
        f.map(Some(end(tr)) -> _)
      } else {
        f.map(None -> _)
      }
    }

    sources.map(populatePlaceHolders).map(addFormat _ andThen { path => validateUrl(path); timeWrapIfNeeded(path, retryFetchingIfWasEmpty(path)).map(handler) })
  }

  def extractBody(tRes: (Option[TimedRequest],Either[String,Dataset])): (Option[TimedRequest],Either[String,Dataset]) = {
    tRes._1 -> tRes._2
  }

  def addTimingAndOrGraphAsNeeded(ds: Dataset, perf: String): String = {
    val sb = new StringBuilder

    if(isTiming)
      sb.append(s"Time metrics:\n$timedRequestTitle\n$perf\n\n")

    if(isDumping) {
      val os = new ByteArrayOutputStream
      if (rp.quads)
        RDFDataMgr.write(os, ds, Lang.NQUADS)
      else
        RDFDataMgr.write(os, ds.getDefaultModel, Lang.NTRIPLES)

      sb.append(s"Graph:\n${new String(os.toByteArray, "UTF-8")}\n")
    }

    sb.toString
  }

  def populatePlaceHolders(s: String): String = {
    "%([a-zA-Z]+)%".r.replaceAllIn(s, `match` => {
      val placeholder = `match`.group(1)
      val key = placeholder.toLowerCase
      rp.customParams.getOrElse(key, placeholder)
    })
  }
}

case class SparqlQuery( sources: Seq[String],
                        imports: Seq[String],
                        queries: Seq[String],
                        rp: RequestParameters) extends PopulateAndQuery {

  override type T = Either[String,Dataset]

  import scala.concurrent.ExecutionContext.Implicits.global

  val concreteQueries = queries.map(populatePlaceHolders)

  override def evaluate(jarsImporter: JarsImporter,queriesImporter: QueriesImporter,sourcesImporter: SourcesImporter): Future[String] = {

    //todo t._2 t._1.map m._1 + t._1 ... WAT? Please use case classes or something to make this fold more readable.
    //todo also - why there's the `first`? Can't we reduceLeft instead? This is not DRY.

    val ntriplesFutures: Seq[Future[(Option[TimedRequest], Either[String,Dataset])]] = populate(extractBody)
    val first = ntriplesFutures.head.map{t =>
      t._2 match {
        case Left(errMsg) => {
          t._1.map(_.toString(_.padOrEllide(64)) + " 0       ").getOrElse("") + errMsg + "\n" -> DatasetFactory.create()
        }
        case Right(firstDs) => {
          t._1.map(_.toString(_.padOrEllide(64)) + " " + JenaUtils.size(firstDs) + "\n").getOrElse("") -> firstDs
        }
      }
    }
    val perfAndDatasetFuture = (first /: ntriplesFutures.tail) {
      case (fm, fs) => fm.flatMap { m =>
        fs.map { t =>
          t._2 match {
            case Left(errMsg) => {
              m._1 + t._1.map(_.toString(_.padOrEllide(64)) + " 0       ").getOrElse("") + errMsg + "\n" -> DatasetFactory.create()
            }
            case Right(ds) => {
              val curDs = JenaUtils.merge(ds, m._2)
              m._1 + t._1.map(_.toString(_.padOrEllide(64)) + " " + JenaUtils.size(curDs) + "\n").getOrElse("") -> curDs
            }
          }
        }
      }
    }

    //TODO: FIXME! DON'T AFFECT GLOBALLY
//    if(rp.disableImportsCache) { Importers.all.foreach(_.invalidateCaches()) }

    import cmwell.util.collections.partition3
    import cmwell.util.collections.opfut

    val (jarImportsPaths, sprqlImportsPaths, sourceImportPaths) = partition3(imports) {
      case e if e.endsWith(".jar") => 0
      case e if e.endsWith(".scala") => 2
      case _ => 1
    }

    val importsFut = for {
      ij <- jarsImporter.fetch(jarImportsPaths)
      iq <- queriesImporter.fetch(sprqlImportsPaths).map(_.map(populatePlaceHolders))
      is <- sourcesImporter.fetch(sourceImportPaths)
    } yield (ij,iq,is)

    perfAndDatasetFuture.flatMap {
      perfAndDs => {

        val (perf, ds) = (perfAndDs._1, perfAndDs._2)
        val os = new ByteArrayOutputStream

        importsFut.map { case (ij,iq,is) =>

          ij.foreach(jenaFunc => FunctionRegistry.get.put(s"jar:${jenaFunc.getClass.getName}", jenaFunc.getClass))
          is.foreach{ case NamedAnonJenaFuncImpl(name, impl) => FunctionRegistry.get.put(s"jar:$name", impl.getClass) }

          val sparqlTime = concreteQueries.map { query =>
            if (isTiming) {

              val verboseImportsSb = new StringBuilder

              val expandedDs = if(iq.isEmpty) ds else {
                val dss = iq.map { q =>
                  val tr = start(q.replace("\n", " "), Import)
                  val curDs = JenaUtils.expandDataset(ds, q)
                  verboseImportsSb.append(end(tr).toString(_.padOrEllide(64)) + " " + JenaUtils.size(curDs) + "\n")
                  curDs
                }
                JenaUtils.flatten(dss :+ ds)
              }

              val queryOneLined = query.replace("\n", " ")
              val tr = start(queryOneLined, Sparql)
              val size = evaluateSparql(query, expandedDs, rp, os)
              verboseImportsSb.toString() + end(tr).toString(_.padOrEllide(64)) + " " + size
            } else {
              val expandedDs = if(iq.isEmpty) ds else JenaUtils.flatten(iq.map( q => JenaUtils.expandDataset(ds, q)) :+ ds)
              evaluateSparql(query, expandedDs, rp, os)
              ""
            }
          }.mkString("\n")

          val verbosePart = addTimingAndOrGraphAsNeeded(ds, perf + sparqlTime)
          val res = verbosePart + new String(os.toByteArray, "UTF-8")

          os.flush()
          os.close()

          res
        }
      }
    }
  }

  def evaluateSparql(sparql: String, ds: Dataset, rp: RequestParameters, outputStream: OutputStream): Long = {
    val dsWithoutSsytemBlankNodes = JenaUtils.filter(ds) { s => !(s.getSubject.isAnon && s.getPredicate.getNameSpace.contains("meta/sys")) }

    val query = QueryFactory.create(sparql)
    val qExec = QueryExecutionFactory.create(query, dsWithoutSsytemBlankNodes)

    val tempOs = new ByteArrayOutputStream

    if(rp.execLogging) {
      val arqLgr = ARQ.getExecLogger.asInstanceOf[ch.qos.logback.classic.Logger]

      val encoder = new PatternLayoutEncoder()
      encoder.setContext(arqLgr.getLoggerContext)
      encoder.setPattern("%d{HH:mm:ss} %-5p :: %m%n")
      encoder.start()

      val osAppender = new OutputStreamAppender[ILoggingEvent]()
      osAppender.setContext(new ContextBase)
      osAppender.setEncoder(encoder)
      osAppender.setOutputStream(tempOs)
      arqLgr.addAppender(osAppender)
      osAppender.start()

      qExec.getContext.set(ARQ.symLogExec, Explain.InfoLevel.ALL)

      tempOs.write("Execution Logging:\n".getBytes)
    }

    // todo wrap query types more nicely (cmwell-881)

    if(!(query.isSelectType || query.isConstructType))
      throw new Exception("This query type is not supported.")

    if(query.isSelectType) {
      val results = qExec.execSelect

      if(rp.execLogging) tempOs.write("\n".getBytes)
      if(rp.verbose) tempOs.write("Results:\n".getBytes)

      (rp.format match {
        case "tsv" => Some(ResultsFormat.FMT_RS_TSV)
        case "xml" => Some(ResultsFormat.FMT_RS_XML)
        case "json" => Some(ResultsFormat.FMT_RS_JSON)
        case "rdf" => Some(ResultsFormat.FMT_RDF_NT)
        case "ascii" => None
        case _ => throw new IllegalArgumentException(s"Unsupported format for SELECT: ${rp.format}")
      }
        ).map(ResultSetFormatter.output(tempOs, results, _))
        .getOrElse(ResultSetFormatter.out(tempOs, results, query)) // ascii
    }

    if(query.isConstructType) {
      // todo 1. do we want to allow the user choose RDF Format?
      // todo 2. what about quads in this case? see http://stackoverflow.com/questions/18345908/construct-into-a-named-graph
      val results = qExec.execConstruct

      if(rp.execLogging) tempOs.write("\n".getBytes)
      if(rp.verbose) tempOs.write("Results:\n".getBytes)

      RDFDataMgr.write(tempOs, results, RDFFormat.NTRIPLES)
    }

    val results = new String(tempOs.toByteArray, "UTF-8")
    outputStream.write((results+"\n").getBytes)

    if(rp.format != "ascii" || results.replace("\n","").trim.isEmpty) {
      0
    } else {
      val headersAndBorders = 4
      results.count(_=='\n') - headersAndBorders
    }
  }
}

case class GremlinQuery(sources: Seq[String], imports: Seq[String], queries: Seq[String], rp: RequestParameters) extends PopulateAndQuery {
  override type T = Either[String,Dataset]

  import scala.concurrent.ExecutionContext.Implicits.global

  // engine is def and not val to allow this Case Class to be transported to a remote actor. otherwise akka will throw some Serialization exception.
  // it won't hurt performance since its body is a lazy-val property of an Object.
  def engine = SgEngines.engines.getOrElse("Gremlin", throw new Exception("Could not load Gremlin engine!"))

  // todo. Stay DRY. Massage some of PopulateAndQuery code to have a basic String evaluate.
  override def evaluate(jarsImporter: JarsImporter,queriesImporter: QueriesImporter,sourcesImporter: SourcesImporter): Future[String] = {
    val ntriplesFutures: Seq[Future[(Option[TimedRequest],Either[String,Dataset])]] = populate(extractBody)

    //todo finish implement verbodeMode for Gremlin as well
    val first = ntriplesFutures.head.map(_._2).map { case Left(_) => DatasetFactory.create() case Right(ds) => ds }
    val modelFuture = (first /: ntriplesFutures.tail) {
      case (fm, fs) => fm.flatMap(m => fs.map(_._2 match { case Left(_) => m case Right(ds) => JenaUtils.merge(ds, m) }))
    }

    modelFuture.map { m =>
      // todo support system fields in Gremlin
      val ds = DatasetFactory.create(JenaUtils.filter(m.getDefaultModel){ s => !s.getPredicate.getNameSpace.contains("meta/sys") })
      engine.eval(ds, queries.head) //todo for each query in queries
    }
  }
}

case class OverallSparqlQuery(query: String, host: String, rp: RequestParameters)

case object StatusRequest

case class RequestParameters(format: String, quads: Boolean, verbose: Boolean, showGraph: Boolean,
                             forceUsingFile: Boolean, disableImportsCache: Boolean, execLogging: Boolean,
                             doNotOptimize: Boolean, intermediateLimit: Long, resultsLimit: Long,
                             explainOnly: Boolean, customParams: Map[String,String] = Map(), bypassCache: Boolean)
object RequestParameters {
  def apply(req: Request[_], formatByRest: Option[String]) = new RequestParameters(
    // todo formatByRest is deprecated. One day we should be stop respecting it.
    format = formatByRest.getOrElse(req.getQueryString("format").getOrElse("ascii")),
    quads = req.getQueryString("quads").isDefined,
    verbose = req.getQueryString("verbose").isDefined,
    showGraph = req.getQueryString("show-graph").isDefined,
    forceUsingFile = req.getQueryString("x-write-file").isDefined, // this is a secret one
    disableImportsCache = req.getQueryString("x-no-cache").isDefined, // this is a secret one as well
    execLogging = req.getQueryString("x-verbose-exec").isDefined, // this is a secret one as well. WARNING! Do not run parallel _sp requests with this one!
    doNotOptimize = req.getQueryString("x-do-not-optimize").isDefined, // this is a secret one for _sparql, it's not in use in _sp
    intermediateLimit = req.getQueryString("intermediate-limit").map(_.toLong).getOrElse(10000), // this one is for _sparql, it's not in use in _sp
    resultsLimit = req.getQueryString("results-limit").map(_.toLong).getOrElse(10000), // this one is for _sparql, it's not in use in _sp
    explainOnly = req.getQueryString("explain-only").isDefined, // this one is for _sparql, it's not in use in _sp
    customParams = req.queryString.
      toSeq.
      collect{ case (key, values) if key.startsWith("sp.") => "(?i)sp\\.".r.replaceAllIn(key, "").toLowerCase -> values.head }.
      toMap,
    bypassCache = req.getQueryString("bypass-cache").isDefined
  )
}

object SgEngines extends LazyLogging {
  lazy val engines: Map[String,SgEngineClient] = {
    val jarsFolder = new File("plugins/sg-engines").getAbsoluteFile
    if(jarsFolder.exists) {
      jarsFolder.listFiles.map { f =>
        val langName = f.getName.replace(".jar", "").capitalize
        loadSgEngine(f.getAbsolutePath, langName).map(langName -> _)
      }.collect { case Some(t) => t }.toMap
    } else {
      Map[String,SgEngineClient]()
    }
  }

  private def loadSgEngine(jarPath: String, langName: String): Option[SgEngineClient] = {
    Try {
      val className = s"cmwell.plugins.impl.${langName}Parser"
      val excludes = Seq("cmwell.plugins.spi", "org.apache.jena.")
      ChildFirstURLClassLoader.loadClassFromJar[SgEngineClient](className, jarPath, excludes)
    }.toOption
  }
}

trait Importer[A] { this: LazyLogging =>
  type Fields = Map[String,Set[FieldValue]]

  def crudServiceFS: CRUDServiceFS

  case class FileInfotonContent(content: Array[Byte], fields: Fields)

  def fetch(paths: Seq[String]): Future[Vector[A]]

  def invalidateCaches(): Unit

  def readFileInfoton(path: String, nbg: Boolean): Future[FileInfotonContent] = {
    crudServiceFS.getInfoton(path, None, None, nbg).map(res => (res: @unchecked) match {
      case Some(Everything(FileInfoton(_, _, _, _, fields, Some(content),_))) => FileInfotonContent(content.data.get, fields.getOrElse(Map()))
      case x => logger.debug(s"Could not fetch $path, got $x"); throw new RuntimeException(s"Could not fetch $path")
    })
  }

  protected def extractFileNameFromPath(path: String) = path.substring(path.lastIndexOf("/")+1)
}

//TODO: if really needed, do it appropriately with injection
//object Importers { def all = Seq[Importer[_]](QueriesImporter, JarsImporter, SourcesImporter) }

class QueriesImporter(override val crudServiceFS: CRUDServiceFS, nbg: Boolean) extends Importer[String] with LazyLogging {
  import cmwell.util.collections.LoadingCacheExtensions
  import cmwell.util.concurrent.travector

  private val wildcards = Set("_", "*")
  private val defaultBasePath = "/meta/sp"
  private val importDirective = "#cmwell-import"

  private lazy val dataCache: LoadingCache[String, String] =
    CacheBuilder.newBuilder().maximumSize(200).expireAfterWrite(15, TimeUnit.MINUTES).
      build(new CacheLoader[String, String] { override def load(key: String) = Await.result(readTextualFileInfoton(key), 5.seconds) })

  private lazy val directoriesCache: LoadingCache[String, Seq[String]] =
    CacheBuilder.newBuilder().maximumSize(20).expireAfterWrite(15, TimeUnit.MINUTES).
      build(new CacheLoader[String, Seq[String]] { override def load(key: String) = Await.result(listChildren(key), 5.seconds) })

  def fetch(importsPaths: Seq[String]) = explodeWildcardImports(importsPaths).flatMap(actualImports => fetchRec(actualImports.toSet))

  def fetchRec(importsPaths: Set[String], alreadyFetched: Set[String] = Set.empty[String]): Future[Vector[String]] = {
    if(importsPaths.isEmpty) Future.successful(Vector.empty[String])
    else {
      doFetch(importsPaths).flatMap { queries =>
        val nextAlreadyFetched = alreadyFetched ++ importsPaths
        val nextToFetch = queries.flatMap(extractInlineImportsFrom).toSet &~ nextAlreadyFetched
        fetchRec(nextToFetch, nextAlreadyFetched).map(_ ++ queries)
      }
    }
  }

  private def doFetch(paths: Set[String]) = travector(paths)(dataCache.getAsync)

  private def extractInlineImportsFrom(query: String): Set[String] = {
    query.lines.filter(_.startsWith(importDirective)).
      flatMap(_.replace(importDirective+" ","").split(",")).
      toSet
  }

  override def invalidateCaches() = {
    directoriesCache.invalidateAll()
    dataCache.invalidateAll()
  }

  private def explodeWildcardImports(importsPaths: Seq[String]) = {
    val (wildcardImports, plainImports) = importsPaths.map(toAbsolute).partition(imprt => wildcards.exists(imprt.endsWith))
    val normalizedWildcardImports = wildcardImports.map(_.dropRight(2)) // get rid of `/<wildcard>`
    Future.traverse(normalizedWildcardImports)(directoriesCache.getAsync).map(_.flatten ++ plainImports).map(_.distinct)
  }

  private def toAbsolute(path: String) = if(path.startsWith("/")) path else s"$defaultBasePath/$path"

  private def listChildren(path: String) =
    crudServiceFS.thinSearch(Some(PathFilter(path, descendants = false))).map(_.thinResults.map(_.path))

  private def readTextualFileInfoton(path: String) = readFileInfoton(path, nbg).map{ case FileInfotonContent(data, _) => new String(data, "UTF-8")}
}

class JarsImporter(override val crudServiceFS: CRUDServiceFS, nbg: Boolean) extends Importer[JenaFunction] with SpFileUtils { self =>
  import cmwell.util.collections.LoadingCacheExtensions
  import cmwell.util.concurrent.travector
  import cmwell.util.loading._

  private val mandatoryBaseJarsPath = "/meta/lib/"

  case class LoadedJar(tempPhysicalPath: String, jenaFunctions: Seq[JenaFunction])

  private lazy val jarsCache: LoadingCache[String, LoadedJar] =
    CacheBuilder.newBuilder().maximumSize(200).expireAfterAccess(15, TimeUnit.MINUTES).removalListener(new RemovalListener[String, LoadedJar] {
      override def onRemoval(notification: RemovalNotification[String, LoadedJar]): Unit = { new File(notification.getValue.tempPhysicalPath).delete()
      }}).build(new CacheLoader[String, LoadedJar] {
        override def load(key: String) = self.load(Await.result(readFileInfoton(key, nbg), 15.seconds))
      })

  override def fetch(paths: Seq[String]): Future[Vector[JenaFunction]] =
    travector(paths) { path => jarsCache.getAsync(s"$mandatoryBaseJarsPath$path") }.map(_.flatMap(_.jenaFunctions))

  private def load(fileInfotonContent: FileInfotonContent): LoadedJar = {
    val FileInfotonContent(data, fields) = fileInfotonContent
    val excludes = fields.getOrElse("exclude", Set()).collect { case FString(value,_,_) => value }

    val path = generateTempFileName
    writeToFile(path)(data) // temp file will be deleted onRemoval from cache

    val loader = ChildFirstURLClassLoader.Loader(path, Seq("org.apache.jena") ++ excludes)
    val classNames = extractClassNamesFromJarFile(path)
    val jenaFunctions = classNames.map(cn => Try(loader.load[JenaFunction](cn))).collect { case Success(jf) => jf }

    LoadedJar(path, jenaFunctions)
  }

  override def invalidateCaches() = jarsCache.invalidateAll()
}

// The reason we need this class is: Toolbox reflective compilation wraps evaluation in some wrapper objects.
// So the class loaded from source string is available for "normal" usage, i.e. when one invokes any of its overrides (in our case - Jena),
// however, its reflective information does not work as expected, e.g. the results of .getClass.getName is a very long ugly String.
// Since we know the className in advance, we can use it aside with the implementation in order to register in Jena's FunctionRegistry.
case class NamedAnonJenaFuncImpl(name: String, impl: JenaFunction)

class SourcesImporter(override val crudServiceFS: CRUDServiceFS, nbg: Boolean) extends Importer[NamedAnonJenaFuncImpl] with LazyLogging {
  import cmwell.util.collections.LoadingCacheExtensions
  import cmwell.util.concurrent.travector

  private val mandatoryBaseSourcesPath = "/meta/lib/sources/"

  override def fetch(paths: Seq[String]): Future[Vector[NamedAnonJenaFuncImpl]] =
    travector(paths) { path => functionsCache.getAsync(s"$mandatoryBaseSourcesPath$path") }

  override def invalidateCaches(): Unit = functionsCache.invalidateAll()

  private lazy val functionsCache: LoadingCache[String, NamedAnonJenaFuncImpl] =
    CacheBuilder.newBuilder().maximumSize(200).expireAfterAccess(15, TimeUnit.MINUTES).
      build(new CacheLoader[String, NamedAnonJenaFuncImpl] {
        override def load(key: String) = eval(new String(Await.result(readFileInfoton(key, nbg), 15.seconds).content,"UTF-8"), className = extractFileNameFromPath(key).replace(".scala",""))
      })

  def eval(source: String, className: String): NamedAnonJenaFuncImpl = { // WARNING: Black magic.
    import tools.reflect.ToolBox
    val tb = reflect.runtime.currentMirror.mkToolBox()
    val impl = tb.eval(tb.parse(s"import scala._\nimport Predef._\n$source\n new $className()")).asInstanceOf[JenaFunction]
    NamedAnonJenaFuncImpl(className, impl)
  }
}

object JenaUtils {
  def getNamedModels(ds: Dataset): Map[String,Model] = {
    import scala.collection.JavaConversions._
    ds.listNames().map(name => name -> ds.getNamedModel(name)).toMap
  }

  def expandDataset(dataset: Dataset, constructQuery: String) = {
    val query = QueryFactory.create(constructQuery)
    require(query.isConstructType, "cannot expand dataset using non-construct query")
    QueryExecutionFactory.create(query, dataset).execConstructDataset()
  }

  def size(dataset: Dataset): Long = {
    dataset.getDefaultModel.size + getNamedModels(dataset).map{case(n,m)=>m.size}.sum
  }

  def merge(ds1: Dataset, ds2: Dataset): Dataset = flatten(Seq(ds1,ds2))

  def flatten(dss: Seq[Dataset]): Dataset = {
    val resDs = DatasetFactory.create()
    dss.foreach { ds =>
      resDs.getDefaultModel.add(ds.getDefaultModel)
      getNamedModels(ds).foreach { case (name, namedModel) =>
          resDs.addNamedModel(name, namedModel)
      }
    }
    resDs
  }

  def filter(m: Model)(predicate: Statement => Boolean): Model = {
    import scala.collection.JavaConversions._
    val filteredModel = ModelFactory.createDefaultModel()
    m.listStatements().filter(predicate).foreach(filteredModel.add)
    filteredModel
  }

  def filter(ds: Dataset)(predicate: Statement => Boolean): Dataset = {
    val filteredDs = DatasetFactory.create()
    filteredDs.setDefaultModel(filter(ds.getDefaultModel)(predicate))
    getNamedModels(ds).foreach { case (name,model) => filteredDs.addNamedModel(name, filter(model)(predicate)) }
    filteredDs
  }

  def discardQuadsAndFlattenAsTriples(ds: Dataset): Model = {
    import scala.collection.JavaConversions._
    val model = ModelFactory.createDefaultModel()
    val allTriples = ds.getDefaultModel.listStatements ++ getNamedModels(ds).values.flatMap(_.listStatements)
    allTriples.foreach(model.add)
    model
  }
}

trait SpFileUtils extends LazyLogging {
  private val basePath = scala.sys.env.getOrElse("TMPDIR", "/tmp/") + Settings.queryResultsTempFileBaseName

  protected def generateTempFileName = s"${basePath}_${java.lang.System.currentTimeMillis}_${Random.alphanumeric.take(10).mkString}"

  protected def writeToFile(filename: String)(content: Array[Byte]) = {
    val path = Paths.get(filename)
    Files.createDirectories(path.getParent)
    val out = Files.newOutputStream(path)
    out.write(content)
    out.flush()
    out.close()
  }

  protected def deleteTempFiles(): Unit = {
    val (directory, prefix) = basePath.splitAt(basePath.lastIndexOf('/') + 1)
    if (prefix.isEmpty) {
      logger.warn("CW is trying to delete tempFiles but file prefix is empty. HALTING AND NOT DELETING // we only want to delete our temp files...")
    } else {
      Try {
        Option(new File(directory)).foreach { f =>
          val cwTmpFiles = f.list(new FilenameFilter() { override def accept(file: File, filename: String) = filename.startsWith(prefix) })
          cwTmpFiles.foreach(new File(_).delete())
        }
      }
    }
  }
}
