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


package cmwell.it

import java.io.{ByteArrayInputStream, StringWriter}
import java.util.concurrent.Executors

import cmwell.util.concurrent.SimpleScheduler.scheduleFuture
import cmwell.util.http.{SimpleResponse, SimpleResponseHandler, StringPath}
import com.ning.http.client
import com.typesafe.scalalogging.LazyLogging
import org.apache.jena.rdf.model.{Model, ModelFactory, Statement}
import play.api.libs.json._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.util._


trait Helpers { self: LazyLogging =>

  val dcName = "lh"
  System.setProperty("dataCenter.id",dcName)
  System.setProperty("clusterName","cm-well-p")
  // scalastyle:off
  val tokenHeader:List[(String, String)] = ("X-CM-WELL-TOKEN" -> sys.env.getOrElse("PUSER_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJwVXNlciIsImV4cCI6NDYzODkwMjQwMDAwMCwicmV2IjoxfQ.j-tJCGnWHbJ-XAUJ1wyHxMlnMaLvO6IO0fKVjsXOzYM")) :: Nil
  // scalastyle:on
  val requestTimeout = 2 minutes
  val spinCheckTimeout = 3 minutes
  val cacheEviction = 30 seconds
  val indexingDuration = 8 seconds
  val cmw = StringPath.host("localhost:9000")
  val _out = cmw / "_out"
  val _in = cmw / "_in"
  val _ow = cmw / "_ow"
  val _consume = cmw / "_consume"
  val _bulkconsume = cmw / "_bulk-consume"
  val cmt = cmw / "cmt" / "cm" / "test"
  val metaNs = cmw / "meta" / "ns"
  val textPlain = Some("text/plain;charset=UTF-8")
  val jsonSuccess = Json.parse("""{"success":true}""")
  val jsonSuccessPruner: JsValue => JsValue = _.validate((__ \ 'message).json.prune).get.validate((__ \ 'messages).json.prune).get
  val jsonSimpleResponseSuccess = Json.parse("""{"success":true,"type":"SimpleResponse"}""")
  val uuidDateEraser = (__ \ 'system \ 'lastModified).json.prune andThen
                       (__ \ 'system \ 'protocol).json.prune     andThen
                       (__ \ 'system \ 'uuid).json.prune         andThen
                       (__ \ 'system \ 'indexTime).json.prune
  val jsonlUuidDateIdEraser = (__ \ "lastModified.sys").json.prune andThen
                              (__ \ "uuid.sys").json.prune         andThen
                              (__ \ "protocol.sys").json.prune     andThen
                              (__ \ "indexTime.sys").json.prune    andThen
                              (__ \ "@id.sys").json.prune
  val jsonlInfotonArraySorterAndUuidDateIdEraser = (__ \ "infotons").json.update(
    Reads.JsArrayReads.map{
      case JsArray(xs: collection.IndexedSeq[JsObject @unchecked]) => JsArray(xs.map { obj: JsValue =>
        obj.transform(jsonlUuidDateIdEraser andThen jsonlSorter).get
      }.sortWith { case (i,j) =>
        val si = ((i \ "path.sys")(0) \ "value").as[String]
        val sj = ((j \ "path.sys")(0) \ "value").as[String]
        si < sj
      })
      case x @ JsArray(_) => logger.error(s"Unexpected input. Received: $x"); ???
    }
  )
  val bagUuidDateEraserAndSorter =  (__ \ "infotons").json.update(
    Reads.JsArrayReads.map{
      case JsArray(xs: collection.IndexedSeq[JsObject @unchecked]) => JsArray(xs.map { obj: JsValue =>
        obj.transform(uuidDateEraser andThen fieldsSorter).get
      }.sortWith { case (i,j) =>
        val si = (i \ "system" \ "path").as[String]
        val sj = (j \ "system" \ "path").as[String]
        si < sj
      })
      case x @ JsArray(_) => logger.error(s"Unexpected input. Received: $x"); ???
    }
  )
  val fieldsSorter = __.json.update(Reads.JsObjectReads.map{
    case js@JsObject(xs) if xs.exists(_._1 == "fields") => {
      js.transform((__ \ 'fields).json.update(
        Reads.JsObjectReads.map{
          case JsObject(xs) => JsObject(
            xs.map{
              case (n,jv) => {
                n -> ((jv: @unchecked) match {
                  case JsArray(arr) => JsArray(arr.sortBy(_.toString))
                })
              }
            }
          )
        }
      )).get
    }
    case js => js
  })
  val jsonlSorter = __.json.update(
    Reads.JsObjectReads.map{
      case JsObject(xs) => JsObject(
        xs.map{
          case (n,jv) => {
            n -> (jv match {
              case JsArray(arr) => JsArray(arr.sortBy(_.toString))
              case x => x
            })
          }
        }
      )
    }
  )

  object StatusCode extends (client.Response => Int) {
    def apply(r: client.Response) = {
      r.getStatusCode
    }
  }

  object BodyAsJson extends (client.Response => JsValue) {
    def apply(r: client.Response) = Try(Json.parse(r.getResponseBodyAsBytes)).getOrElse{
      logger.error("was expecting a json, but failed parsing:")
      logger.error(s"return code: ${r.getStatusCode}")
      logger.error(s"return body: ${r.getResponseBody}")
      logger.error(s"the URI: ${r.getUri}")
      logger.error("returning null...")
      JsNull
    }
  }

  object BodyAndContentType extends (client.Response => (Array[Byte],String)) {
    def apply(r: client.Response) = (r.getResponseBodyAsBytes,r.getContentType)
  }

  implicit def jsonToString(jv: JsValue): String = Json.stringify(jv)

  implicit def deadLine2Expiry(d: Deadline): Expiry = new Expiry(d)

  def encodeUrl(str: String): String = java.net.URLEncoder.encode(str,"UTF-8")

  // compare two rdf strings.
  def compareRDF(s1 : String , s2 : String , format : String, excludes : Array[String]) : Either[String, Unit] = {
    import scala.collection.JavaConverters._
    val model1: Model = ModelFactory.createDefaultModel
    val model2: Model = ModelFactory.createDefaultModel
    model1.read(new ByteArrayInputStream(s1.getBytes("UTF-8")), null, format)
    model2.read(new ByteArrayInputStream(s2.getBytes("UTF-8")), null, format)

    //remove blank nodes, because these will always be different
    val selector = new org.apache.jena.rdf.model.SimpleSelector(null,null, null.asInstanceOf[org.apache.jena.rdf.model.RDFNode]) {
      override def selects(s: Statement): Boolean = s.getSubject.isAnon
    }
    model1.listStatements(selector).asScala.toList.foreach(model1.remove)
    model2.listStatements(selector).asScala.toList.foreach(model2.remove)

    // remove the values that varies according to time.
    model2.remove(getStatementsExcludes(model2, excludes))
    val rv = model1.isIsomorphicWith(model2) || (model1.difference(model2).isEmpty && model2.difference(model1).isEmpty)
    if(!rv) {
      val strWriter1 = new StringWriter
      val strWriter2 = new StringWriter
      model1.write(strWriter1,format,null)
      model2.write(strWriter2,format,null)
      Left(s"${strWriter1.toString}\n\n" +
               "differs from:\n\n"          +
              s"${strWriter2.toString}\n\n" +
               "with difference: \n\n"      +
              s"${
                val strWriter3 = new StringWriter
                val strWriter4 = new StringWriter
                model1.difference(model2).write(strWriter3,format,null)
                model2.difference(model1).write(strWriter4,format,null)
                strWriter3.toString + "\n\n^\n\n" + strWriter4.toString
              }")
    }
    else Right(())
  }

  def compareRDFwithoutSys(s1 : String , s2 : String , format : String) : Boolean = {
    val model1: Model = ModelFactory.createDefaultModel
    val model2: Model = ModelFactory.createDefaultModel
    model1.read(new ByteArrayInputStream(s1.getBytes("UTF-8")), null, format)
    model2.read(new ByteArrayInputStream(s2.getBytes("UTF-8")), null, format)

    def getSysStatementsExcludes(model : Model) : Array[Statement] = {
      var a = ArrayBuffer[Statement]()
      val it = model.listStatements()

      while(it.hasNext){
        val s = it.next()
        if(s.getPredicate.getNameSpace.endsWith("/meta/sys#")){
          a = a :+ s
        }
      }
      a.toArray
    }

    model2.remove(getSysStatementsExcludes(model2))
    val rv = model1.isIsomorphicWith(model2) || model1.difference(model2).toString.equals("<ModelCom   {} | >")
    if(!rv) {
      val strWriter1 = new StringWriter
      val strWriter2 = new StringWriter
      model1.write(strWriter1,format,null)
      model2.write(strWriter2,format,null)
    }
    rv
  }

  def getStatementsExcludes(model : Model , excludes : Array[String]) : Array[Statement] = {
    var a = ArrayBuffer[Statement]()
    val it = model.listStatements()

    while(it.hasNext){
      val s = it.next()
      if(excludes.contains(s.getPredicate.getLocalName)){
        a = a :+ s
      }
    }
    a.toArray
  }

/*
  implicit class YamlWrapper[T](m: java.lang.Object) {
    /**
     * Method name is a special character since it's an implicit for java.lang.Object (i.e. anything), and we don't want ambiguities
     * @param key
     * @tparam T
     * @return
     */
    def ⚡[T](key:T) = m.asInstanceOf[java.util.LinkedHashMap[T,java.lang.Object]].get(key)
  }
*/

  object Http {

    import cmwell.util.concurrent.unsafeRetryUntil
    import cmwell.util.http.{SimpleHttpClient, SimpleResponse, SimpleResponseHandler}
    import SimpleHttpClient.{Body, SimpleMessageHandler}
    val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(10))

    private def retryOn503Ingests[T](request: => Future[SimpleResponse[T]]): Future[SimpleResponse[T]] =
      unsafeRetryUntil[SimpleResponse[T]](_.status != 503, 23, 42.millis, 2)(request)(ec)

    def ws[T: SimpleMessageHandler](uri: String,
                                    initiationMessage: T,
                                    subprotocol: Option[String] = None,
                                    queryParams: Seq[(String, String)] = Nil,
                                    headers: Seq[(String, String)] = Nil)(react: T => Option[T]) =
      SimpleHttpClient.ws(uri, initiationMessage, subprotocol, queryParams, headers)(react)(implicitly[SimpleMessageHandler[T]], ec)

    def get[T: SimpleResponseHandler](uri: String,
                                      queryParams: Seq[(String, String)] = Nil,
                                      headers: Seq[(String, String)] = Nil) = retryOn503Ingests {
      SimpleHttpClient.get(uri, queryParams, headers)(implicitly[SimpleResponseHandler[T]], ec)
    }

    def put[T: SimpleResponseHandler](uri: String,
                                      body: Body,
                                      contentType: Option[String] = None,
                                      queryParams: Seq[(String, String)] = Nil,
                                      headers: Seq[(String, String)] = Nil) = retryOn503Ingests {
      SimpleHttpClient.put(uri, body, contentType, queryParams, headers)(implicitly[SimpleResponseHandler[T]], ec)
    }

    def post[T: SimpleResponseHandler](uri: String,
                                       body: Body,
                                       contentType: Option[String] = None,
                                       queryParams: Seq[(String, String)] = Nil,
                                       headers: Seq[(String, String)] = Nil) = retryOn503Ingests {
      SimpleHttpClient.post(uri, body, contentType, queryParams, headers)(implicitly[SimpleResponseHandler[T]], ec)
    }

    def delete[T: SimpleResponseHandler](uri: String,
                                         queryParams: Seq[(String, String)] = Nil,
                                         headers: Seq[(String, String)] = Nil) =
      SimpleHttpClient.delete(uri, queryParams, headers)(implicitly[SimpleResponseHandler[T]], ec)
  }

  object Successfulness {
    implicit def intToSuccessfulness: Int => Successfulness = {
      case 200 => Successful
      case 404 | 422 | 503 => Recoverable
      case _ => UnRecoverable
    }

    implicit def bolleanToSuccessfulness: Boolean => Successfulness = {
      case true => Successful
      case false => Recoverable
    }
  }
  sealed trait Successfulness
  case object Successful extends Successfulness
  case object Recoverable extends Successfulness
  case object UnRecoverable extends Successfulness

  def spinCheck[T : SimpleResponseHandler](interval: FiniteDuration,
                                           returnOriginalReqOnFailure: Boolean = false,
                                           maxTimeUntilGivingUp: FiniteDuration = spinCheckTimeout)
                                          (httpReq: =>Future[SimpleResponse[T]])
                                          (isSuccessful: SimpleResponse[T] => Successfulness): Future[SimpleResponse[T]] = {
    val startTime = System.currentTimeMillis()

    def wait(): Future[SimpleResponse[T]] = {
      httpReq.flatMap { res =>
        val timeSpent = System.currentTimeMillis() - startTime

        Try(isSuccessful(res)) match {
          case Success(Recoverable) if timeSpent < maxTimeUntilGivingUp.toMillis => scheduleFuture(interval)(wait())
          case Success(Successful) => Future.successful(res)
          case Success(_) if returnOriginalReqOnFailure => Future.successful(res)
          case Success(_) => Future.failed(new IllegalStateException(s"got a bad response: $res"))
          case Failure(exception) =>
            logger.error("got exception",exception)
            if(timeSpent < maxTimeUntilGivingUp.toMillis)scheduleFuture(interval)(wait())
            else Future.failed(new IllegalStateException(s"got a bad response: $res",exception))
        }
      }(ExecutionContext.Implicits.global)
    }

    wait()
  }

  def executeAfterCompletion[T](f: Future[_])(body: =>Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val p = Promise[T]()
    f.onComplete {
      case _ => p.completeWith(body)
    }
    p.future
  }
}

class Expiry(val d: Deadline) extends AnyVal {
  def expiring(f: => Unit): Unit = {
    import concurrent.ExecutionContext.Implicits.global
    import concurrent.Future
    Future {
      Await.ready(Promise().future, d.timeLeft)
    } onComplete {_ => f}
  }
  def block: Unit = Try(Await.ready(Promise().future, d.timeLeft))
}
