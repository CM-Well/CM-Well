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
import java.io.File
import java.nio.file.{Files, Paths}

import akka.actor.{ActorSystem, Terminated}
import akka.stream.ActorMaterializer
import cmwell.util.build.BuildInfo
import cmwell.util.http.{SimpleResponse, SimpleHttpClient => Http}
import cmwell.util.concurrent.unsafeRetryUntil
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Created by michael on 12/8/15.
  */
class DataInitializer(h: Host, jwt: String, rootDigest: String, rootDigest2: String) {

  //TODO: why is `sys` & `mat` needed? if it's just for http requests, we can drop it, and use util's defaults.
  private[this] lazy val sys = {
    val config = ConfigFactory.load()
    ActorSystem("SimpleHttpClient", config.getConfig("cmwell.util.http"))
  }
  private[this] lazy val mat = ActorMaterializer()(sys)

  def shutdownMaterializerAndActorSystem(shouldRetry: Boolean = true): Unit = {
    mat.shutdown()
    sys.terminate().andThen {
      case _: Success[Terminated] => mat.shutdown()
      case Failure(_)             => shutdownMaterializerAndActorSystem(false)
    }
  }

  implicit class StringExtensions(s: String) {
    def takeRightWhile(p: (Char) => Boolean): String = s.takeRight(s.reverseIterator.takeWhile(p).length)
  }

  private val chunkSize = 100

  def waitForWs(): Unit = withTry("wait for ws") {
    import SimpleResponse.Implicits.UTF8StringHandler
    import scala.concurrent.duration._

    val totalWaitPeriod = 90.seconds
    val interval = 250.millis

    val numRetries = (totalWaitPeriod / interval).toInt
    await(
      unsafeRetryUntil[SimpleResponse[String]](_.status == 200, numRetries, interval)(
        Http.get(s"http://${h.ips.head}:9000/proc/node", Seq("format" -> "json"))(UTF8StringHandler,
                                                                                  implicitly[ExecutionContext],
                                                                                  sys,
                                                                                  mat)
      ).map { response =>
        if (response.status != 200) {
          print(s"Waiting for ws failed. Last response was: $response\n")
          scala.sys.exit(1)
        }
      }
    )
  }

  @deprecated("1.5.x", "old API")
  def uploadMetaDataOld(): Unit = withTry("upload meta data (old)") {
    val jsons = Source.fromFile("resources/meta_ns_prefixes_snapshot_infotons.json").getLines()
    val lb = new scala.collection.mutable.ListBuffer[String]()

    jsons.foreach(lb.append(_))
    lb.append(
      """{"type" : "ObjectInfoton","system" : {"path" : "/meta/sys"}, "fields" : {"known-cmwell-hosts" : [""" + h.ips
        .map(""""""" + _ + """:9000"""")
        .mkString(",") + """], "desc" : ["CM-Well system namespace"]}}"""
    )
    lb.append(
      """{"type" : "ObjectInfoton","system" : {"path" : "/meta/nn"}, "fields" : {"desc" : ["CM-Well private namespace"]}}"""
    )
    lb.append("""{"type" : "ObjectInfoton","system" : {"path" : "/meta/nn/mang"}, "fields" : {"mang" : ["s"]}}""")

    awaitSeq(
      lb.sliding(chunkSize, chunkSize)
        .map { l =>
          val bagOfInfotons = """{"type" : "BagOfInfotons","infotons" : [""" + l.mkString(",") + """]}"""
          httpPost(s"http://${h.ips.head}:9000/_cmd",
                   bagOfInfotons,
                   NoType,
                   TextPlain,
                   Seq("op" -> "init", "format" -> "jsonw", accessLogMarker("uploadMetaDataOld")))
        }
        .toSeq
    )
  }

  def uploadNameSpaces(): Unit = withTry("upload namespaces") {
    await(
      httpPost(
        s"http://${h.ips.head}:9000/_in",
        ResourseFilename("meta_ns_prefixes_snapshot_infotons.nt"),
        NoType,
        TextPlain,
        Seq("format" -> "ntriples", accessLogMarker("uploadNameSpaces"))
      )
    )
  }

  def uploadMetaData(): Unit = withTry("upload meta data") {
    val lb = new scala.collection.mutable.ListBuffer[String]()

    lb.append(
      """{"type" : "ObjectInfoton","system" : {"path" : "/meta/sys"}, "fields" : {"known-cmwell-hosts" : [""" + h.ips
        .map(""""""" + _ + """:9000"""")
        .mkString(",") + """], "desc" : ["CM-Well system namespace"]}}"""
    )
    lb.append(
      """{"type" : "ObjectInfoton","system" : {"path" : "/meta/nn"}, "fields" : {"desc" : ["CM-Well private namespace"]}}"""
    )
    lb.append("""{"type" : "ObjectInfoton","system" : {"path" : "/meta/nn/mang"}, "fields" : {"mang" : ["s"]}}""")

    awaitSeq(
      lb.sliding(chunkSize, chunkSize)
        .map { l =>
          val bagOfInfotons = """{"type" : "BagOfInfotons","infotons" : [""" + l.mkString(",") + """]}"""
          httpPost(s"http://${h.ips.head}:9000/_cmd",
                   bagOfInfotons,
                   NoType,
                   TextPlain,
                   Seq("op" -> "init", "format" -> "jsonw"))
        }
        .toSeq
    )
  }

  def uploadSampleData(): Unit = withTry("upload sample data") {
    await(
      httpPost(s"http://${h.ips.head}:9000/_in",
               ResourseFilename("tr.sample.n3"),
               NoType,
               TextPlain,
               Seq("format" -> "n3"))
    )
  }

  def updateKnownHosts(): Unit = withTry("update known hosts") {
    val markReplace = "<cmwell://meta/sys> <cmwell://meta/sys#markReplace> <cmwell://meta/nn#known-cmwell-hosts> ."
    val payload = (Seq(markReplace) ++ h.ips.map(
      ip => s"""<cmwell://meta/sys> <cmwell://meta/nn#known-cmwell-hosts> "$ip:9000" . """
    )).mkString("\n")
    await(
      httpPost(s"http://${h.ips.head}:9000/_in",
               payload,
               NoType,
               TextPlain,
               Seq("op" -> "init", "format" -> "ntriples", accessLogMarker("updateKnownHosts")))
    )
  }

  def uploadBasicUserInfotons(host: String): Unit = withTry("upload built-in UserInfotons") {
    def uploadUserInfotonIfNotExist(username: String, payload: String, p: Progress) = {
      val url = s"http://$host:9000/meta/auth/users/$username"
      Http.get(url, headers = Seq("X-CM-WELL-TOKEN" -> jwt)).flatMap { resp =>
        if (resp.status != 200) { // todo we could have used '<$url> <cmwell://meta/sys#prevUUID> "" .' if we were using _in - and saved an http call.
          httpPost(url, payload, File, Json, Seq(accessLogMarker("uploadBasicUserInfotons"))).map(p.sideEffectInc)
        } else {
          Future.successful("")
        }
      }
    }

    def allowRoot(levels: String) = s"""{"id":"/","recursive":true,"sign":"+","permissions":"$levels"}"""
    val denyForAnon =
      Seq("/meta/auth").map(p => s"""{"id":"$p","recursive":true,"sign":"-","permissions":"rw"}""").mkString(",")


    // scalastyle:off
    // WARNING: modifying any hard-coded UserInfoton won't affect any environment even on upgrade. You will have to upload them manually!
    val userInfotons = Map(
      "anonymous" -> s"""{"paths":[${allowRoot("r")},$denyForAnon],"roles":[]}""",
      "root" -> s"""{"digest":"$rootDigest","digest2":"$rootDigest2","paths":[${allowRoot("rw")}],"operations":["Admin","Overwrite","PriorityWrite"],"roles":[]}""",
      "pUser" -> s"""{"paths":[${allowRoot("rw")}],"operations":["Admin","Overwrite","PriorityWrite"],"roles":[]}""",
      "dca" -> s"""{"paths":[${allowRoot("rw")}],"operations":["Overwrite"],"roles":[]}""",
      "docu" -> """{"paths":[{"id":"/","recursive":true,"sign":"+","permissions":"r"},{"id":"/meta/docs","recursive":true,"sign":"+","permissions":"rw"}],"rev":0}""",
      "stpAgent" -> s"""{"paths":[${allowRoot("rw")}],"roles":[]}"""
    )
    // scalastyle:on

    val p = Progress(userInfotons.size)
    val results =
      awaitSeq(userInfotons.map { case (user, payload) => uploadUserInfotonIfNotExist(user, payload, p) }.toSeq)
    await(Http.get(s"http://$host:9000/_auth/invalidate-cache", headers = Seq("X-CM-WELL-TOKEN" -> jwt)))
    p.complete()
    results
  }

  def uploadDirectory(path: String, url: String, exclude: Set[String] = Set()): Unit =
    withTry(s"upload directory to $url") {
      val goodUrl = if (!url.endsWith("/")) url + "/" else url
      val pathSize = if (path.endsWith("/")) path.size else path.size + 1
      val files = listFiles(new File(path))

      val filesToUpload = files.filterNot(file => exclude(file.getName))
      val p = Progress(filesToUpload.length)
      val res = awaitSeq(
        filesToUpload.map(
          file =>
            uploadFile(file.getPath, goodUrl + file.getPath.substring(pathSize)).map(p.sideEffectInc).recover {
              case err =>
                h.info(s"Failed to upload eli3 ${file.getName}")
                h.info(cmwell.util.exceptions.stackTraceToString(err))
            }
        )
      )
      p.complete()
      res
    }

  private def listFiles(f: File): Array[File] = {
    val (ds, fs) = f.listFiles.partition(_.isDirectory)
    fs ++ ds.flatMap(listFiles)
  }

  private def uploadFile(path: String, url: String): Future[String] = {
    def readBinaryFile(path: String): Array[Byte] = Files.readAllBytes(Paths.get(path))

    val fileTypes = Map(
      "html" -> "text/html",
      "js" -> "application/javascript",
      "jsx" -> "application/javascript",
      "css" -> "text/css",
      "ico" -> "image/x-icon",
      "jpg" -> "image/jpeg",
      "gif" -> "image/gif",
      "png" -> "image/png",
      "svg" -> "image/svg+xml",
      "md" -> "text/x-markdown",
      "woff2" -> "font/woff2",
      "appcache" -> "text/cache-manifest"
    )
    val extension = path.takeRightWhile(_ != '.') toLowerCase
    val default = "text/plain"
    val payload = readBinaryFile(path)
    httpPost(url, payload, File, ContetType(fileTypes.getOrElse(extension, default)), Nil)
  }

  def logVersionUpgrade(host: String): Unit = withTry("log version") {
    val httpPath = s"http://$host:9000/meta/logs/version-history/${System.currentTimeMillis}"
    val payload =
      s"""{"logType":"system-version","git_commit_version":"${BuildInfo.gitCommitVersion}","release":"${BuildInfo.release}"}"""
    await(httpPost(httpPath, payload, Obj, Json, Nil))
  }

  private def httpPost(url: String,
                       payload: ResourseFilename,
                       postType: PostType,
                       contentType: ContentType,
                       queryParameters: Seq[(String, String)]): Future[String] =
    httpPost(url,
             scala.io.Source.fromFile(s"resources/${payload.fileName}").map(_.toByte).toArray,
             postType,
             contentType,
             queryParameters)

  private def httpPost(url: String,
                       payload: String,
                       postType: PostType,
                       contentType: ContentType,
                       queryParameters: Seq[(String, String)]): Future[String] =
    httpPost(url, payload.getBytes("UTF-8"), postType, contentType, queryParameters)

  private def httpPost(url: String,
                       payload: Array[Byte],
                       postType: PostType,
                       contentType: ContentType,
                       queryParameters: Seq[(String, String)]): Future[String] = {
    import SimpleResponse.Implicits.UTF8StringHandler
    import scala.concurrent.duration._

    def isOk(response: SimpleResponse[String]): Boolean = response.status == 200

    val headers = Seq("X-CM-WELL-TOKEN" -> jwt) ++ (postType match {
      case NoType        => Seq()
      case knownPostType => Seq("X-CM-Well-Type" -> knownPostType.toString)
    })

    unsafeRetryUntil(isOk, 12, 250.millis)(
      Http.post(url, payload, Some(contentType.toString), queryParameters, headers)(UTF8StringHandler,
                                                                                    implicitly[ExecutionContext],
                                                                                    sys,
                                                                                    mat)
    ).map {
        case resp if isOk(resp) => resp.body._2
        case badResp => {
          // scalastyle:off
          println(s" Failed to upload eli1 $url and payload ${payload.mkString} with response $badResp")
          // scalastyle:on
          ""
        }
      }
      .recover {
        case err =>
          // scalastyle:off
          println(s" Failed to upload eli2 $url")
          println(cmwell.util.exceptions.stackTraceToString(err))
          // scalastyle:on
          ""
      }
  }

  trait PostType
  case object File extends PostType
  case object Obj extends PostType
  case object NoType extends PostType

  trait ContentType
  case object Json extends ContentType { override def toString = "application/json" }
  case object TextPlain extends ContentType { override def toString = "text/plain" }
  case class ContetType(contentType: String) extends ContentType { override def toString: String = contentType }

  case class ResourseFilename(fileName: String)

  private def accessLogMarker(value: String) = "access-log-marker" -> value

  private def awaitSeq[T](futures: Seq[Future[T]]) = await(Future.sequence(futures))
  private def await[T](future: Future[T]) = Await.result(future, 5.minutes) // yes, five minutes.

  case class Progress(total: Int) {
    var counter = 0d

    def sideEffectInc[T](value: T): T = {
      inc()
      value
    }

    def inc(): Unit = {
      /*
        using += on var from multiple Futures is a bad practice.
        But this is cons, and it is better to have some idea of what's going on while uploading docs/SPA and such,
        than to stay in darkness (is it hung? it is running? etc.)
        Users of this code are not customers!
        Even if they see 98% and then 73% and then 100% it'll be totally fine.
        Having it perfect does not worth the implementation time and performance price.
       */
      counter += 1
      printf("\r\t%.2f%%", 100 / (total / counter))
    }

    // scalastyle:off
    def complete(): Unit = println("\r\t100.0%  ")
    // scalastyle:on
  }

  private def withTry(desc: String)(block: => Unit): Unit = Try(block).recover {
    case t =>
      // scalastyle:off
      println(s"\t*** FAILED to $desc due to ${t.getMessage} ...Moving on!")
      // scalastyle:on
  }
}
