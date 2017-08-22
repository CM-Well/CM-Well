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

import cmwell.ctrl.utils.ProcUtil
import cmwell.ws.Settings
import cmwell.ws.util.DateParser.fdf
import com.typesafe.config.ConfigFactory
import logic.CRUDServiceFS
import play.api.mvc._
import javax.inject._

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.sys.process._
import scala.util._
import org.joda.time._

import scala.language.postfixOps

/**
 * Created by michael on 8/11/14.
 */
object HealthUtils {
  val config = ConfigFactory.load()
  val ip = config.getString("ftsService.transportAddress")
  val path = config.getString("user.dir")

  /**
    * to avoid abuse, we guard the nodetool with this proxy.
    * can't have more than 1 request per node in 3 minutes,
    * but also, we always save an up to date status within
    * the last 20 minutes.
    */
  object CassandraNodetoolProxy {
    private[this] val timeout = Settings.cacheTimeout seconds
    @volatile private[this] var status: String = null
    @volatile private[this] var modified: DateTime = new DateTime(0L)

    private[this] val nodetoolDaemonCancellable = {
      cmwell.util.concurrent.SimpleScheduler.scheduleAtFixedRate(30 seconds, 20 minutes){
        getStatus
      }
    }

    private[this] def getStatus: Future[String] = {
      val f = Future(ProcUtil.executeCommand(s"JAVA_HOME=$path/../java/bin $path/../cas/cur/bin/nodetool -h $ip status").get)
      val p = Promise[String]()
      f.onComplete{
        case Failure(e) => p.failure(e)
        case Success(s) => {
          modified = new DateTime()
          status = s"${fdf(modified)}\n$s"
          p.success(status)
        }
      }
      p.future
    }

    def get: String = (new DateTime()).minus(modified.getMillis).getMillis match {
      case ms if (ms milliseconds) < (3 minutes) => status
      case _ => Try(Await.result(getStatus, timeout)).getOrElse(status)
    }
  }

  def CassNodetoolStatus: String = CassandraNodetoolProxy.get
}

@Singleton
class Health @Inject()(crudServiceFS: CRUDServiceFS) extends Controller {

  import HealthUtils._

  def getCassandaraHealth = Action.async {implicit req =>
    Future(Ok(CassNodetoolStatus))
  }

  def getElasticsearchHealth = Action.async { implicit req =>
    val res = Seq("curl", s"http://$ip:9201/_cluster/health?pretty&level=shards") !!

    Future(Ok(res))
  }

  def getElasticsearchTop = Action.async {implicit req =>
    val res = Seq("curl", s"http://$ip:9201/_nodes/hot_threads") !!

    Future(Ok(res))
  }

  def getElasticsearchStats = Action.async {implicit req =>
    val res = Seq("curl", s"http://$ip:9201/_cluster/stats?human&pretty") !!

    Future(Ok(res))
  }
  def getElasticsearchSegments = Action.async {implicit req =>
    val res = Seq("curl", s"http://$ip:9201/_segments?pretty") !!

    Future(Ok(res))
  }

  def getElasticsearchStatus = Action.async {implicit req =>
    val res = Seq("curl", s"http://$ip:9201/_status?pretty") !!

    Future(Ok(res))
  }



  def getBatchHealth {

  }

  def getKafkaStatus = Action.async {implicit req =>

    val res = Seq(s"$path/../kafka/cur/bin/kafka-topics.sh","--zookeeper", s"$ip:2181", "--describe") !!

    Future(Ok(res))
  }

  def getZkStat = Action.async {implicit req =>

    val res = Seq("echo", "stats" ) #| Seq("nc", ip, "2181") !!

    Future(Ok(res))
  }

  def getZkRuok = Action.async {implicit req =>

    val res = Seq("echo", "ruok" ) #| Seq("nc", ip, "2181") !!

    Future(Ok(res))
  }

  def getZkMntr  = Action.async {implicit req =>

    val res = Seq("echo", "mntr" ) #| Seq("nc", ip, "2181") !!

    Future(Ok(res))
  }


  def getIndex = Action.async {implicit req =>
    Future{
      val xml =
      """
        |<html>
        | <head>
        |   <title>CM-Well Cluster Health</title>
        | </head>
        | <body>
        |   <a href="/health/cas">Cassandra Ring</a><br>
        |   <!-- a href="/health/cas_cfh">Cassandra cfhistograms</a><br -->
        |   <a href="/health/es">Elasticsearch Cluster Health</a><br>
        |   <a href="/health/es_top">Elasticsearch Top</a><br>
        |   <a href="/health/es_stats">Elasticsearch Stats</a><br>
        |   <a href="/health/es_seg">Elasticsearch Segments</a><br>
        |   <a href="/health/es_status">Elasticsearch Status</a><br>
        |   <a href="/health/kafka">Kafka</a><br>
        |   <a href="/health/zk-stat">zk-stat</a><br>
        |   <a href="/health/zk-ruok">zk-ruok</a><br>
        |   <a href="/health/zk-mntr">zk-mntr</a><br>
        |   <a href="/health/ws">ws</a><br>
        | </body>
        |</html>
      """.stripMargin
      Ok(xml).as("text/html")
    }
  }

  def getWsHealth = Action { implicit req =>
    Ok(s"Old IRW ReadCache Size: ${crudServiceFS._irwService.dataCahce.size()}\n" +
      s"New IRW ReadCache Size: ${crudServiceFS._irwService2.dataCahce.size()}\n")
  }
}
