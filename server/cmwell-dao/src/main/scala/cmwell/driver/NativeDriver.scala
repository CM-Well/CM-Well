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
package cmwell.driver

import java.net.InetSocketAddress

import com.datastax.driver.core._
import com.datastax.driver.core.policies.DefaultRetryPolicy
import com.google.common.util.concurrent.{FutureCallback, Futures, MoreExecutors}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * Created with IntelliJ IDEA.
  * User: markz
  * Date: 3/2/14
  * Time: 1:04 PM
  * To change this template use File | Settings | File Templates.
  */
trait Dao extends LazyLogging {
  def init()
  def getSession: Session
  def getKeyspace: String
  def shutdown()
}

class NativeDriver(clusterName: String, keyspaceName: String, host: String, port: Int, maxConnections: Int = 10, initCommands: Option[List[String]])
    extends Dao {

  private val pools: PoolingOptions = new PoolingOptions();
  pools.setNewConnectionThreshold(HostDistance.LOCAL, 128)
  // TODO: need understand what have been changed
  /*
  pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections)
  pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections)
  pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections)
  pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections)
   */

  val hosts = host.split(",")

  private val cluster = new Cluster.Builder()
    .addContactPointsWithPorts(new InetSocketAddress(host, port))
    .withPoolingOptions(pools)
    .withSocketOptions(new SocketOptions().setTcpNoDelay(true))
    .withoutJMXReporting() // datastax client depends on old io.dropwizard.metrics (3.2.2),
    .build();              // while metrics4.scala depends on newer version (4.0.1).
                           // The 4.0.x release removed the jmx module to another artifact (metrics-jmx)
                           // and package (com.codahale.metrics.jmx). while this is true,
                           // we are better off without JMX reporting of the client.
                           // In future: consider to re-enable this.


  initCommands.foreach { commands =>
    Try {
      val initSession: Session = cluster.connect()
      commands.foreach(initSession.execute)
    }
    match {
      case Success(_) =>
      case Failure(err) =>
        logger.error("Initial session (needed for keyspace creation and initial tables) creation failed. Killing the process. The exception was: ", err)
        sys.exit(1)
    }
  }

  private val session: Session = Try(cluster.connect(keyspaceName)) match {
    case Success(s) => s
    case Failure(err) =>
      logger.error(err.getMessage, err)
      logger.info("Now will kill the process.")
      sys.exit(1)
  }

  def init() {
    import scala.collection.JavaConverters._
    val metaData: Metadata = cluster.getMetadata
    val allHosts = metaData.getAllHosts
    logger.info(
      s"Connected to cluster: ${metaData.getClusterName}, and Hosts: ${allHosts.asScala.map(_.toString).mkString("[", ",", "]")}"
    )
  }

  def getSession: Session = session

  def getKeyspace: String = keyspaceName

  def shutdown() {
    cluster.close()
  }
}

object Dao {
  def apply(clusterName: String, keyspaceName: String, host: String, port: Int, maxConnections: Int = 10, initCommands: Option[List[String]]) =
    new NativeDriver(clusterName, keyspaceName, host, port, maxConnections, initCommands)
}

trait DaoExecution {
  def prepare(stmt: String)(implicit daoProxy: Dao): PreparedStatement = daoProxy.getSession.prepare(stmt)

  // NOTE: not using any retires here. any client of this trait may wrap this func call with its own retry logic, if any
  def executeAsyncInternal(statmentToExec: Statement)(implicit daoProxy: Dao): Future[ResultSet] = {
    val p = Promise[ResultSet]()
    Try {
      daoProxy.getSession
        .executeAsync(
          statmentToExec
            .setIdempotent(true)
            .setRetryPolicy(DefaultRetryPolicy.INSTANCE)
        )
    } match {
      case Failure(e) => p.failure(e)
      case Success(f: ResultSetFuture) =>
        Futures.addCallback(
          f,
          new FutureCallback[ResultSet]() {
            def onSuccess(result: ResultSet): Unit = p.success(result)
            def onFailure(t: Throwable): Unit = p.failure(t)
          },
          MoreExecutors.directExecutor()
        )
    }
    p.future
  }
}
