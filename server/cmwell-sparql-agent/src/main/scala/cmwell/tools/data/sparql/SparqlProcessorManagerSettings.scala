package cmwell.tools.data.sparql

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.ArgsManipulations.HttpAddress
import com.typesafe.config
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.Try

class SparqlProcessorManagerSettings {
  val stpSettings: config.Config = ConfigFactory.load()
  val hostConfigFile: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.host-config-file")
  val hostUpdatesSource: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.host-updates-source")
  val hostWriteOutput: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.host-write-output")
  val materializedViewFormat: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.format")
  val pathAgentConfigs: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.path-agent-configs")
  val writeToken: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.write-token")
  val initDelay: FiniteDuration =
    stpSettings.getDuration("cmwell.agents.sparql-triggered-processor.init-delay").toMillis.millis
  val maxDelay: FiniteDuration =
    stpSettings.getDuration("cmwell.agents.sparql-triggered-processor.max-delay").toMillis.millis
  val interval: FiniteDuration =
    stpSettings.getDuration("cmwell.agents.sparql-triggered-processor.config-polling-interval").toMillis.millis
  //val httpPool: Flow[(HttpRequest, ByteString), (Try[HttpResponse], ByteString), Http.HostConnectionPool] = {
  //val HttpAddress(_, host, port, _) = ArgsManipulations.extractBaseUrl(hostConfigFile)
  //Http().cachedHostConnectionPool[ByteString](host, port)
  //}
}
