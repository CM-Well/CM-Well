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
package cmwell.common.metrics

import java.util.concurrent.TimeUnit
//import com.codahale.metrics.graphite.{GraphiteReporter, Graphite}
import com.codahale.metrics.{MetricFilter, MetricRegistry, Slf4jReporter}
import com.codahale.metrics.jmx.JmxReporter
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/**
  * User: israel
  * Date: 8/27/14
  * Time: 21:10
  */
object MetricsMain {
  // Application wide metric registry
  val metricRegistry = new MetricRegistry()
  val config = ConfigFactory.load()
  // report metrics through JMX, if enabled
  val reportMetricsJMX = config.getBoolean("metrics.reportMetricsJMX")
  if (reportMetricsJMX) {
    val jmxReporter = JmxReporter.forRegistry(metricRegistry).build()
    jmxReporter.start()
  }

  // report metrics through Graphite, if enabled
//  val reportMetricsGraphite = config.getBoolean("metrics.reportMetricsGraphite")
//  if(reportMetricsGraphite) {
//    val graphiteHost = config.getString("metrics.graphite.host")
//    val graphiteClient = new Graphite(new InetSocketAddress(graphiteHost,2003))
//    val graphiteReporter = GraphiteReporter.forRegistry(metricRegistry)
//      .prefixedWith(s"cm-well@${java.net.InetAddress.getLocalHost.getHostName}-${new Date().toString}")
//      .convertRatesTo(TimeUnit.SECONDS)
//      .convertDurationsTo(TimeUnit.MILLISECONDS)
//      .filter(MetricFilter.ALL)
//      .build(graphiteClient)
//    graphiteReporter.start(1, TimeUnit.SECONDS)
//  }

  // report metrics through log4j, if requested
  val reportMetrcisSlf4j = config.getBoolean("metrics.reportMetricsSlf4j")
  if (reportMetrcisSlf4j) {
    val slf4jReporter = Slf4jReporter
      .forRegistry(metricRegistry)
      .outputTo(LoggerFactory.getLogger("cmwell.metrics"))
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build()
    slf4jReporter.start(1, TimeUnit.SECONDS)
  }
}

trait WithMetrics extends nl.grons.metrics4.scala.InstrumentedBuilder {
  val metricRegistry = MetricsMain.metricRegistry
}
