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
package cmwell.tools.neptune.export

import org.rogach.scallop.ScallopConf


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val sourceCluster = opt[String]("source-cluster", required = true, descr = "the source cluster which data is being exported from")
  val neptuneCluster = opt[String]("neptune-cluster", required = true, descr="neptune cluster which data is being exported to")
  val ingestConnectionPoolSize = opt[Int]("ingest-connection-pool-size", default = Some(5), validate = 50.>=, descr="number of connection pool that should be created by the tool in order to ingest to neptune")
  val lengthHint = opt[Int]("length-hint", default = Some(16000), validate = 300000.>=, descr="number of infotons that should be consumed in each bulk-consume call")
  val qp = opt[String](name="qp-param", default=None, descr = "cm well qp param")
  val updateInfotons = opt[Boolean]("update-infotons", descr = "enable this parameter when you use an update mode or delete of infotons")
  val bulkLoader = opt[Boolean]("bulk-loader", descr = "enable this parameter in order to export by using s3-bulk loader api. bulk loader is only for initial load")
  val proxyHost = opt[String]("proxy-host", default=None, descr = "proxy host is provided when you use bulk loader and your machine use proxy")
  val proxyPort = opt[Int]("proxy-port", default=None, descr = "proxy port is provided when you use bulk loader and your machine use proxy")
  val s3Bucket = opt[String](name="s3-bucket", default=Some("cm-well/sync"), descr = "s3 directory which neptune read data from")

  verify()
}

object Main {
  def main(args: Array[String]) {
    val conf = new Conf(args)
    println("Source cluster is: " + conf.sourceCluster())
    println("Neptune cluster is: " + conf.neptuneCluster())
    println("Connection pool size is: " + conf.ingestConnectionPoolSize())
    println("length-hint: " + conf.lengthHint())
    println("update infotons: " + conf.updateInfotons())
    println("qp: " + conf.qp.getOrElse("(not provided)"))
    println("bulk loader: " + conf.bulkLoader())
    println("proxy host: " + conf.proxyHost.getOrElse("not provided"))
    println("proxy port: " + conf.proxyPort.getOrElse(-1))
    println("s3 bucket:" + conf.s3Bucket())
    val qpParam :Option[String]= conf.qp.toOption.map(s => s",$s")
    val proxyHost :Option[String]= conf.proxyHost.toOption
    val proxyPort :Option[Int]= conf.proxyPort.toOption
    println("About to Export..")
    val exportToNeptuneManager = new ExportToNeptuneManager(conf.ingestConnectionPoolSize())
    exportToNeptuneManager.exportToNeptune(conf.sourceCluster(), conf.neptuneCluster(), conf.lengthHint(), conf.updateInfotons(), qpParam,
      conf.bulkLoader(), proxyHost, proxyPort, conf.s3Bucket(), None)
  }

}