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
  val neptuneCluster = opt[String]("neptune-cluster", required = true, descr="neptune cluster which data is being imported to")
  val ingestConnectionPoolSize = opt[Int]("ingest-connection-pool-size", required = true, descr="number of connection pool that should be created by the tool in order to ingest to neptune")
  val lengthHint = opt[Int]("length-hint", default = Some(16000), validate = 300000.>=, descr="number of infotons that should be consumed in each bulk-consume call")
  val qp = opt[String](name="qp-param", default=None)
  val updateInfotons = opt[Boolean]("update-infotons")

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
    val qpParam :Option[String]= conf.qp.toOption.map(s => s",$s")
    println("About to Export-Import..")
    val exportImportHandler = new ExportImportToNeptuneHandler(conf.ingestConnectionPoolSize())
    exportImportHandler.exportImport(conf.sourceCluster(), conf.neptuneCluster(), conf.lengthHint(), conf.updateInfotons(), qpParam)
  }

}