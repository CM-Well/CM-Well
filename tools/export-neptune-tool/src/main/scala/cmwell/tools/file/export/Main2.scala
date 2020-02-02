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
package cmwell.tools.file.export

import cmwell.tools.neptune.export.ExportToNeptuneManager
import org.rogach.scallop.ScallopConf


class Conf1(arguments: Seq[String]) extends ScallopConf(arguments) {
  val sourceCluster = opt[String]("source-cluster", required = true, descr = "the source cluster which data is being exported from")
  val lengthHint = opt[Int]("length-hint", default = Some(16000), validate = 300000.>=, descr="number of infotons that should be consumed in each bulk-consume call")
  val qp = opt[String](name="qp-param", default=None, descr = "cm well qp param")
  val directory = opt[String](name="directory", required = true, default=Some("./"), descr = "s3 directory which neptune read data from")

  verify()
}

object Main2 {
  def main(args: Array[String]) {
    val conf = new Conf1(args)
    println("Source cluster is: " + conf.sourceCluster())
    println("length-hint: " + conf.lengthHint())
    println("qp: " + conf.qp.getOrElse("(not provided)"))
    println("s3 bucket:" + conf.directory())
    val qpParam :Option[String]= conf.qp.toOption.map(s => s",$s")
    println("About to Export..")
    val exportToNeptuneManager = new ExportToNeptuneManager(1)
    exportToNeptuneManager.exportToNeptune(conf.sourceCluster(), "", conf.lengthHint(), false, qpParam, false, None, None, "", Some(conf.directory()))
  }

}