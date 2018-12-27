package com.poc
import org.rogach.scallop.ScallopConf


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val sourceCluster = opt[String]("source-cluster", required = true, descr = "the source cluster which data is being exported from")
  val neptuneCluster = opt[String]("neptune-cluster", required = true, descr="neptune cluster which data is being imported to")
  val ingestConnectionPoolSize = opt[Int]("ingest-connection-pool-size", required = true, descr="number of connection pool that should be created by the tool in order to ingest to neptune")
  val lengthHint = opt[Int]("length-hint", default = Some(16000), validate = (16000>=), descr="number of infotons that should be consumed in each bulk-consume call")
  val qp = opt[String](name="qp-param", default=Some("None"))
  val updateInfotons = opt[Boolean]("update-infotons")
  val withDeleted = opt[Boolean]("with-deleted")

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
    println("qp: " + conf.qp())
    println("with-deleted: " + conf.withDeleted())
    val qpParam :Option[String]= if(conf.qp() == "None") None else Some("," + conf.qp())
    println("About to Export-Import..")
    val exportImportHandler = new ExportImportToNeptuneHandler(conf.ingestConnectionPoolSize())
    exportImportHandler.exportImport(conf.sourceCluster(), conf.neptuneCluster(), conf.lengthHint(), conf.updateInfotons(), qpParam, conf.withDeleted())
  }

}