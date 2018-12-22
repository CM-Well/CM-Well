package com.poc
import org.rogach.scallop.ScallopConf


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val sourceCluster = opt[String]("source-cluster", required = true, descr = "the source cluster which data is being exported from")
  val neptuneCluster = opt[String]("neptune-cluster", required = true, descr="neptune cluster which data is being imported to")
  val ingestThreadsNum = opt[Int]("ingest-threads", required = true, descr="number of threads that should be created by the tool when ingest to neptune")
  verify()
}

object Main {
  def main(args: Array[String]) {
    val conf = new Conf(args)
    println("Source cluster is: " + conf.sourceCluster())
    println("Neptune cluster is: " + conf.neptuneCluster())
    println("Threads num is: " + conf.ingestThreadsNum())
    println("About to Export-Import..")
    val exportImportHandler = new ExportImportToNeptuneHandler(conf.ingestThreadsNum())
    exportImportHandler.exportImport(conf.sourceCluster(), conf.neptuneCluster())
  }

}