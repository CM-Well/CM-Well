package cmwell.analytics.data

import cmwell.analytics.data.InfotonAndIndexWithSystemFields.{isConsistent, isWellFormed}
import cmwell.analytics.util.CmwellConnector

object CountInfotonAndIndexWithSystemFields {

  def main(args: Array[String]): Unit = {

    CmwellConnector(
      cmwellUrl = "http://localhost:9000",
      appName = "Test Count InfotonAndIndexWithSystemFields",
      sparkShell = true
    ).withSparkSessionDo { implicit spark =>

      val ds = InfotonAndIndexWithSystemFields().cache()

      import org.apache.spark.sql.functions._

      ds.write.parquet("infoton-index.parquet")

      val notWellFormed = ds.filter(not(isWellFormed(ds))).count()
      println(s"There are $notWellFormed rows in InfotonAndIndexWithSystemFields that are not well formed.")

      val notConsistent = ds.filter(not(isWellFormed(ds) && isConsistent(ds))).count()
      println(s"There are $notConsistent rows in InfotonAndIndexWithSystemFields that are not consistent.")
    }
  }
}
