package cmwell.analytics.data

import cmwell.analytics.data.IndexWithSystemFields.{isConsistent, isWellFormed}
import cmwell.analytics.util.CmwellConnector

object CountIndexWithSystemFields {

  def main(args: Array[String]): Unit = {

    CmwellConnector(
      cmwellUrl = "http://localhost:9000",
      appName = "Test Count IndexWithSystemFields",
      sparkShell = true
    ).withSparkSessionDo { implicit spark =>

      val ds = IndexWithSystemFields().toDF().cache()

      import org.apache.spark.sql.functions._

      val notWellFormed = ds.filter(not(isWellFormed(ds))).count()
      println(s"There are $notWellFormed rows in IndexWithSystemFields that are not well formed.")

      val notConsistent = ds.filter(not(isWellFormed(ds) && isConsistent(ds))).count()
      println(s"There are $notConsistent rows in IndexWithSystemFields that are not consistent.")
    }
  }
}
