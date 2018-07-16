package cmwell.analytics.data

import cmwell.analytics.data.InfotonWithSystemFields.{isConsistent, isWellFormed}
import cmwell.analytics.util.CmwellConnector

object CountInfotonWithSystemFields {

  def main(args: Array[String]): Unit = {

    CmwellConnector(
      cmwellUrl = "http://localhost:9000",
      appName = "Test Count InfotonWithSystemFields",
      sparkShell = true
    ).withSparkSessionDo { implicit spark =>

      val ds = InfotonWithSystemFields().toDF().cache()

      import org.apache.spark.sql.functions._

      val notWellFormed = ds.filter(not(isWellFormed(ds))).count()
      println(s"There are $notWellFormed rows in InfotonWithSystemFields that are not well formed.")

      val notConsistent = ds.filter(not(isWellFormed(ds) && isConsistent(ds))).count()
      println(s"There are $notConsistent rows in InfotonWithSystemFields that are not consistent.")
    }
  }
}
