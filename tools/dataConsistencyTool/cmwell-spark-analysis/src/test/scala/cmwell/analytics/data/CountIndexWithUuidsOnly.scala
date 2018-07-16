package cmwell.analytics.data

import cmwell.analytics.util.CmwellConnector

object CountIndexWithUuidsOnly {

  def main(args: Array[String]): Unit = {

    CmwellConnector(
      cmwellUrl = "http://localhost:9000",
      appName = "Test Count IndexWithUuidsOnly",
      sparkShell = true
    ).withSparkSessionDo { implicit spark =>

      val ds = IndexWithUuidsOnly()

      println(ds.count())
    }
  }
}
