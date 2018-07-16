package cmwell.analytics.data

import cmwell.analytics.util.CmwellConnector

object CountInfotonAndPathWithSystemFields {

  def main(args: Array[String]): Unit = {

    CmwellConnector(
      cmwellUrl = "http://localhost:9000",
      appName = "Test Count InfotonAndPathWithSystemFields",
      sparkShell = true
    ).withSparkSessionDo { implicit spark =>

      val ds = InfotonAndIndexWithSystemFields()
      println(ds.count())
    }
  }
}
