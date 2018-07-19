package cmwell.analytics.data

import cmwell.analytics.util.CmwellConnector

object CheckDuplicatedSystemFields {

  def main(args: Array[String]): Unit = {

    // Since we expect this to be run on a CM-Well node, the default parallelism is to use half the processors
    // so as to avoid starving the CM-Well node from processor resources. A higher level of parallelism might
    // be possible (without interfering with CM-Well) since most of the work will actually be on the ES side.

    CmwellConnector(
      cmwellUrl = "http://localhost:9000",
      appName = "Find infotons with duplicated system fields",
      sparkShell = true
    ).withSparkSessionDo { implicit spark =>

      InfotonWithDuplicatedSystemFields()
        .coalesce(1) // combine into a single partition/file
        .saveAsTextFile("duplicates")
    }
  }
}
