package cmwell.analytics.data

import cmwell.analytics.util.CmwellConnector

object CheckInfotonDataIntegrity {

  def main(args: Array[String]): Unit = {
    CmwellConnector(
      cmwellUrl = "http://localhost:9000",
      appName = "Check infoton data integrity",
      sparkShell = true
    ).withSparkSessionDo { implicit spark =>

      val ds = InfotonDataIntegrity()

      val damagedInfotons = ds.filter(infoton =>
        infoton.hasIncorrectUuid ||
          infoton.hasDuplicatedSystemFields ||
          infoton.hasInvalidContent ||
          infoton.hasMissingOrIllFormedSystemFields
      ).cache()

      println(s"There are ${damagedInfotons.count()} damaged infotons.")

      damagedInfotons.show(truncate = false)
    }
  }
}