package cmwell.analytics.util

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object Connector {
  private val config = ConfigFactory.load
  private val sparkMaster = config.getString("cmwell-spark-analysis.spark-master")
}

case class Connector(sparkShell: Boolean = false, // start a (small) Spark shell to run the job
                     appName: String) {

  def withSparkSessionDo(f: (SparkSession) => Unit): Unit = {

    val sessionBuilder = SparkSession.builder.appName(appName)

    if (sparkShell) {
      sessionBuilder.master(Connector.sparkMaster)
    }

    val spark = sessionBuilder.getOrCreate()

    try {
      f(spark)
    }
    finally {
      spark.stop()
    }
  }
}
