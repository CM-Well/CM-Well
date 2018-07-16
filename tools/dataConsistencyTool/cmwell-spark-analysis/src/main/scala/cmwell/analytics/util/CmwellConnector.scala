package cmwell.analytics.util

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.slf4j.LoggerFactory

object CmwellConnector {
  private val config = ConfigFactory.load
  private val sparkMaster = config.getString("cmwell-spark-analysis.spark-master")

  val coalesceParallelismMultiplier = config.getInt("cmwell-spark-analysis.coalesce-parallelism-multiplier")
}

case class CmwellConnector(cmwellUrl: String,
                           sparkShell: Boolean = false, // start a (small) Spark shell to run the job
                           appName: String) {

  def withSparkSessionDo(f: (SparkSession) => Unit): Unit = {

    val logger = LoggerFactory.getLogger(classOf[CmwellConnector])

    val esContactPoint = FindContactPoints.es(cmwellUrl)
    logger.info(s"Using $esContactPoint to connect to Elasticsearch.")

    val casContactPoint = FindContactPoints.cas(cmwellUrl)
    logger.info(s"Using $casContactPoint to connect to Cassandra.")

    val sessionBuilder = SparkSession.builder.appName(appName)

    if (esContactPoint != null) {
      val Array(host, port) = esContactPoint.split(":")

      sessionBuilder.config(ES_NODES, host)
      sessionBuilder.config(ES_PORT, port)

      // We were seeing what looks like scroll ids expiring, so doubling the default in an attempt to avoid this.
      sessionBuilder.config(ES_SCROLL_KEEPALIVE, "10m")

      // The default number of documents to retrieve per scroll is 50, which is safe if there are many large documents,
      // but results in very slow performance. As long as we don't get any memory issues, upping this to 10K should
      // give orders of magnitude better reading performance.
      sessionBuilder.config(ES_SCROLL_SIZE, "10000")

      sessionBuilder.config("es.internal.es.version", "1.7.6") // otherwise, it will send scrollId in json, which 1.7.6 rejects
    }

    if (casContactPoint != null) {
      sessionBuilder.config("spark.cassandra.connection.host", casContactPoint)
      sessionBuilder.config("spark.cassandra.input.consistency.level", "QUORUM")
    }

    if (sparkShell) {
      sessionBuilder.master(CmwellConnector.sparkMaster)
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
