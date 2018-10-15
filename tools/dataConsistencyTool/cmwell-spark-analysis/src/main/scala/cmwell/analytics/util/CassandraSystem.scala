package cmwell.analytics.util

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object CassandraSystem {

  /**
    * Estimate the number of rows in a Cassandra table.
    * This uses the system tables, which are an estimate.
    * Note the odd way of getting a C* session, but this is so we play well with the C* connector.
    */
  def rowCount(keyspace: String = "data2",
               table: String)
              (implicit spark: SparkSession): Long = {

    CassandraConnector(spark.sparkContext).withSessionDo { session =>

      session.execute(s"SELECT partitions_count from system.size_estimates where keyspace_name = '$keyspace' and table_name = '$table';")
        .all.asScala.map(_.getLong(0)).sum
    }
  }
}
