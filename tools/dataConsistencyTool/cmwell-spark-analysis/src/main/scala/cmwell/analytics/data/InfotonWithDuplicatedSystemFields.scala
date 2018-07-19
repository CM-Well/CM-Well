package cmwell.analytics.data

import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * Get an RDD[String] for all infotons with duplicated system fields.
  */
object InfotonWithDuplicatedSystemFields {

  def apply()
           (implicit spark: SparkSession): RDD[String] = {

    val infotonRdd = spark.sparkContext.cassandraTable("data2", "infoton")
      .select("uuid", "field")
      .where("quad = ?", "cmwell://meta/sys") // only look at system fields
      .spanBy(row => row.getString("uuid"))

    // Map the grouped data to Infoton objects containing only the system fields.
    infotonRdd.filter { case (_, fields) =>
      val fieldNames = fields.map(row => row.getString("field")).toSeq
        .filter(_ != "data")  // The data field can legitimately be duplicated

      fieldNames.length != fieldNames.distinct.length
    }
      .map { case (uuid, _) => uuid }
  }
}
