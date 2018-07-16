package cmwell.analytics.data

import org.apache.spark.sql.Column

object InfotonType {

  // In Elasticsearch indexes, the infoton type string is a full word in Pascal casing.
  private val ElasticsearchRepresentation = Seq(
    "ObjectInfoton",
    "FileInfoton",
    "LinkInfoton",
    "DeletedInfoton",
    "CompoundInfoton",
    "GhostInfoton")

  // In the infoton table, the type field is the first letter of the infoton type name in lower case.
  private val CassandraRepresentation: Seq[String] = ElasticsearchRepresentation.map(_.substring(0, 1).toLowerCase)

  def isWellFormedCas(column: Column): Column =
    column.isin(CassandraRepresentation: _*)

  def isWellFormedEs(column: Column): Column =
    column.isin(ElasticsearchRepresentation: _*)
}
