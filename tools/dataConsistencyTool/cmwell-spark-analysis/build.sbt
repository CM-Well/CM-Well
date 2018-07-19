name := "cmwell-spark-analysis"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
  "commons-codec" % "commons-codec" % "1.11",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "6.2.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.7",
  "org.rogach" %% "scallop" % "3.1.1",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "com.typesafe" % "config" % "1.3.2",
  "org.scalatest" % "scalatest_2.11" % "3.0.1",
  "org.scalactic" % "scalactic_2.11" % "3.0.1"
)
