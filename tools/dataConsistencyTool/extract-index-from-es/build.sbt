name := "DumpUuidsFromEs"

version := "0.1"

scalaVersion := "2.11.12"

name := "extract-index-from-es"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.rogach" %% "scallop" % "3.1.1",
  "com.typesafe.akka" %% "akka-http" % "10.0.11",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "commons-io" % "commons-io" % "2.4",
  "log4j" % "log4j" % "1.2.17",
  "joda-time" % "joda-time" % "2.3",
  "org.apache.parquet" % "parquet-avro" % "1.9.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.5")

assembly / mainClass := Some("cmwell.analytics.main.DumpUuidOnlyFromEs")

assembly / assemblyMergeStrategy :=  {
  case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

