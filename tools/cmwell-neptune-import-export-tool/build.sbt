name := "neptune-poc-export-tool"

version := "0.1"

scalaVersion := "2.12.7"


libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.18",
  "org.rogach" %% "scallop" % "3.1.1",
  "org.apache.jena" % "apache-jena-libs" % "3.9.0" pomOnly(),
  "org.slf4j" % "slf4j-api" % "1.7.7",
  "ch.qos.logback" % "logback-classic" % "1.0.1",
  "ch.qos.logback" % "logback-core" % "1.0.1"
//"com.amazonaws" % "aws-java-sdk" % "1.11.466"
)