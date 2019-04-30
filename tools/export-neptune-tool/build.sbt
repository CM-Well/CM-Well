name := "neptune-poc-export-tool"

version := "0.1"

scalaVersion := "2.12.7"


libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.18",
  "org.rogach" %% "scallop" % "3.1.1",
  "org.apache.jena" % "apache-jena-libs" % "3.9.0" pomOnly(),
  "org.slf4j" % "slf4j-api" % "1.7.7",
  "ch.qos.logback" % "logback-classic" % "1.0.1",
  "ch.qos.logback" % "logback-core" % "1.0.1",
  "net.liftweb" %% "lift-json" % "3.3.0",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.483" excludeAll ExclusionRule(organization = "commons-logging"),
  "net.liftweb" %% "lift-json" % "3.3.0",
  "io.circe" %% "circe-yaml" % "0.8.0",
  "org.yaml" % "snakeyaml" % "1.8",
  "com.google.guava" % "guava" % "12.0"
  //"org.apache.commons" % "commons-compress" % "1.5"

)