name := "neptune-poc-export-tool"

version := "0.1"

scalaVersion := "2.12.7"


libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.18",
  "org.rogach" %% "scallop" % "3.1.1",
//  "org.apache.jena" % "jena-arq" % "2.9.3",
  "org.apache.jena" % "apache-jena-libs" % "3.9.0" pomOnly(),
//"com.amazonaws" % "aws-java-sdk" % "1.11.466"
)

//
//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case PathList("reference.conf") => MergeStrategy.concat
//  case x => MergeStrategy.first
//}