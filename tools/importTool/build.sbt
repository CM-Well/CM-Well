name := "Documents"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.18",
  "org.rogach" %% "scallop" % "3.1.1",
  "com.typesafe.akka" %% "akka-http" % "10.1.5",
  "com.typesafe.akka" %% "akka-http-testkit" % "10.1.5" % Test
)

