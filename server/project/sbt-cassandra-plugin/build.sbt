sbtPlugin := true

organization := "com.github.hochgi"

name := "sbt-cassandra-plugin"

description := "SBT plugin to allow launching Cassandra during tests, and test your application against it"

libraryDependencies ++= Seq("org.apache.thrift" % "libthrift" % "0.9.2" exclude("commons-logging","commons-logging"),
                            "org.slf4j" % "slf4j-api" % "1.7.12",
                            "org.slf4j" % "jcl-over-slf4j" % "1.7.12",
                            "org.yaml" % "snakeyaml" % "1.15",
                            "nl.gn0s1s" %% "bump" % "0.1.3")

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-language:postfixOps")