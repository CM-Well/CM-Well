
libraryDependencies ++=  Seq(
  "joda-time" % "joda-time" % "2.3",
  "org.joda" % "joda-convert" % "1.6")

val s = Seq(
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
  scalaVersion := "2.10.6"
)

lazy val pluginsUtil       = (project in file("plugins-util"))        .settings(s:_*)
lazy val cmwellBuildPlugin = (project in file("cmwell-build-plugin")) .settings(s:_*).dependsOn(pluginsUtil)
lazy val cassandraPlugin   = (project in file("sbt-cassandra-plugin")).settings(s:_*).dependsOn(pluginsUtil)
lazy val zookeeperPlugin   = (project in file("sbt-zookeeper-plugin")).settings(s:_*).dependsOn(pluginsUtil)
lazy val kafkaPlugin       = (project in file("sbt-kafka-plugin"))    .settings(s:_*).dependsOn(pluginsUtil,zookeeperPlugin)

val root = Project("plugins", file(".")).dependsOn(cmwellBuildPlugin,cassandraPlugin,kafkaPlugin)