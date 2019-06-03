import cmwell.build.PluginVersions

libraryDependencies ++=  Seq(
  "joda-time" % "joda-time" % "2.3",
  "org.joda" % "joda-convert" % "1.6",
  "net.leibman" %% "semverfi" % "0.2.0")

val s = Seq(
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
  scalaVersion := "2.12.8"
)

sourceGenerators in Compile += Def.task {
    val file = sourceManaged.value / "cmwell" / "build" / "Versions.scala"
    IO.write(file, s"""
     |package cmwell.build
     |
     |object Versions {
     |  val cassandra = "3.11.4"
     |  val elasticsearch = "7.1.1"
     |  val kafka = "2.2.1"
     |  val play = "${PluginVersions.play}"
     |  val zookeeper = "3.4.13"
     |}
     """.stripMargin)
  Seq(file
  )
}.taskValue

lazy val pluginsUtil       = (project in file("plugins-util"))        .settings(s:_*)
lazy val cmwellBuildPlugin = (project in file("cmwell-build-plugin")) .settings(s:_*).dependsOn(pluginsUtil)
lazy val cassandraPlugin   = (project in file("sbt-cassandra-plugin")).settings(s:_*).dependsOn(pluginsUtil)
lazy val zookeeperPlugin   = (project in file("sbt-zookeeper-plugin")).settings(s:_*).dependsOn(pluginsUtil,cmwellBuildPlugin)
lazy val kafkaPlugin       = (project in file("sbt-kafka-plugin"))    .settings(s:_*).dependsOn(pluginsUtil,zookeeperPlugin)

val root = Project("plugins", file(".")).dependsOn(cmwellBuildPlugin,cassandraPlugin,kafkaPlugin)