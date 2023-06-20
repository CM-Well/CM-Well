version := "0.4"

organization := "com.github.israel"

name := "sbt-kafka-plugin"

sbtPlugin := true

addSbtPlugin("com.github.israel" % "sbt-zookeeper-plugin" % "0.2")

Compile / sourceGenerators += task[Seq[File]] {
  val file =  (Compile / sourceManaged).value / "com" / "github" / "israel" / "sbt" / "kafka" / "BuildUtils.scala"
  IO.write(file,
    s"""
       |package com.github.israel.sbt.kafka
       |
       |object KafkaPluginMeta {
       |  val pluginVersion = "${version.value}"
       |  val pluginSbtVersion = "${sbtVersion.value}"
       |  val pluginArtifactId = "${name.value}"
       |  val pluginGroupId = "${organization.value}"
       |}
  """.stripMargin)
  Seq(file)
}
