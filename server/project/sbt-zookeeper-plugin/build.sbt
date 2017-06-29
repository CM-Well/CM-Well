name := "sbt-zookeeper-plugin"

version := "0.1"

organization := "com.github.israel"

sbtPlugin := true

sourceGenerators in Compile += task[Seq[File]] {
  val file =  (sourceManaged in Compile).value / "com" / "github" / "israel" / "sbt" / "zookeeper" / "BuildUtils.scala"
  IO.write(file,
    s"""
       |package com.github.israel.sbt.zookeeper
       |
    |object ZookeeperPluginMeta {
       |  val pluginVersion = "${version.value}"
       |  val pluginSbtVersion = "${sbtVersion.value}"
       |  val pluginArtifactId = "${name.value}"
       |  val pluginGroupId = "${organization.value}"
       |}
  """.stripMargin)
  Seq(file)
}
