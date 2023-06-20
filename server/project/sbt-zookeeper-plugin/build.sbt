name := "sbt-zookeeper-plugin"

version := "0.2"

organization := "com.github.israel"

sbtPlugin := true

Compile / sourceGenerators += task[Seq[File]] {
  val file =  (Compile / sourceManaged).value / "com" / "github" / "israel" / "sbt" / "zookeeper" / "BuildUtils.scala"
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
