import sbt.addSbtPlugin

name := "cmwell-build-plugin"

addSbtPlugin("com.github.tkawachi" % "sbt-doctest" % "0.7.1")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0")

sbtPlugin := true 
