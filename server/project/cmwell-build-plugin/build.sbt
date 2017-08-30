import sbt.addSbtPlugin

name := "cmwell-build-plugin"

addSbtPlugin("com.github.tkawachi" % "sbt-doctest" % "0.5.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")
addSbtPlugin("com.alpinenow" % "junit_xml_listener" % "0.5.1")
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0-M15-1")

sbtPlugin := true 
