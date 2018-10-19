import sbt.addSbtPlugin

name := "cmwell-build-plugin"

addSbtPlugin("com.github.tkawachi" % "sbt-doctest" % "0.7.1")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.4.0")
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "3.0.0")

sbtPlugin := true
