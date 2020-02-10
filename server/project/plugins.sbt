import build.PluginVersions

logLevel := Level.Warn

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % PluginVersions.play)
addSbtPlugin("com.typesafe.sbt" % "sbt-twirl" % "1.4.2")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.4.1")
addSbtPlugin("com.github.tkawachi" % "sbt-doctest" % "0.9.5")
addSbtPlugin("ch.epfl.scala" % "sbt-bloop" % "1.4.0-RC1")

//todo: Disabled due to lack of sbt>=1.3.x support
//addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")
addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.4.0")

addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.0.0")
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.12")

libraryDependencies ++=  Seq(
  "joda-time" % "joda-time" % "2.3",
  "org.joda" % "joda-convert" % "1.6",
  "nl.gn0s1s" %% "bump" % "0.1.3",
  "io.get-coursier" %% "coursier" % PluginVersions.coursier,
  "io.get-coursier" %% "coursier-core" % PluginVersions.coursier,
  "io.get-coursier" %% "coursier-cache" % PluginVersions.coursier
)

resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"
resolvers += "JBoss" at "https://repository.jboss.org/"
resolvers += Resolver.jcenterRepo
