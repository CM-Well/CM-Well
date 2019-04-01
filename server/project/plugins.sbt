import cmwell.build.PluginVersions

logLevel := Level.Warn

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % PluginVersions.play)

addSbtPlugin("com.typesafe.sbt" % "sbt-twirl" % "1.4.0")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.19")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.10.1")

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "JBoss" at "https://repository.jboss.org/"
