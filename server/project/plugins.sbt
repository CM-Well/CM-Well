import cmwell.build.PluginVersions

logLevel := Level.Warn

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % PluginVersions.play)

addSbtPlugin("com.typesafe.play" % "sbt-twirl" % "1.5.2")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.11.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.4.1")

resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

resolvers += "JBoss" at "https://repository.jboss.org/"

libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always