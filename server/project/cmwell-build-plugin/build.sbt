
name := "cmwell-build-plugin"
sbtPlugin := true

val coursierVersion = cmwell.build.PluginVersions.coursier
libraryDependencies ++= Seq(
  "io.get-coursier" %% "coursier-core" % coursierVersion,
  "io.get-coursier" %% "coursier-cache" % coursierVersion
)