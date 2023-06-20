name := "cmwell-grid-ng"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe.akka", "akka-cluster"),
    dm("com.typesafe.akka", "akka-cluster-metrics"),
    dm("com.typesafe.akka", "akka-cluster-tools"),
		dm("ch.qos.logback", "logback-classic") % "test",
    dm("com.typesafe.akka", "akka-slf4j") % "test"
  )
}

assembly / assemblyMergeStrategy := {
  case PathList("bus-extensions.txt") => MergeStrategy.discard
  case PathList("application.conf") => MergeStrategy.concat
  case PathList("reference.conf") => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) =>
    xs.map(_.toLowerCase) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  case _ => MergeStrategy.first
}

assembly / test := {}

Test / fork := true

Test / javaOptions ++= Seq(
  s"-Dgrid.test.assembly-jar-name=${(Test / assembly).value.getAbsolutePath}",
  s"-Dgrid.test.root-dir=${target.value}"
)

Test / test := Def.task((Test / test).value).tag(cmwell.build.CMWellCommon.Tags.Grid).value
