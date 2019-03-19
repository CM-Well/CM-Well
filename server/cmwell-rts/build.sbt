name := "cmwell-rts-ng"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe.akka", "akka-cluster"),
    dm("com.typesafe.akka", "akka-cluster-tools"),
  )
}

test in Test := Def.task((test in Test).value).tag(cmwell.build.CMWellCommon.Tags.Grid).value

fullTest := (test in Test).value