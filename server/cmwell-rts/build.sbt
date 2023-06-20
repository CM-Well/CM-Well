name := "cmwell-rts-ng"

//libraryDependencies ++= {
//  val dm = dependenciesManager.value
//  Seq(
//    dm("com.typesafe.akka", "akka-cluster"),
//    dm("com.typesafe.akka", "akka-cluster-tools"),
//  ).map(_.exclude("io.netty","netty"))
//}

Test / test := Def.task((Test / test).value).tag(cmwell.build.CMWellCommon.Tags.Grid).value
