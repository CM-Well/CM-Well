name := "cmwell-indexer"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe.akka", "akka-actor")
  )
}

fullTest := (test in Test).value