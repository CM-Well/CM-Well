name := "cmwell-indexer"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe.akka", "akka-actor"),
    dm("nl.grons", "metrics-scala")
  )
}

fullTest := (test in Test).value