name := "cmwell-sparql-triggered-processor-agent"

libraryDependencies += {
  val dm = dependenciesManager.value
  dm("net.jcazevedo","moultingyaml")
}

fullTest := (test in Test).value