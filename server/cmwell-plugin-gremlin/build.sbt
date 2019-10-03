name := "plugin-gremlin"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.github.andrewoma.dexx", "collection"),
    dm("org.apache.jena", "apache-jena")
      .exclude("commons-logging", "commons-logging")
      .exclude("org.slf4j", "slf4j-log4j12")
      .exclude("log4j", "log4j"),
    dm("com.tinkerpop.blueprints", "blueprints-core"),
    dm("com.tinkerpop.gremlin", "gremlin-groovy"))
}

assemblyMergeStrategy in assembly := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}
