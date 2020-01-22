name := "cmwell-data-tools-app"

packJvmOpts := Map(
  "consumer"                   -> Seq("-Xmx1500m", "-XX:+UseG1GC"),
  "downloader"                 -> Seq("-Xmx1500m", "-XX:+UseG1GC"),
  "sparql-processor"           -> Seq("-Xmx1500m", "-XX:+UseG1GC"),
  "ingester"                   -> Seq("-Xmx1500m", "-XX:+UseG1GC"),
  "sparql-triggered-processor" -> Seq("-Xmx1500m", "-XX:+UseG1GC", "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=${PROG_HOME}/logs")
)

packMain := Map(
  "consumer"                   -> "cmwell.tools.data.downloader.ConsumerMain",
  "downloader"                 -> "cmwell.tools.data.downloader.StreamsMain",
  "sparql-processor"           -> "cmwell.tools.data.sparql.SparqlProcessorMain",
  "ingester"                   -> "cmwell.tools.data.ingester.IngesterMain",
  "sparql-triggered-processor" -> "cmwell.tools.data.sparql.SparqlTriggeredProcessorMain"
)

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("org.rogach", "scallop"),
    dm("net.jcazevedo", "moultingyaml"),
    dm("ch.qos.logback", "logback-classic")
  )
}
