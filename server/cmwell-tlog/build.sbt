name := "cmwell-tlog-ng"
packAutoSettings
libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("joda-time", "joda-time"),
    dm("org.joda", "joda-convert"),
    dm("com.typesafe", "config"),
    dm("net.jpountz.lz4", "lz4"),
    dm("com.google.guava", "guava"),
    dm("com.google.code.findbugs", "jsr305"),
    dm("org.rogach", "scallop"),
    dm("org.scala-lang", "scala-compiler"),
    dm("org.codehaus.groovy", "groovy-all") % "test",
    dm("org.slf4j", "log4j-over-slf4j") % "test"
  )
}
							
mainClass in assembly := Some("cmwell.util.TLogTool")

test in assembly := {}

javaOptions in test +=  "-Dcmwell.home=target/tlog-test"

testOptions in Test += {
  val t = (target in Test).value
  Tests.Setup(() => {
    val data = t / "tlog-test" / "data"
    s"rm -r ${data.getAbsolutePath}" !
  })
}

assemblyJarName in assembly := {
  val n = name.value
  val s = scalaBinaryVersion.value
  val v = version.value
  s"${n}_$s-$v-selfexec.jar"
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "cxf", "bus-extensions.txt") => MergeStrategy.concat
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

lazy val publishAssembly = TaskKey[sbt.File]("publish-assembly", "publish the assembly artifact")

publish := {
  assembly.value
  publish.value
}

publishLocal := {
  assembly.value
  publishLocal.value
}

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("selfexec"))
}

fullTest := (test in Test).value