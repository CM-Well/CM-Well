import java.nio.file.FileAlreadyExistsException

import sbt.LocalProject
packAutoSettings
name := "cmwell-cons"

unmanagedResources := Seq()

/*
FIXME: if coursier issue is resolved ( https://gitter.im/coursier/coursier?at=59533c41ceb5bef82ebaf760 ) use regular dependnecies, and ditch `getExternalComponents` task
resolvers ++= Seq(
  Resolver.url("apache-zk") artifacts "http://www-eu.apache.org/dist/[organisation]/[module]-[revision]/[artifact].[ext]",
  Resolver.url("apache-kafka") artifacts "http://www-eu.apache.org/dist/[organisation]/[revision]/[artifact].[ext]"
)
*/

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("org.apache.cassandra", "apache-cassandra") artifacts (Artifact("apache-cassandra", "tar.gz", "tar.gz", "bin")) intransitive(),
    dm("org.elasticsearch", "elasticsearch") artifacts (Artifact("elasticsearch", "tar.gz", "tar.gz")) intransitive(),
    dm("mx4j", "mx4j-tools")/*,
    "zookeeper" % "zookeeper" % Versions.zookeeper artifacts (Artifact("zookeeper", "tar.gz", "tar.gz")) intransitive(),
    "kafka" %% "kafka" % Versions.zookeeper artifacts (Artifact("kafka", "tgz", "tgz")) intransitive()
    */
  )
}

classpathTypes ~=  (_ + "tar.gz" + "tgz")

getExternalComponents := {
  val logger = streams.value
  val list = Map(
    s"kafka-dist-${Versions.kafka}.tgz" -> s"http://www-eu.apache.org/dist/kafka/${Versions.kafka}/kafka_2.11-${Versions.kafka}.tgz",
    s"zookeeper-${Versions.zookeeper}.tar.gz" -> s"http://www-eu.apache.org/dist/zookeeper/zookeeper-3.4.6/zookeeper-${Versions.zookeeper}.tar.gz"
  )
  val bd = baseDirectory.value

  for ((component, url) <- list) {
    val binaryFile = bd / "app" / "components" / component

    if ( binaryFile.exists ) {
      logger.log.info(s"file $component already exists in machine")
    } else {
      logger.log.info(s"downloading $url")
      sbt.IO.download(new java.net.URL(url), binaryFile)
    }
  }

  list.map{ case (name, _) => bd / "app" / "components" / name }
}

getData := {
  val logger = streams.value
  val baseDir = baseDirectory.value
  val spaDir = (dataFolder in LocalProject("spa") in Compile).value
  val docsDir = (dataFolder in LocalProject("docs") in Compile).value
  val sysDir = (sourceDirectory in LocalProject("ws") in Compile).value / ".." / "public" / "sys"

  logger.log.info(sysDir.absolutePath)

  logger.log.info("copying data files")
  val data: java.io.File = baseDir / "app" / "data"
  val docs: java.io.File = baseDir / "app" / "docs"
  val sys: java.io.File = baseDir / "app" / "sys"
  sbt.IO.copyDirectory(spaDir, data, true)
  sbt.IO.copyDirectory(docsDir , docs, true)
  sbt.IO.copyDirectory(sysDir , sys, true)
  sbt.IO.delete(data / ".idea")
  Seq(data,docs)
}

getTlog := {
  val str = streams.value
  val bd = baseDirectory.value
  val tlog = (assembly in LocalProject("tlog") in Compile).value

  str.log.info(s"tlog tool file to copy is: ${tlog.getAbsolutePath}")
  val destination = bd / "app" / "components" / tlog.getName
  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filter(_.getAbsolutePath.contains("cmwell-tlog_"))
  filesToDelete.foreach(sbt.IO.delete)
  sbt.IO.copyFile(tlog,destination,preserveLastModified = true)
  destination
}

getCons := {
  val str = streams.value
  val bd = baseDirectory.value
  val cons = (packageBin in Compile).value

  str.log.info(s"cons file to copy is: ${cons.getAbsolutePath}")
  val destination = bd / "app" / "components" / cons.getName
  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filter(_.getAbsolutePath.contains("cmwell-cons_"))
  filesToDelete.foreach(sbt.IO.delete)
  sbt.IO.copyFile(cons,destination,preserveLastModified = true)
  destination
}

getWs := {
  val str = streams.value
  val bd = baseDirectory.value
  val ws = (packageBin in LocalProject("ws") in Universal).value

  //make sure to also generate app data & docs
  getData.value

  str.log.info(s"ws file to copy is: ${ws.getAbsolutePath}")
  val destination = bd / "app" / "components" / ws.getName
  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filter(_.getAbsolutePath.contains("cmwell-ws_"))
  filesToDelete.foreach(sbt.IO.delete)
  sbt.IO.copyFile(ws,destination,preserveLastModified = true)
  destination
}

getBg := {
  val str = streams.value
  val bd = baseDirectory.value
  val batch = (oneJar in LocalProject("batch") in oneJar).value

  str.log.info(s"bg file to copy is: ${batch.getAbsolutePath}")
  val destination = bd / "app" / "components" / batch.getName
  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filter(_.getAbsolutePath.contains("cmwell-batch_"))
  filesToDelete.foreach(sbt.IO.delete)
  sbt.IO.copyFile(batch,destination,preserveLastModified = true)
  destination
}

getDc := {
  val str = streams.value
  val bd = baseDirectory.value
  val dc = (oneJar in LocalProject("dc") in oneJar).value

  str.log.info(s"dc file to copy is: ${dc.getAbsolutePath}")
  val destination = bd / "app" / "components" / dc.getName
  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filter(_.getAbsolutePath.contains("cmwell-dc"))
  filesToDelete.foreach(sbt.IO.delete)
  sbt.IO.copyFile(dc,destination,preserveLastModified = true)
  destination
}

getCtrl := {
  val str = streams.value
  val bd = baseDirectory.value
  val ctrl = (assembly in LocalProject("ctrl") in Compile).value

  str.log.info(s"crtl file to copy is: ${ctrl.getAbsolutePath}")
  val destination = bd / "app" / "components" / ctrl.getName
  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filter(_.getAbsolutePath.contains("cmwell-ctrl"))
  filesToDelete.foreach(sbt.IO.delete)
  sbt.IO.copyFile(ctrl,destination,preserveLastModified = true)
  destination
}


def packCons(ctrl : File, cons : File , bd : File) = {

  val libDir = ctrl.getAbsoluteFile / "lib"

  val lib = bd / "app" / "cons-lib"

  if(lib.exists()) sbt.IO.delete(lib.listFiles())
  else lib.mkdir()


  libDir.listFiles().foreach {
    file =>
      if(!file.name.contains("apache-cassandra"))
        sbt.IO.copyFile(file,lib / file.getName ,preserveLastModified = true)
  }

  sbt.IO.copyFile(cons , bd / "app" / "cons-lib" / cons.getName , preserveLastModified =  true)
}

def packProject(projName : String, proj : File, bd : File, confDir: File, logger : Logger) = {
  val libDir = proj.getAbsoluteFile / "lib"

  val lib = bd / "app" / "lib"
  val dependencies = bd / "app" / "dependencies"

  if(!lib.exists()) lib.mkdir()

  val depList = libDir.listFiles().map(_.getName).mkString("\n")
  sbt.IO.write(dependencies / projName, depList)


  libDir.listFiles().foreach {
    file =>
      val target = lib / file.getName
      try {
        java.nio.file.Files.createLink(target.toPath, file.toPath)
      } catch {
        case t:FileAlreadyExistsException => // todo: handle handle duplicated jar names.
        case t:Throwable =>
          logger.error(s"Error while creating hard link, will copy instead: $t ${t.getMessage}")
          sbt.IO.copyFile(file,target ,preserveLastModified = true)
      }
  }

  //copy conf files
  proj.listFiles(new FileFilter {
    override def accept(file: File): Boolean = !Set("lib","bin","VERSION","Makefile")(file.getName)
  }).foreach{file =>
    if(file.isDirectory) sbt.IO.copyDirectory(file,confDir / file.getName())
    else {
      sbt.IO.copyFile(file, confDir / file.getName())
    }
  }
}

getGremlin := {
  val str = streams.value
  val bd = baseDirectory.value
  val gremlin = (assembly in LocalProject("pluginGremlin")).value
  val pluginDir = bd / "app" / "plugins"
  if( ! (pluginDir.exists()) ) pluginDir mkdir
  val destination = pluginDir / "gremlin.jar"
  sbt.IO.copyFile(gremlin,destination,preserveLastModified = true)
  destination
}

getLib := {
  val bd = baseDirectory.value

  implicit val logger = streams.value.log

  val lib = bd / "app" / "lib"

  if(lib.exists()) sbt.IO.delete(lib.listFiles())

  val dependencies = bd / "app" / "dependencies"

  if(!lib.exists()) lib.mkdir()
  if(!dependencies.exists()) dependencies.mkdir() else sbt.IO.delete(dependencies.listFiles())

  packProject("ctrl",(pack in LocalProject("ctrl") in pack).value,bd, bd / "app" / "conf" / "ctrl",logger)
  packProject("dc",(pack in LocalProject("dc") in pack).value,bd, bd / "app" / "conf" / "dc",logger)
  packProject("ws",(pack in LocalProject("ws") in pack).value,bd, bd / "app" / "conf" / "ws",logger)
  packProject("batch",(pack in LocalProject("batch") in pack).value,bd, bd / "app" / "conf" / "batch",logger)
  packProject("bg",(pack in LocalProject("bg") in pack).value,bd, bd / "app" / "conf" / "bg",logger)
  packProject("tlog",(pack in LocalProject("tlog") in pack).value,bd, bd / "app" / "conf" / "tlog",logger)
  packCons((pack in LocalProject("ctrl") in pack).value, getCons.value , bd)

  lib
}

packageCMWell := {
  val str = streams.value
  val bd = baseDirectory.value
  val cp = (dependencyClasspath in Compile).value

  getGremlin.value

  //get all generated components as well

  getData.value
  getLib.value
  def wanted(f: sbt.File): Boolean = {
    val jars = List("aspectjweaver","mx4j-tools","zookeeper", "kafka")
    val name = f.getName
    name.endsWith("tar.gz") || name.endsWith("tgz") || jars.exists(name.contains(_))
  }

  val classpath = Attributed.data(cp).filter(wanted) ++ getExternalComponents.value
  str.log.info(s"files from classpath to copy are: ${classpath.map(_.getAbsolutePath).mkString("\n\t","\n\t","")}")
  val destinations = classpath.map(f => bd / "app" / "components" / f.getName)
  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filterNot(f => destinations.exists(_.getAbsolutePath == f.getAbsolutePath))
  sbt.IO.delete(filesToDelete)
  sbt.IO.copy(classpath zip destinations)
  destinations
}

peScript := {
  val components = packageCMWell.value
  streams.value.log.info(s"components: ${components.map(_.getAbsolutePath).mkString("[",",","]")}")
  baseDirectory.value / "app" / "cmwell-pe.sh"
}

uploadInitContent := {
  baseDirectory.value / "app" / "upload-content.sh"
}

test in Test := {
  peScript.value
  (test in Test).value
}//.tag(CMWellTags.Grid)

fork in Test := true

baseDirectory in Test := file("cmwell-cons/app")

fullTest := {}