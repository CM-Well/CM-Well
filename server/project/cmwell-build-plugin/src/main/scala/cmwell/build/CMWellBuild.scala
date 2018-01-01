/**
  * Copyright 2015 Thomson Reuters
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package cmwell.build

import com.github.tkawachi.doctest.DoctestPlugin
import coursier.CoursierPlugin
import net.virtualvoid.sbt.graph.DependencyGraphPlugin
import sbt.Keys._
import sbt._

import scala.concurrent._
import scala.util.{Failure, Success, Try}

object CMWellBuild extends AutoPlugin {

	type PartialFunction2[-T1,-T2,+R] = PartialFunction[Tuple2[T1,T2],R]

	object autoImport {
//		val configSettingsResource = TaskKey[Seq[sbt.File]]("config-settings-resource", "gets the .conf resource")
		val dependenciesManager = settingKey[PartialFunction2[String, String, ModuleID]]("a setting containing versions for dependencies. if we only use it to declare dependencies, we can avoid a lot of version collisions.")
		val iTestsLightMode = settingKey[Boolean]("a flag, which if turns on, does not take cm-well down after integration tests, but rather purge everything in it, so it will be ready for next time. on startup, it checks if there is an instance running, and if so, does not re-install cm-well.")
		val peScript = TaskKey[sbt.File]("pe-script", "returns the script that executes cmwell in PE mode.")
		val uploadInitContent = TaskKey[sbt.File]("upload-init-content", "returns the script that uploads the initial content in to PE Cm-Well.")
		val packageCMWell = TaskKey[Seq[java.io.File]]("package-cmwell", "get components from dependencies and sibling projects.")
		val getLib = TaskKey[java.io.File]("get-lib", "creates a lib directory in cmwell-cons/app/ that has all cm-wll jars and their dependency jars.")
		val getCons = TaskKey[java.io.File]("get-cons", "get cons from cons project.")
		val getWs = TaskKey[java.io.File]("get-ws", "get ws from ws project.")
		val getTlog = TaskKey[java.io.File]("get-tlog", "get tlog tool tlog project.")
		val getBg = TaskKey[java.io.File]("get-bg", "get bg from batch project.")
		val getCtrl = TaskKey[java.io.File]("get-ctrl", "get ctrl from ctrl project.")
		val getDc = TaskKey[java.io.File]("get-dc", "get dc from dc project.")
		val getGremlin = TaskKey[java.io.File]("get-gremlin", "get gremlin plugin into cons.")
		val install = TaskKey[Map[Artifact, File]]("install", "build + test, much like 'mvn install'")
		val dataFolder = TaskKey[File]("data-folder", "returns the directory of static data to be uploaded")
		val printDate = TaskKey[Unit]("print-date", "prints the date")
		val fullTest = TaskKey[Unit]("full-test", "executes all tests in project in parallel (with respect to dependent tests)")
		val getData = TaskKey[Seq[java.io.File]]("get-data", "get data to upload to cm-well")
		val getExternalComponents = TaskKey[Iterable[File]]("get-external-components", "get external dependencies binaries")
	}

	import autoImport._
	import DoctestPlugin.autoImport._
	import CoursierPlugin.autoImport._

  lazy val apacheMirror = {
    val zoneID = java.util.TimeZone
      .getDefault()
      .getID
    if(zoneID.startsWith("America") || zoneID.startsWith("Pacific") || zoneID.startsWith("Etc")) "us"
    else "eu"
  }

	def fetchZookeeperApacheMirror(version: String): Future[File] = {
		val ext = "tar.gz"
		val url = s"http://www-$apacheMirror.apache.org/dist/zookeeper/zookeeper-$version/zookeeper-$version.$ext"
		fetchArtifact(url, ext)
	}

	def fetchZookeeperApacheArchive(version: String): Future[File] = {
		val ext = "tar.gz"
		val url = s"https://archive.apache.org/dist/zookeeper/zookeeper-$version/zookeeper-$version.$ext"
		fetchArtifact(url, ext)
	}

	def fetchZookeeperSourcesFromGithub(version: String, ext: String): Future[File] = {
		require(ext == "zip" || ext == "tar.gz", s"invalid sources extension [$ext]")
		alternateUnvalidatedFetchArtifact(s"https://github.com/apache/zookeeper/archive/release-$version.$ext", ext)
	}

	def fetchZookeeper(version: String, buildFromSources: Option[(String,File => Future[File])] = None) = {
		import scala.concurrent.ExecutionContext.Implicits.global
		import CMWellCommon.combineThrowablesAsCauseAsync

		fetchZookeeperApacheMirror(version).recoverWith {
			case err1: Throwable => fetchZookeeperApacheArchive(version).recoverWith {
				case err2: Throwable => {
					buildFromSources.fold(combineThrowablesAsCauseAsync[File](err1, err2){ cause =>
						new Exception("was unable to fetch zookeeper binaries, and build from sources function isn't supplied", cause)
					}) {
						case (ext, build) => fetchZookeeperSourcesFromGithub(version, ext).flatMap(build)
					}
				}
			}
		}
	}

	def fetchKafkaApacheMirror(scalaVersion: String, version: String): Future[File] = {
		val ext = "tgz"
    val url = s"http://www-$apacheMirror.apache.org/dist/kafka/$version/kafka_$scalaVersion-$version.$ext"
		fetchArtifact(url, ext)
	}

	def fetchKafkaApacheArchive(scalaVersion: String, version: String): Future[File] = {
		val ext = "tgz"
    val url = s"https://archive.apache.org/dist/kafka/$version/kafka_$scalaVersion-$version.$ext"
		fetchArtifact(url, ext)
	}

	def fetchKafkaSourcesFromGithub(version: String, ext: String): Future[File] = {
		require(ext == "zip" || ext == "tar.gz", s"invalid sources extension [$ext]")
		alternateUnvalidatedFetchArtifact(s"https://github.com/apache/kafka/archive/$version.$ext", ext)
	}

  def fetchKafka(scalaVersion: String, version: String, buildFromSources: Option[(String,File => Future[File])] = None) = {
		import scala.concurrent.ExecutionContext.Implicits.global
		import CMWellCommon.combineThrowablesAsCauseAsync

		fetchKafkaApacheMirror(scalaVersion, version).recoverWith {
			case err1: Throwable => fetchKafkaApacheArchive(scalaVersion, version).recoverWith {
				case err2: Throwable => {
					buildFromSources.fold(combineThrowablesAsCauseAsync[File](err1, err2) { cause =>
						new Exception("was unable to fetch kafka binaries, and build from sources function isn't supplied", cause)
					}) {
						case (ext, build) => fetchKafkaSourcesFromGithub(version, ext).flatMap(build)
					}
				}
			}
		}
	}

	def fetchMvnArtifact(moduleID: ModuleID, scalaVersion: String, scalaBinaryVersion: String): Future[Seq[java.io.File]] = {
		import coursier._
		import java.io.File
		import scala.concurrent.Future
		import scala.concurrent.ExecutionContext.Implicits.global
		import scalaz.EitherT
		import scalaz.concurrent.Task

		val (module, version) = FromSbt.moduleVersion(moduleID, scalaVersion, scalaBinaryVersion)
		val repositories: Seq[coursier.Repository] = Seq(MavenRepository("https://repo1.maven.org/maven2"))
		val fetch = Cache.fetch()

		val tasks = FromSbt.dependencies(moduleID, scalaVersion, scalaBinaryVersion).map {
			case (_, dep) =>
				coursier.Fetch.find(repositories, module, version, fetch)
					.fold[Seq[Artifact]](
					{ _ => Seq.empty[Artifact] }, { case (src, p) => src.artifacts(dep, p, None) })
		}

		val farts: Future[Seq[List[File]]] = Future.traverse(tasks) { task =>
			CMWellCommon.scalazTaskAsScalaFuture(task.flatMap { arts =>
				val x = arts.map { art =>
					Cache.file(art).bimap(e => List(e), f => List(f))
				}
				val y = x.reduce[EitherT[Task, List[FileError], List[File]]] {
					case (a, b) =>
						a.flatMap(files => b.map(_ ::: files).orElse(a)).orElse(b)
				}

				y.fold({ errs =>
					Failure[List[File]](new Exception(errs.map(err => err.message + ": " + err.describe).mkString("[\n\t", ",\n\t", "\n]")))
				}, Success.apply)
			})
		}

		farts.map(_.flatten)
	}

	def fetchArtifact(url: String, ext: String) = {
		import coursier.core.{Artifact, Attributes}
		val sig = Artifact(

			url + ".asc",
			Map.empty,
			Map.empty,
			Attributes("asc", ""),
			changing = false,
			authentication = None
		)

		val art = Artifact(
			url,
			Map(
				"MD5" -> (url + ".md5"),
				"SHA-1" -> (url + ".sha1")),
			Map("sig" -> sig),
			Attributes(ext, ""),
			changing = false,
			None)

		val task = coursier.Cache.file(art).fold({ err =>
			Failure[java.io.File](new Exception(err.message + ": " + err.describe))
		},Success.apply)

		CMWellCommon.scalazTaskAsScalaFuture(task)
	}

	def alternateUnvalidatedFetchArtifact(url: String, ext: String) = {
		import coursier.core.{Artifact, Attributes}

		val art = Artifact(
			url,
			Map.empty,
			Map.empty,
			Attributes(ext, ""),
			changing = false,
			None)

		val task = coursier.Cache.file(art).fold({ err =>
			Failure[java.io.File](new Exception(err.message + ": " + err.describe))
		},Success.apply)

		CMWellCommon.scalazTaskAsScalaFuture(task)
	}

	override def requires = CoursierPlugin && DoctestPlugin && DependencyGraphPlugin

	override def projectSettings = Seq(
		coursierMaxIterations := 200,
		Keys.fork in Test := true,
//		doctestWithDependencies := false,
		libraryDependencies ++= {
			val dm = dependenciesManager.value
			Seq(
				dm("org.scalatest","scalatest") % "test",
				dm("org.scalacheck","scalacheck") % "test")
		},
		testListeners := Seq(new sbt.JUnitXmlTestsListener(target.value.getAbsolutePath)),
		doctestTestFramework := DoctestTestFramework.ScalaTest,
		exportJars := true,
		shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
		fullTest := {},
		install in Compile := {
			fullTest.value
			packagedArtifacts.value
		},
		concurrentRestrictions in ThisBuild ++= Seq(
			Tags.limit(CMWellCommon.Tags.ES, 1),
			Tags.limit(CMWellCommon.Tags.Cassandra, 1),
			Tags.limit(CMWellCommon.Tags.Kafka, 1),
			Tags.limit(CMWellCommon.Tags.Grid, 1),
			Tags.exclusive(CMWellCommon.Tags.IntegrationTests)
		)
	)
}
