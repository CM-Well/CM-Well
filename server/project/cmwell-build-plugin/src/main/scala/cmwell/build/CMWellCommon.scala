/**
  * © 2019 Refinitiv. All Rights Reserved.
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

import sbt._
import scala.concurrent.Future

object CMWellCommon {

  val release = "Atom"

  object Tags {
    val ES = sbt.Tags.Tag("elasticsearch")
    val Cassandra = sbt.Tags.Tag("cassandra")
    val Kafka = sbt.Tags.Tag("kafka")
    val Grid = sbt.Tags.Tag("grid")
    val IntegrationTests = sbt.Tags.Tag("integrationTests")
  }

  //why work hard? see: http://www.scala-sbt.org/release/docs/Detailed-Topics/Mapping-Files.html#relative-to-a-directory
  def files2TupleRec(pathPrefix: String, dir: File): Seq[Tuple2[File,String]] = {
    sbt.IO.listFiles(dir) flatMap {
      f => {
        if(f.isFile) Seq((f,s"${pathPrefix}${f.getName}"))
        else files2TupleRec(s"${pathPrefix}${f.getName}/",f)
      }
    }
  }

//  case class ProcessLoggerImpl(f: File) extends ProcessLogger {
//
//    def out(o: => String) = sbt.IO.append(f, s"[OUT]: $o\n")
//    def err(e: => String) = sbt.IO.append(f, s"[ERR]: $e\n")
//  }

  def copyLogs(destinationDir: File, sourceDir: File): Unit = {
    val listLogs = sbt.IO.listFiles(new java.io.FileFilter{def accept(f: File): Boolean = f.getName.endsWith(".log") && f.isFile}) _
    val listDirs = sbt.IO.listFiles(new java.io.FileFilter{def accept(f: File): Boolean = f.isDirectory}) _
    def recHelper(dstDir: File, srcDir: File): Unit = {
      val nextDir = dstDir / srcDir.getName
      listLogs(srcDir).foreach(log => sbt.IO.copyFile(log, nextDir / log.getName))
      listDirs(srcDir).foreach(dir => recHelper(nextDir, dir))
    }
    recHelper(destinationDir, sourceDir)
  }

  def generateLogbackXML(filename: String, pwd: String): String = {
    val xml = s"""<configuration>
		<appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
			<file>logs/${filename}.log</file>
			<encoder>
				<pattern>%date %level [%thread] %logger{10} [%file:%line] %msg%n</pattern>
			</encoder>
			<rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
				<maxIndex>5</maxIndex>
				<FileNamePattern>logs/${filename}.log.%i.gz</FileNamePattern>
			</rollingPolicy>
			<triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
				<maxFileSize>10MB</maxFileSize>
			</triggeringPolicy>
		</appender>
		<root level="debug">
			<appender-ref ref="FILE"/>
		</root>
	</configuration>"""

    val xmlFile = file(pwd) / filename / "logback.xml"
    sbt.IO.write(xmlFile, xml)
    xmlFile.getAbsolutePath
  }

  def combineThrowablesAsCause(t1: Throwable, t2: Throwable)(f: Throwable => Throwable): Throwable =
    f(Option(t1.getCause).fold(t1.initCause(t2)){ _ =>
      Option(t2.getCause).fold(t2.initCause(t1)){ _ =>
        t2
      }
    })

  def combineThrowablesAsCauseAsync[T](t1: Throwable, t2: Throwable)(f: Throwable => Throwable): Future[T] =
    Future.failed[T](combineThrowablesAsCause(t1,t2)(f))
}
