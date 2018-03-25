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
package cmwell.tools.data.helpers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, _}

import scala.concurrent.Await
import scala.concurrent.duration._

trait BaseStreamSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  protected implicit val system = {
    def config =
      ConfigFactory
        .parseString(s"akka.stream.materializer.auto-fusing=$autoFusing")
        .withFallback(ConfigFactory.load())
    ActorSystem("default", config)
  }

  protected implicit val mat = ActorMaterializer()

  override protected def afterAll() = {
    Await.ready(system.terminate(), 42.seconds)
    super.afterAll()
  }

  protected def autoFusing: Boolean
}
