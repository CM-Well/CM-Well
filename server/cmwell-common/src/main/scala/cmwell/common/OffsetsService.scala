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


package cmwell.common

import scala.concurrent.Await
import scala.concurrent.duration._
import cmwell.zstore.ZStore

/**
  * Created by israel on 14/03/2017.
  */
trait OffsetsService {

  def read(id:String):Option[Long]
  def write(id:String, offset:Long):Unit
}

class ZStoreOffsetsService(zStore:ZStore) extends OffsetsService {

  override def read(id: String): Option[Long] = Await.result(zStore.getLongOpt(id), 10.seconds)

  override def write(id: String, offset: Long): Unit = Await.result(zStore.putLong(id, offset, batched = true), 10.seconds)
}
