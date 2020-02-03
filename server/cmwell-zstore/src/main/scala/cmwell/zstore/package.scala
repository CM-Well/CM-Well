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
package cmwell

import cmwell.driver.DaoExecution
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by yaakov on 12/19/16.
  */
package object zstore {
  implicit class Extensions(zs: ZStore) extends DaoExecution {
    private val lsHardLimit = 4096

    // DESIGN CHOICE: This method should not be part of ZStore trait. It is only meant for debugging.
    def ls(limit: Int = 20)(implicit ec: ExecutionContext): Future[Seq[String]] = zs match {
      case zsImpl: ZStoreImpl =>
        implicit val daoProxy = zsImpl.daoProxy
        val l: java.lang.Integer = Math.min(limit, lsHardLimit)
        val stmt = prepare("select distinct uzid from data2.zstore limit ?").bind(l)
        executeAsyncInternal(stmt).map(_.all().asScala.map(_.getString("uzid")).toSeq)

      case zsMem: ZStoreMem =>
        Future.successful(zsMem.keySet.toSeq)

      case _ => ???
    }
  }
}
