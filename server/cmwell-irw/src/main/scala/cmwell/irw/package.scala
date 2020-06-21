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

/**
  * Created by markz on 11/6/14.
  */
package object irw {
  type ConsistencyLevel = com.datastax.driver.core.ConsistencyLevel
  val ONE: ConsistencyLevel = com.datastax.driver.core.ConsistencyLevel.ONE
  val QUORUM: ConsistencyLevel = com.datastax.driver.core.ConsistencyLevel.QUORUM
  val ANY: ConsistencyLevel = com.datastax.driver.core.ConsistencyLevel.ANY
}
