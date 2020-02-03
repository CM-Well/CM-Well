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
package ld.exceptions

/**
  * Proj: server
  * User: gilad
  * Date: 2/13/18
  * Time: 11:44 AM
  */
sealed trait ConflictingNsEntriesException extends Exception
object ConflictingNsEntriesException {
  def format(kind: String, entry: String, nsIDs: Iterable[String]): String =
    nsIDs.mkString(s"Conflicting ns identifiers found for $kind [$entry] [", ",", "]")

  def byURL(url: String, nsIDs: Iterable[String]) =
    new Exception(format("url", url, nsIDs)) with ConflictingNsEntriesException
  def byURL(url: String, nsIDs: Iterable[String], cause: Throwable) =
    new Exception(format("url", url, nsIDs), cause) with ConflictingNsEntriesException
  def byPrefix(prefix: String, nsIDs: Iterable[String]) =
    new Exception(format("prefix", prefix, nsIDs)) with ConflictingNsEntriesException
  def byPrefix(prefix: String, nsIDs: Iterable[String], cause: Throwable) =
    new Exception(format("prefix", prefix, nsIDs), cause) with ConflictingNsEntriesException
}
