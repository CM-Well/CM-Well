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
package cmwell.util.jmx

import com.google.common.cache.Cache

/**
  * Created by israel on 22/11/2016.
  */
trait GuavaCacheJMXMBean {

  def getRequestCount(): Long

  def getHitCount(): Long

  def getHitRate(): Double

  def getMissCount(): Long

  def getMissRate(): Double

  def getLoadCount(): Long

  def getLoadSuccessCount(): Long

  def getLoadExceptionCount(): Long

  def getLoadExceptionRate(): Double

  def getTotalLoadTime(): Long

  def getAverageLoadPenalty(): Double

  def getEvictionCount(): Long

  def getSize(): Long

  def cleanUp(): Unit

  def invalidateAll(): Unit
}

class GuavaCacheJMX[K, V](var _cache: Cache[K, V]) extends GuavaCacheJMXMBean {

  override def getRequestCount(): Long = _cache.stats().requestCount()

  override def getHitCount(): Long = _cache.stats().hitCount()

  override def getHitRate(): Double = _cache.stats().hitRate()

  override def getMissCount(): Long = _cache.stats().missCount()

  override def getMissRate(): Double = _cache.stats().missRate()

  override def getLoadCount(): Long = _cache.stats().loadCount()

  override def getLoadSuccessCount(): Long = _cache.stats().loadSuccessCount()

  override def getLoadExceptionCount(): Long = _cache.stats().loadExceptionCount()

  override def getLoadExceptionRate(): Double = _cache.stats().loadExceptionRate()

  override def getTotalLoadTime(): Long = _cache.stats().totalLoadTime()

  override def getAverageLoadPenalty(): Double = _cache.stats().averageLoadPenalty()

  override def getEvictionCount(): Long = _cache.stats().evictionCount()

  override def getSize(): Long = _cache.size()

  override def cleanUp(): Unit = _cache.cleanUp()

  override def invalidateAll(): Unit = _cache.invalidateAll()
}
