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


package cmwell.tlog

import scala.io.Source
import scala.reflect.io.File
import cmwell.util.resource.using

/**
 * Created with IntelliJ IDEA.
 * User: markz
 * Date: 5/28/13
 * Time: 3:09 PM
 * To change this template use File | Settings | File Templates.
 */
trait TLogState {
  def saveState(ts : Long)
  def loadState : Option[Long]
  def position : Long
}


class TLogStateIml(agentName : String ,tlogName : String , partitionName : String , homePath : String , readOnly : Boolean = false ) extends TLogState {
  val key : String = agentName + "_" + tlogName

  val dataPath = homePath + File.separator + "data" + File.separator + "tlog" + File.separator

  val filename = dataPath + key + partitionName + ".pos"

  def saveState(ts : Long) {
    if ( readOnly == false )
      scala.tools.nsc.io.File(filename).writeAll(ts.toString)
  }

  // TODO: if not data set to 0
  def loadState : Option[Long] =  {
    //val data : String = storageDao.read(key , partitionName )
    try {
      val data: String = using(Source.fromFile(filename))(_.mkString)
      Some(data.toLong)
    } catch {
      case _ : Throwable => None
    }

  }

  def position : Long = {
    loadState match {
      case Some(pos) => pos
      case None => 0
    }
  }
}


object TLogState {
  def apply(agentName : String , tlogName : String , partitionName : String, readOnly : Boolean = false) = new TLogStateIml(agentName,tlogName,partitionName.replace('-','_'),System.getProperty("cmwell.home"),readOnly)
}
