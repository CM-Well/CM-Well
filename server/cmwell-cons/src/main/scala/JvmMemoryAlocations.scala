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
/**
  * Created by michael on 8/27/14.
  */
case class Resources(processors: Int, freePhysicalMemory: Long, totalPhysicalMemory: Long)

case class JvmMemoryAllocations(mxmx: Int, mxms: Int, mxmn: Int, mxss: Int) {

  def getMxmx: String = if (mxmx > 0) s"-Xmx${mxmx}M" else ""
  def getMxms: String = if (mxms > 0) s"-Xms${mxms}M" else ""
  def getMxmn: String = if (mxmn > 0) s"-Xmn${mxmn}M" else ""
  def getMxss: String = if (mxss > 0) s"-Xss${mxss}k" else ""

}

case class ModuleJvmSettings(cas: JvmMemoryAllocations,
                             es: JvmMemoryAllocations,
                             bg: JvmMemoryAllocations,
                             ws: JvmMemoryAllocations,
                             ctrl: JvmMemoryAllocations = JvmMemoryAllocations(1024, 0, 0, 0))

trait ModuleAllocations {
  def getJvmAllocations: ModuleJvmSettings
  def getElasticsearchMasterAllocations = JvmMemoryAllocations(1600, 0, 0, 256)
}

/*case class CalculatedAllocations(r: Resources) extends ModuleAllocations {



  override def getJvmAllocations : ModuleJvmSettings = {

  }
}*/

case class FixedAllocations() extends ModuleAllocations {
  override def getJvmAllocations: ModuleJvmSettings = {
    ModuleJvmSettings(
      JvmMemoryAllocations(4000, 4000, 1000, 256),
      JvmMemoryAllocations(5000, 5000, 1000, 256),
      JvmMemoryAllocations(1000, 1000, 512, 256),
      JvmMemoryAllocations(1000, 1000, 512, 256)
    )
  }
}

case class DevAllocations() extends ModuleAllocations {
  override def getJvmAllocations: ModuleJvmSettings = {
    ModuleJvmSettings(JvmMemoryAllocations(1024, 400, 256, 256),
                      JvmMemoryAllocations(1024, 400, -1, 256),
                      JvmMemoryAllocations(1024, 0, 0, 0),
                      JvmMemoryAllocations(1024, 0, 0, 0))
  }
}

case class CustomAllocations(cas: JvmMemoryAllocations,
                             es: JvmMemoryAllocations,
                             bg: JvmMemoryAllocations,
                             ws: JvmMemoryAllocations)
    extends ModuleAllocations {
  override def getJvmAllocations: ModuleJvmSettings = {
    ModuleJvmSettings(cas, es, bg, ws)
  }
}

case class DefaultAllocations() extends ModuleAllocations {
  override def getJvmAllocations: ModuleJvmSettings = {
    ModuleJvmSettings(JvmMemoryAllocations(0, 0, 0, 0),
                      JvmMemoryAllocations(0, 0, 0, 0),
                      JvmMemoryAllocations(0, 0, 0, 0),
                      JvmMemoryAllocations(0, 0, 0, 0))
  }
}
