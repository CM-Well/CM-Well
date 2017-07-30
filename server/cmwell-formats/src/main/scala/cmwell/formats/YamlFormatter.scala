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


package cmwell.formats

import cmwell.domain.Formattable
import cmwell.util.string.dateStringify

import org.joda.time.DateTime
import org.yaml.snakeyaml.constructor.SafeConstructor
import org.yaml.snakeyaml.introspector.PropertyUtils
import org.yaml.snakeyaml.nodes.{Node, Tag}
import org.yaml.snakeyaml.representer.{Represent, Representer}
import org.yaml.snakeyaml.{Yaml, DumperOptions}

import scala.collection.JavaConverters._


class YamlFormatter(override val fieldNameModifier: String => String) extends TreeLikeFormatter {

  private[this] val yaml = {
    val propUtils: PropertyUtils = new PropertyUtils
    propUtils.setAllowReadOnlyProperties(true)
    val repr: Representer = new Representer//new DateTimeRepresenter
    repr.setPropertyUtils(propUtils)
    val options = new DumperOptions
    options.setIndent(4)
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    new Yaml( new SafeConstructor(), repr, options )
  }

  override type Inner = java.lang.Object

  override def format = YamlType
  override def empty: Inner = null
  override def makeFromTuples(tuples: Seq[(String, Inner)]): Inner = {
    val hm = new java.util.HashMap[String,java.lang.Object]()
    tuples.foreach {
      case (key, inner) => hm.put(key, inner)
    }
    hm
  }
  override def makeFromValues(values: Seq[Inner]): Inner = scala.collection.mutable.Buffer(cleanDuplicatesPreserveOrder(values):_*).asJava
  override def mkString(inner: Inner): String = yaml.dump(inner)
  override def single[T](value: T): Inner = value.asInstanceOf[java.lang.Object]
}



//class DateTimeRepresenter extends Representer {
//    representers.put(classOf[DateTime], new RepresentDateTime)
//
//    private class RepresentDateTime extends Represent {
//        def representData(data: java.lang.Object): Node = {
//            val dateTime: DateTime = data.asInstanceOf[DateTime]
//            val value: String  = dateStringify(dateTime)
//            representScalar(new Tag("!DateTime"), value)
//        }
//    }
//}