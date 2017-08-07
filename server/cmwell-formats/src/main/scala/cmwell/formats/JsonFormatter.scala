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

import java.math.{BigDecimal, BigInteger}

import cmwell.domain._
import cmwell.syntaxutils._
import cmwell.util.string.dateStringify
import org.apache.jena.datatypes.xsd.XSDDatatype
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.binary._
import org.joda.time.DateTime
import play.api.libs.json._

/**
 * Created by gilad on 12/4/14.
 */
abstract class AbstractJsonFormatter(override val fieldNameModifier: String => String) extends TreeLikeFormatter {
  override def format: FormatType = JsonType
  override type Inner = JsValue

  override def makeFromTuples(tuples: Seq[(String, Inner)]): Inner = JsObject(tuples)
  override def makeFromValues(values: Seq[Inner]): Inner = JsArray(values)
  override def empty: Inner = JsNull
  override def single[T](value: T): Inner = value match {
    case i: Int =>         JsNumber(new BigDecimal(i.toString))
    case i: Long =>        JsNumber(new BigDecimal(i.toString))
    case i: BigInteger =>  JsNumber(new BigDecimal(i.toString))
    case i: Float =>       JsNumber(new BigDecimal(i.toString))
    case i: Double if i == Double.PositiveInfinity => JsString("Infinity") //JsNumber(BigDecimal.valueOf(Double.MaxValue))
    case i: Double if i == Double.NegativeInfinity => JsString("-Infinity") //JsNumber(BigDecimal.valueOf(Double.MinValue))
    case i: Double if i.toString == "NaN" => JsString("NaN")
    case i: Double =>      JsNumber(new BigDecimal(i.toString))
    case i: BigDecimal =>  JsNumber(i)
    case b: Boolean =>     JsBoolean(b)
    case d: Array[Byte] => JsString(new Base64(0).encodeToString(d))
    case d: DateTime =>    JsString(dateStringify(d))
    case _ =>              JsString(value.toString)
  }
}

/**
 * wrap a json output with a callback
 */
trait JsonP extends Formatter {
  def callback: Option[String]

  abstract override def render(formattable: Formattable): String = callback match {
    case None => super.render(formattable)
    case Some(cb) => {
      val j = super.render(formattable)
      s"$cb($j);"
    }
  }

  abstract override def format = callback match {
    case Some(_) => JsonpType
    case None => super.format
  }
}

class JsonFormatter(fieldNameModifier: String => String, override val callback: Option[String] = None) extends AbstractJsonFormatter(fieldNameModifier) with JsonP {
  override def makeFromValues(values: Seq[Inner]): Inner = JsArray(cleanDuplicatesPreserveOrder(values))
  override def mkString(inner: Inner) = Json.stringify(inner)
}

class PrettyJsonFormatter(fieldNameModifier: String => String, override val callback: Option[String] = None) extends AbstractJsonFormatter(fieldNameModifier) with JsonP {
  override def makeFromValues(values: Seq[Inner]): Inner = JsArray(cleanDuplicatesPreserveOrder(values))
  override def mkString(inner: Inner) = Json.prettyPrint(inner)
}

abstract class AbstractJsonlFormatter(hashToUrlAndPrefix: String => Option[(String,String)], quadToAlias: String => Option[String]) extends AbstractJsonFormatter(identity) {
  //todo add @id
  override def format: FormatType = JsonlType
  override def mkString(value: Inner): String = Json.stringify(value)
  override def empty: JsObject = JsObject(Seq())

  protected def toTupleSeq[T](t: (String,Option[T])*) =
    t.toSeq.flatMap(x => x._2 match {
      case Some(v) => Seq(x._1 -> single(v))
      case None => Seq()
    })

  val m = scala.collection.mutable.Map.empty[String, String]
  val s = scala.collection.mutable.Set.empty[String]

  private[this] def f(q: String): Option[String] = {
    if(m.contains(q)) m.get(q)
    else if(s(q)) None
    else {
      val opt = quadToAlias(q)
      opt match {
        case Some(x) => m.update(q,x)
        case None => s += q
      }
      opt
    }
  }

  protected def mkVal[V](value: V, quad: Option[String] = None, lang: Option[String] = None, dataType: Option[String] = None): Inner = {
    JsObject(Seq("value" -> single(value)) ++ toTupleSeq("quad" -> quad.flatMap(f), "lang" -> lang, "type" -> dataType))
  }
  protected def mkValWrappedInArray[V](value: V, quad: Option[String] = None, lang: Option[String] = None, dataType: Option[String] = None): Inner =
    JsArray(Seq(mkVal(value, quad, lang, dataType)))

  override def singleFieldValue(fv: FieldValue): Inner = fv match {
    case FString(value: String, lang, quad) => mkVal(value, quad, lang, Some(XSDDatatype.XSDstring.getURI))
    case FInt(value: Int, quad) => mkVal(value, quad, dataType = Some(XSDDatatype.XSDint.getURI))
    case FLong(value: Long, quad) => mkVal(value, quad, dataType = Some(XSDDatatype.XSDlong.getURI))
    case FBigInt(value: java.math.BigInteger, quad) => mkVal(value, quad, dataType = Some(XSDDatatype.XSDinteger.getURI))
    case FFloat(value: Float, quad) => mkVal(value, quad, dataType = Some(XSDDatatype.XSDfloat.getURI))
    case FDouble(value: Double, quad) => mkVal(value, quad, dataType = Some(XSDDatatype.XSDdouble.getURI))
    case FBigDecimal(value: java.math.BigDecimal, quad) => mkVal(value, quad, dataType = Some(XSDDatatype.XSDdecimal.getURI))
    case FBoolean(value: Boolean, quad) => mkVal(value, quad, dataType = Some(XSDDatatype.XSDboolean.getURI))
    case FDate(value: String, quad) => mkVal(value, quad, dataType = Some(XSDDatatype.XSDdateTime.getURI))
    case FReference(value: String, quad) => mkVal({if(value.startsWith("cmwell://")) value.drop("cmwell:/".length) else value}, quad, dataType = Some(XSDDatatype.XSDanyURI.getURI))
    case FExternal(value: String, dataTypeURI: String, quad) => mkVal(value, quad, None, Some(dataTypeURI))
    case FNull(_) => !!! //this is just a marker for IMP, should not index it anywhere...
    case _:FExtra[_] => !!! // FExtra is just a marker for outputting special properties, should not index it anywhere...
  }

  override def system(i: Infoton): JsObject = JsObject(Seq("@id.sys"->mkValWrappedInArray(i.path),"type.sys"->mkValWrappedInArray(i.kind)) ++ system(i,(s:String)=>s"$s.sys",mkValWrappedInArray(_),mkValWrappedInArray(_),mkValWrappedInArray(_)))

  override def fields(i: Infoton): JsObject = i.fields match {
    case None => JsObject(Seq())
    case Some(xs) => JsObject(xs.filter(_._1.head != '$').flatMap(field => Seq(convertToCorrectPrefixedForm(field._1) -> JsArray(field._2.map(singleFieldValue).toSeq))).toSeq)
  }

  override def makeFromTuples(tuples: Seq[(String, Inner)]): Inner = JsObject(tuples)
  override def makeFromValues(values: Seq[Inner]) = JsArray(values)

  override def fileContent(fileContent: FileContent) =
    JsObject(super.fileContent(fileContent,(s:String)=>s"$s.content.sys",v=>mkValWrappedInArray(v),v=>mkValWrappedInArray(v),v=>mkValWrappedInArray(v)))

  override def infoton(i: Infoton): Inner = {
    val iSystem = system(i)
    val iFields = i.fields.collect {
      case fm if fm.exists(_._1.head != '$') => fields(i)
    }.getOrElse(empty)
    (i: @unchecked) match {
      case CompoundInfoton(_, _, _, _, _, children, offset, length, total,_) =>
        iSystem ++ iFields ++
          JsObject(Seq(
          "children.sys" -> infotons(children),
          "offset.sys" ->  mkValWrappedInArray(offset),
          "length.sys" -> mkValWrappedInArray(length),
          "total.sys" -> mkValWrappedInArray(total)
        ))
      case ObjectInfoton(_, _, _, _, _,_) => iSystem ++ iFields
      case FileInfoton(_, _, _, _, _, content,_) =>
        iSystem ++ iFields ++ (content match {
            case Some(c) => fileContent(c)
            case None => JsObject(Seq())
          })
      case LinkInfoton(_, _, _, _, _, linkTo, linkType,_) =>
        iSystem ++ iFields ++ JsObject(Seq("linkTo" -> mkValWrappedInArray(linkTo), "linkType" -> mkValWrappedInArray(linkType)))
      case d: DeletedInfoton => iSystem ++ iFields
    }
  }

  private def convertToCorrectPrefixedForm(fieldKey: String) = {
    lazy val splitted = fieldKey.split('.')
    lazy val opt = hashToUrlAndPrefix(splitted.last)
    if (fieldKey.contains(".") && opt.isDefined)
      s"${splitted.init.mkString(".")}.${opt.get._2}"
    else
      s"$fieldKey.nn"
  }
}

class JsonlFormatter(hashToUrlAndPrefix: String => Option[(String,String)], quadToAlias: String => Option[String], override val callback: Option[String] = None) extends AbstractJsonlFormatter(hashToUrlAndPrefix,quadToAlias) with JsonP {
  override def mkString(value: Inner): String = Json.stringify(value)
}

class PrettyJsonlFormatter(hashToUrlAndPrefix: String => Option[(String,String)], quadToAlias: String => Option[String], override val callback: Option[String] = None) extends AbstractJsonlFormatter(hashToUrlAndPrefix,quadToAlias) with JsonP {
  override def mkString(value: Inner): String = Json.prettyPrint(value)
}

