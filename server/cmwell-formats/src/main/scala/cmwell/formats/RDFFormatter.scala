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

import java.io._
import java.math.BigInteger
import cmwell.domain._
import cmwell.syntaxutils._
import cmwell.util.string.dateStringify
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.rdf.model.{AnonId, Model, ModelFactory, Property, RDFNode, Resource, ResourceFactory}
import org.apache.jena.vocabulary.XSD
import org.apache.commons.codec.binary.Base64.{encodeBase64String => base64}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.joda.time.DateTime

/**
 * Created by gilad on 12/4/14.
 */
abstract class RDFFormatter(hostForNs: String, hashToPrefixAndUri: String => Option[(String,Option[String])], withoutMeta: Boolean, filterOutBlanks: Boolean, forceUniqueness: Boolean) extends Formatter {

  lazy val host = {
    if(hostForNs.startsWith("http")) hostForNs
    else {
      logger.warn(s"host provided for RDF formatter ($hostForNs) does not contain a known protocol ('http://' or 'https://'), assigned with 'http://' as default.")
      s"http://$hostForNs"
    }
  }

  protected implicit class DatasetExtension(ds: Dataset) {
    import scala.collection.JavaConversions._
    def foreachNamedModel(f: Model => Unit): Dataset = {
      ds.listNames.foreach(name => f(ds.getNamedModel(name)))
      ds
    }
    def foreachModel(f: Model => Unit): Dataset = {
      foreachNamedModel(f)
      f(ds.getDefaultModel)
      ds
    }
  }

  def formattableToDataset(formattable: Formattable): Dataset = {
    implicit val dataset = DatasetFactory.createGeneral()

    implicit val model = ModelFactory.createDefaultModel()

    formattable match {
      case i: Infoton => {
        infoton(i)
        dataset.foreachModel {
          model => {
            //TODO: will call isPathADomain & excludeParent inside 'infoton' method. probably can optimize.
            //at this level we only set the prefixes. 'infoton' method can be called from other methods like 'bagOfInfotons'
            if (excludeParent(i.path)) {
              model.setNsPrefix("o", s"${uriFromPath(i.path)}/")
            } else {
              model.setNsPrefix("o", s"${uriFromPath(i.parent)}/")
              if (i.isInstanceOf[CompoundInfoton]) {
                model.setNsPrefix("o2", s"${uriFromPath(i.path)}/")
              }
            }
          }
        }
      }
      case r: SearchThinResult => thinResult(r)
      case sr: SimpleResponse => simpleResponse(sr)
      case bag: BagOfInfotons => bagOfInfotons(bag)
      case ihv: InfotonHistoryVersions => infotonHistoryVersions(ihv)
      case rp: RetrievablePaths => retrievablePaths(rp)
      case pi: PaginationInfo => pagination(pi)
      case sr: SearchResults => searchResults(sr)
      case sr: SearchResponse => searchResponse(sr)
      case ir: IterationResults => iterationResults(ir)
      case _ => throw new NotImplementedError(s"formattableToDataset implementation not found for ${formattable.getClass.toString}")
    }

    if (!withoutMeta) {
      dataset.foreachModel {
        model => {
          model.setNsPrefix("sys", s"$host/meta/sys#")
          model.setNsPrefix("nn", s"$host/meta/nn#")
          model.setNsPrefix("xsd", XSD.getURI)
        }
      }
    } else dataset
  }

  val stringToSysProp: String => Property = ResourceFactory.createProperty(s"$host/meta/sys#", _)
  val stringToNnProp: String => Property = ResourceFactory.createProperty(s"$host/meta/nn#", _)

  val stringToLtrl: (String,Option[String]) => RDFNode = (v: String, lang: Option[String]) => lang match {
    case None => ResourceFactory.createPlainLiteral(v)
    case Some(l) => ResourceFactory.createLangLiteral(v,l)
  }
  val dateToLtrl: DateTime => RDFNode = (dt: DateTime) => ResourceFactory.createTypedLiteral(dateStringify(dt), NodeFactory.getType(XSD.dateTime.getURI))
  val refToLtrl: String => RDFNode = (v: String) => ResourceFactory.createResource(v)
  val intToLtrl: Int => RDFNode = (v: Int) => ResourceFactory.createTypedLiteral(v.toString, NodeFactory.getType(XSD.xint.getURI))
  val longToLtrl: Long => RDFNode = (v: Long) => ResourceFactory.createTypedLiteral(v.toString, NodeFactory.getType(XSD.xlong.getURI))
  val bigIntToLtrl: BigInteger => RDFNode = (v: BigInteger) => ResourceFactory.createTypedLiteral(v.toString, NodeFactory.getType(XSD.integer.getURI))
  val floatToLtrl: Float => RDFNode = (v: Float) => ResourceFactory.createTypedLiteral(v.toString, NodeFactory.getType(XSD.xfloat.getURI))
  val doubleToLtrl: Double => RDFNode = (v: Double) =>ResourceFactory.createTypedLiteral(v.toString, NodeFactory.getType(XSD.xdouble.getURI))
  val bigDecimalToLtrl: BigDecimal => RDFNode = (v: BigDecimal) => ResourceFactory.createTypedLiteral(v.toString, NodeFactory.getType(XSD.decimal.getURI))
  val boolToLtrl: Boolean => RDFNode = (v: Boolean) => ResourceFactory.createTypedLiteral(v.toString, NodeFactory.getType(XSD.xboolean.getURI))
  val bytesToLtrl: Array[Byte] => RDFNode = (v: Array[Byte]) => ResourceFactory.createTypedLiteral(base64(v), NodeFactory.getType(XSD.base64Binary.getURI))

  private val emptySeq = Seq[(Property, RDFNode)]()
  private val mEmptySeq = Seq[(Property, RDFNode, Model)]()

  def fields(theFields: Option[Map[String,Set[FieldValue]]])(implicit ds: Dataset): Seq[(Property,RDFNode,Model)] = theFields match {
    case None => mEmptySeq
    case Some(m) => (mEmptySeq /: m) {
      case (seq, (f, set)) => {
        val (prop: Property, nsOpt: Option[(String,String)]) = f.lastIndexOf('.') match {
          case -1 => stringToNnProp(f) -> None
          // family name method
          case index => {
            val firstName = f.substring(0, index)
            val hash = f.substring(index + 1)
            // get full path from /meta/ns (if exist)
            hashToPrefixAndUri(hash) match {
              case Some((url,prefix)) => ResourceFactory.createProperty(url, firstName) -> prefix.map(_ -> url)
              case None => stringToNnProp(f) -> None
            }
          }
        }
        val modifiedSet: Set[(Property, RDFNode, Model)] = set.map {fv =>
          val model = fv.quad match {
            case None => ds.getDefaultModel
            case Some(graphName) => ds.getNamedModel(graphName)
          }
          nsOpt.foreach{case (last,url) => model.setNsPrefix(last,url)}
          fv match {
            case FString(v,l,_) if v.startsWith("cmwell://") => (prop, refToLtrl(v.replaceFirst("cmwell:/",host)), model)
            case FString(v,l,_) => (prop, stringToLtrl(v,l), model)
            case FReference(v,_) => (prop, refToLtrl({if(v.startsWith("cmwell://")) v.replaceFirst("cmwell:/",host) else v}), model)
            case FInt(v,_) => (prop, intToLtrl(v), model)
            case FLong(v,_) => (prop, longToLtrl(v), model)
            case FBigInt(v,_) => (prop, bigIntToLtrl(v), model)
            case FFloat(v,_) => (prop, floatToLtrl(v), model)
            case FDouble(v,_) => (prop, doubleToLtrl(v), model)
            case FBigDecimal(v,_) => (prop, bigDecimalToLtrl(v), model)
            case FBoolean(v,_) => (prop, boolToLtrl(v), model)
            case FDate(v,_) => (prop, ResourceFactory.createTypedLiteral(v, NodeFactory.getType(XSD.dateTime.getURI)), model)
            case f@FExternal(v, _,_) => (prop, ResourceFactory.createTypedLiteral(v, NodeFactory.getType(f.getDataTypeURI)), model)
            case FNull(_) => !!! //this is just a marker for IMP, should not index it anywhere...
            case _:FExtra[_] => !!!
          }
        }
        seq ++ modifiedSet
      }
    }
  }


  /*
   * if path is: '/example.org'
   * then parent is: '/'
   * so what would be the subject in RDF?
   * so, when viewing in RDF, we have an exception for 'root domains'
   * to exclude the parent attribute
   */
  def isPathADomain(path: String): Boolean = path.dropWhile(_ == '/').takeWhile(_ != '/').contains('.')
  def excludeParent(path: String, isKnownAsADomain: Boolean = false): Boolean = (isKnownAsADomain || isPathADomain(path)) && path.split('/').filterNot(_.isEmpty).size <= 1

  def uriFromPath(path: String, isADomainOpt: Option[Boolean] = None): String = {
    if ((isADomainOpt.isDefined && isADomainOpt.get) ||
        (isADomainOpt.isEmpty && isPathADomain(path))) {
      if(path.startsWith("/https.")) s"https:/${path.drop("/https.".length)}"
      else s"http:/${path}"
    }
    else
      s"$host${path}"
  }

  private val memoizedBreakOut = scala.collection.breakOut[Map[String,Set[FieldValue]],(Property,RDFNode),Seq[(Property,RDFNode)]]
  def infoton(i: Infoton, distinctPathWithUuid: Boolean = false)(implicit ds: Dataset): Dataset = {//(implicit model: Model): Model = {
    val subject = {
      val s = if(distinctPathWithUuid || forceUniqueness) s"${i.path}#${i.uuid}" else i.path
      ResourceFactory.createResource(uriFromPath(s))
    }
    val fieldsData = fields(i.fields.map(_.filter(_._1.head != '$')))
    val extras = i.fields.fold(Seq.empty[(Property,RDFNode)])(_.collect {
      case (k,vs) if k.head == '$' => {
        val t = k.tail
        val prop = stringToSysProp(t)
        val rdfNode: RDFNode = t match {
          case "score" => floatToLtrl(vs.head.value.asInstanceOf[Float]) //FIXME: what an ugly hack...
          case _ => ???
        }
        (prop,rdfNode)
      }
    }(memoizedBreakOut))
    val t = stringToSysProp("type") -> stringToLtrl(i.kind,None)
    val systemData: Seq[(Property,RDFNode)] = t +: extras ++: system[Property,RDFNode](i, stringToSysProp,stringToLtrl(_,None),dateToLtrl,longToLtrl,!excludeParent(i.path))
    val selfAttributes: Seq[(Property,RDFNode)] = i match {

      // no system blanks

      case _ if withoutMeta => Nil

      // ObjectInfoton

      case ObjectInfoton(path, _, _, lastModified, iFields,_) => systemData

      // CompoundInfoton

      case CompoundInfoton(_,_,_,_,_,children,offset,length,total,_) => {
        val childrenProp = stringToSysProp("children")
        val props = systemData ++:
          Seq(stringToSysProp("offset") -> longToLtrl(offset),stringToSysProp("length") -> longToLtrl(length),stringToSysProp("total") -> longToLtrl(total))

        (props /: children) {
          case (s,i) => {
            infoton(i)
            s :+ (childrenProp -> refToLtrl(uriFromPath(i.path)))
          }
        }
      }

      // FileInfoton

      case FileInfoton(_,_,_,_,_,content,_) => {
        val t = content.map(fileContent[Property,RDFNode](_, stringToSysProp, stringToLtrl(_,None), longToLtrl, bytesToLtrl))
        val res = systemData ++: t.getOrElse(emptySeq)
//        logger.info(res.toString)
        res
      }

      // LinkInfoton

      case LinkInfoton(_,_,_,_,_,linkTo,linkType,_) => {
        systemData ++:
          Seq(stringToSysProp("linkTo") -> stringToLtrl(linkTo,None), stringToSysProp("linkType") -> intToLtrl(linkType))
      }

      // DeletedInfoton

      case _: DeletedInfoton => systemData

      case _ => ???
    }

    val models = {
      val ms = fieldsData.map(_._3).toSet
      if(ms.isEmpty) Seq(ds.getDefaultModel)
      else ms
    }
    /*models.foreach(m => */{
      val m = ds.getDefaultModel //TODO: make special internal graph for cm-well system properties. should be represented by a VirtualInfoton under /meta/quad/ with a fixed alias (`cm-well`?)
      //val m = ds.getNamedModel("cmwell://meta/sys")
      selfAttributes.foreach {
        case (p, v) => {
          m.add (ResourceFactory.createStatement (subject, p, v) )
        }
      }
    }//)
    fieldsData.foreach{
      case (p,v,m) => m.add(ResourceFactory.createStatement(subject,p,v))
    }
    ds
  }

  def thinResult(r: SearchThinResult)(implicit ds: Dataset): Dataset = {
    val model = ds.getDefaultModel
    val subject = model.createResource(AnonId.create("ThinResult"))
    (model /: super.thinResult(r,stringToSysProp,stringToLtrl(_,None),longToLtrl,floatToLtrl)) {
      case (m,(p,l)) => m.add(ResourceFactory.createStatement(subject,p,l))
    }
    ds
  }

  def simpleResponse(sr: SimpleResponse)(implicit ds: Dataset): Dataset = {
    val model = ds.getDefaultModel
    val subject = model.createResource(AnonId.create("simpleResponse"))
    (model /: super.simpleResponse(sr,stringToSysProp,stringToLtrl(_,None),boolToLtrl)) {
      case (m,(p,l)) => m.add(ResourceFactory.createStatement(subject,p,l))
    }
    ds
  }
  def bagOfInfotons(bag: BagOfInfotons)(implicit ds: Dataset): Dataset = {
    if(withoutMeta) {
      bag.infotons.foreach(infoton(_)(ds))
    }
    else {
      val model = ds.getDefaultModel

      val subject = model.createResource(AnonId.create("BagOfInfotons"))
      val infotons = stringToSysProp("infotons")

      if(!filterOutBlanks) {
        model
          .add(ResourceFactory.createStatement(subject, stringToSysProp("type"),stringToLtrl("BagOfInfotons",None)))
          .add(ResourceFactory.createStatement(subject, stringToSysProp("size"), intToLtrl(bag.infotons.size)))

      }
      (model /: bag.infotons) {
        case (m,i) => {
          if(!filterOutBlanks) {
            m.add(ResourceFactory.createStatement(subject, infotons, refToLtrl(uriFromPath(i.path))))
          }
          infoton(i)(ds).getDefaultModel
        }
      }
    }
    ds
  }
  def infotonHistoryVersions(ihv: InfotonHistoryVersions)(implicit ds: Dataset): Dataset = {
    if(withoutMeta) {
      ihv.versions.foreach(infoton(_,true)(ds))
    }
    else {
      val model = ds.getDefaultModel

      val subject = model.createResource(AnonId.create("infotonHistoryVersions"))
      val versions = stringToSysProp("versions")

      if(!filterOutBlanks) {
        model.add(ResourceFactory.createStatement(subject, stringToSysProp("type"), stringToLtrl("InfotonHistoryVersions", None)))
      }

      (model /: ihv.versions) {
        case (m,i) => {
          if(!filterOutBlanks) {
            m.add(ResourceFactory.createStatement(subject, versions, refToLtrl(uriFromPath(i.path))))
          }
          infoton(i,true)(ds).getDefaultModel
        }
      }
    }
    ds
  }
  def retrievablePaths(rp: RetrievablePaths)(implicit ds: Dataset): Dataset = {
    if(withoutMeta) {
      rp.infotons.foreach(infoton(_)(ds))
    }
    else {
      val model = ds.getDefaultModel

      val subject = model.createResource(AnonId.create("retrievablePaths"))

      val infotons = stringToSysProp("infotons")
      val irretrievablePaths = stringToSysProp("irretrievablePaths")

      if(!filterOutBlanks) {
        model
          .add(ResourceFactory.createStatement(subject, stringToSysProp("type"), stringToLtrl("RetrievablePaths", None)))
          .add(ResourceFactory.createStatement(subject, stringToSysProp("size"), intToLtrl(rp.infotons.size)))

        (model /: rp.irretrievablePaths) {
          case (m, p) => m.add(ResourceFactory.createStatement(subject, irretrievablePaths, stringToLtrl(p, None)))
        }
      }

      (model /: rp.infotons) {
        case (m,i) => {
          if(!filterOutBlanks) {
            m.add(ResourceFactory.createStatement(subject, infotons, refToLtrl(uriFromPath(i.path))))
          }
          infoton(i)(ds).getDefaultModel
        }
      }
    }
    ds
  }
  def pagination(pi: PaginationInfo,subject: Resource = ResourceFactory.createResource())(implicit ds: Dataset): Dataset = {
    val model = ds.getDefaultModel
    (model /: super.pagination(pi,stringToSysProp,stringToLtrl(_,None),refToLtrl)) {
      case (m,(p,l)) => m.add(ResourceFactory.createStatement(subject, p, l))
    }
    ds
  }
  def searchResults(sr: SearchResults,subject: Resource = ResourceFactory.createResource())(implicit ds: Dataset): Dataset = {
    if(withoutMeta) {
      sr.infotons.foreach(infoton(_)(ds))
    }
    else {
      val model = ds.getDefaultModel

      if(!filterOutBlanks) {
        model.add(ResourceFactory.createStatement(subject, stringToSysProp("type"), stringToLtrl("SearchResults", None)))
        sr.fromDate.foreach(dt => model.add(ResourceFactory.createStatement(subject, stringToSysProp("fromDate"), dateToLtrl(dt))))
        sr.toDate.foreach(dt => model.add(ResourceFactory.createStatement(subject, stringToSysProp("toDate"), dateToLtrl(dt))))
        model.add(ResourceFactory.createStatement(subject, stringToSysProp("total"), longToLtrl(sr.total)))
        model.add(ResourceFactory.createStatement(subject, stringToSysProp("offset"), longToLtrl(sr.offset)))
        model.add(ResourceFactory.createStatement(subject, stringToSysProp("length"), longToLtrl(sr.length)))
      }

      (model /: sr.infotons) {
        case (m,i) => {
          if(!filterOutBlanks) {
            m.add(ResourceFactory.createStatement(subject, stringToSysProp("infotons"), refToLtrl(uriFromPath(i.path))))
          }
          infoton(i)(ds).getDefaultModel
        }
      }
    }
    ds
  }
  def searchResponse(sr: SearchResponse)(implicit ds: Dataset): Dataset = {
    if(withoutMeta || filterOutBlanks) searchResults(sr.results, null)(ds)
    else {
      val model = ds.getDefaultModel
      val subject = model.createResource(AnonId.create("searchResponse"))
      val results = model.createResource(AnonId.create("searchResults"))
      val pgntion = model.createResource(AnonId.create("pagination"))
      model.add(ResourceFactory.createStatement(subject, stringToSysProp("type"),stringToLtrl("SearchResponse",None)))
      model.add(ResourceFactory.createStatement(subject, stringToSysProp("results"),results))
      model.add(ResourceFactory.createStatement(subject, stringToSysProp("pagination"),pgntion))
      pagination(sr.pagination, pgntion)(ds)
      searchResults(sr.results, results)(ds)
    }
  }
  def iterationResults(ir: IterationResults)(implicit ds: Dataset): Dataset = {
    if(withoutMeta || filterOutBlanks) {
      ir.infotons.foreach(_.foreach(infoton(_)(ds)))
    }
    else {
      val model = ds.getDefaultModel
      val subject = model.createResource(AnonId.create("iterationResults"))
      model.add(ResourceFactory.createStatement(subject, stringToSysProp("type"), stringToLtrl("IterationResults", None)))
      model.add(ResourceFactory.createStatement(subject, stringToSysProp("iteratorId"), stringToLtrl(ir.iteratorId, None)))
      model.add(ResourceFactory.createStatement(subject, stringToSysProp("total"), longToLtrl(ir.totalHits)))
      ir.infotons.map(infotons => (model /: infotons) {
        case (m, i) => {
          infoton(i)(ds)
          m.add(ResourceFactory.createStatement(subject, stringToSysProp("infotons"), refToLtrl(uriFromPath(i.path))))
        }
      }).getOrElse(model)
    }
    ds
  }
}

/**
 * json-ld comes out pretty printed.
 * in case you want it in a single line, mix in the trait
 */
trait UnPretty extends RDFFormatter {

  def pretty: Boolean

  abstract override def render(formattable: Formattable): String = {
    val res = super.render(formattable)
    if(pretty) res
    else res.replace('\n',' ')
  }
}

abstract class SimpleRDFFormatter(host: String, hashToPrefixAndUri: String => Option[(String,Option[String])], withoutMeta: Boolean, filterOutBlanks: Boolean, forceUniqueness: Boolean) extends RDFFormatter(host, hashToPrefixAndUri, withoutMeta, filterOutBlanks, forceUniqueness) {

  protected def getFlavor: String

  override def render(formattable: Formattable): String = {
    import scala.util.{Failure, Success, Try}
    val dataset = formattableToDataset(formattable)
    val default = dataset.getDefaultModel
    dataset.foreachNamedModel(m => default.add(m))
    val strWriter = new StringWriter
    Try(default.write(strWriter,getFlavor,null)) match {
      case Success(v) => strWriter.toString
      case Failure(e) => {
        logger.error("error from jena serializer",e)
        throw e
      }
    }
  }
}
class N3Formatter(host: String, hashToPrefixAndUri: String => Option[(String,Option[String])], withoutMeta: Boolean, filterOutBlanks: Boolean, forceUniqueness: Boolean) extends SimpleRDFFormatter(host, hashToPrefixAndUri, withoutMeta, filterOutBlanks, forceUniqueness) {
  override val format: FormatType = RdfType(N3Flavor)
  override protected def getFlavor: String = "N3"
}
class NTriplesFormatter(host: String, hashToUri: String => Option[(String,Option[String])], withoutMeta: Boolean, filterOutBlanks: Boolean, forceUniqueness: Boolean) extends SimpleRDFFormatter(host, hashToUri, withoutMeta, filterOutBlanks, forceUniqueness) {
  override val format: FormatType = RdfType(NTriplesFlavor)
  override protected def getFlavor: String = "N-TRIPLE"
}
class RDFXmlFormatter(host: String, hashToPrefixAndUri: String => Option[(String,Option[String])], withoutMeta: Boolean, filterOutBlanks: Boolean, forceUniqueness: Boolean) extends SimpleRDFFormatter(host, hashToPrefixAndUri, withoutMeta, filterOutBlanks, forceUniqueness) {
  override val format: FormatType = RdfType(RdfXmlFlavor)
  override protected def getFlavor: String = "RDF/XML"
}
class TurtleFormatter(host: String, hashToPrefixAndUri: String => Option[(String,Option[String])], withoutMeta: Boolean, filterOutBlanks: Boolean, forceUniqueness: Boolean) extends SimpleRDFFormatter(host, hashToPrefixAndUri, withoutMeta, filterOutBlanks, forceUniqueness) {
  override val format: FormatType = RdfType(TurtleFlavor)
  override protected def getFlavor: String = "TURTLE"
}
class JsonLDFormatter private(
  host: String,
  hashToPrefixAndUri: String => Option[(String,Option[String])],
  withoutMeta: Boolean,
  filterOutBlanks: Boolean,
  forceUniqueness: Boolean,
  val pretty: Boolean,
  val callback: Option[String]) extends SimpleRDFFormatter(host, hashToPrefixAndUri, withoutMeta, filterOutBlanks, forceUniqueness) {
  override def format: FormatType = RdfType(JsonLDFlavor)
  override protected def getFlavor: String = "JSON-LD"
}

/**
 * initialization is forced through the companion's apply method
 * (by marking the constructor as private)
 * to allow `JsonP` & `UnPretty` mixin in the right linearization order.
 */
object JsonLDFormatter {
  def apply(
    host: String,
    hashToPrefixAndUri: String => Option[(String,Option[String])],
    withoutMeta: Boolean,
    filterOutBlanks: Boolean,
    forceUniqueness: Boolean,
    pretty: Boolean = true,
    callback: Option[String] = None): JsonLDFormatter = new JsonLDFormatter(host,hashToPrefixAndUri,withoutMeta,filterOutBlanks,forceUniqueness,pretty,callback) with UnPretty with JsonP
}

abstract class QuadsFormatter(host: String, hashToPrefixAndUri: String => Option[(String,Option[String])], quadToAlias: String => Option[String], withoutMeta: Boolean, filterOutBlanks: Boolean, forceUniqueness: Boolean) extends RDFFormatter(host, hashToPrefixAndUri, withoutMeta, filterOutBlanks, forceUniqueness) {

  protected def getLang: Lang

  override def render(formattable: Formattable): String = {
    import scala.collection.JavaConversions._

    val dataset = formattableToDataset(formattable)
    dataset.listNames.foreach{
      name => {
        val alias = quadToAlias(name)
        alias.foreach{
          prefix => {
            logger.info(s"about to set prefix: $prefix for $name")
            dataset.foreachModel(_.setNsPrefix(prefix,name))
          }
        }
      }
    }
    val strWriter = new StringWriter
    RDFDataMgr.write(strWriter, dataset, getLang)
    strWriter.toString
  }
}
class NQuadsFormatter(host: String, hashToPrefixAndUri: String => Option[(String,Option[String])], withoutMeta: Boolean, filterOutBlanks: Boolean, forceUniqueness: Boolean) extends QuadsFormatter(host,hashToPrefixAndUri, { _=> None }, withoutMeta, filterOutBlanks, forceUniqueness) {
  override val format: FormatType = RdfType(NquadsFlavor)
  override protected def getLang: Lang = Lang.NQUADS
}
class TriGFormatter(host: String, hashToPrefixAndUri: String => Option[(String,Option[String])], quadToAlias: String => Option[String], withoutMeta: Boolean, filterOutBlanks: Boolean, forceUniqueness: Boolean) extends QuadsFormatter(host, hashToPrefixAndUri, quadToAlias, withoutMeta, filterOutBlanks, forceUniqueness) {
  override val format: FormatType = RdfType(TriGFlavor)
  override protected def getLang: Lang = Lang.TRIG
}

class TriXFormatter(host: String, hashToPrefixAndUri: String => Option[(String,Option[String])], quadToAlias: String => Option[String], withoutMeta: Boolean, filterOutBlanks: Boolean, forceUniqueness: Boolean) extends QuadsFormatter(host, hashToPrefixAndUri, quadToAlias, withoutMeta, filterOutBlanks, forceUniqueness) {
  override val format: FormatType = RdfType(TriXFlavor)
  override protected def getLang: Lang = Lang.TRIX
}

class JsonLDQFormatter private(
  host: String,
  hashToPrefixAndUri: String => Option[(String,Option[String])],
  quadToAlias: String => Option[String],
  withoutMeta: Boolean,
  filterOutBlanks: Boolean,
  forceUniqueness: Boolean,
  val pretty: Boolean,
  val callback: Option[String]) extends QuadsFormatter(host, hashToPrefixAndUri, quadToAlias, withoutMeta, filterOutBlanks, forceUniqueness) {
  override def format: FormatType = RdfType(JsonLDQFlavor)
  override protected def getLang: Lang = Lang.JSONLD
}

/**
 * initialization is forced through the companion's apply method
 * (by marking the constructor as private)
 * to allow `JsonP` & `UnPretty` mixin in the right linearization order.
 */
object JsonLDQFormatter {
  def apply(
    host: String,
    hashToPrefixAndUri: String => Option[(String,Option[String])],
    quadToAlias: String => Option[String],
    withoutMeta: Boolean,
    filterOutBlanks: Boolean,
    forceUniqueness: Boolean,
    pretty: Boolean = true,
    callback: Option[String] = None): JsonLDQFormatter = new JsonLDQFormatter(host,hashToPrefixAndUri,quadToAlias,withoutMeta,filterOutBlanks,forceUniqueness,pretty,callback) with UnPretty with JsonP
}

