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
package cmwell.formats

import cmwell.domain.{AggregationResponse, _}
import cmwell.common.file.MimeTypeIdentifier.isTextual
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

/**
  * Created by gilad on 12/4/14.
  */
trait Formatter extends LazyLogging {
  def format: FormatType
  def mimetype: String = format.mimetype
  def render(formattable: Formattable): String

  //sharing what I can as high in the inheritance hierarchy that is possible
  //TODO: why `type` isn't a part of `system`?
  protected def system[K, V](infoton: Infoton,
                             mkKey: String => K,
                             mkVal: String => V,
                             mkDateVal: DateTime => V,
                             mkLongVal: Long => V,
                             includeParent: Boolean = true): Seq[(K, V)] = {
    val indexTime = infoton.systemFields.indexTime.map(it => Seq(mkKey("indexTime") -> mkLongVal(it))).getOrElse(Nil)
    val protocol = Seq(mkKey("protocol") -> mkVal(infoton.systemFields.protocol))
    val seq = Seq(
//      mkKey("type") -> mkVal(infoton.kind), //TODO: add type to system instead of outer level
      mkKey("uuid") -> mkVal(infoton.uuid),
      mkKey("lastModified") -> mkDateVal(infoton.systemFields.lastModified),
      mkKey("lastModifiedBy") -> mkVal(infoton.systemFields.lastModifiedBy),
      mkKey("path") -> mkVal(infoton.systemFields.path),
      mkKey("dataCenter") -> mkVal(infoton.systemFields.dc)
    ) ++ indexTime ++ protocol ++ Seq(mkKey("parent") -> mkVal(infoton.parent))
    if (includeParent) seq
    else seq.init
  }

  protected def thinResult[K, V](result: SearchThinResult,
                                 mkKey: String => K,
                                 mkVal: String => V,
                                 mkLongVal: Long => V,
                                 mkFloatVal: Float => V): Seq[(K, V)] = {
    Seq(
      mkKey("path") -> mkVal(result.path),
      mkKey("uuid") -> mkVal(result.uuid),
      mkKey("lastModified") -> mkVal(result.lastModified),
      mkKey("indexTime") -> mkLongVal(result.indexTime)
    ) ++ result.score.map { score =>
      mkKey("score") -> mkFloatVal(score)
    }
  }

  protected def fileContent[K, V](fileContent: FileContent,
                                  mkKey: String => K,
                                  mkVal: String => V,
                                  mkLongVal: Long => V,
                                  mkByteArrayVal: Array[Byte] => V): Seq[(K, V)] = fileContent match {
    case c @ FileContent(data, mime, dl, _) => {
      val xs = Seq(mkKey("mimeType") -> mkVal(mime), mkKey("length") -> mkLongVal(data.fold(dl)(_.length)))
      data match {
        case Some(data) if isTextual(mime) => xs :+ (mkKey("data") -> mkVal(c.asString))
        case Some(data)                    => xs :+ (mkKey("base64-data") -> mkByteArrayVal(data))
        case None                          => xs
      }
    }
  }

  protected def pagination[K, V](pagination: PaginationInfo,
                                 mkKey: String => K,
                                 mkVal: String => V,
                                 mkRefVal: String => V): Seq[(K, V)] =
    Seq(
      Some("type" -> mkVal("PaginationInfo")),
      Some("first" -> mkRefVal(pagination.first)),
      pagination.previous.map(x => "previous" -> mkRefVal(x)),
      Some("self" -> mkRefVal(pagination.self)),
      pagination.next.map(x => "next" -> mkRefVal(x)),
      Some("last" -> mkRefVal(pagination.last))
    ).collect {
      case Some((k, v)) => mkKey(k) -> v
    }

  protected def simpleResponse[K, V](simpleResponse: SimpleResponse,
                                     mkKey: String => K,
                                     mkVal: String => V,
                                     mkBoolVal: Boolean => V): Seq[(K, V)] =
    Seq(
      Some("type" -> mkVal("SimpleResponse")),
      Some("success" -> mkBoolVal(simpleResponse.success)),
      simpleResponse.msg.map(msg => "message" -> mkVal(msg))
    ).collect {
      case Some((k, v)) => mkKey(k) -> v
    }

//  //attempting to reuse code at a higher level of abstraction (currently, achieved only for flat types)
//  protected def infoton[K,V](i: Infoton,
//                             mkKey: String => K,
//                             mkVal: String => V,
//                             mkLongVal: Long => V,
//                             mkIntVal: Int => V,
//                             mkRefVal: String => V,
//                             compose: Seq[(K,V)] => V): Seq[(K,V)] = {
//    val empty = Seq.empty[(K,V)]
//    (mkKey("type") -> mkVal(i.kind)) +: (i match {
//      case CompoundInfoton(_, _, _, _, offset, length, total) => Seq(
//          mkKey("offset") ->  mkLongVal(offset),
//          mkKey("length") -> mkLongVal(length),
//          mkKey("total") -> mkLongVal(total)
//        )
//      case ObjectInfoton(_, _, _) => empty
//      case FileInfoton(_, _, _, _) => empty
//      case LinkInfoton(_, _, _, linkTo, linkType) => Seq(
//          mkKey("linkTo") -> mkRefVal(linkTo),
//          mkKey("linkType") -> mkIntVal(linkType)
//        )
//      case d: DeletedInfoton => empty
//    })
//  }

  def cleanDuplicatesPreserveOrder[T](values: Seq[T]): Seq[T] =
    values.foldLeft((Seq.empty[T] -> Set.empty[T])) {
      case (tup @ (seq, set), v) if set(v) => tup
      case ((seq, set), v)                 => (seq :+ v) -> set
    }._1
}

abstract class SimpleFormater extends Formatter {
  protected def thinResultToString(r: SearchThinResult): String
  protected def infotonToString(i: Infoton): String
  override def render(formattable: Formattable): String = formattable match {
    case i: Infoton                  => infotonToString(i)
    case str: SearchThinResult       => thinResultToString(str)
    case bag: BagOfInfotons          => bag.infotons.map(infotonToString).mkString("", "\n", "\n")
    case ihv: InfotonHistoryVersions => ihv.versions.map(infotonToString).mkString("", "\n", "\n")
    case rp: RetrievablePaths        => rp.infotons.map(infotonToString).mkString("", "\n", "\n")
    case sr: SearchResults           => sr.infotons.map(infotonToString).mkString("", "\n", "\n")
    case sr: SearchResponse          => sr.results.infotons.map(infotonToString).mkString("", "\n", "\n")
    case ir: IterationResults        => ir.infotons.map(_.map(infotonToString).mkString("", "\n", "\n")).getOrElse("")
    case _ =>
      throw new NotImplementedError(
        s"PathFormatter.render implementation not found for ${formattable.getClass.toString}"
      )
  }
}

object TsvFormatter extends SimpleFormater {
  override def format: FormatType = TsvType
  override protected def thinResultToString(r: SearchThinResult): String =
    s"${r.path}\t${r.lastModified}\t${r.uuid}\t${r.indexTime}" + r.score.fold("")(score => s"\t$score")
  override protected def infotonToString(i: Infoton): String =
    s"${i.systemFields.path}\t${cmwell.util.string.dateStringify(i.systemFields.lastModified)}\t${i.uuid}\t${i.systemFields.indexTime.fold("")(
      _.toString)}${i.fields
      .flatMap(_.get("$score").flatMap(_.headOption.map("\t" + _.value.toString)))
      .getOrElse("")}"
}

object PathFormatter extends SimpleFormater {
  override def format: FormatType = TextType
  override protected def thinResultToString(r: SearchThinResult): String = r.path
  override protected def infotonToString(i: Infoton): String = i.systemFields.path
}

trait TreeLikeFormatter extends Formatter {

  type Inner

  val fieldNameModifier: String => String
  def makeFromTuples(tuples: Seq[(String, Inner)]): Inner
  def makeFromValues(values: Seq[Inner]): Inner
  def single[T](value: T): Inner
  def singleFieldValue(fv: FieldValue): Inner = fv match {
    case FString(value: String, _, _)                => single(value)
    case FInt(value: Int, _)                         => single(value)
    case FLong(value: Long, _)                       => single(value)
    case FBigInt(value: java.math.BigInteger, _)     => single(value)
    case FFloat(value: Float, _)                     => single(value)
    case FDouble(value: Double, _)                   => single(value)
    case FBigDecimal(value: java.math.BigDecimal, _) => single(value)
    case FBoolean(value: Boolean, _)                 => single(value)
    case FDate(value: String, _)                     => single(value)
    case FReference(value: String, _) =>
      single({ if (value.startsWith("cmwell://")) value.drop("cmwell:/".length) else value })
    case FExternal(value: String, dataTypeURI: String, _) => single(value)
    case FExtra(v, _)                                     => single(v)
    case FNull(_)                                         => ??? //this is just a marker for IMP, should not index it anywhere...
  }
  def empty: Inner
  protected def mkString(value: Inner): String
  override def render(formattable: Formattable): String = mkString(formattable2Inner(formattable))

  def formattable2Inner(formattable: Formattable): Inner = formattable match {
    case r: SearchThinResult                       => makeFromTuples(thinResult(r, identity, single, single, single))
    case i: Infoton                                => infoton(i)
    case bag: BagOfInfotons                        => bagOfInfotons(bag)
    case ihv: InfotonHistoryVersions               => infotonHistoryVersions(ihv)
    case rp: RetrievablePaths                      => retrievablePaths(rp)
    case pi: PaginationInfo                        => pagination(pi)
    case sr: SearchResults                         => searchResults(sr)
    case sr: SearchResponse                        => searchResponse(sr)
    case ir: IterationResults                      => iterationResults(ir)
    case sr: SimpleResponse                        => simpleResponse(sr)
    case ar: AggregationsResponse                  => aggregationsResponse(ar)
    case tar: TermsAggregationResponse             => termsAggregationsResponse(tar)
    case sar: StatsAggregationResponse             => statsAggregationsResponse(sar)
    case har: HistogramAggregationResponse         => histogramAggregationResponse(har)
    case star: SignificantTermsAggregationResponse => significantTermsAggregationResponse(star)
    case car: CardinalityAggregationResponse       => cardinalityAggregationResponse(car)
    case bar: BucketsAggregationResponse           => bucketsAggregationResponse(bar)
    case ar: AggregationResponse                   => aggregationResponse(ar)
    case taf: TermAggregationFilter                => termAggregationFilter(taf)
    case saf: StatsAggregationFilter               => statsAggregationFilter(saf)
    case haf: HistogramAggregationFilter           => histogramAggregationFilter(haf)
    case staf: SignificantTermsAggregationFilter   => significantTermsAggregationFilter(staf)
    case caf: CardinalityAggregationFilter         => cardinalityAggregationFilter(caf)
    case baf: BucketAggregationFilter              => bucketAggregationFilter(baf)
    case af: AggregationFilter                     => aggregationFilter(af)
    case sb: SignificantTermsBucket                => significantTermsBucket(sb)
    case b: Bucket                                 => bucket(b)
    case _ =>
      throw new NotImplementedError(s"formattable2Inner implementation not found for ${formattable.getClass.toString}")
  }

  def system(i: Infoton): Inner = makeFromTuples(system(i, identity, single, single, single))

  def fields(i: Infoton): Inner = i.fields match {
    case None => empty
    case Some(xs) =>
      makeFromTuples(xs.view.collect {
        case (fieldName, set) if fieldName.head != '$' =>
          fieldNameModifier(fieldName) -> makeFromValues(set.toSeq.map(singleFieldValue))
      }.to(Seq))
  }

  def extra(i: Infoton): Seq[(String, Inner)] = i.fields match {
    case Some(xs) if xs.exists(_._1.head == '$') =>
      Seq("extra" -> makeFromTuples(xs.view.collect {
        case (k, vs) if k.head == '$' => {
          fieldNameModifier(k.tail) -> makeFromValues(vs.view.collect {
            case FExtra(v, _) => single(v)
          }.to(Seq))
        }
      }.to(Seq)))
    case _ => Nil
  }

  def fileContent(c: FileContent): Inner = makeFromTuples(super.fileContent(c, identity, single, single, single))

  def infotons(is: Seq[Infoton], withDeleted: Boolean = false) = makeFromValues(is.map(infoton))

  def infoton(i: Infoton): Inner = {
    val iSystem: Inner = system(i)
    val iFields: Seq[(String, Inner)] = i.fields
      .flatMap {
        case m if m.forall(_._1.head == '$') => None
        case m                               => Some(m)
      }
      .fold(Seq.empty[(String, Inner)])(_ => Seq("fields" -> fields(i)))
    val iExtra: Seq[(String, Inner)] = extra(i)
    (i: @unchecked) match {
      case CompoundInfoton(_, _, children, offset, length, total) =>
        makeFromTuples(
          Seq(
            "type" -> single(i.kind),
            "system" -> iSystem,
            "children" -> infotons(children),
            "offset" -> single(offset),
            "length" -> single(length),
            "total" -> single(total)
          ) ++ iExtra ++ iFields
        )
      case ObjectInfoton(_, _) =>
        makeFromTuples(
          Seq(
            "type" -> single(i.kind),
            "system" -> iSystem
          ) ++ iExtra ++ iFields
        )
      case FileInfoton(_, _, Some(content)) =>
        makeFromTuples(
          Seq(
            "type" -> single(i.kind),
            "system" -> iSystem,
            "content" -> fileContent(content)
          ) ++ iExtra ++ iFields
        )
      case FileInfoton(_, None, _) =>
        makeFromTuples(
          Seq(
            "type" -> single(i.kind),
            "system" -> iSystem
          ) ++ iExtra ++ iFields
        )
      case LinkInfoton(_, _, linkTo, linkType) =>
        makeFromTuples(
          Seq(
            "type" -> single(i.kind),
            "system" -> iSystem,
            "linkTo" -> single(linkTo),
            "linkType" -> single(linkType)
          ) ++ iExtra ++ iFields
        )
      case d: DeletedInfoton =>
        makeFromTuples(
          Seq(
            "type" -> single(i.kind),
            "system" -> iSystem
          )
        )
    }
  }

  def bagOfInfotons(bag: BagOfInfotons, withDeleted: Boolean = false) =
    makeFromTuples(
      Seq(
        "type" -> single("BagOfInfotons"),
        "infotons" -> infotons(bag.infotons, withDeleted)
      )
    )

  def infotonHistoryVersions(ihv: InfotonHistoryVersions, withDeleted: Boolean = true) =
    makeFromTuples(
      Seq(
        "type" -> single("InfotonHistoryVersions"),
        "versions" -> infotons(ihv.versions, withDeleted)
      )
    )

  def retrievablePaths(rp: RetrievablePaths, withDeleted: Boolean = false) =
    makeFromTuples(
      Seq(
        "type" -> single("RetrievablePaths"),
        "infotons" -> infotons(rp.infotons, withDeleted),
        "irretrievablePaths" -> makeFromValues(rp.irretrievablePaths.map(single))
      )
    )
  def pagination(pagination: PaginationInfo) = makeFromTuples(super.pagination(pagination, identity, single, single))
  def searchResults(searchResults: SearchResults, withDeleted: Boolean = false) = {
    val searchQueryStr =
      if (searchResults.debugInfo.isDefined)
        Seq(Some("searchQueryStr" -> single(searchResults.debugInfo.get)))
      else
        Nil

    makeFromTuples(
      (Seq(
        Some("type" -> single("SearchResults")),
        searchResults.fromDate.map(d => "fromDate" -> single(d)),
        searchResults.toDate.map(d => "toDate" -> single(d)),
        Some("total" -> single(searchResults.total)),
        Some("offset" -> single(searchResults.offset)),
        Some("length" -> single(searchResults.length)),
        Some("infotons" -> infotons(searchResults.infotons, withDeleted))
      ) ++ searchQueryStr).collect { case Some(t) => t }
    )
  }

  def searchResponse(SearchResponse: SearchResponse, withDeleted: Boolean = false) =
    makeFromTuples(
      Seq(
        "type" -> single("SearchResponse"),
        "pagination" -> pagination(SearchResponse.pagination),
        "results" -> searchResults(SearchResponse.results)
      )
    )
  def iterationResults(iterationResults: IterationResults, withDeleted: Boolean = false) = {
    val s = Seq(
      "type" -> single("IterationResults"),
      "iteratorId" -> single(iterationResults.iteratorId),
      "totalHits" -> single(iterationResults.totalHits),
      "infotons" -> infotons(iterationResults.infotons.getOrElse(Seq.empty[Infoton]), withDeleted)
    )
    makeFromTuples(iterationResults.debugInfo.fold(s)(d => s :+ ("searchQueryStr", single(d))))
  }
  def simpleResponse(sr: SimpleResponse) = makeFromTuples(super.simpleResponse(sr, identity, single, single))

  def aggregationsResponse(ar: AggregationsResponse) =
    makeFromTuples(
      Seq("AggregationResponse" -> makeFromValues(ar.responses.map(formattable2Inner)))
        ++ ar.debugInfo.map("searchQueryStr" -> single(_))
    )

  def termsAggregationsResponse(tar: TermsAggregationResponse) =
    makeFromTuples(
      Seq(
        "name" -> single(tar.name),
        "type" -> single(tar.`type`),
        "filter" -> formattable2Inner(tar.filter),
        "buckets" -> makeFromValues(tar.buckets.map(formattable2Inner))
      )
    )

  def statsAggregationsResponse(sar: StatsAggregationResponse) =
    makeFromTuples(
      Seq(
        "name" -> single(sar.name),
        "type" -> single(sar.`type`),
        "filter" -> formattable2Inner(sar.filter),
        "count" -> single(sar.count),
        "min" -> single(sar.min),
        "max" -> single(sar.max),
        "avg" -> single(sar.avg),
        "sum" -> single(sar.sum)
      )
    )

  def histogramAggregationResponse(har: HistogramAggregationResponse) =
    makeFromTuples(
      Seq(
        "name" -> single(har.name),
        "type" -> single(har.`type`),
        "filter" -> formattable2Inner(har.filter),
        "buckets" -> makeFromValues(har.buckets.map(formattable2Inner))
      )
    )

  def significantTermsAggregationResponse(star: SignificantTermsAggregationResponse) =
    makeFromTuples(
      Seq(
        "name" -> single(star.name),
        "type" -> single(star.`type`),
        "filter" -> formattable2Inner(star.filter),
        "objects" -> single(star.docCount),
        "buckets" -> makeFromValues(star.buckets.map(formattable2Inner))
      )
    )

  def cardinalityAggregationResponse(car: CardinalityAggregationResponse) =
    makeFromTuples(
      Seq(
        "name" -> single(car.name),
        "type" -> single(car.`type`),
        "filter" -> formattable2Inner(car.filter),
        "count" -> single(car.count)
      )
    )

  def bucketsAggregationResponse(bar: BucketsAggregationResponse) =
    makeFromTuples(
      Seq(
        "name" -> single(bar.name),
        "type" -> single(bar.`type`),
        "filter" -> formattable2Inner(bar.filter),
        "buckets" -> makeFromValues(bar.buckets.map(formattable2Inner))
      )
    )

  def aggregationResponse(ar: AggregationResponse) =
    makeFromTuples(
      Seq(
        "name" -> single(ar.name),
        "type" -> single(ar.`type`),
        "filter" -> formattable2Inner(ar.filter)
      )
    )

  def aggregationFilter(af: AggregationFilter) =
    makeFromTuples(
      Seq(
        "name" -> single(af.name),
        "type" -> single(af.`type`)
      )
    )

  def bucketAggregationFilter(baf: BucketAggregationFilter) =
    makeFromTuples(
      Seq(
        "name" -> single(baf.name),
        "type" -> single(baf.`type`),
        "subFilters" -> makeFromValues(baf.subFilters.map(formattable2Inner))
      )
    )

  def statsAggregationFilter(saf: StatsAggregationFilter) =
    makeFromTuples(
      Seq(
        "name" -> single(saf.name),
        "type" -> single(saf.`type`),
        "field" -> single(saf.field.value)
      )
    )

  def termAggregationFilter(taf: TermAggregationFilter) =
    makeFromTuples(
      Seq(
        "name" -> single(taf.name),
        "type" -> single(taf.`type`),
        "field" -> single(taf.field.value),
        "size" -> single(taf.size),
        "subFilters" -> makeFromValues(taf.subFilters.map(formattable2Inner))
      )
    )

  def histogramAggregationFilter(haf: HistogramAggregationFilter) =
    makeFromTuples(
      Seq(
        "name" -> single(haf.name),
        "type" -> single(haf.`type`),
        "field" -> single(haf.field.value),
        "interval" -> single(haf.interval),
        "minimum doc count" -> single(haf.minDocCount)
      )
        ++ haf.extMin.map("extMin" -> single(_))
        ++ haf.extMax.map("extMax" -> single(_))
        ++ Seq("subFilters" -> makeFromValues(haf.subFilters.map(formattable2Inner)))
    )

  def significantTermsAggregationFilter(staf: SignificantTermsAggregationFilter) =
    makeFromTuples(
      Seq(
        "name" -> single(staf.name),
        "type" -> single(staf.`type`),
        "field" -> single(staf.field.value),
        "minimum doc count" -> single(staf.minDocCount),
        "size" -> single(staf.size)
      )
        ++ staf.backgroundTerm.map("background term" -> single(_))
        ++ Seq("subFilters" -> makeFromValues(staf.subFilters.map(formattable2Inner)))
    )

  def cardinalityAggregationFilter(caf: CardinalityAggregationFilter) =
    makeFromTuples(
      Seq("name" -> single(caf.name), "type" -> single(caf.`type`), "field" -> single(caf.field.value))
        ++ caf.precisionThreshold.map("precision threshold" -> single(_))
    )

  def bucket(b: Bucket) =
    makeFromTuples(
      Seq("key" -> single(b.key.value), "objects" -> single(b.docCount))
        ++ b.subAggregations.map("subAggregations" -> formattable2Inner(_))
    )

  def significantTermsBucket(b: SignificantTermsBucket) =
    makeFromTuples(
      Seq("key" -> single(b.key.value),
          "objects" -> single(b.docCount),
          "score" -> single(b.score),
          "bgCount" -> single(b.bgCount))
        ++ b.subAggregations.map("subAggregations" -> formattable2Inner(_))
    )
}
