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

import java.util.Date

import cmwell.domain._
import cmwell.fts.{FieldFilter, FieldOperator, MultiFieldFilter, SingleFieldFilter}
import cmwell.util.string.Hash.crc32
import cmwell.util.string.dateStringify
import org.apache.abdera._
import org.apache.abdera.ext.opensearch.OpenSearchConstants
import org.apache.abdera.model.{Entry, Feed}
import org.apache.abdera.parser.stax.FOMExtensibleElement

import scala.language.postfixOps

/**
  * Created by gilad on 1/13/15.
  */
class AtomFormatter private (reqHost: String,
                             uri: String,
                             withDataFormatter: Option[Formatter] = None,
                             fieldsFilter: Option[FieldFilter] = None,
                             offset: Option[Long] = None,
                             length: Option[Long] = None)
    extends Formatter {
  //make sure no one is doing something stupid:
  require(withDataFormatter.isEmpty || withDataFormatter.get.getClass != classOf[AtomFormatter],
          "Atom format with Atom content?!?!?")

  //if the host looks like: localhost:9000, then the links in atom format aren't valid, and though clickable, cannot be understood by the browser.
  private val host =
    if (reqHost.startsWith("http")) reqHost
    else s"http://$reqHost"

  val xsltRef =
    """<?xml version="1.0" encoding="UTF-8"?><?xml-stylesheet type="text/xsl" href="/meta/app/old-ui/style/atom.xsl"?>"""

  private[this] val fillInfotonEntry: (Entry, Infoton) => Entry = withDataFormatter match {
    case Some(formatter) => { (entry, i) =>
      entry.setTitle(i.systemFields.path)
      val content = entry.setContent(formatter.render(i))
      content.setMimeType(formatter.mimetype)
      entry.setId(s"$host${i.systemFields.path}")
      entry.setUpdated(i.systemFields.lastModified.toDate)
      entry
    }
    case None => { (entry, i) =>
      entry.setTitle(i.systemFields.path)
      entry.addLink(s"$host${i.systemFields.path}", "alternate").setMimeType("text/html")
      entry.addLink(s"$host/ii/${i.uuid}", "alternate").setMimeType("text/html")
//      entry.addLink(s"/ii/${i.uuid}/format=json", "alternate").setMimeType("application/json")
//      entry.addLink(s"/ii/${i.uuid}/format=jsonld", "alternate").setMimeType("application/ld+json")
//      entry.addLink(s"/ii/${i.uuid}/format=n3", "alternate").setMimeType("text/rdf+n3")
//      entry.addLink(s"/ii/${i.uuid}/format=ntriples", "alternate").setMimeType("text/plain")
//      entry.addLink(s"/ii/${i.uuid}/format=nquads", "alternate").setMimeType("text/n-quads")
//      entry.addLink(s"/ii/${i.uuid}/format=turtle", "alternate").setMimeType("text/turtle")
//      entry.addLink(s"/ii/${i.uuid}/format=rdfxml", "alternate").setMimeType("application/rdf+xml")
//      entry.addLink(s"/ii/${i.uuid}/format=yaml", "alternate").setMimeType("text/yaml")
      entry.setId(i.systemFields.path)
      entry.setUpdated(i.systemFields.lastModified.toDate)
      entry
    }
  }

  override def mimetype: String = "application/xml"
  override def format = AtomType

  override def render(formattable: Formattable): String = {
    val feed = formattableToAtom(formattable)(AtomFormatter.getFeed(host))

    formattable match {
      case f: IterationResults => feed.toString
      case _                   => xsltRef + feed.toString
    }
  }

  def formattableToAtom(formattable: Formattable)(implicit feed: Feed): Feed = formattable match {
    case bag: BagOfInfotons          => bagOfInfotons(bag)
    case ihv: InfotonHistoryVersions => infotonHistoryVersions(ihv)
    case rp: RetrievablePaths        => retrievablePaths(rp)
    case sr: SearchResponse          => searchResponse(sr)
    case ir: IterationResults        => iterationResults(ir)
    case _ =>
      throw new NotImplementedError(s"formatting implementation not found for ${formattable.getClass.toString} as Atom")
  }

  private[this] def infotonsAsEntries(feed: Feed, infotons: Seq[Infoton]): Feed = {
    infotons.foreach(fillInfotonEntry(feed.addEntry(), _))
    feed
  }

  private def bagOfInfotons(bag: BagOfInfotons)(implicit feed: Feed): Feed = {
    feed.setTitle("Collection of Infotons")
    feed.setSubtitle(s"collection of size: ${bag.infotons.size}")
    feed.setId(uri)
    infotonsAsEntries(feed, bag.infotons)
    feed
  }
  private def infotonHistoryVersions(ihv: InfotonHistoryVersions)(implicit feed: Feed): Feed = {
    feed.setTitle("Infoton's History")
    feed.setSubtitle(
      s"Showing ${ihv.versions.headOption.map(_.systemFields.path).getOrElse("N/A")} history: ${ihv.versions.size} historical results found."
    )
    feed.setId(uri)
    val x: FOMExtensibleElement = feed.addExtension(OpenSearchConstants.OPENSEARCH_NS,
                                                    OpenSearchConstants.QUERY_TOTALRESULTS_LN,
                                                    OpenSearchConstants.OS_PREFIX)
    x.setText(ihv.versions.size.toString)
    ihv.versions.foreach { i =>
      fillInfotonEntry(feed.addEntry(), i).setId(s"${i.systemFields.path}#${i.uuid}")
    }
    feed
  }
  private def retrievablePaths(rp: RetrievablePaths)(implicit feed: Feed): Feed = rp match {
    case RetrievablePaths(infotons, na) => {
      feed.setTitle("Retrievable Paths")
      feed.setSubtitle(s"managed to find ${infotons.size} infotons, out of ${infotons.size + na.size} requested paths.")
      //since this is a response of a post request, uri (_out) is not a good id, so we need to add something unique
      val allPaths = infotons.map(i => s"/ii/${i.uuid}") ++ na
      val hash = crc32(allPaths.sorted.mkString("\n"))
      feed.setId(s"$uri#$hash")
      infotonsAsEntries(feed, infotons)
      val e = feed.addEntry()
      e.setTitle("irretrievablePaths")
      e.setId("irretrievablePaths")
      e.setUpdated(feed.getUpdated)
      na.foreach(p => e.setContent(na.mkString("\n")))
      feed
    }
  }
  private def searchResponse(sr: SearchResponse)(implicit feed: Feed): Feed = {

    def fieldsFiltersToString(fieldsFilter: FieldFilter) = {
      val sb = new StringBuilder()
      sb.append(", Predicates: ")
      def appendFilter(filter: FieldFilter): Unit = filter match {
        case SingleFieldFilter(fieldOperator, valueOperator, name, valueOpt) =>
          sb.append(s"$fieldOperator + ( $name $valueOperator ${valueOpt.getOrElse("")} )")
        case MultiFieldFilter(fieldOperator, filters) =>
          sb.append(s"$fieldOperator { ${filters.foreach(appendFilter)}")
      }
      sb.append(". ").mkString
    }

    (sr, offset, length) match {
      case (SearchResponse(pi, searchResult), Some(o), Some(l)) => {
        feed.setTitle("Infotons search results")
        val criteriaStr = fieldsFilter.fold("")(fieldsFiltersToString)
        val subtitle = searchResult.fromDate.map(d => s"From date (UTC): ${dateStringify(d)}, ").getOrElse("") +
          searchResult.toDate.map(d => s"To date (UTC): ${dateStringify(d)}").getOrElse("") +
          s"${criteriaStr}Window: Offset = $o, Length = $l, out of ${searchResult.total} results."
        feed.setSubtitle(subtitle)
        feed.setId(java.net.URLEncoder.encode(uri, "UTF-8"))
        val x: FOMExtensibleElement = feed.addExtension(OpenSearchConstants.OPENSEARCH_NS,
                                                        OpenSearchConstants.QUERY_TOTALRESULTS_LN,
                                                        OpenSearchConstants.OS_PREFIX)
        x.setText(searchResult.total.toString)
        pagination[String, Option[String]](pi, identity, _ => None, Some.apply)
          .collect {
            case (key, Some(value)) => key -> value
          }
          .foreach {
            case (k, v) => feed.addLink(v, k).setMimeType(mimetype)
          }
        infotonsAsEntries(feed, searchResult.infotons)
        feed
      }
      case _ => throw new IllegalArgumentException("missing data for atom formatting")
    }
  }
  private def iterationResults(ir: IterationResults)(implicit feed: Feed): Feed = ir match {
    case IterationResults(id, total, infotons, _, _) => {
      feed.setTitle("Infotons iteration results")
      feed.setSubtitle(s"current chunk contains: ${infotons.size} infotons, out of ${total} results.")
      feed.setId(s"${id}_${feed.getUpdated.getTime}")
      val x: FOMExtensibleElement = feed.addExtension(OpenSearchConstants.OPENSEARCH_NS,
                                                      OpenSearchConstants.QUERY_TOTALRESULTS_LN,
                                                      OpenSearchConstants.OS_PREFIX)
      x.setText(total.toString)
      infotons.map(infotonsAsEntries(feed, _)).getOrElse(feed)
    }
  }
}

object AtomFormatter {
  private[this] val abdera: Abdera = new Abdera

  private def getFeed(host: String): Feed = {
    val feed: Feed = abdera.newFeed
    feed.setUpdated(new Date)
    feed.addAuthor("CM-Well").setUri(host)
    feed
  }

  def apply(host: String, uri: String): AtomFormatter = new AtomFormatter(host, uri, None, None, None, None)
  def apply(host: String, uri: String, withDataFormatter: Option[Formatter]): AtomFormatter =
    new AtomFormatter(host, uri, withDataFormatter, None, None, None)
//  def apply(host: String, uri: String, fieldFilters: List[FieldFilter], offset: Long, length: Long): AtomFormatter =
//    new AtomFormatter(host,uri,None,Some(fieldFilters),Some(offset),Some(length))
  def apply(host: String,
            uri: String,
            fieldsFilter: Option[FieldFilter],
            offset: Long,
            length: Long,
            withDataFormatter: Option[Formatter]): AtomFormatter =
    new AtomFormatter(host, uri, withDataFormatter, fieldsFilter, Some(offset), Some(length))
}
