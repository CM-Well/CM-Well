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


package cmwell.fts

import java.util.concurrent.Executors

import cmwell.common.formats.JsonSerializerForES
import cmwell.domain._
import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.scalatest._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import domain.testUtil.InfotonGenerator.genericSystemFields

/**
 * User: Israel
 * Date: 11/18/12
 * Time: 6:15 PM
 */

// !!!!!!!!!! REMOVE INFOCLONE ANYWHERE IN THIS FILE. TYPE IS DEPRECATED !!!!!!!!!!!

//sealed trait FTSMixin extends BeforeAndAfterAll { this: Suite =>
//  def ftsService: FTSServiceOps
//  def refreshAll(): Unit
//  def getUUID(uuid: String, isCurrent: Boolean = true) = ftsService match {
//    case es: FTSService => es.client.prepareGet("cm_well_p0_0","infoclone", uuid).execute().actionGet()
//    case es: FTSServiceES => {
//      val index = if(isCurrent) "cmwell_current" else "cmwell_history"
//      es.client.prepareGet(index, "infoclone", uuid).execute().actionGet()
//    }
//  }
//  val isoDateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
//  implicit val executionContext =  scala.concurrent.ExecutionContext.global
//  implicit val logger = Logger(LoggerFactory.getLogger(getClass.getName))
//}

//trait FTSServiceESTest extends FTSMixin { this: Suite =>
//  var ftsServiceES:FTSServiceES = _
//
//  override def ftsService: FTSServiceOps = ftsServiceES
//
//  override protected def beforeAll() {
//    ftsServiceES = FTSServiceES.getOne("FTSServiceESTest.yml")
//
//    // wait for green status
//    ftsServiceES.client.admin().cluster()
//      .prepareHealth()
//      .setWaitForGreenStatus()
//      .setTimeout(TimeValue.timeValueMinutes(5))
//      .execute()
//      .actionGet()
//
//
//    // delete all existing indices
//    ftsServiceES.client.admin().indices().delete(new DeleteIndexRequest("_all"))
//
//    // load indices template
//    val indicesTemplate = Source.fromURL(this.getClass.getResource("/indices_template.json")).getLines.reduceLeft(_ + _)
//    ftsServiceES.client.admin().indices().preparePutTemplate("indices_template").setSource(indicesTemplate).execute().actionGet()
//
//    // override with test-only settings
//    val testOnlyTemplate = Source.fromURL(this.getClass.getResource("/test_indices_template_override.json")).getLines.reduceLeft(_ + _)
//    ftsServiceES.client.admin().indices().preparePutTemplate("test_indices_template").setSource(testOnlyTemplate).execute().actionGet()
//
//    // create current index
//    ftsServiceES.client.admin().indices().prepareCreate("cmwell_current").execute().actionGet()
//
//    // create history index
//    ftsServiceES.client.admin().indices().prepareCreate("cmwell_history").execute().actionGet()
//
//    super.beforeAll()
//  }
//
//  override protected def afterAll() {
//    ftsServiceES.close()
//    super.afterAll()
//    Thread.sleep(10000)
//    logger debug s"FTSSpec is over"
//  }
//
//  override def refreshAll() = ftsServiceES.client.admin().indices().prepareRefresh("*").execute().actionGet()
//}

trait FTSServiceTestTrait extends BeforeAndAfterAll with LazyLogging{ this: Suite =>
  var ftsService:FTSService = _
//  var embeddedElastic:EmbeddedElastic = _
  implicit val executionContext =  ExecutionContext.fromExecutor(Executors.newWorkStealingPool(10))
  implicit val loger = logger

  override protected def beforeAll() {

//    val indicesTemplate = Source.fromURL(this.getClass.getResource("/indices_template.json")).getLines.reduceLeft(_ + _)
//    val testIndicesTemplate = Source.fromURL(this.getClass.getResource("/test_indices_template_override.json")).getLines.reduceLeft(_ + _)
//
//    embeddedElastic = EmbeddedElastic.builder()
//      .withElasticVersion(BuildInfo.elasticsearchVersion)
//      .withSetting(PopularProperties.CLUSTER_NAME, "fts_test_cluster")
//      .withTemplate("indices_template", indicesTemplate)
//      .withTemplate("test_indices_template", testIndicesTemplate)
//      .withIndex(testIndexName)
//      .build()
//      .start()

//    embeddedElastic.recreateIndices()

    ftsService = FTSService()

    super.beforeAll()
  }

  override protected def afterAll() {
    ftsService.shutdown()
    super.afterAll()
  }

  def refreshAll() = ftsService.client.admin().indices().prepareRefresh().execute().actionGet()

  val testIndexName = "cm_well_p0_0"
  def getUUID(uuid:String) = ftsService
    .client
    .prepareGet()
    .setIndex(testIndexName)
    .setId(uuid)
    .execute()
    .actionGet()
}

class FTSServiceEsSpec extends FlatSpec with Matchers with FTSServiceTestTrait with LazyLogging{

  System.setProperty("dataCenter.id" , "dc_test")
  val isoDateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
  val timeout =  FiniteDuration(10, SECONDS)
  // scalastyle:off
//
//  val m : Map[String, Set[FieldValue]] = Map("name" -> Set(FString("yehuda"), FString("moshe")))
//  val infotonToIndex = ObjectInfoton("/fts-test/objinfo1/a/b/c","dc_test", Some(System.currentTimeMillis()), m)
//  "indexing new infoton" should "store it in current index" in {
//    Await.result(ftsService.executeIndexRequests(Iterable(ESIndexRequest(
//      Requests.indexRequest(testIndexName).`type`("infoclone").id(infotonToIndex.uuid).create(true)
//        .source(JsonSerializerForES.encodeInfoton(infotonToIndex), XContentType.JSON),
//      None
//    )) ), timeout)
//    refreshAll()
//    val result = getUUID(infotonToIndex.uuid)
//    // result: ftsServiceES.search(Some(PathFilter("/fts-test/objinfo1/a/b", false)),FieldFilter(Must, Contains, "name", "moshe") :: Nil, None, DefaultPaginationParams,false, "cmwell")
//    result.isExists should equal (true)
//  }

//  "indexing existing infoton" should "store it in current index" in {
//    val lastModified = new DateTime()
//    val m : Map[String, Set[FieldValue]] = Map("name" -> Set(FString("yehuda"), FString("moshe")), "family" -> Set(FString("smith")))
//    val updatedInfotonToIndex = ObjectInfoton("/fts-test/objinfo1/a/b/c", "dc_test", Some(System.currentTimeMillis()),lastModified, m)
//    Await.result(ftsService.executeIndexRequests(Iterable(ESIndexRequest(
//      Requests.indexRequest(testIndexName).`type`("infoclone").id(updatedInfotonToIndex.uuid).create(true)
//        .source(JsonSerializerForES.encodeInfoton(updatedInfotonToIndex), XContentType.JSON),
//      None
//    )) ), timeout)
//    refreshAll()
//    val result = Await.result(ftsService.search(None, Some(FieldFilter(Must, Equals, "system.path","/fts-test/objinfo1/a/b/c")), None, DefaultPaginationParams), timeout)
//    result.infotons.size should equal (1)
//    result.infotons(0).lastModified should equal (lastModified)
//  }
//
//  it should "store its previous version in the history index" in {
//    val result = Await.result(ftsService.search(None, Some(FieldFilter(Must, Equals, "system.path","/fts-test/objinfo1/a/b/c")), None,DefaultPaginationParams, SortParam.empty, true), timeout)
//    result.infotons.size should equal (2)
//    val res = getUUID(infotonToIndex.uuid)
//    withClue(s"${res.getIndex}, ${res.getSource}, ${res.getType}, ${res.getVersion}, ${res.isSourceEmpty}") {
//      res.isExists should equal(true)
//    }
//  }

  val dateTimeAsMillies = (new DateTime(DateTimeZone.UTC)).getMillis
  val bulkInfotons = Seq.tabulate(500){ i =>
    val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/fts-test/bulk1/info$i", indexTime = Some(dateTimeAsMillies)),
      Map("name" + i -> Set[FieldValue](FString("value" + i), FString("another value" + i))))
    ESIndexRequest(
      Requests.indexRequest(testIndexName).id(infoton.uuid).create(true)
        .source(JsonSerializerForES.encodeInfoton(infoton), XContentType.JSON),
      None
    )
  }

//  "bulk indexing infotons" should "store them in current index" in {
//
//    Await.result(ftsService.executeIndexRequests(bulkInfotons, true), 20.seconds)
////    embeddedElastic.refreshIndices()
//    Thread.sleep(15000)
//    val result = Await.result(ftsService.search(Some(PathFilter("/fts-test/bulk1", true)),None,None,DefaultPaginationParams), timeout)
//    result.total should equal (500)
//  }

//  "bulk indexing existing infotons" should "store their previous version in history index and current version in current index" in {
//
//    val bulkUpdatedInfotons = Seq.tabulate(500){ i =>
//      val infoton = ObjectInfoton("/fts-test/bulk/info" + i,"dc_test", Some(System.currentTimeMillis()),
//        Map("name" + i -> Set[FieldValue](FString("moshe" + i), FString("shirat" + i))))
//      ESIndexRequest(
//        Requests.indexRequest(testIndexName).`type`("infoclone").id(infoton.uuid).create(true)
//          .source(JsonSerializerForES.encodeInfoton(infoton), XContentType.JSON),
//        None
//      )
//    }
//    Await.result(ftsService.executeIndexRequests(bulkUpdatedInfotons), timeout)
//    refreshAll()
//
//    Await.result(ftsService.search(Some(PathFilter("/fts-test/bulk", true)),None,None,DefaultPaginationParams), timeout).total should equal (500)
//    Await.result(ftsService.search(pathFilter = Some(PathFilter("/fts-test/bulk", true)),None,None,DefaultPaginationParams, withHistory = true), timeout).total should equal (1000)
//  }

//  "deleting infoton" should "remove it from current index" in {
//    val infotonToDelete = ObjectInfoton("/fts-test/infoToDel","dc_test", Some(System.currentTimeMillis()), Map("country" -> Set[FieldValue](FString("israel"), FString("spain"))))
//    Await.result(ftsService.executeIndexRequests(Iterable(ESIndexRequest(
//      Requests.indexRequest(testIndexName).`type`("infoclone").id(infotonToDelete.uuid).create(true)
//        .source(JsonSerializerForES.encodeInfoton(infotonToDelete), XContentType.JSON),
//      None
//    ))), timeout)
//    refreshAll()
//    val resultBeforeDelete = Await.result(ftsService.search(None, Some(FieldFilter(Must, Equals, "system.path","/fts-test/infoToDel")), None, DefaultPaginationParams), timeout)
//    resultBeforeDelete.total should equal (1)
//    val deletedInfoton = DeletedInfoton("/fts-test/infoToDel","dc_test",Some(System.currentTimeMillis()))
//    Await.result(ftsService.delete(infotonToDelete.uuid), timeout)
//    refreshAll()
//    val resultAfterDelete = Await.result(ftsService.search(None, Some(FieldFilter(Must, Equals, "system.path","/fts-test/infoToDel")), None, DefaultPaginationParams), timeout)
//    resultAfterDelete.total should equal (0)
//
//  }
//
//  it should "move it to history index and add tombstone" in {
//    val resultWithHistory = Await.result(ftsService.search(None, Some(FieldFilter(Must, Equals, "system.path","/fts-test/infoToDel")), None,DefaultPaginationParams, SortParam.empty, true), timeout)
//    resultWithHistory.total should  equal (2)
//    resultWithHistory.infotons.exists(_.isInstanceOf[DeletedInfoton])  should equal (true)
//  }

//  "purging infoton" should "permanently delete infoton with given UUID from history index" in {
//    val infotonToPurge = ObjectInfoton("/fts-test/infoToPurge","dc_test",Some(System.currentTimeMillis()))
//    Await.result(ftsService.index(infotonToPurge, None), timeout)
//    val updatedInfotonToPurge = ObjectInfoton("/fts-test/infoToPurge","dc_test",Some(System.currentTimeMillis()))
//    Await.result(ftsService.index(updatedInfotonToPurge, Some(infotonToPurge)), timeout)
//    refreshAll()
//    val result = Await.result(ftsService.search(None, Some(FieldFilter(Must, Equals, "system.uuid",infotonToPurge.uuid)), None, DefaultPaginationParams, SortParam.empty, true), timeout)
//    result.length should equal(1)
//    Await.result(ftsService.purge(infotonToPurge.uuid), timeout)
//    refreshAll()
//    Await.result(ftsService.search(None, Some(FieldFilter(Must, Equals, "system.uuid",infotonToPurge.uuid)), None, DefaultPaginationParams, SortParam.empty, true), timeout).length should equal(0)
//  }
//
//  "purgeAll infoton" should "permanently delete all infoton's versions with given path from all indices" in {
//    val infotonToPurgeAll = ObjectInfoton("/fts-test/infoToPurgeAll","dc_test")
//    Await.result(ftsService.index(infotonToPurgeAll, None), timeout)
//    val updatedInfotonToPurgeAll = ObjectInfoton("/fts-test/infoToPurgeAll","dc_test")
//    Await.result(ftsService.index(updatedInfotonToPurgeAll, Some(infotonToPurgeAll)), timeout)
//    refreshAll()
//    Await.result(ftsService.search(None, Some(FieldFilter(Must, Equals, "system.path", infotonToPurgeAll.path)), None, DefaultPaginationParams, SortParam.empty, true), timeout).length should equal (2)
//    Await.result(ftsService.purgeAll(infotonToPurgeAll.path,true,ftsService.defaultPartition), timeout)
//    refreshAll()
//    val f = ftsService.search(None, Some(FieldFilter(Must, Equals, "system.path", infotonToPurgeAll.path)), None, DefaultPaginationParams, SortParam.empty, true)
//    f.onSuccess{
//      case FTSSearchResponse(total, offset, length, infotons, None) =>
//        logger.debug(s"before failing: total: $total, offset: $offset, length: $length and infotons:\n${infotons.map(_.path).mkString("\t","\n\t","\n")} ")
//    }(scala.concurrent.ExecutionContext.Implicits.global)
//    Await.result(f, timeout).length should equal (0)
//  }
//
//  "listChildren" should "return a list of given infoton's current version children" in {
//    val infotonToList1 = ObjectInfoton("/fts-test/infotons/infotonToList1","dc_test",Some(System.currentTimeMillis()))
//    val infotonToList2 = ObjectInfoton("/fts-test/infotons/infotonToList2","dc_test", Some(System.currentTimeMillis()), Map("city" -> Set[FieldValue](FString("Or-Yehuda"), FString("Modiin"))))
//    val infotonToList3 = LinkInfoton(path= "/fts-test/infotons/infotonToList3" ,dc = "dc_test",linkTo = "/fts-test/infotons/infotonToList2", linkType = LinkType.Temporary).copy(indexTime = Some(System.currentTimeMillis()))
//    Await.result(ftsService.index(infotonToList1,None), timeout)
//    Await.result(ftsService.index(infotonToList2,None), timeout)
//    Await.result(ftsService.index(infotonToList3,None), timeout)
//    refreshAll()
//    Await.result(ftsService.listChildren("/fts-test/infotons",0,20,false,ftsService.defaultPartition), timeout).infotons.length should equal (3)
//  }

/*
  "search API" should "find infotons using path filter with descendants sorted by lastModified (desc)" in {
    //prepare infotons to index
    val objectInfotonToSearch = ObjectInfoton(
      "/fts-test/search/objectInfotonToSearch",
      "dc_test", Some(System.currentTimeMillis()),
      isoDateFormatter.parseDateTime("2013-01-01T10:00:00Z"),
      Map(
        "car" -> Set[FieldValue](FString("Mazda"), FString("Mitsubishi")),
        "food" -> Set[FieldValue](FString("Sushi"), FString("Falafel")),
        "copyright" -> Set[FieldValue](FString("Cm well team ©"))), None)

    val objectInfotonToSearch2 = ObjectInfoton(
      "/fts-test/search/objectInfotonToSearch2",
      "dc_test",
      Some(System.currentTimeMillis()),
      isoDateFormatter.parseDateTime("2013-01-01T10:01:00Z"),
      Map(
        "os" -> Set[FieldValue](FString("osx")),
        "ver" -> Set[FieldValue](FString("9.2")),
        "copyright" -> Set[FieldValue](FString("Cm well team ©"))), None)

    // This is for the withHistory flag test coming up in a few tests
    val updatedObjectInfotonToSearch = ObjectInfoton(
      "/fts-test/search/objectInfotonToSearch",
      "dc_test",
      Some(System.currentTimeMillis()),
      isoDateFormatter.parseDateTime("2013-01-02T10:02:00Z"),
      Map(
        "car" -> Set[FieldValue](FString("Mazda"), FString("Mitsubishi")),
        "food" -> Set[FieldValue](FString("Sushi"), FString("Falafel"), FString("Malabi")),
        "copyright" -> Set[FieldValue](FString("Cm well team ©"))), None)

    val fileInfotonToSearch = FileInfoton(
      "/fts-test/search/objectInfotonToSearch/fileInfotonToSearch",
      "dc_test",
      Some(System.currentTimeMillis()),
      isoDateFormatter.parseDateTime("2013-01-02T10:03:00Z"),
      Map("copyright" -> Set[FieldValue](FString("Cm-well team ©")), "since" -> Set[FieldValue](FString("2009"))),
      FileContent("My test file content is great".getBytes, "text/plain"), None)

    val linkInfotonToSearch = LinkInfoton(
      "/fts-test/search/linkInfotonToSearch",
      "dc_test",
      isoDateFormatter.parseDateTime("2013-01-05T10:04:00Z"),
      Map("since" -> Set[FieldValue](FString("2009"))),
      "/fts-test/search/objectInfotonToSearch/fileInfotonToSearch",
      LinkType.Temporary, None).copy(indexTime = Some(System.currentTimeMillis()))

    val infotonsToIndex = Iterable(
      objectInfotonToSearch,
      objectInfotonToSearch2,
      updatedObjectInfotonToSearch,
      fileInfotonToSearch
    ).map{ i =>
      ESIndexRequest(
        Requests.indexRequest(testIndexName).`type`("infoclone").id(i.uuid).create(true)
          .source(JsonSerializerForES.encodeInfoton(i), XContentType.JSON),
        None
      )
    }
    // index them
    Await.result(ftsService.executeIndexRequests(infotonsToIndex), timeout)

    refreshAll()

    val response = Await.result(ftsService.search(Some(PathFilter("/fts-test/search", true)),None,None,DefaultPaginationParams), timeout)
    response.infotons.length should equal (4)
    response.infotons.head.path should equal ("/fts-test/search/linkInfotonToSearch")
    response.infotons.last.path should equal ("/fts-test/search/objectInfotonToSearch2")


  }

  it should "find infotons using path filter with descendants sorted by infoton's type" in {
    val response = Await.result(
      ftsService.search(
        pathFilter = Some(PathFilter("/fts-test/search", true)),
        fieldsFilter = None,
        datesFilter = None,
        paginationParams = DefaultPaginationParams,
        sortParams = FieldSortParams(List("type" -> Asc))),
      timeout)
    response.infotons.head.path should equal ("/fts-test/search/objectInfotonToSearch/fileInfotonToSearch")
    response.infotons.last.path should equal ("/fts-test/search/objectInfotonToSearch")
  }

  it should "find infotons using PathFilter without descendants" in {
    Await.result(ftsService.search(Some(PathFilter("/fts-test/search", false)),None,None,DefaultPaginationParams), timeout).infotons.length should equal(3)
  }

  it should "find infotons using FieldFilter" in {
    Await.result(
      ftsService.search(
        None,Some(FieldFilter(Must, Contains, "car", "mazda")),None,DefaultPaginationParams), timeout).infotons.length should equal (1)
    Await.result(
      ftsService.search(
        Some(PathFilter("/fts-test/search", true)),
        Some(FieldFilter(MustNot, Contains, "copyright", "team")),
        None,
        DefaultPaginationParams),
      timeout).infotons.length should equal (1)
    // in case of only one "should" supplied it acts as a "must"
    Await.result(
      ftsService.search(
        Some(PathFilter("/fts-test/search", true)),
        Some(FieldFilter(Should, Contains, "copyright", "well")),
        None,
        DefaultPaginationParams),
      timeout).infotons.length should equal (3)
    Await.result(
      ftsService.search(
        Some(PathFilter("/fts-test/search", true)),
        Some(MultiFieldFilter(Must, Seq(FieldFilter(Must, Contains, "copyright", "well"), FieldFilter(Should, Equals, "since", "2009")))),
        None,
        DefaultPaginationParams),
      timeout).infotons.length should equal (3)
  }

  it should "find infotons using FieldFilters using 'Should Exist'" in {
    Await.result(
      ftsService.search(
        None,
        Some(MultiFieldFilter(Must, Seq(SingleFieldFilter(Should,Contains, "car", None), SingleFieldFilter(Should,Contains, "ver", None)))),
        None,
        DefaultPaginationParams),
      timeout).infotons.length should equal (2)
  }

  it should "find infotons using DateFilter " in {
    Await.result(ftsService.search(None,None, Some(DatesFilter(Some(isoDateFormatter.parseDateTime("2013-01-01T10:00:00Z")),
      Some(isoDateFormatter.parseDateTime("2013-01-03T10:00:00Z")))),DefaultPaginationParams), timeout).infotons.length should equal (3)
  }

  it should "find infotons using DateFilter limited with pagination params" in {
   Await.result(ftsService.search(None,None,datesFilter = Some(DatesFilter(Some(isoDateFormatter.parseDateTime("2013-01-01T10:00:00Z")),
      Some(isoDateFormatter.parseDateTime("2013-01-06T10:00:00Z")))),DefaultPaginationParams), timeout).infotons.length should equal (4)
   Await.result(ftsService.search(None,None,datesFilter = Some(DatesFilter(Some(isoDateFormatter.parseDateTime("2013-01-01T10:00:00Z")),
      Some(isoDateFormatter.parseDateTime("2013-01-06T10:00:00Z")))), paginationParams = PaginationParams(0, 2)), timeout).infotons.length should equal (2)
   Await.result(ftsService.search(None,None,datesFilter = Some(DatesFilter(Some(isoDateFormatter.parseDateTime("2013-01-01T10:00:00Z")),
      Some(isoDateFormatter.parseDateTime("2013-01-06T10:00:00Z")))), paginationParams = PaginationParams(2, 1)), timeout).infotons.length should equal (1)
  }

  it should "include history versions when turning on the 'withHistory' flag" in {
   Await.result(
     ftsService.search(
       None,
       fieldsFilter = Some(FieldFilter(Must, Contains, "car", "mazda")),
       None,
       DefaultPaginationParams,
       withHistory = true),
     timeout).infotons.length should equal (2)
  }

  it should "use exact value when sorting on string field" in {

    val i1 = ObjectInfoton(path = "/fts-test2/sort-by/1", indexTime = None, dc = "dc_test",
      fields = Map("car" -> Set[FieldValue](FString("Mitsubishi Outlander"))), protocol = None)

    val i2 = ObjectInfoton(path = "/fts-test2/sort-by/2", indexTime = None, dc = "dc_test",
      fields = Map("car" -> Set[FieldValue](FString("Subaru Impreza"))), protocol = None)

    val infotonsToIndex = Iterable(i1, i2).map{ i =>
      ESIndexRequest(
        Requests.indexRequest(testIndexName).`type`("infoclone").id(i.uuid).create(true)
          .source(JsonSerializerForES.encodeInfoton(i), XContentType.JSON),
        None
      )
    }

    Await.result(ftsService.executeIndexRequests(infotonsToIndex), timeout)
    refreshAll()

    val response = Await.result(ftsService.search(pathFilter = Some(PathFilter("/fts-test2/sort-by", true)),
      fieldsFilter = Some(SingleFieldFilter(Must, Equals, "car", None)),None,DefaultPaginationParams,
      sortParams = FieldSortParams(List(("car" -> Desc))), debugInfo = true), timeout)

    withClue(response) {
      response.infotons.head.path should equal("/fts-test2/sort-by/2")
      response.infotons.last.path should equal("/fts-test2/sort-by/1")
    }
  }

  "Scroll API" should "allow start scrolling and continue scrolling" in {
    val l = System.currentTimeMillis()
    val infotons = Seq.tabulate(500){ n =>
      val i = ObjectInfoton(
        s"/fts-test/scroll/info$n",
        "dc_test",
        Some(l + n),
        Map("name" + n -> Set[FieldValue](FString("value" + n), FString("another value" + n)))
      )
      ESIndexRequest(
        Requests.indexRequest(testIndexName).`type`("infoclone").id(i.uuid).create(true)
          .source(JsonSerializerForES.encodeInfoton(i), XContentType.JSON),
        None
      )
    }
    Await.result(ftsService.executeIndexRequests(infotons), timeout)
    refreshAll()
    val startScrollResult =Await.result(
      ftsService.startScroll(pathFilter=Some(PathFilter("/fts-test/scroll", false)),None,None, paginationParams = PaginationParams(0, 60)), timeout)
    startScrollResult.total should equal (500)

    var count = 0
    var scrollResult =Await.result(ftsService.scroll(startScrollResult.scrollId), timeout)
    while(scrollResult.infotons.nonEmpty) {
      count += scrollResult.infotons.length
      scrollResult =Await.result(ftsService.scroll(scrollResult.scrollId), timeout)
    }

    count should equal (500)

  }
*/
  // scalastyle:on
}

//class FTSoldTests extends FTSServiceEsSpec with FTSServiceESTest with FTSMixin
//TODO: uncomment, and make fts tests to run on new FTS as well
//class FTSnewTests extends FTSServiceEsSpec with FTSServiceTest with FTSMixin
