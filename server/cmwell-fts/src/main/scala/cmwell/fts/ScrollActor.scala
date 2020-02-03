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
//package cmwell.fts
//
//import akka.actor.Actor
//import akka.pattern.pipe
//
//import akka.actor.Actor.Receive
//import cmwell.domain.Infoton
//
///**
// * Created by israel on 11/1/15.
// */
//class ScrollActor extends Actor{
//
//  var nextScrollId:String = _
//  val ftsService = FTSServiceES.getOne()
//  var fetchFromCassandra = false
//
//  override def receive: Receive = {
//    case StartScroll(pathFilter, fieldsFilters, datesFilter, paginationParams, scrollTTL, withHistory, indices, withData) =>
//      fetchFromCassandra = withData
//
//      ftsService.startScroll(
//        pathFilter,
//        fieldsFilters,
//        datesFilter,
//        paginationParams,
//        scrollTTL,
//        withHistory,
//        indices,
//        true
//      ).flatMap{ ssr =>
//        ftsService.scroll(ssr.scrollId, scrollTTL)
//      }.map{ sr =>
//        nextScrollId = sr.scrollId
//        ScrollResults(sr.infotons)
//      }
//  }
//
//
//}
//
//case class StartScroll(pathFilter: Option[PathFilter] = None, fieldsFilters: List[(FieldOperator,
//                       List[FieldFilter])] = Nil, datesFilter: Option[DatesFilter] = None,
//                       paginationParams: PaginationParams = DefaultPaginationParams, scrollTTL:Long,
//                       withHistory: Boolean = false, indexNames:Seq[String] = Seq.empty, withData:Boolean = false)
//
//
//case class StartScrollReply(total:)
//case class ScrollResults(infotons:Seq[Infoton])
