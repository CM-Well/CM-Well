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
package cmwell.util.formats

import cmwell.domain._
import play.api.libs.json._
import Encoders._
import cmwell.ws.Settings
import wsutil.zeroTime

/**
  * Created with IntelliJ IDEA.
  * User: Michael
  * Date: 3/18/14
  * Time: 11:16 AM
  * To change this template use File | Settings | File Templates.
  */
object JsonEncoder {

  private def alterFieldsBase(fields: Map[String, Set[FieldValue]],
                              oldBase: String,
                              newBase: String): Map[String, Set[FieldValue]] =
    fields.map(
      x =>
        x._1.replace(oldBase, newBase) -> x._2.map {
          case FString(str, l, q) => FString(str.replace(oldBase, newBase), l, q)
          case FReference(ref, q) => FReference(ref.replace(oldBase, newBase), q)
          case fv: FieldValue     => fv
      }
    )

  /*
    Will return an infoton with the correct domain name in the urls in its fields values.
   */
  private def alterBase(infoton: Infoton, newBase: String): Infoton = {

    val cmwellDefaultBase = "cmwell:/"

    infoton match {
      case oi @ ObjectInfoton(systemFields, Some(fields)) =>
        new ObjectInfoton(systemFields, Some(alterFieldsBase(fields, cmwellDefaultBase, newBase))) {
          override def kind = "ObjectInfoton"
          override def uuid = oi.uuid
        }
      case ci @ CompoundInfoton(systemFields, Some(fields), children, offset, length, total) =>
        new CompoundInfoton(systemFields,
                            Some(alterFieldsBase(fields, cmwellDefaultBase, newBase)),
                            children,
                            offset,
                            length,
                            total) {
          override def kind = "CompoundInfoton"
          override def uuid = ci.uuid
        }
      case fi @ FileInfoton(systemFields, Some(fields), content) =>
        new FileInfoton(systemFields,
                        Some(alterFieldsBase(fields, cmwellDefaultBase, newBase)),
                        content) {
          override def kind = "FileInfoton"
          override def uuid = fi.uuid
        }
      case li @ LinkInfoton(systemFields, Some(fields), linkTo, linkType) =>
        new LinkInfoton(systemFields,
                        Some(alterFieldsBase(fields, cmwellDefaultBase, newBase)),
                        linkTo,
                        linkType) {
          override def kind = "LinkInfoton"
          override def uuid = li.uuid
        }
      case _ => infoton
    }
  }

  /**
    * Transforms a json representation of a BagOfInfotons into the object BagOfInfotons.
    * @param json
    * @return
    */
  def decodeBagOfInfotons(json: String): Option[BagOfInfotons] = {
    val jsonNode: JsValue = Json.parse(json)
    jsonNode.asOpt[BagOfInfotons]
  }

  /**
    * Transforms a json representation of a BagOfInfotons into the object BagOfInfotons.
    * @param json
    * @return
    */
  def decodeBagOfInfotons(json: Array[Byte]): Option[BagOfInfotons] = {
    val jsonNode: JsValue = Json.parse(json)
    jsonNode.asOpt[BagOfInfotons]
  }

  /**
    * Transforms a json representation of an infotons into the object Infoton.
    * @param json
    * @return
    */
  def decodeInfoton(json: String): Option[Infoton] = {
    val jsonNode: JsValue = Json.parse(json)
    jsonNode.asOpt[Infoton]
  }

  /**
    * Transforms a json representation of an infotons into the object Infoton.
    * @param json
    * @return
    */
  def decodeInfoton(json: Array[Byte]): Option[Infoton] = {
    val jsonNode: JsValue = Json.parse(json)
    jsonNode.asOpt[Infoton]
  }

  /**
    * Transforms the object IterationResults into its json representation.
    * @param scrollResults
    * @return
    */
  def encodeIterationResults(scrollResults: IterationResults): JsValue = {
    Json.toJson(scrollResults)
  }

  /**
    * Transforms the object IterationResults into its json representation and will correct the domains in its infoton collection.
    * @param scrollResults
    * @param cmwellBase
    * @return
    */
  def encodeIterationResults(scrollResults: IterationResults, cmwellBase: String): JsValue = {
    if (cmwellBase != null) {
      Json.toJson(IterationResults(scrollResults.iteratorId, scrollResults.totalHits, scrollResults.infotons match {
        case Some(vi) => Some(vi.map(i => alterBase(i, cmwellBase)));
        case None     => None
      }))
    } else
      Json.toJson(scrollResults)
  }

  def encodeAggregationResponse(aggregationsResponse: AggregationsResponse): JsValue = {
    Json.toJson(aggregationsResponse)
  }

  /**
    * Transforms the object Infoton into its json representation.
    * @param infoton
    * @return
    */
  def encodeInfoton(infoton: Infoton): JsValue = {
    Json.toJson(infoton)
  }

  /**
    * Transforms the object Infoton into its json representation and will correct the domains in its fields collection.
    * @param infoton
    * @param cmwellBase
    * @return
    */
  def encodeInfoton(infoton: Infoton, cmwellBase: String): JsValue = {
    if (cmwellBase != null)
      Json.toJson(alterBase(infoton, cmwellBase))
    else
      Json.toJson(infoton)
  }

  /**
    * Transforms the object BagOfInfotons into its json representation.
    * @param bag
    * @return
    */
  def encodeBagOfInfotons(bag: BagOfInfotons): JsValue = {
    Json.toJson(bag)
  }

  /**
    * Transforms the object BagOfInfotons into its json representation and will correct the domains in its infoton collection.
    * @param bag
    * @param cmwellBase
    * @return
    */
  def encodeBagOfInfotons(bag: BagOfInfotons, cmwellBase: String): JsValue = {
    if (cmwellBase != null)
      Json.toJson(BagOfInfotons(bag.infotons.map(i => alterBase(i, cmwellBase))))
    else
      Json.toJson(bag)

  }

  /**
    * Transforms a json representation of a list of paths to the object InfotonPaths
    * @param json
    * @return
    */
  def decodeInfotonPathsList(json: String): Option[InfotonPaths] = {
    val jsonNode: JsValue = Json.parse(json)
    decodeInfotonPathsList(jsonNode)
  }

  /**
    * Transforms a json representation of a list of paths to the object InfotonPaths
    * @param json
    * @return
    */
  def decodeInfotonPathsList(json: Array[Byte]): Option[InfotonPaths] = {
    val jsonNode: JsValue = Json.parse(json)
    decodeInfotonPathsList(jsonNode)
  }

  /**
    * Transforms a json representation of a list of paths to the object InfotonPaths
    * @param json
    * @return
    */
  def decodeInfotonPathsList(json: JsValue): Option[InfotonPaths] = {
    json.asOpt[InfotonPaths]
  }

  def encodeInfotonPaths(infotonPaths: InfotonPaths): JsValue = {
    Json.toJson(infotonPaths)
  }

}
