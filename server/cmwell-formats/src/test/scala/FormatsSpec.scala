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


import cmwell.domain.{TermsAggregationResponse, _}
import cmwell.formats._
import cmwell.util.string.dateStringify
import org.joda.time.DateTime
import org.scalatest.{FunSpec, Matchers}
import play.api.libs.json.{JsValue, Json}

import scala.language.postfixOps

/**
 * Created by yaakov on 3/2/15.
 */
class FormatsSpec extends FunSpec with Matchers with Helpers {

  describe("JSONL tests") {
    val jsonlFormatter = new JsonlFormatter(_=>None,_=>None) // for simplicity, assuming nothing is under meta/ns

    val date = new DateTime("1985-11-25T18:43:00.000Z")
    val defaultFields = Map("field1"->Set[FieldValue](FString("value1")))
    val infotons = Map(
      'simpleObjectInfoton -> ObjectInfoton("simpleObjectInfoton","dc_test", None, date, defaultFields),
      'objectInfotonWithQuads -> ObjectInfoton("objectInfotonWithQuads","dc_test", None, date, Map("field1"->Set[FieldValue](FString("value1", None, Some("spiderman"))))),
      'fileInfoton -> FileInfoton("fileInfoton","dc_test", None, date, defaultFields, FileContent("CAFEBABE".getBytes("UTF8"), "application/java-byte-code")),
      'compoundInfoton -> CompoundInfoton("compoundInfoton","dc_test", None, date, Some(defaultFields), Seq(ObjectInfoton("first-child","dc_test",None,date)), 0, 1, 1)
    )

    def assertSystemFields(i: Infoton, jsonl: JsValue) = {
      Seq("@id"->i.path, "type" -> i.kind, "uuid" -> i.uuid, "path" -> i.path, "parent" -> i.parent, "lastModified" -> dateStringify(i.lastModified)).foreach {
        case (key, expectedValue) => jsonl.getValueOfFirst(s"$key.sys") should be(expectedValue)
      }
    }

    it("should serialize a simple object infoton") {
      val i = infotons('simpleObjectInfoton)
      val jsonlResult = Json.parse(jsonlFormatter.render(i))

      assertSystemFields(i, jsonlResult)
      jsonlResult.getValueOfFirst("field1.nn") should be("value1")
    }

    it("should serialize an object infoton with quads") {
      val i = infotons('objectInfotonWithQuads)
      val jsonlResult = Json.parse(new JsonlFormatter(_=>None,_=>Some("spiderman")).render(i))

      assertSystemFields(i, jsonlResult)

      val field1 = jsonlResult getFirst "field1.nn"
      field1.getValue should be("value1")
      field1.getQuad should be("spiderman")
    }

    it("should serialize a FileInfoton") {
      val i = infotons('fileInfoton)
      val jsonlResult = Json.parse(jsonlFormatter.render(i))

      assertSystemFields(i, jsonlResult)

      Seq("field1.nn"->"value1","mimeType.content.sys"->"application/java-byte-code","base64-data.content.sys"->"Q0FGRUJBQkU=").foreach{
        case (key,expectedValue) => jsonlResult.getValueOfFirst(key) should be(expectedValue)
      }

      (jsonlResult.getFirst("length.content.sys")\"value").as[Int] should be(8)
    }

    it("should serialize a CompoundInfoton") {
      // todo...
    }

    // todo - test all Formatables!
  }

  describe("aggregate tests") {
    // sample request: `curl 'cmwell:9000/en.wikipedia.org/wiki?op=aggregate&ap=type:term,field::category.jX2eAA'`
    /* sample response:
    {
      "AggregationsResponse": [
        {
          "name": "TermAggregation",
          "type": "TermsAggregationResponse",
          "filter": {
            "field": "category.jX2eAA",
            "size": 10
          },
          "buckets": [
            {
              "key": "http://en.wikipedia.org/cat/living_people_d45c539",
              "objects": 710585
            },
            {
              "key": "http://en.wikipedia.org/cat/year_of_birth_missing__living_people__170e89c5",
              "objects": 56019
            },
            {
              "key": "http://en.wikipedia.org/cat/american_films_1fbf60ae",
              "objects": 39532
            },
            {
              "key": "http://en.wikipedia.org/cat/english_language_films_d8965cab",
              "objects": 33083
            },
            {
              "key": "http://en.wikipedia.org/cat/place_of_birth_missing__living_people__3f637be9",
              "objects": 26766
            },
            {
              "key": "http://en.wikipedia.org/cat/local_coibot_reports_4886b897",
              "objects": 24153
            },
            {
              "key": "http://en.wikipedia.org/cat/year_of_birth_unknown_3f2a0f8a",
              "objects": 19506
            },
            {
              "key": "http://en.wikipedia.org/cat/association_football_midfielders_174418ae",
              "objects": 19207
            },
            {
              "key": "http://en.wikipedia.org/cat/the_football_league_players_88abe0b6",
              "objects": 18937
            },
            {
              "key": "http://en.wikipedia.org/cat/year_of_birth_missing_62acd394",
              "objects": 18228
            }
          ]
        }
      ]
    }*/
    val aggregationsResponse1 = AggregationsResponse(Seq(
      TermsAggregationResponse(
        filter = TermAggregationFilter(
          name = "TermAggregation",
          field = Field(NonAnalyzedField, "category.jX2eAA"),
          size = 5
        ),
        buckets = Seq(
          Bucket(k = FieldValue("http://en.wikipedia.org/cat/living_people_d45c539"), dc = 710585, subAgg = None),
          Bucket(k = FieldValue("http://en.wikipedia.org/cat/year_of_birth_missing__living_people__170e89c5"), dc = 56019, subAgg = None),
          Bucket(k = FieldValue("http://en.wikipedia.org/cat/american_films_1fbf60ae"), dc = 39532, subAgg = None),
          Bucket(k = FieldValue("http://en.wikipedia.org/cat/english_language_films_d8965cab"), dc = 33083, subAgg = None),
          Bucket(k = FieldValue("http://en.wikipedia.org/cat/place_of_birth_missing__living_people__3f637be9"), dc = 26766, subAgg = None)
        ))),
      Option("test debug info also")
    )

    // sample request: `curl 'cmwell:9000/en.wikipedia.org/wiki?op=aggregate&ap=type:sig,field::referTo.jX2eAA,size:3~type:term,field::category.jX2eAA,size:3'`
    /* sample response:
    {
      "AggregationsResponse": [
        {
          "name": "SignificantTermsAggregation",
          "type": "SignificantTermsAggregationResponse",
          "filter": {
            "field": "referTo.jX2eAA",
            "minimum doc count": 0,
            "size": 3
          },
          "objects": 13884624,
          "buckets": [
            {
              "key": "http://en.wikipedia.org/wiki/united_states_b01f3e84",
              "objects": 282203,
              "score": 19307.54729346422,
              "bgCount": 282203,
              "AggregationsResponse": []
            },
            {
              "key": "http://en.wikipedia.org/wiki/france",
              "objects": 127296,
              "score": 3928.554773532682,
              "bgCount": 127296,
              "AggregationsResponse": []
            },
            {
              "key": "http://en.wikipedia.org/wiki/association_football_b70932fc",
              "objects": 115031,
              "score": 3207.989815760563,
              "bgCount": 115031,
              "AggregationsResponse": []
            }
          ]
        },
        {
          "name": "TermAggregation",
          "type": "TermsAggregationResponse",
          "filter": {
            "field": "category.jX2eAA",
            "size": 3
          },
          "buckets": [
            {
              "key": "http://en.wikipedia.org/cat/living_people_d45c539",
              "objects": 710585
            },
            {
              "key": "http://en.wikipedia.org/cat/year_of_birth_missing__living_people__170e89c5",
              "objects": 56019
            },
            {
              "key": "http://en.wikipedia.org/cat/american_films_1fbf60ae",
              "objects": 39532
            }
          ]
        }
      ]
    }*/
    val aggregationsResponse2 = AggregationsResponse(Seq(
      SignificantTermsAggregationResponse(
        filter = SignificantTermsAggregationFilter(
          name = "SignificantTermsAggregation",
          field = Field(NonAnalyzedField, "referTo.jX2eAA"),
          backgroundTerm = None,
          minDocCount = 0,
          size = 3
        ),
        docCount = 13884624,
        buckets = Seq(
          SignificantTermsBucket(key = FieldValue("http://en.wikipedia.org/wiki/united_states_b01f3e84"), docCount = 282203, score = 19307.54729346422, bgCount = 282203, subAggregations = None),
          SignificantTermsBucket(key = FieldValue("http://en.wikipedia.org/wiki/france"), docCount = 127296, score = 3928.554773532682, bgCount = 127296, subAggregations = None),
          SignificantTermsBucket(key = FieldValue("http://en.wikipedia.org/wiki/association_football_b70932fc"), docCount = 115031, score = 3207.989815760563, bgCount = 115031, subAggregations = None)
        )),
      TermsAggregationResponse(
        filter = TermAggregationFilter(
          name = "TermAggregation",
          field = Field(NonAnalyzedField, "category.jX2eAA"),
          size = 3
        ),
        buckets = Seq(
          Bucket(k = FieldValue("http://en.wikipedia.org/cat/living_people_d45c539"), dc = 710585, subAgg = None),
          Bucket(k = FieldValue("http://en.wikipedia.org/cat/year_of_birth_missing__living_people__170e89c5"), dc = 56019, subAgg = None),
          Bucket(k = FieldValue("http://en.wikipedia.org/cat/american_films_1fbf60ae"), dc = 39532, subAgg = None)
        ))),
      Option("test debug info also")
    )

    // sample request: `curl 'cmwell:9000/permid.org?op=aggregate&ap=type:sig,field::organizationCountryCode.-Jamjg,size:5<type:term,field::organizationFoundedDay.-Jamjg,size:3>'`
    /* sample response:
    {
      "AggregationsResponse": [
        {
          "name": "SignificantTermsAggregation",
          "type": "SignificantTermsAggregationResponse",
          "filter": {
            "field": "organizationCountryCode.-Jamjg",
            "minimum doc count": 0,
            "size": 5
          },
          "objects": 87766563,
          "buckets": [
            {
              "key": "100319",
              "objects": 2178650,
              "score": 81724.20031278803,
              "bgCount": 2178650,
              "AggregationsResponse": [
                {
                  "name": "TermAggregation",
                  "type": "TermsAggregationResponse",
                  "filter": {
                    "field": "organizationFoundedDay.-Jamjg",
                    "size": 3
                  },
                  "buckets": [
                    {
                      "key": "1",
                      "objects": 7579
                    },
                    {
                      "key": "28",
                      "objects": 5849
                    },
                    {
                      "key": "15",
                      "objects": 5699
                    }
                  ]
                }
              ]
            },
            {
              "key": "100317",
              "objects": 300590,
              "score": 1555.693292939183,
              "bgCount": 300590,
              "AggregationsResponse": [
                {
                  "name": "TermAggregation",
                  "type": "TermsAggregationResponse",
                  "filter": {
                    "field": "organizationFoundedDay.-Jamjg",
                    "size": 3
                  },
                  "buckets": [
                    {
                      "key": "1",
                      "objects": 3011
                    },
                    {
                      "key": "19",
                      "objects": 2933
                    },
                    {
                      "key": "14",
                      "objects": 2822
                    }
                  ]
                }
              ]
            },
            {
              "key": "105758",
              "objects": 285892,
              "score": 1407.2746968832143,
              "bgCount": 285892,
              "AggregationsResponse": [
                {
                  "name": "TermAggregation",
                  "type": "TermsAggregationResponse",
                  "filter": {
                    "field": "organizationFoundedDay.-Jamjg",
                    "size": 3
                  },
                  "buckets": [
                    {
                      "key": "1",
                      "objects": 14867
                    },
                    {
                      "key": "28",
                      "objects": 3760
                    },
                    {
                      "key": "15",
                      "objects": 3634
                    }
                  ]
                }
              ]
            },
            {
              "key": "100052",
              "objects": 279990,
              "score": 1349.7703790699475,
              "bgCount": 279990,
              "AggregationsResponse": [
                {
                  "name": "TermAggregation",
                  "type": "TermsAggregationResponse",
                  "filter": {
                    "field": "organizationFoundedDay.-Jamjg",
                    "size": 3
                  },
                  "buckets": [
                    {
                      "key": "1",
                      "objects": 1560
                    },
                    {
                      "key": "30",
                      "objects": 554
                    },
                    {
                      "key": "31",
                      "objects": 520
                    }
                  ]
                }
              ]
            },
            {
              "key": "100148",
              "objects": 273764,
              "score": 1290.4093594152891,
              "bgCount": 273764,
              "AggregationsResponse": [
                {
                  "name": "TermAggregation",
                  "type": "TermsAggregationResponse",
                  "filter": {
                    "field": "organizationFoundedDay.-Jamjg",
                    "size": 3
                  },
                  "buckets": [
                    {
                      "key": "1",
                      "objects": 4532
                    },
                    {
                      "key": "2",
                      "objects": 758
                    },
                    {
                      "key": "15",
                      "objects": 643
                    }
                  ]
                }
              ]
            }
          ]
        }
      ]
    }*/
    val aggregationsResponse3 = AggregationsResponse(Seq(
      SignificantTermsAggregationResponse(
        filter = SignificantTermsAggregationFilter(
          name = "SignificantTermsAggregation",
          field = Field(NonAnalyzedField, "organizationCountryCode.-Jamjg"),
          backgroundTerm = None,
          minDocCount = 0,
          size = 3
        ),
        docCount = 87766563,
        buckets = Seq(
          SignificantTermsBucket(key = FieldValue("100319"), docCount = 2178650, score = 81723.15093613388, bgCount = 2178650, subAggregations = Some(AggregationsResponse(Seq(
            TermsAggregationResponse(
              filter = TermAggregationFilter(
                name = "TermAggregation",
                field = Field(NonAnalyzedField, "organizationFoundedDay.-Jamjg"),
                size = 3
              ),
              buckets = Seq(
                Bucket(k = FieldValue(1), dc = 7579, subAgg = None),
                Bucket(k = FieldValue(28), dc = 5849, subAgg = None),
                Bucket(k = FieldValue(15), dc = 5699, subAgg = None)
              )))))),
          SignificantTermsBucket(key = FieldValue("100317"), docCount = 300590, score = 1555.693292939183, bgCount = 300590, subAggregations = Some(AggregationsResponse(Seq(
            TermsAggregationResponse(
              filter = TermAggregationFilter(
                name = "TermAggregation",
                field = Field(NonAnalyzedField, "organizationFoundedDay.-Jamjg"),
                size = 3
              ),
              buckets = Seq(
                Bucket(k = FieldValue(1), dc = 3011, subAgg = None),
                Bucket(k = FieldValue(19), dc = 2933, subAgg = None),
                Bucket(k = FieldValue(14), dc = 2822, subAgg = None)
              ))))))
        ))))

    it("check json") {
      val formatString = "json"
      val formatter = formatString match {
        case FormatExtractor(JsonType) => new JsonFormatter(identity)
      }
      //TODO: test beyond thrown exceptions
      noException should be thrownBy formatter.render(aggregationsResponse3)
    }

    it("check yaml") {
      val formatString = "yaml"
      var formatter: YamlFormatter = null
      //TODO: test beyond thrown exceptions
      noException should be thrownBy (formatString match {
        case FormatExtractor(YamlType) =>
          formatter = new YamlFormatter(identity)
      })
      noException should be thrownBy formatter.render(aggregationsResponse1)
    }

    it("check csv") {
      val formatString = "csv"
      val formatter = formatString match {
        case FormatExtractor(CsvType) => CSVFormatter(identity)
      }
      //TODO: test beyond thrown exceptions
      noException should be thrownBy formatter.render(aggregationsResponse1)
    }
  }
}
