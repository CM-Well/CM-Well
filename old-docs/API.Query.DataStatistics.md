# Function: *Query with Data Statistics* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Query.ApplyGremlinToQueryResults.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.QueryForQuadsByTheirLabel.md)  

----

## Description ##

Elastic Search is the underlying search engine that CM-Well uses when performing a query (which involves a full-text search on infoton field values). Elastic Search supports several types of statistical metrics of field values within a given group of infotons. For example, using statistical features, you can discover how many distinct values there are for a certain field in a certain group of infotons. (The Elastic Search statistical feature is called "aggregations", and you may see some references to this term in the search syntax and results.) 

CM-Well passes the statistical query to Elastic Search, which performs the analysis and returns its results, which are passed back to the caller. You can learn more about Elastic Search aggregation options [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html).

>**Note:** You can use all the regular CM-Well query parameters such as **qp**, **recursive**, date filters and so on, before applying a statistical query. The statistical analysis is applied on the subset of the data that passes the filters.

## Syntax ##

**URL:** \<hostURL\>/\<PATH\>
**REST verb:** GET
**Mandatory parameters:** op=stats&ap=type:\<statsType\>,field:<statsField>

----------

**Template:**

    <cmwellPath>?op=stats&ap=type:<statsType>,field:<statsField>,name:<outputName>&format=<outputFormat>

**URL example:**

    <cm-well-host>/permid.org?op=stats&ap=type:card,name:MyCurrencyStats,field:iso4217.currency&format=json&pretty

**Curl example (REST API):**

    Curl -X GET "<cm-well-host>/permid.org?op=stats&ap=type:card,name:MyCurrencyStats,field:iso4217.currency&format=json&pretty"

## Special Parameters ##

Parameter | Description | Values | Example 
:----------|:-------------|:--------|:---------
ap | Aggregation parameters that define the statistical query type and field. | See below in this table. |
type | The type of statistical query to perform. See [Using Elastic Search Statistics](DevGuide.UsingElasticSearchStatistics.md) to learn more. | card, stats, term, sig | ap=type:card
name | Optional. If supplied, its value is returned as the **name** value in the response. | Any string | ap=name:MyQueryName
field | The name of the field on whose values you want to apply the query. | Any valid CM-Well field name | ap=field:CommonName.mdaas

## Code Example ##

### Call ###

    curl "<cm-well-host>/permid.org?op=stats&ap=type:card,name:MyCurrencyStats,field:iso4217.currency&format=json&pretty"

### Results ###

    {
      "AggregationResponse" : [ {
    	"name" : "MyCurrencyStats",
    	"type" : "CardinalityAggregationResponse",
    	"filter" : {
      		"name" : "MyCurrencyStats",
      		"type" : "CardinalityAggregation",
      		"field" : "iso4217.currency"
    	},
    	"count" : 266
      } ]
    }

## Notes ##

* All counts returned by statistical queries are **approximate**. This is because Elastic Search is a distributed application, and data updates may take time to replicate on all machines. Usually counts are accurate to within 5%-10% of the true value. Accuracy is affected by the optional [precision_threshold](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html#_precision_control) parameter.
* The values defined in **ap** (aggregation parameters) are passed on to Elastic Search as they are.
* The aggregation parameters must be passed in this order: 

Type | Parameter Order | Defaults
:----|:----------------|:---------
term | type:term[,name:MyName],field(:\|::)MyFieldName[,size:MySize][subaggregations] | size = 10
sig | type:sig[,name:MyName],field(:\|::)MyFieldName[,backgroundTerm:FieldName*Value][,minDocCount:MyCount][,size:MySize][subaggregations] | size = 10, minDocCount = 10
card | type:card[,name:MyName],field(:\|::)MyFieldName[,precisionThreshold:MyLong] |
stats | type:stats[,name:MyName],field(:\|::)MyFieldName |

* The output format must be one of: csv, json, jsonl.
* When using sub-queries, you can only request a total of 2 queries with the csv format, as a table only has 2 dimensions. For larger numbers of queries, use the json format, which has no limit on its nesting levels.

## Related Topics ##
[Using Elastic Search Statistics](DevGuide.UsingElasticSearchStatistics.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Query.ApplyGremlinToQueryResults.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.QueryForQuadsByTheirLabel.md)  

----