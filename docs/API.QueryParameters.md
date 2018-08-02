# CM-Well Query Parameters #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.InputAndOutputFormats.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.FieldConditionSyntax.md)

----

The following table describes parameters that you can add to most CM-Well calls. Some parameters take values and some serve as Boolean flags (see details in table). If multiple parameters appear in the call, they must be separated by the & character.

>**NOTE:** When performing a search operation, query parameters can be added either to the request URL (using the GET syntax) or to the request body (using the POST syntax). See [Query for Infotons Using Field Conditions](API.Query.QueryForInfotonsUsingFieldConditions.md) to learn more.

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Values&nbsp;&nbsp; | Example | Reference
:----------|:-----------------------|:--------|:---------|:----------
blocking | This flag causes update requests to be blocking, i.e. to behave synchronously. | None | <cm-well-host>/_in?format=ttl&replace-mode&blocking | [Using the blocking Flag](API.UsingTheBlockingFlag.md)
fields | The subset of fields you want to get in results. If fields are not specified and the with-data flag is used, all fields are returned. | A single field name or comma-separated list of field names. | fields=CommonName.mdaas,organizationFoundedYear.mdaas | See [Field Condition Syntax](API.FieldConditionSyntax.md) for syntax of field names
format | The format to apply to the results. The default value is **ntriples**. | json, jsonl, jsonld, n3, rdfxml, ntriples, nquads, turtle/ttl, yaml, trig, trix | format=n3 | [CM-Well Input and Output Formats](API.InputAndOutputFormats.md)
from | A lower limit on the date and time the infoton was last updated. | ISO 8601 datetime value | from=2015-04-15T09:24:09.284Z | [From/To Datetime Formatting](API.FromAndToDatetimeFormatting.md)
length | The number of query results (infotons) you want to receive | Any positive integer | length=20 | [Paging through Results with offset and length Parameters](API.PagingThroughResultsWithOffsetAndLengthParameters.md)
gqp | Filters results by outbound and/or inbound links of the matched infotons | [See reference](API.Traversal.TOC.md) | See reference | [Traversing Outbound and Inbound Links](API.Traversal.TOC.md)
offset | The offset of the first result you want to retrieve, relative to the complete list of results. | 0 or any positive integer | offset=40 | [Paging through Results with offset and length Parameters](API.PagingThroughResultsWithOffsetAndLengthParameters.md)
pretty | If this flag appears, formats of the JSON family are tabulated and arranged in a visually friendly style. | None | N/A | N/A
priority | If this flag appears in an update call, that call receives priority relative to non-priority calls. You can add this flag to any call to the **_in** endpoint or POST command. To use this flag, you must also pass the X-CM-WELL-TOKEN request header, with a token that grants access to the priority feature. This feature should be used with discretion so as not to flood the priority queue. | None | \<cm-well-host\>/_in?format=ttl&priority...
qp | Field conditions (a.k.a query parameters). One or more matching conditions on field values.  | [See reference](API.FieldConditionSyntax.md) | qp=CommonName.mdaas:Coca%20Cola | [Field Condition Syntax](API.FieldConditionSyntax.md)
recursive | If true, queries and delete command are recursive, i.e. apply to all infotons who are path-wise descendants of the infotons in the request. (An equivalent but deprecated name for this flag is with-descendants.)| false / true (the default) | recursive=true | [Using the recursive Flag](API.UsingTheRecursiveFlag.md)
sort-by | If this flag is used, query results are returned after sorting them by the value of the specified field. | Any field name, optionally preceded by - to sort in ascending order or * to sort in descending order (the default).| sort-by=*organizationStatusCode.mdaas | [Sorting Results with the sort-by Parameter](API.SortingResultsWithTheSort-byFlag.md)
to | An upper limit on the date and time the infoton was last updated. | ISO 8601 datetime value | to=2015-04-15T14:12:40.540Z | [From/To Datetime Formatting](API.FromAndToDatetimeFormatting.md)
tracking | Using this flag returns a tracking ID that you can use to poll for an update request's status. | None | <cm-well-host>/_in?format=ntriples&tracking | [Tracking API](API.Update.TrackUpdates.md)
with-data | If this flag is used, all infoton fields are returned in the query results. Otherwise, only the infotons' URLs and system metadata fields are returned. | Either any format value that CM-Well supports, or no value, in which case the default format is N3 | Query example: <cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Coca%20Cola&format=n3&with-data=json | [Using the with-data Flag](API.UsingTheWith-dataFlag.md)
with-deleted | If this flag is used, all deleted versions of infotons are retrieved, instead of only "live" versions. | None; the flag either appears or doesn't | Query example: <cm-well-host>/permid.org/1-5046625212?with-deleted | [Using the with-deleted Flag](API.UsingTheWith-deletedFlag.md)
with-history | If this flag is used, all historical versions of infotons are retrieved, instead of just the latest. | None; the flag either appears or doesn't | Query example: <cm-well-host>/permid.org/1-5046625212?with-history | [Using the with-history Flag](API.UsingTheWith-historyFlag.md)
xg | Requests outbound links of the matched infotons | [See reference](API.Traversal.TOC.md) | See reference | [Traversing Outbound and Inbound Links](API.Traversal.TOC.md)
yg | Requests outbound and/or inbound links of the matched infotons | [See reference](API.Traversal.TOC.md) | See reference | [Traversing Outbound and Inbound Links](API.Traversal.TOC.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.InputAndOutputFormats.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.FieldConditionSyntax.md)

----

