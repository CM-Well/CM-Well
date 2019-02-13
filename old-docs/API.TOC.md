# CM-Well API: Table of Contents #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) 

----

The following list shows general topics relating to many CM-Well API calls:

* [Input and Output Formats](API.InputAndOutputFormats.md)
* [Query Parameters](API.QueryParameters.md)
* [Field Condition Syntax](API.FieldConditionSyntax.md)
* [Field Name Formats](API.FieldNameFormats.md)
* [Metadata Fields](API.MetadataFields.md)
* [From and To Datetime Formatting](API.FromAndToDatetimeFormatting.md)
* [Paging through Results with Offset and Length Parameters](API.PagingThroughResultsWithOffsetAndLengthParameters.md)
* [Sorting Results with the sort-by Flag](API.SortingResultsWithTheSort-byFlag.md)
* [Using the recursive Flag](API.UsingTheRecursiveFlag.md)
* [Using the with-data Flag](API.UsingTheWith-dataFlag.md)
* [Using the with-history Flag](API.UsingTheWith-historyFlag.md)
* [Using the with-deleted Flag](API.UsingTheWith-deletedFlag.md)
* [Using the blocking Flag](API.UsingTheBlockingFlag.md)
* [Using Conditional Updates](API.UsingConditionalUpdates.md)
* [Traversing Outbound and Inbound Links (*xg*, *yg* and *gqp*)](API.Traversal.TOC.md)
* [Return Codes](API.ReturnCodes.md)

The following table shows all the CM-Well API calls:

<table>
  <tr>
    <th><h3>Category</h3></th>
    <th align=left><h3>Function</h3></th>
  </tr>
  <tr>
    <th><i>Login</i></th>
    <td><a href="API.Login.Login.md">Login</a></td>
  </tr>
  <tr>
    <th rowspan="3"><i>Get</i></th>
    <td><a href="API.Get.GetSingleInfotonByURI.md">Get Single Infoton by URI</a></td>
  </tr>
  <tr>
    <td><a href="API.Get.GetMultipleInfotonsByURI.md">Get Multiple Infotons by URI</a></td>
  </tr>
<tr>
    <td><a href="API.Get.GetSingleInfotonByUUID.md">Get Single Infoton by UUID</a></td>
  </tr>
  <tr>
    <th rowspan="6"><i>Query</i></th>
    <td><a href="API.Query.QueryForInfotonsUsingFieldConditions.md">Query for Infotons Using Field Conditions</a></td>
  </tr>
  <tr>
    <td><a href="API.Query.ApplySPARQLToQueryResults.md">Apply SPARQL to Query Results</a></td>
  </tr>
<tr>
    <td><a href="API.Query.ApplySPARQLToEntireGraph.md">Apply SPARQL to Entire Graph</a></td>
  </tr>
<tr>
    <td><a href="API.Query.ApplyGremlinToQueryResults.md">Apply Gremlin to Query Results</a></td>
  </tr>
<tr>
    <td><a href="API.Query.DataStatistics.md">Query for Data Statistics</a></td>
  </tr>
<tr>
    <td><a href="API.Query.QueryForQuadsByTheirLabel.md">Query for Quads by their Label</a></td>
  </tr>
<tr>
    <th><i>Stream</i></th>
    <td><a href="API.Stream.StreamInfotons.md">Stream Infotons</a></td>
  </tr>
<tr>
    <th rowspan="5"><i>Bulk Download</i></th>
    <td><a href="API.Stream.CreateIterator.md">Create Iterator</a></td>
  </tr>
<tr>
    <td><a href="API.Stream.GetNextChunk.md">Get Next Chunk</a></td>
  </tr>
<tr>
    <td><a href="API.Stream.ConsumeNextBulk.md">Consume Next Bulk</a></td>
  </tr>
<tr>
    <td><a href="API.Stream.CreateConsumer.md">Create Consumer</a></td>
  </tr>
<tr>
    <td><a href="API.Stream.ConsumeNextChunk.md">Consume Next Chunk</a></td>
  </tr>
<tr>
    <th rowspan="4"><i>Subscribe</i></th>
    <td><a href="API.Subscribe.SubscribeForPushedData.md">Subscribe for Push Updates</a></td>
  </tr>
  <tr>
    <td><a href="API.Subscribe.SubscribeForPulledData.md">Subscribe for Pull Updates</a></td>
  </tr>
<tr>
    <td><a href="API.Subscribe.PullNewData.md">Pull New Data</a></td>
  </tr>
<tr>
    <td><a href="API.Subscribe.Unsubscribe.md">Terminate an Active Subscription</a></td>
  </tr>
<tr>
    <th rowspan="12"><i>Update</i></th>
    <td><a href="API.Update.AddInfotonsAndFields.md">Add Infotons and Fields</a></td>
  </tr>
  <tr>
    <td><a href="API.Update.ReplaceFieldValues.md">Replace Field Values</a></td>
  </tr>
  <tr>
    <td><a href="API.Update.DeleteASingleInfoton.md">Delete a Single Infoton</a></td>
  </tr>
  <tr>
    <td><a href="API.Update.DeleteMultipleInfotons.md">Delete Multiple Infotons</a></td>
  </tr>
  <tr>
    <td><a href="API.Update.DeleteFields.md">Delete Fields</a></td>
  </tr>
 <tr>
    <td><a href="API.Update.DeleteSpecificFieldValues.md">Delete Specific Field Values</a></td>
  </tr>
  <tr>
    <td><a href="API.Update.AddInfotonsAndFieldsToSubGraph.md">Add Infotons and Fields to a Named Sub-Graph</a></td>
  </tr>
  <tr>
    <td><a href="API.Update.DeleteOrReplaceValuesInNamedSubGraph.md">Delete and Replace Values in a Named Sub-Graph</a></td>
  </tr>
  <tr>
    <td><a href="API.Update.Purge.md">Purge a Single Infoton's Versions</a></td>
  </tr>
  <tr>
    <td><a href="API.Update.AddFileInfoton.md">Add a File Infoton</a></td>
  </tr>
  <tr>
    <td><a href="API.Update.AddLinkInfoton.md">Add a Link Infoton</a></td>
  </tr>
<tr>
    <td><a href="API.Update.TrackUpdates.md">Track Updates</a></td>
  </tr>
<tr>
    <th rowspan="3"><i>Authorization</i></th>
    <td><a href="API.Auth.GeneratePassword.md">Generate Password</a></td>
  </tr>
  <tr>
    <td><a href="API.Auth.ChangePassword.md">Change Password</a></td>
  </tr>
<tr>
    <td><a href="API.Auth.InvalidateCache.md">Invalidate Cache</a></td>
  </tr>
</table>

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) 

----

















