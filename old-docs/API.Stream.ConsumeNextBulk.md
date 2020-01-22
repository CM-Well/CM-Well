# Function: *Consume Next Bulk* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Stream.GetNextChunk.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Stream.CreateConsumer.md)  

----

## Description ##
If you wish to retrieve a large number of infotons, but you want to quickly iterate over "chunks" of data in a controlled fashion, you can use the **create-consumer** API and the `_bulk-consume` endpoint. 

The `_bulk-consume` API is similar to the [_consume](API.Stream.ConsumeNextChunk.md) API, with the following differences:

* `_bulk-consume` is significantly faster than `_consume`.
* Results returned by `_consume` are sorted by individual infoton, according to their **lastModified** value. (This is what makes `_consume` so much slower than `_bulk-consume`.)

The process requires two different API calls:
1. Call **create-consumer** to receive a position identifier (in the **X-CM-WELL-POSITION** header).
2. Repeatedly call `_bulk-consume`, to receive chunks of infotons. When you call `_bulk-consume`, you pass the position identifier and you receive and new one in the response. Pass the new position identifier to the next call to `_bulk-consume`. The process ends when CM-Well returns a 204 status code (no more content).

Up to 1 million results are returned in each chunk. The **X-CM-WELL-N** header in the response indicates the number of infotons you should expect to receive in the chunk. This allows you to verify that you've received all the expected data. If you receive fewer infotons due to errors, you can retry the **_bulk-consume** call, using the same position identifier and adding the optional **to-hint** parameter. You set the **to-hint** value to the value you received in the **X-CM-WELL-TO** header of the response. The **X-CM-WELL-TO** value is a timestamp which is the upper limit on the update times of the infotons in the bulk.

>**Note:** The position identifier enables you to restart consuming from the same position even if the consume process fails in the middle.

## Syntax ##

**URL:** \<cm-well-host\>/_bulk-consume
**REST verb:** GET
**Mandatory parameters:** position=\<position identifier\>

----------

**Template:**

    <cm-well-host>/_bulk-consume?position=<position identifier>

**URL example:** N/A

**Curl example (REST API):**

    curl -vX GET <cm-well-host>/_bulk-consume?position=eQA5eJwzNDE1sDQ3NDI0tjQ2rjGoMTE0MDI1NjS0BIEa_dSKxNyCnFS9_KJ0fc-8lMyyzJTSxJzimpoa&format=json

## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Values&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example
:----------|:-------------|:--------|:---------
position | Defines the position of the chunk in the stream |  Position ID returned by create-consumer or last call to _bulk-consume | position=eQA5eJwzNDE1sDQ3NDI0tjQ2rjGoMTE0MDI1NjS0BIEa_dSKxNyCnFS9_KJ0fc-8lMyyzJTSxJzimpoa
slow-bulk | Optional. If added, streaming is slower than the regular bulk-consume streaming. You may want to use this flag for slow network connections, to avoid timeouts. | None; Boolean flag. | <cm-well-host>_bulk-consume?format=json&slow-bulk
to-hint | Optional; to be used in retries. A timestamp which is the upper limit on the update times of the infotons in the bulk. Take the value returned in the **X-CM-WELL-TO** header of the bulk response you're retrying. | Timestamp values | to-hint=1425817923290
parallelism | The number of threads used for concurrent streaming. | Positive integers. Default is 10 | parallelism=15


## Code Example ##

### Call ###

    curl -vX GET <cm-well-host>/_bulk-consume?position=eQA5eJwzNDE1sDQ3NDI0tjQ2rjGoMTE0MDI1NjS0BIEa_dSKxNyCnFS9_KJ0fc-8lMyyzJTSxJzimpoa&format=json&to-hint=1425817923290

### Results ###

    < HTTP/1.1 200 OK
    < Transfer-Encoding: chunked
    < X-CM-WELL-N: 592421
    < X-CMWELL-BG: O
    < X-CMWELL-RT: 5986ms
    < X-CM-WELL-TO: 1425817923290
    < X-CMWELL-Version: 1.5.51
    < X-CMWELL-Hostname: cmwellhost.com
    < X-CM-WELL-POSITION: 5gAAMTQyNTgxNzkyMzI5MCwxNDI1ODE4MDcwNDU4fC9wZXJtaWQub3JnfDEwMDAwMDA
    < Content-Type: application/json;charset=UTF8
    < Date: Wed, 01 Nov 2017 15:09:16 GMT
    <
    {"type":"ObjectInfoton","system":{"uuid":"0388ef115736cff149e51c2afde61058","lastModified":"2015-03-08T12:31:38.095Z","path":"/permid.org/1-21477092480","dataCenter":"dc1","indexTime":1460123264522,"parent":"/permid.org"},"fields":{"IsTradingIn.mdaas":["EUR"],"QuoteExchangeCode.mdaas":["FRA"],"CommonName.mdaas":["BNP SND BNZT 12"],"IsQuoteOf.mdaas":["http://permid.org/1-21477084414"],"type.rdf":["http://ont.thomsonreuters.com/mdaas/Quote"],"TRCSAssetClass.mdaas":["Buffered Risk / Bonus Certificates"],"RCSAssetClass.mdaas":["CRTBON"],"RIC.mdaas":["DEBP1R7Q.F^L11"]}}
    {"type":"ObjectInfoton","system":{"uuid":"f995e339b1d67f1fee39eca19b7d6439","lastModified":"2015-03-08T12:30:47.602Z","path":"/permid.org/1-21476179024","dataCenter":"dc1","indexTime":1460123171404,"parent":"/permid.org"},"fields":{"IsTradingIn.mdaas":["EUR"],"QuoteExchangeCode.mdaas":["EWX"],"CommonName.mdaas":["HVM SX5E CAL 12C"],"IsQuoteOf.mdaas":["http://permid.org/1-21476166691"],"type.rdf":["http://ont.thomsonreuters.com/mdaas/Quote"],"TRCSAssetClass.mdaas":["Traditional Warrants"],"RCSAssetClass.mdaas":["TRAD"],"RIC.mdaas":["DEHV5K4Y.EW^C12"]}}
    {"type":"ObjectInfoton","system":{"uuid":"8378d4d7e9c7ee9ceb27359b198431ce","lastModified":"2015-03-08T12:30:40.552Z","path":"/permid.org/1-21476043443","dataCenter":"dc1","indexTime":1460123158656,"parent":"/permid.org"},"fields":{"QuoteExchangeCode.mdaas":["RCT"],"CommonName.mdaas":["DAX 30"],"IsQuoteOf.mdaas":["http://permid.org/1-21475980349"],"type.rdf":["http://ont.thomsonreuters.com/mdaas/Quote"],"TRCSAssetClass.mdaas":["Discount Certificates"],"RCSAssetClass.mdaas":["CRTDISC"],"RIC.mdaas":["DECK413N=COBA^B13"]}}
	...
	TRUNCATED

## Notes ##

* Although `_bulk-consume` results are not sorted at the level of the single infoton, they are sorted "per bulk", that is, every bulk contains infotons that are newer than those of the previous bulk, and older than those of the next bulk.
* It may take some time for a `_bulk-consume` command to "warm up". This is because before results are returned, there is a pre-processing stage that arranges results in time-ordered bulks. Once the pre-processing stage is over, retrieval of results is very fast.
* Because the start position of the next bulk to be read is returned in the response header, you can actually process two (or sometimes more) bulks in parallel, by creating a reader thread for each position you receive.
* The same location token is used for both the **consume** and the **bulk-consume** APIs. You can also switch between calls to **consume** and **bulk-consume**, using the same token instance. You may want to switch from **bulk-consume** to **consume** if network load is causing **bulk-consume** operations to fail.
* Only the following formats are supported for the `_bulk-consume` API:

| Format   | format=&lt;query param&gt; | Mimetype            |
|:----------|:----------------------------|:---------------------|
Text | text | text/plain 
Tab-Separated Values | tsv | text/plain 
| NTriples | ntriples&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | text/plain          |
| NQuads   | nquads                     | text/turtle         |


## Related Topics ##
[Create Iterator](API.Stream.CreateIterator.md)
[Create Consumer](API.Stream.CreateConsumer.md)
[Consume Next Chunk](API.Stream.ConsumeNextChunk.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Stream.GetNextChunk.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Stream.CreateConsumer.md)  

----

