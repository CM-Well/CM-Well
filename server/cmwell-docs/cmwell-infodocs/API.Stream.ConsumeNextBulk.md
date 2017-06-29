# Function: *Consume Next Bulk* #

## Description ##
If you wish to retrieve a large number of infotons, but you want to quickly iterate over "chunks" of data in a controlled fashion, you can use the **create-consumer** API and the `_bulk-consume` endpoint. 

The `_bulk-consume` API is similar to the [_consume](API.Stream.ConsumeNextChunk.md) API, with the following differences:

* `_bulk-consume` is significantly faster than `_consume`.
* Results returned by `_consume` are sorted by individual infoton, according to their **lastModified** value. (This is what makes `_consume` so much slower than `_bulk-consume`.)

The process requires two different API calls:
1. Call **create-consumer** to receive a position identifier (in the **X-CM-WELL-POSITION** header).
2. Repeatedly call `_bulk-consume`, to receive chunks of infotons. When you call `_bulk-consume`, you pass the position identifier and you receive and new one in the response. Pass the new position identifier to the next call to `_bulk-consume`. The process ends when CM-Well returns a 204 status code (no more content).

About 1 million results are returned in each chunk.

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
slow-bulk | Optional. If added, streaming is slower than the regular bulk-consume streaming. You may want to use this flag for slow network connections, to avoid timeouts. | None; Boolean flag. | <cm-well-host>/example.org/Individuals/_bulk-consume?format=json&slow-bulk

## Code Example ##

### Call ###

    curl -vX GET <cm-well-host>/example.org/Individuals/_bulk-consume?position=eQA5eJwzNDE1sDQ3NDI0tjQ2rjGoMTE0MDI1NjS0BIEa_dSKxNyCnFS9_KJ0fc-8lMyyzJTSxJzimpoa&format=json

### Results ###

    {
    	"type":"ObjectInfoton",	
    	"system": {
    		"uuid":"19074cd458994e2355058749f63c6ff3",
    		"lastModified":"2016-04-19T07:28:27.840Z",
    		"path":"/example.org/Individuals/JohnSmith",
    		"dataCenter":"dc1",
    		"indexTime":1461050909358,
    		"parent":"/example.org/Individuals"
    		},
    	"fields":{
    		"colleagueOf.rel":"http://example.org/Individuals/JaneDoe"
    		}
    	}
    }

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


