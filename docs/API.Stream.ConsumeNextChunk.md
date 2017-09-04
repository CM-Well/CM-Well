# Function: *Consume Next Chunk* #

## Description ##
If you wish to retrieve a large number of infotons, but you want to iterate over small "chunks" of data in a controlled fashion, you can use the **create-consumer** API and the `_consume` endpoint. 

The process requires two different API calls:
1. Call **create-consumer** to receive a position identifier (in the **X-CM-WELL-POSITION** header).
2. Repeatedly call `_consume`, to receive chunks of infotons. When you call `_consume`, you pass the position identifier and you receive and new one in the response. Pass the new position identifier to the next call to `_consume`. The process ends when CM-Well returns a 204 status code (no more content).

>**Note:** If during the streaming process a chunk is encountered that contains some invalid data, that chunk arrives with a 206 (Partial Content) result code. You can still continue to consume chunks after receiving this error.

The **consume** API does not support a guaranteed chunk length (number of infotons in a chunk). About 100 results are returned in each bulk.

>**Note:** The position identifier enables you to restart consuming from the same position even if the consume process fails in the middle.

## Syntax ##

**URL:** \<cm-well-host\>/_consume
**REST verb:** GET
**Mandatory parameters:** position=\<position identifier\>

----------

**Template:**

    <cm-well-host>/_consume?position=<position identifier>

**URL example:** N/A

**Curl example (REST API):**

    curl -vX GET <cm-well-host>/_consume?position=eQA5eJwzNDE1sDQ3NDI0tjQ2rjGoMTE0MDI1NjS0BIEa_dSKxNyCnFS9_KJ0fc-8lMyyzJTSxJzimpoa&format=json

## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Values&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example
:----------|:-------------|:--------|:---------
position | Defines the position of the chunk in the stream |  Position ID returned by create-consumer or last call to _consume | position=eQA5eJwzNDE1sDQ3NDI0tjQ2rjGoMTE0MDI1NjS0BIEa_dSKxNyCnFS9_KJ0fc-8lMyyzJTSxJzimpoa

## Code Example ##

### Call ###

    curl -vX GET <cm-well-host>/example.org/Individuals/_consume?position=eQA5eJwzNDE1sDQ3NDI0tjQ2rjGoMTE0MDI1NjS0BIEa_dSKxNyCnFS9_KJ0fc-8lMyyzJTSxJzimpoa&format=json

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

For a faster implementation of recoverable streaming, see [bulk consume](API.Stream.ConsumeNextBulk.md).

## Related Topics ##
[Create Iterator](API.Stream.CreateIterator.md)
[Create Consumer](API.Stream.CreateConsumer.md)
[Consume Next Chunk](API.Stream.ConsumeNextChunk.md)
[Consume Next Bulk](API.Stream.ConsumeNextBulk.md)


