# Function: *Create Consumer* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Stream.ConsumeNextBulk.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Stream.ConsumeNextChunk.md)  

----

## Description ##
If you wish to retrieve a large number of infotons, but you want to iterate over small "chunks" of data in a controlled fashion, you can use the **create-consumer** API and the `_consume` endpoint. 

The process requires two different API calls:
1. Call **create-consumer** to receive a position identifier (in the **X-CM-WELL-POSITION** response header).
2. Repeatedly call `_consume`, to receive chunks of infotons. When you call `_consume`, you pass the position identifier and you receive a new one in the response. Pass the new position identifier to the next `_consume` call. The process ends when CM-Well returns a 204 status code ("No more content").

The **consume** API does not support a guaranteed chunk length (number of infotons in a chunk). About 100 results are returned in each chunk.

>**Note:** The position identifier enables you to restart consuming from the same position even if the consume process fails in the middle.

## Syntax 1 ##

**URL:** \<cm-well-host\>/\<cm-well-path\>
**REST verb:** GET
**Mandatory parameters:** op=create-consumer

----------

**Template:**

    <cm-well-host>/<cm-well-path>?op=create-consumer

**URL example:** N/A

**Curl example (REST API):**

    curl -vX GET '<cm-well-host>/example.org/Individuals?op=create-consumer'

## Syntax 2 ##

**URL:** \<cm-well-host\>/\<cm-well-path\>
**REST verb:** POST
**Mandatory parameters:** op=create-consumer

----------

**Template:**

    <cm-well-host>/<cm-well-path>?op=create-consumer

**URL example:** N/A

**Curl example (REST API):**

    curl -vX POST <cm-well-host>/permid.org?op=create-consumer -H "Content-Type:application/x-www-form-urlencoded" --data-binary "index-time=123456789&qp=\*type.rdf::http://permid.org/ontology/organization/Organization,\*type.rdf::http://ont.com/mdaas/Organization"

>**Note:** You can use this syntax if you need to define a very long string for the **qp** value. This syntax allows sending the **qp** value as data rather than part of the URL, which avoids the length limitation that some clients impose on GET commands.

## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; |  Values&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example
:----------|:-------------|:--------|:---------
index-time | An optional parameter that contains the time from which you want to start streaming. If omitted, all results are returned, regardless of their index time. Set the index-time value to be the indexTime value returned in the last infoton you read. | An integer value. | index-time=1476811252896

## Code Example ##

### Call ###

    curl -vX GET '<cm-well-host>/example.org/Individuals?op=create-consumer'

> **Note:** Use the -v (verbose) flag, as you'll need to retrieve the position identifier from the **X-CM-WELL-POSITION** response header.

### Results ###

    < HTTP/1.1 200 OK
    < Content-Type: text/plain; charset=utf-8
    < X-CM-WELL-POSITION: -AAteJwzqDGoMTE0MDI1NjS0BIEa_dSKxNyCnFS9_KJ0fc-8lMyyzJTSxJzimpoa
    < X-CMWELL-Hostname: michael-laptop
    < X-CMWELL-RT: 1
    < X-CMWELL-Version: 1.3.x-SNAPSHOT
    < Content-Length: 0

## Notes ##
For a faster implementation of streaming, see [bulk consume](API.Stream.ConsumeNextBulk.md).

## Related Topics ##
[Consume Next Chunk](API.Stream.ConsumeNextChunk.md)
[Create Iterator](API.Stream.CreateIterator.md)
[Get Next Chunk](API.Stream.GetNextChunk.md)
[Consume Next Bulk](API.Stream.ConsumeNextBulk.md)



----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Stream.ConsumeNextBulk.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Stream.ConsumeNextChunk.md)  

----