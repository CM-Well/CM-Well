# Function: *Create Iterator* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Stream.StreamInfotons.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Stream.GetNextChunk.md)  

----

## Description ##
If you wish to retrieve a large number of infotons, but you want to iterate over small "chunks" of data in a controlled fashion, you can use the **create-iterator** and **next-chunk** APIs. This allows you to request the number of infotons you want to process, and receive only that number during each iteration.

The process requires two different API calls:
1. Call **create-iterator** to receive an iterator ID (in the **iteratorId** field) for the query.
2. Repeatedly call **next-chunk**, specifying a **length** value, to receive that number of infotons. When you call **next-chunk**, you pass the iterator ID you received when you called **create-iterator**. The process ends when CM-Well returns an empty list.

## Syntax ##

**URL:** \<cm-well-host\>/\<cm-well-path\>
**REST verb:** GET
**Mandatory parameters:** op=create-iterator

----------

**Template:**

    <cm-well-host>/<cm-well-path>?op=create-iterator&<parameters>

**URL example:**
   `<cm-well-host>/permid.org?op=create-iterator&session-ttl=15&length=500`

**Curl example (REST API):**

    Curl -X GET <cm-well-host>/permid.org?op=create-iterator&session-ttl=15&length=500

## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; |  Values&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example
:----------|:-------------|:--------|:---------
session-ttl | The time, in seconds, until the iteration session expires. The iteration token is only valid for this length of time. The default value is 15 seconds; the maximal value is 60 seconds. | A positive integer up to 60. | session-ttl=20 (20 seconds)

## Code Example ##

### Call ###

    <cm-well-host>/permid.org?op=create-iterator&length=5

### Results ###
       {"type":"IterationResults","iteratorId":"YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjE3Ny40OjM5MjczL3VzZXIvJHVEaSMtMTEzMjgyNDQ5OA","totalHits":90964672,"infotons":[]}

## Notes ##

* If the iteration process fails in the middle for any reason, you might be able to resume with last token within the TTL window. Otherwise you'll have to restart the process from the beginning (that is, iterate again over all infotons that match the query).
* An alternative is to use the **consumer** API (see **Related Topics**), which allows you to save the iteration state and restart from the same point after a failure.

## Related Topics ##
[Get Next Chunk](API.Stream.GetNextChunk.md)
[Create Consumer](API.Stream.CreateConsumer.md)
[Consume Next Chunk](API.Stream.ConsumeNextChunk.md)



----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Stream.StreamInfotons.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Stream.GetNextChunk.md)  

----
