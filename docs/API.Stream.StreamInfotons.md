# Function: *Stream Infotons* #

## Description ##
If you want to perform a one-time retrieval of a large amount of CM-Well data, you can do this by using the **stream** operation. You can define the data you want by its path in CM-Well, and optionally add a query filter and query parameters. The data returned is the list of infotons under the path you specified that match your query filter.

## Syntax ##

**URL:** \<CMWellHost\>
**REST verb:** GET
**Mandatory parameters:** op=stream

----------

**Template:**

    <cm-well-host>?op=stream&format=<format>&qp=<field conditions>

**URL example:** N/A

**Curl example (REST API):**

   `curl -X GET '<cm-well-host>/data.com?op=stream&format=ntriples&qp=type.rdf::http://ont.com/mdaas/Organization'`

## Code Example ##

### Call ###

    `curl -X GET '<cm-well-host>/data.com?op=stream&format=ntriples&qp=type.rdf::http://ont.com/mdaas/Organization'`

### Results ###

    /data.com/2-d9c64e914747b50cc0580159cafd672977f87a2d8f7c424cc0177d60699faa01
    /data.com/11-5040863809
    /data.com/2-66080e4c7e1711c91d26b608749e7c25a6add39595692be6d001604e2aac28ff
    /data.com/2-d82480d63501cecdd2b252087043e3a694c6cb1c20d126cbacfdc1aafa9478a0
    /data.com/2-91d3d82402a1ef373c3fa47510ffbf3eddab0cb10e8c14061fc1816e0972d8a8
    /data.com/2-f0a213ab46218e324fe706fecef492c2413b7a660bdfd7ce3c283f098a7c7e08
    /data.com/11-5014941732
    /data.com/2-1c1244bb2da2c93b3713c3268797fb3eda3955b794f742baa6c42c9e74a8749a
    /data.com/2-8d210ecb2c6e6c8407e7ad26df6a663bb75f48f1409e569104b54fac94869adc
    /data.com/1-39535720416
    /data.com/2-5755b0fc4a315fe7a4c161e49694aedf7a1e4bf0114b13399427aa3974ca7958
    /data.com/11-5027817174
    /data.com/2-cc45840b28a43791097e0dc092e3c6cc76c54692e55aa4e04585fc18f685cc01
    /data.com/2-26cfa845e59944ebdee5079e8dd622db85b593e214d17915b7a3431c646f3dc2
    /data.com/2-8550d1186554359939b3da9dd8c9809aba7ee03ff0a50d8b2916ac1370face28
    /data.com/2-5b4999182e5243784fedccfda13acbb31e8ef081206dcd3bc9cc6a84dbdc7aec
    /data.com/11-5015222445
    /data.com/2-2b152d666e8b3ed47a974a3212999b81d0c2ffcd483a3ad459a7efa51ce10fbe
    /data.com/2-672d1ece5bb588880c7aeafc23f05927da23fd759914ed4d6e36726eb2059ebf
    /data.com/2-948cad8a8a3bade9af7576495928a292256f3264641361c70c6fc092fe430289
    ...

## Notes ##

* A common way to handle data streams is to use the Chunked-Response HTTP Encoding protocol. As the size of the response is unknown when streaming starts, chunks of data are sent, each preceded by its size. A chunk of zero length indicates the end of the stream.
* If for any reason the streaming operation fails, you will have to retrieve all the data from the beginning. An alternative method is the [Consumer API](API.Stream.CreateConsumer.md), which is slower than streaming, but if interrupted allows you to resume from where you left off.
* There are several different implementations of the streaming feature, with different operation names (stream, nstream, mstream, qstream, sstream). They have different approaches and may work better for different types of data (e.g. a small collection vs. a larger collection). 
* If the format is supplied in the call, the resulting infotons' data is returned. If you omit the format parameter, only the infotons' URIs are returned.
* Only the following formats are supported for the **stream** command:

| Format   | format=&lt;query param&gt; | Mimetype            |
|:----------|:----------------------------|:---------------------|
Text | text | text/plain 
Tab-Separated Values | tsv | text/plain 
| NTriples | ntriples&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | text/plain          |
| NQuads   | nquads                     | text/turtle         |

## Related Topics ##
[Subscribing to Real-Time Updates](DevGuide.SubscribingToReal-TimeUpdates.md)


