# Streaming Data from CM-Well

If you want to perform a one-time retrieval of a large amount of CM-Well data, you can do this by using the **stream** operation. You can define the data you want by its path in CM-Well, and optionally add a query filter and query parameters. The data returned is the list of infotons under the path you specified that match your query filter. 

For example, the following command streams all Organization infotons from the CM-Well pre-production environment:

    curl -X GET '<cm-well-host>/data.com/?op=stream&format=ntriples&qp=type.rdf::http://ont.thomsonreuters.com/mdaas/Organization'

> **Notes:** 
>
>* If for any reason the streaming operation fails, you will have to retrieve all the data from the beginning. An alternative method is the [Consumer API](API.Stream.CreateConsumer.md), which is slower than streaming, but if interrupted allows you to resume from where you left off.
>* There are several different implementations of the streaming feature, with different operation names (stream, mstream, qstream, sstream). They have different approaches and may work better for different types of data (e.g. a small collection vs. a larger collection). 
>* You can also use the [bulk consume](API.Stream.ConsumeNextBulk.md) operation to stream large numbers of infotons. This is a "hybrid" approach between streaming and iteration.

## Streaming Formats

Only the following formats are supported for the **stream** command:

| Format   | format=&lt;query param&gt; | Mimetype            |
|:----------|:----------------------------|:---------------------|
| NTriples | ntriples                   | text/plain          |
| NQuads   | nquads                     | text/turtle         |


> **Note:** If the format is supplied in the call, the resulting infotons' data is returned. If you omit the format parameter, only the infotons' URIs are returned.
