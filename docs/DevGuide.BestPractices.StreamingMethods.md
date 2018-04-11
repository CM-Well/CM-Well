# Streaming Methods #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.BestPractices.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](DevGuide.BestPractices.ManagingRetries.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](DevGuide.BestPractices.DeletingKnownFieldValues.md)  

----

There are various methods for streaming large numbers of infotons from CM-Well. The following table describes the three main methods and their pros and cons:

Method | Description | Reference
:-------|:-------------|:-----------
Stream | Implemented with thin management layers over the underlying indexing module's streaming feature. The operators **stream**, **mstream** and **sstream** are variants of the same type of streaming. This type of streaming is fast, but the results are unsorted, and there is no way to resume from the point of failure, if a failure occurs. | [Streaming](API.Stream.StreamInfotons.md)
Consume | To use this API, you use the **create-consumer** operator to retrieve a position token, and then call the **_consume** endpoint. This API is relatively slow, but its results are sorted by their **indexTime** value, and you can resume from the point of failure, if a failure occurs. Alternatively, you can use the **qstream** operator, which is also sorted and resumable. | [Consume](API.Stream.ConsumeNextChunk.md)
Bulk Consume | This is the fastest option. To use this API, you use the **create-consumer** operator to retrieve a position token, and then call the **_bulk-consume** endpoint. Results are not sorted within a chunk, but the time ranges the chunk are in sequence. This means you can both resume from the point of failure, and use parallel threads to download the data of each chunk's infoton URIs. Note that you receive the next chunk's token in the response header before you finish downloading the current chunk's data. So you can use the next token to start streaming the next chunk (or the next several chunks, using additional tokens received) in parallel to the current chunk. | [Bulk Consume](API.Stream.ConsumeNextBulk.md) 

The package cmwell.tools.data.downloader contains Scala [command line applications](Tools.UsingTheCM-WellDownloader.md) for exercising these methods. The examples directory also includes a simple Python script exercising the consume and bulk-consume methods.

## Downloading Infoton Data while Streaming ##

By default, all streaming methods retrieve only the relevant infoton URIs. To download the infoton's data, you can use one of the following options:

* Add the **with-data** flag to the streaming request. The results stream then contains infoton fields as well as URIs.
* Stream URIs only (the faster option), then retrieve each infoton's fields by dereferencing its URI. The advantage of this option is that you can run threads in parallel to retrieve the data.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.BestPractices.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](DevGuide.BestPractices.ManagingRetries.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](DevGuide.BestPractices.DeletingKnownFieldValues.md) 

----
