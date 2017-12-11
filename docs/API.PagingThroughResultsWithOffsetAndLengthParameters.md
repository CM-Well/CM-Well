# Paging through Results with Offset and Length Parameters #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.FromAndToDatetimeFormatting.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.SortingResultsWithTheSort-byFlag.md)  

----


Query results may often be long lists of infotons. Rather than receive all the results at once, you may want to "page through" the results, that is, iteratively receive subsets of the results.

To do this, you use two parameters: the **offset** parameter and the **length** parameter. 

For example, suppose you want to search for the string “Marriott”, and you want to receive the results in 40-infoton increments. You would request the first "page" using this query:

    <cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Marriott&offset=0&length=40

To get the second page, you use use this query:

    <cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Marriott&offset=40&length=40

And so on. To retrieve the results of page N, you define the **offset** value as (N-1)*length.

> **Notes:** 
> * If **length** is supplied without an **offset** value, an offset of 0 is assumed.
> * CM-Well allows a maximum **offset** of 1000 and a maximum **length** of 10,000. For queries with very large result sets, rather than use a **search** query, it is recommended to use the [Iterator](API.Stream.CreateIterator.md) or [Consumer](API.Stream.CreateConsumer.md) APIs. 

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.FromAndToDatetimeFormatting.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.SortingResultsWithTheSort-byFlag.md)  

----