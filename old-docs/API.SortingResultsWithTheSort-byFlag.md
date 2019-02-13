# Sorting Results with the sort-by Parameter #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.PagingThroughResultsWithOffsetAndLengthParameters.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.UsingTheRecursiveFlag.md)  

----

By default, query results are not sorted. In your query, you can specify a field by whose values you want the results to be sorted. You can also specify whether you want the results to be sorted in ascending or descending order.

To sort results, add the **sort-by** parameter to the query, with a value of the field name by which you want to sort.

For example, to sort by the CommonName field, you can run this query:

    <cm-well-host>/permid.org?op=search&format=n3&length=30&sort-by=CommonName.mdaas

You can also sort by several field values. When you provide several comma-separated field names in the **sort-by** parameter, the results are sorted first by the first field in the list, then by the second field, and so on.

For example, to sort first by exchange code and then by CommonName, you can run this query:

    <cm-well-host>/permid.org?op=search&format=n3&length=30&sort-by=IsTradingIn.mdaas,CommonName.mdaas

## Sort Order ##

By default, results are sorted in descending order of their modified time (i.e. newer infotons appear before older infotons). If you add the **sort-by** parameter with no order operator, then results are sorted in ascending order of the given field's values. You can also add an explicit sort-order operator before the field name: ```*``` for ascending order and ```-``` for descending order.

Ascending: `sort-by=*CommonName.mdaas`

Descending: `sort-by=-CommonName.mdaas`

>**Notes:** 
>* Sort order is only guaranteed when using text/list output formats (as opposed to RDF output formats): tsv, text, json, or yaml.
>* Adding `sort-by=system.score` to the query causes the results to be sorted in descending order of the matching score. The matching score is produced by Elastic Search, as a TFIDF (Term Frequency Inverse Document Frequency) score on the search terms. This means that the score is higher if the search terms appear more frequently in a specific infoton than in other infotons on average. Note that this feature does not work when the **with-data** flag is included in the query.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.PagingThroughResultsWithOffsetAndLengthParameters.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.UsingTheRecursiveFlag.md)  

----