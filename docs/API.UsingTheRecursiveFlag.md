# Using the recursive flag #

By default, when you query CM-Well under a certain path, CM-Well searches directly under that path and not in its child paths.

For example, if the Person infoton `<cm-well-host>/example/Individuals/JohnSmith` exists, and you perform the following query:

    <cm-well-host>/example?op=search&qp=type.rdf:http://data.com/Person

\- no results are returned. But if you add the **recursive** flag as follows:

    <cm-well-host>/example?op=search&qp=type.rdf:http://data.com/Person&recursive
    
\- the infoton is returned.

When you perform a search on a certain path using the recursive flag , all child paths are searched as well, and their child paths, and so on recursively.

>**Note:** If you're not getting the query results you expect, you may need to add the **recursive** flag to the query.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.SortingResultsWithTheSort-byFlag.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.UsingTheWith-dataFlag.md)  

----
