# Apply SPARQL to Query Results #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Query.QueryForInfotonsUsingFieldConditions.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.ApplySPARQLToEntireGraph.md)  

----

## Description ##

SPARQL (SPARQL Protocol and RDF Query Language - pronounced "sparkle") is an industry-standard query language designed to work with graph databases. It is similar in syntax to SQL.

You can use SPARQL queries on CM-Well in order to apply database operations to infotons, which are the type of "database records" that CM-Well provides. 

A SPARQL query to CM-Well has two parts. The first part is a set of paths within CM-Well on which you want to run the SPARQL query. The "paths" are in fact regular queries to CM-Well, which produce a list of infotons. After retrieving this list, CM-Well applies the second part - the SPARQL query - to the list.

See [Using SPARQL on CM-Well Infotons](DevGuide.UsingSPARQLOnCM-WellInfotons.md) for detailed explanations and examples.

## Syntax ##

**URL:** <cm-well-host>/_sp
**REST verb:** POST
**Mandatory parameters:** PATHS section, SPARQL section (see below)

----------

**Template:**

    <cm-well-host>/_sp 
    PATHS
    <CMWellPath with query parameters, one per line>
    <CMWellPath with query parameters, one per line>
    <CMWellPath with query parameters, one per line>
    ...
    <empty line>
    SPARQL
    <SPARQL query - one or several separated by empty lines>

    <SPARQL query>

    <SPARQL query>
    ...

**URL example:** N/A

**Curl example (REST API):**

    See Code Example.

## Special Parameters ##

Each line in the PATHS section is a query to CM-Well, and its syntax is identical to that of a regular (non-SPARQL) query.

See [CM-Well Query Parameters](API.QueryParameters.md) to learn more about CM-Well query syntax.

In addition, here are some special parameters you can add to a SPARQL query:

Parameter&nbsp;&nbsp; | Description
:----------|:------------
quads | Returns RDF "quadruples" rather than "triples", meaning that in addition to the subject, predicate and object, it also returns the graph name for each triple.
verbose | Returns the time-metrics of data retrieval and processing operations, as well as the data itself.
show-graph | Shows the list of infotons, which are the result of the initial CM-Well query (and the input to the SPARQL query).

## Code Example ##

### Call ###

    curl -X POST "<cm-well-host>/_sp?format=ascii" @curlInput.txt

### File Contents ###

    PATHS
    /example.org/Individuals?op=search&length=1000&with-data
    /example.org/Individuals/RonaldKhun
    /example.org/Individuals/JohnSmith?xg=3
    
    SPARQL
    SELECT DISTINCT ?name ?active WHERE { ?name <http://www.lbd.com/bold#active> ?active . } ORDER BY DESC(?active)

>**Note:** The empty line between the end of the paths and the "SPARQL" header is mandatory.

### Results ###

    -------------------------------------------------------------
    | name| active  |
    =============================================================
    | <http://example.org/Individuals/BruceWayne> | "true"  |
    | <http://example.org/Individuals/DonaldDuck> | "true"  |
    | <http://example.org/Individuals/HarryMiller>| "true"  |
    | <http://example.org/Individuals/JohnSmith>  | "true"  |
    | <http://example.org/Individuals/MartinOdersky>  | "true"  |
    | <http://example.org/Individuals/NatalieMiller>  | "true"  |
    | <http://example.org/Individuals/PeterParker>| "true"  |
    | <http://example.org/Individuals/RonaldKhun> | "true"  |
    | <http://example.org/Individuals/SaraSmith>  | "true"  |
    | <http://example.org/Individuals/DaisyDuck>  | "false" |
    | <http://example.org/Individuals/RebbecaSmith>   | "false" |
    -------------------------------------------------------------

## Notes ##

* Currently, the only SPARQL operations that CM-Well supports are SELECT and CONSTRUCT.
* When referring to field names in SPARQL queries, you must use the full URI notation.
* For SPARQL queries, you must use the header “Content Type: text/plain”.
* If you need to include non-English characters in a SPARQL query (including the PATHS sections), add the `Content Type: text/plain;charset=utf-8` header to the HTTP request. If you choose to URL-encode the non-English characters, make sure to encode *all* of them (don't mix URL-encoded and non-encoded characters in the same query). This is because once CM-Well detects URL-encoding, it will assume that all characters are URL-encoded.
* Only the following output formats are supported for SPARQL queries: ascii, json, rdf, tsv, xml.
* To learn how to upload and use SPARQL "stored procedures", see [Using SPARQL on CM-Well Infotons](DevGuide.UsingSPARQLOnCM-WellInfotons.md).

## Related Topics ##
[Using SPARQL on CM-Well Infotons](DevGuide.UsingSPARQLOnCM-WellInfotons.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Query.QueryForInfotonsUsingFieldConditions.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.ApplySPARQLToEntireGraph.md)  

----