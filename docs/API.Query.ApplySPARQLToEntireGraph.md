# Apply SPARQL to the Entire Graph #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Query.ApplySPARQLToQueryResults.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.ApplyGremlinToQueryResults.md)  

----

## Description ##

SPARQL (SPARQL Protocol and RDF Query Language - pronounced "sparkle") is an industry-standard query language designed to work with graph databases. It is similar in syntax to SQL.

You can use SPARQL queries on CM-Well in order to apply database operations to infotons, which are the type of "database records" that CM-Well provides. 

You can apply SPARQL queries to specific paths within the CM-Well graph, using the `_sp` endpoint (see [Apply SPARQL to Query Results](API.Query.ApplySPARQLToQueryResults.md)).

The `_sparql` endpoint initiates a query that is optimized to handle the **entire** CM-Well graph. Use this endpoint when you want to process the whole graph and not just specific paths within it.

## Syntax ##

**URL:** <CMWellHost>/_sparql
**REST verb:** POST
**Mandatory parameters:** SPARQL query (see below)

----------

**Template:**

    <CMWellHost>/_sparql <SPARQL query>

**URL example:** N/A

**Curl example (REST API):** See Code Example.

## Special Parameters ##

See [CM-Well Query Parameters](API.QueryParameters.md) to learn more about CM-Well query syntax.

In addition, here are some special parameters you can add to a SPARQL query:

Parameter&nbsp;&nbsp; | Description
:----------|:------------
verbose | Displays information about the query planning and execution runtimes.
intermediate-limit | Using this limit causes triples to be fetched in iterative batches of this size. This may help return at least partial results (rather than just getting a timeout), in case of large result sets. The value must be a positive integer.
results-limit | Limits the number of results returned. The value must be a positive integer.
explain-only | You can use this flag to see CM-Well's query optimization and execution plan, without actually running the query. See [Using the explain-only Flag](#explainSection) below for more details.

## Code Example ##

### Call ###

    curl -X POST "<cm-well-host>/_sparql?verbose&results-limit=5" --data-binary @curlInput.txt

### File Contents ###

    SELECT DISTINCT
    ?name ?active
    WHERE {
    ?name <http://www.tr-lbd.com/bold#active> ?active .
    } ORDER BY DESC(?active)


### Results ###

    [Plan 43] 00:00:00.093 Planning started.
    [Plan 43] 00:00:05.399 Planning completed.
    [Exec 43] 00:00:05.400 Executing started.
    [Exec 43] 00:00:05.944 Executing completed.
    
    -----------------------------------------------------------
    | name   | active |
    ===========================================================
    | <http://example.org/Individuals/BruceWayne>| "true" |
    | <http://example.org/Individuals/DonaldDuck>| "true" |
    | <http://example.org/Individuals/HarryMiller>   | "true" |
    | <http://example.org/Individuals/JohnSmith> | "true" |
    | <http://example.org/Individuals/MartinOdersky> | "true" |
    -----------------------------------------------------------

<a name="explainSection"></a>
## Using the explain-only Flag ##

You can use the optional **explain-only** flag to see CM-Well's query optimization and execution plan, without actually running the query. You might want to use this option to investigate why you're getting unexpected results from a whole-graph query, or when composing a new query, to understand how the query will be executed.

Using the **explain-only** flag displays the following information for the query:

1. The Abstract Syntax Tree of the given query, in Server-Sent Events format.
2. The count results of the different object types in the query.
3. Sorted Triple Patterns - the output of the optimizer before executing the ARQ query.

>**Note:** The explain-only output uses the internal CM-Well namespace representation. 

Here is an example of the output that is produced when you use the **explain-only** flag:

    [Expl] 00:00:00.001 AST:
    	(prefix 
			((data: <http://data.com/>)
     		(metadata: <http://data.schemas.financial.com/metadata/2009-09-01/>))
      		(project (?x ?name)
    			(order (?name)
      				(bgp
    					(triple ?typebridge metadata:geographyType data:1-308005)
    					(triple ?x metadata:geographyType ?typebridge)
    					(triple ?x metadata:geographyUniqueName ?name)
      	))))
    [Expl] 00:00:00.017 Objects count for geographyType.NWadZg: 324749
    [Expl] 00:00:00.035 Objects count for geographyUniqueName.NWadZg: 162148
    [Expl] 00:00:00.080 Sorted Triple Patterns:
    	?x @geographyUniqueName.NWadZg ?name
    	?typebridge @geographyType.NWadZg http://data.com/1-308005
    	?x @geographyType.NWadZg ?typebridge

## Notes ##

* Queries to the `_sparql` endpoint will time out if not completed within 90 seconds.
* This feature is currently in beta - not all queries will return
* Queries on quads are not yet supported for the `_sparql` endpoint.
* If only partial results are returned due to a timeout, a message stating this appears in the output.
* Currently, the only SPARQL operations that CM-Well supports are SELECT and CONSTRUCT.
* Using the SPARQL **OPTIONAL** and **FILTER** options is very expensive when performed on the entire graph. Avoid using these options if possible.
* When referring to field names in SPARQL queries, you must use the full URI notation.
* The `_sparql` endpoint does not support multiple output formats, and will ignore any value passed in the **format** parameter. Results of SELECT commands are sent in ASCII, and results of CONSTRUCT commands are sent as ntriples.
* To learn how to upload and use SPARQL "stored procedures", see [Using SPARQL on CM-Well Infotons](DevGuide.UsingSPARQLOnCM-WellInfotons.md).

## Related Topics ##
[Using SPARQL on CM-Well Infotons](DevGuide.UsingSPARQLOnCM-WellInfotons.md)
[Apply SPARQL to Query Results](API.Query.ApplySPARQLToQueryResults.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Query.ApplySPARQLToQueryResults.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.ApplyGremlinToQueryResults.md)  

----