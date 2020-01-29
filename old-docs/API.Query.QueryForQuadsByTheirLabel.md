# Function: *Query for Quads by their Label* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Query.DataStatistics.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Stream.StreamInfotons.md)  

----

## Description ##
Sometimes you may want to group some infotons and/or field values together under a specific label. The CM-Well feature that supports this is called a "named sub-graph", where the label is the name, and the sub-graph are the items in the Linked Data graph that have the specific label. In this case, relationships in the graph are represented not by triples but by quads, which include the subject, predicate, object and label (sub-graph name).

See [Working with Named Sub-Graphs](DevGuide.WorkingWithNamedSub-Graphs.md) to learn more about this feature.

You can retrieve all quads with a certain label with one query to CM-Well.

## Syntax ##

**URL:** \<hostURL\>
**REST verb:** GET
**Mandatory parameters:** op=search&recursive&qp=quad.system::\<quadValue\>

----------

**Template:**

    <cm-well-path>?op=search&recursive&qp=quad.system::<quadValue>

**URL example:**
   <cm-well-host>/example.org?op=search&recursive&qp=quad.system::http://MyOrgs/Startups

**Curl example (REST API):**

    curl <cm-well-host>/example.org?op=search&recursive&qp=quad.system::http://MyOrgs/Startups

## Code Example ##

### Call ###

    curl "<cm-well-host>/example.org/movies?op=search&recursive&format=ttl&qp=quad.system::http://MyOntology/NewYorkTimes"

### Results ###

    @prefix nn:<cm-well-host/meta/nn#> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix sys:   <cm-well-host/meta/sys#> .
    
    <http://example.org/movies/TheAvenger>
    sys:dataCenter"dc1" ;
    sys:indexTime "1469543439610"^^xsd:long ;
    sys:lastModified  "2016-07-26T14:30:38.600Z"^^xsd:dateTime ;
    sys:parent"/example.org/movies" ;
    sys:path  "/example.org/movies/TheAvenger" ;
    sys:type  "ObjectInfoton" ;
    sys:uuid  "fe628dbe6298f263fecf548e85b33b37" .
    
    [ sys:pagination  [ sys:first  <cm-well-host>/example.org/movies?format=ttl?&recursive=&op=search&from=2016-07-06T20%3A38%3A33.085Z&to=2016-07-26T14%3A30%3A38.600Z&qp=quad.system%3A%3Ahttp%3A%2F%2FMyOntology%2FNewYorkTimes&length=4&offset=0> ;
    sys:last   <cm-well-host>/example.org/movies?format=ttl?&recursive=&op=search&from=2016-07-06T20%3A38%3A33.085Z&to=2016-07-26T14%3A30%3A38.600Z&qp=quad.system%3A%3Ahttp%3A%2F%2FMyOntology%2FNewYorkTimes&length=4&offset=4> ;
    sys:self   <cm-well-host>/example.org/movies?format=ttl?&recursive=&op=search&from=2016-07-06T20%3A38%3A33.085Z&to=2016-07-26T14%3A30%3A38.600Z&qp=quad.system%3A%3Ahttp%3A%2F%2FMyOntology%2FNewYorkTimes&length=4&offset=0> ;
    sys:type   "PaginationInfo"
      ] ;
      sys:results [ sys:fromDate  "2016-07-06T20:38:33.085Z"^^xsd:dateTime ;
    sys:infotons  <http://example.org/movies/GoneWithTheWind> , <http://example.org/movies/ET> , <http://example.org/movies/TheAvenger> ;
    sys:length"4"^^xsd:long ;
    sys:offset"0"^^xsd:long ;
    sys:toDate"2016-07-26T14:30:38.600Z"^^xsd:dateTime ;
    sys:total "4"^^xsd:long ;
    sys:type  "SearchResults"
      ] ;
      sys:type"SearchResponse"
    ] .
    
    <http://example.org/movies/GoneWithTheWind>
    sys:dataCenter"dc1" ;
    sys:indexTime "1468938865750"^^xsd:long ;
    sys:lastModified  "2016-07-19T14:34:24.579Z"^^xsd:dateTime ;
    sys:parent"/example.org/movies" ;
    sys:path  "/example.org/movies/GoneWithTheWind" ;
    sys:type  "ObjectInfoton" ;
    sys:uuid  "4d8acc0536c1e3bd299aec9a694b647d" .
    
    <http://example.org/movies/ET>
    sys:dataCenter"dc1" ;
    sys:indexTime "1468938865750"^^xsd:long , "1469467055354"^^xsd:long ;
    sys:lastModified  "2016-07-06T20:38:33.085Z"^^xsd:dateTime , "2016-07-19T14:34:24.579Z"^^xsd:dateTime ;
    sys:parent"/example.org/movies" ;
    sys:path  "/example.org/movies/ET" ;
    sys:type  "ObjectInfoton" ;
    sys:uuid  "ec183a3c152fe16057ed52a5cfc74d80" , "233e1ab260c4d18307e82138cf281050" .

## Notes ##
None.

## Related Topics ##
[Working with Named Sub-Graphs](DevGuide.WorkingWithNamedSub-Graphs.md)
[Add Infotons and Fields to a Sub-Graph](API.Update.AddInfotonsAndFieldsToSubGraph.md)
[Delete and Replace Field Values in Named Sub-Graphs](API.Update.DeleteOrReplaceValuesInNamedSubGraph.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Query.DataStatistics.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Stream.StreamInfotons.md)  

----
