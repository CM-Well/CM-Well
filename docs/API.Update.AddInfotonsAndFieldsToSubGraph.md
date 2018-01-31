# Function: *Add Infotons and Fields to a Sub-Graph* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.DeleteSpecificFieldValues.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.DeleteOrReplaceValuesInNamedSubGraph.md)  

----

## Description ##

Sometimes you may want to group some infotons and/or field values together under a specific label. The CM-Well feature that supports this is called a "named sub-graph", where the label is the name, and the sub-graph are the items in the Linked Data graph that have the specific label. In this case, relationships in the graph are represented not by triples but by quads, which include the subject, predicate, object and label (sub-graph name).

The named sub-graph feature allows you to manipulate data within the sub-graph. For example, you can search for or delete infotons and fields by sub-graph name.

For example, suppose you are maintaining a database of movies under CM-Well. A movie may receive several review scores from several different sources. You want to save all the scores as "Score" field values, but label each value according to the reviewing entity. You can do this using named sub-graphs. 

To create a field that belongs to a named sub-graph, create the field using a quad instead of a triple, where the fourth value of the quad is the sub-graph name that you choose.

>**Note:** You can also create a simple string alias for the graph name URI. See [Using String Labels as Sub-Graph Aliases](DevGuide.WorkingWithNamedSub-Graphs.md#NamedGraphAliases) to learn more.

## Syntax ##

**URL:** \<cm-well-host\>/_in
**REST verb:** POST
**Mandatory parameters:** Quads to add.

----------

**Template:**

    curl -X POST "<cm-well-host>/_in?format=nquads" --data-binary <quads to add>

**URL example:** N/A

**Curl example (REST API):**

    curl -X POST "<cm-well-host>/_in?format=nquads" -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###
    <http://example.org/movies/ET> <http://MyOntology/Score> "8.3" <http://MyOntology/RottenTomatoes>.
    <http://example.org/movies/ET> <http://MyOntology/Score> "8.7" <http://MyOntology/NewYorkTimes>.
    <http://example.org/movies/GoneWithTheWind> <http://MyOntology/Score> "6.5" <http://MyOntology/RottenTomatoes>.
    <http://example.org/movies/GoneWithTheWind> <http://MyOntology/Score> "8.9" <http://MyOntology/NewYorkTimes>.
    <http://example.org/movies/TheAvenger> <http://MyOntology/Score> "7.2" <http://MyOntology/RottenTomatoes>.
    <http://example.org/movies/TheAvenger> <http://MyOntology/Score> "7.7" <http://MyOntology/MovieGoers>.

## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Values&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example
:----------|:-------------|:--------|:---------
priority | If this flag appears in an update call, that call receives priority relative to non-priority calls. To use this flag, you must also pass the X-CM-WELL-TOKEN request header, with a token that grants access to the priority feature. This feature should be used with discretion so as not to flood the priority queue. | None | \<cm-well-host\>/_in?format=ttl&priority...

## Code Example ##

### Call ###

    curl -X POST "<cm-well-host>/_in?format=nquads" -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###
    <http://example.org/movies/ET> <http://MyOntology/Score> "8.3" <http://MyOntology/RottenTomatoes>.
    <http://example.org/movies/ET> <http://MyOntology/Score> "8.7" <http://MyOntology/NewYorkTimes>.
    <http://example.org/movies/GoneWithTheWind> <http://MyOntology/Score> "6.5" <http://MyOntology/RottenTomatoes>.
    <http://example.org/movies/GoneWithTheWind> <http://MyOntology/Score> "8.9" <http://MyOntology/NewYorkTimes>.
    <http://example.org/movies/TheAvenger> <http://MyOntology/Score> "7.2" <http://MyOntology/RottenTomatoes>.
    <http://example.org/movies/TheAvenger> <http://MyOntology/Score> "7.7" <http://MyOntology/MovieGoers>.

### Results ###
    
    {"success":true}

## Notes ##

* The named sub-graph feature is also referred to as "quads", which relates to the fourth quad value which is the sub-graph name or label.
* The sub-graph name must be a valid URI.

## Related Topics ##
[Working with Named Sub-Graphs](DevGuide.WorkingWithNamedSub-Graphs.md)
[Delete and Replace Field Values in Named Sub-Graphs](API.Update.DeleteOrReplaceValuesInNamedSubGraph.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.DeleteSpecificFieldValues.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.DeleteOrReplaceValuesInNamedSubGraph.md)  

----