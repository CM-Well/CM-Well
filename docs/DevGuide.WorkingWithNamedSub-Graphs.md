# Working with Named Sub-Graphs #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](DevGuide.UsingElasticSearchStatistics.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](DevGuide.CM-WellSecurityFeatures.md)  

----

Sometimes you may want to mark certain field values with a label. The CM-Well feature that supports this is called a "named sub-graph", where the label is the name, and the sub-graph are the items in the Linked Data graph that have the specific label. In this case, relationships in the graph are represented not by triples but by quads, which include the subject, predicate, object and label (sub-graph name).

The named sub-graph feature allows you to manipulate data within the sub-graph. For example, you can search for or delete fields by sub-graph name.

>**Note:** The named sub-graph feature is also referred to as "quads", which relates to the fourth quad value which is the sub-graph name or label.

For example, suppose you are maintaining a database of movies under CM-Well. A movie may receive several review scores from several different sources. You want to save all the scores as "Score" field values, but label each value according to the reviewing entity. You can do this using named sub-graphs. 

## Creating a Named Sub-Graph ##
To create a field that belongs to a named sub-graph, create the field using a quad instead of a triple, where the fourth value of the quad is the sub-graph name that you choose.

>**Note:** The sub-graph name must be a valid URI.

For example, let's add some movies to our repository:

    curl -X POST "<cm-well-host>/_in?format=ttl" -H "Content-Type: text/plain" --data-binary 
    '<http://example.org/movies/ET> a <http://dbpedia.org/ontology/Film>.
    <http://example.org/movies/GoneWithTheWind> a <http://dbpedia.org/ontology/Film>.
    <http://example.org/movies/TheAvenger> a <http://dbpedia.org/ontology/Film>.'

Now let's add some review scores to the movies, using quads:

    curl -X POST "<cm-well-host>/_in?format=nquads" -H "Content-Type: text/plain" --data-binary 
    '<http://example.org/movies/ET> <http://MyOntology/Score> "8.3" <http://MyOntology/RottenTomatoes>.
    <http://example.org/movies/ET> <http://MyOntology/Score> "8.7" <http://MyOntology/NewYorkTimes>.
    <http://example.org/movies/GoneWithTheWind> <http://MyOntology/Score> "6.5" <http://MyOntology/RottenTomatoes>.
    <http://example.org/movies/GoneWithTheWind> <http://MyOntology/Score> "8.9" <http://MyOntology/NewYorkTimes>.
    <http://example.org/movies/TheAvenger> <http://MyOntology/Score> "7.2" <http://MyOntology/RottenTomatoes>.
    <http://example.org/movies/TheAvenger> <http://MyOntology/Score> "7.7" <http://MyOntology/MovieGoers>.'
    
## Retrieving Quads by their Label ##

You can retrieve all quads with a certain label by running a command with the following template:

    <cm-well-host>/<cm-well-path>?op=search&recursive&qp=quad.system::<quadValue>

For example, the following command retrieves all quads under the PPE **example.org** folder, whose label (4th quad value) is **http://MyOrgs/Startups**:

    <cm-well-host>/example.org?op=search&recursive&qp=quad.system::http://MyOrgs/Startups

## Deleting/Replacing a Named Sub-Graph ##

You can delete an entire named sub-graph by using the following command syntax:

    curl -X POST "<cm-well-host>/_in?format=ntriples" -H "Content-Type: text/plain" --data '<> <cmwell://meta/sys#replaceGraph> <http://MyOntology/MovieGoers>. '

If the **replaceGraph** data is followed by one or more new quads, they will be inserted into the sub-graph, effectively performing a replacement rather than just a deletion, as follows:

    curl -X POST "<cm-well-host>/_in?format=nquads" -H "Content-Type: text/plain" --data 
    '<> <cmwell://meta/sys#replaceGraph> <http://MyOntology/MovieGoers>. 
    <http://example.org/movies/ET> <http://MyOntology/Score> "10" <http://MyOntology/MovieGoers>.
    <http://example.org/movies/GoneWithTheWind> <http://MyOntology/Score> "10" <http://MyOntology/MovieGoers>.
    <http://example.org/movies/TheAvenger> <http://MyOntology/Score> "10" <http://MyOntology/MovieGoers>.'

> **Note:** 
> A **replaceGraph** call will only succeed under the following conditions:
> * The number of infotons in the sub-graph does not exceed 10,000. 
> * The infotons in the call data can be retrieved within 60 seconds. 
> * The number of graph replace statements does not exceed 20 in a single POST command. 
> 
> If you need to delete a large number of infotons, an alternative to **replaceGraph** is to retrieve all the sub-graph's infotons using an appropriate query, then delete each infoton.
    
## Using a Wildcard to Delete Multiple Fields ##

You can use the special wildcard indicator <*> to delete multiple fields in a single command. You can either delete all values of a specific field, or all values related to a specific sub-graph.

For example, let's add some data for a single infoton, that is associated with two different sub-graphs:

    curl -X POST "<cm-well-host>/_in?format=nquads" -H "Content-Type: text/plain" --data-binary
    '<http://example.org/parents/Dad> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://http://dbpedia.org/ontology/person> <http://example.org/males>.
    <http://example.org/parents/Dad> <http://http://dbpedia.org/ontology/wife> "Betty" <http://example.org/females>.
    <http://example.org/parents/Dad> <http://http://dbpedia.org/ontology/child> "Johnny" <http://example.org/males>.
    <http://example.org/parents/Dad> <http://http://dbpedia.org/ontology/child> "Teddy" <http://example.org/males>.
    <http://example.org/parents/Dad> <http://http://dbpedia.org/ontology/child> "Sarah" <http://example.org/females>.
    <http://example.org/parents/Dad> <http://http://dbpedia.org/ontology/child> "Nancy" <http://example.org/females>.'

We have created a "Dad" person, who has a wife and two children belonging to the "female" named sub-graph, and two children belonging to the "male" named sub-graph.

If we now run this command, using <*> instead of a field name:

    curl -X POST "<cm-well-host>/_in?format=nquads" -H "Content-Type: text/plain" --data-binary '<http://example.org/parents/Dad> <cmwell://meta/sys#markReplace> <*> <http://example.org/males>.'

- all of Dad's fields belonging to the "male" sub-graph are deleted (in other words, all of Dad's male children).

If, on the other hand, we run this command, using <*> instead of a graph name:

    curl -X POST "<cm-well-host>/_in?format=nquads" -H "Content-Type: text/plain" --data-binary '<http://example.org/parents/Dad> <cmwell://meta/sys#markReplace> <http://http://dbpedia.org/ontology/child> <*>.'

- all of Dad's fields of the type "child" are deleted, regardless of whether they belong to the "male" or "female" sub-graph.

<a name="NamedGraphAliases"></a>
## Using String Labels as Sub-Graph Aliases ##

As mentioned above, the sub-graph name must be a valid URI. You can also add a simple string label to the graph, as an alias for the graph's name (the 4th value of its quads). This can be more convenient and readable when working with named sub-graphs.

For example, to create the alias **"superman"** for the graph name ```<http://example.org/graphs/superman>```, you can run the following command:

    curl <cm-well-host>/_in?format=nquads --data-binary '<> <cmwell://meta/sys#graphAlias> "superman" <http://example.org/graphs/superman> .'

You can then use the string label instead of the name URI in any API call that refers to named graphs. 
For example, using the example label created above, this command:

    <cm-well-host>/example.org?op=search&recursive&qp=quad.system::superman

\- is equivalent to this command:

    <cm-well-host>/example.org?op=search&recursive&qp=quad.system::http://example.org/graphs/superman


>**Note:** You can only add one alias per named sub-graph. Creating an alias when one already exists will overwrite the existing alias.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](DevGuide.UsingElasticSearchStatistics.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](DevGuide.CM-WellSecurityFeatures.md)  

----