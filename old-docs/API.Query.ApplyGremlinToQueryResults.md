# Apply Gremlin to Query Results #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Query.ApplySPARQLToEntireGraph.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.DataStatistics.md)  

----

## Description ##

Gremlin is a graph traversal language that provides a convenient, compact syntax for performing queries on graphs. It is a DSL (Domain Specific Language) on top of the Groovy programming language (thus it uses the Groovy syntax). CM-Well supports a plugin that allows you to apply Gremlin queries to infotons.

A Gremlin query to CM-Well has two parts. The first part is a set of paths within CM-Well on which you want to run the Gremlin query. The "paths" are in fact regular queries to CM-Well, which produce a list of infotons. After retrieving this list, CM-Well applies the second part - the Gremlin query - to the list.

When CM-Well retrieves the list of infotons defined by the PATHS section of your query, it assigns the list as the value of the Groovy variable "g". You can then use the "g" variable to run the Gremlin query (see example below).

Literal infoton field values are mapped to Vertex Properties as follows:

* Scalar values are mapped as simple key/value pairs.
* Multiple values of the same field are implemented as arrays.

## Syntax ##

**URL:** <CMWellHost>/_sp
**REST verb:** POST
**Mandatory parameters:** PATHS section, Gremlin section (see below)

----------

**Template:**

    <CMWellHost>/_sp? 
    PATHS
    <CMWellPath with query parameters, one per line>
    <CMWellPath with query parameters, one per line>
    <CMWellPath with query parameters, one per line>
    ...
    <empty line>
    Gremlin
    <Gremlin query>

>**Note:** Unlike the SPARQL plugin, the Gremlin plugin does not support multiple Gremlin queries in a single call.

**URL example:** N/A

**Curl example (REST API):**

See Code Example.

## Special Parameters ##

None.

## Code Example ##

The query example refers to this input data:

    curl -X POST "<cm-well-host>/_in?format=nquads" -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###

      <http://example.org/Individuals/DaisyDuck> <http://purl.org/vocab/relationship/colleagueOf> <http://example.org/Individuals/BruceWayne> .
      <http://example.org/Individuals/DaisyDuck> <http://www.lbd.com/bold#active> "false" .
      <http://example.org/Individuals/BruceWayne> <http://purl.org/vocab/relationship/employedBy> <http://example.org/Individuals/DonaldDuck> .
      <http://example.org/Individuals/BruceWayne> <http://www.lbd.com/bold#active> "true" .
      <http://example.org/Individuals/DonaldDuck> <http://purl.org/vocab/relationship/mentorOf> <http://example.org/Individuals/JohnSmith> .
      <http://example.org/Individuals/DonaldDuck> <http://purl.org/vocab/relationship/knowsByReputation> <http://example.org/Individuals/MartinOdersky> .
      <http://example.org/Individuals/DonaldDuck> <http://www.lbd.com/bold#active> "true" .
      <http://example.org/Individuals/JohnSmith> <http://purl.org/vocab/relationship/friendOf> <http://example.org/Individuals/PeterParker> <http://example.org/graphs/spiderman> .
      <http://example.org/Individuals/JohnSmith> <http://purl.org/vocab/relationship/parentOf> <http://example.org/Individuals/SaraSmith> <http://example.org/graphs/spiderman> .
      <http://example.org/Individuals/JohnSmith> <http://www.lbd.com/bold#active> "true" .
      <http://example.org/Individuals/SaraSmith> <http://purl.org/vocab/relationship/siblingOf> <http://example.org/Individuals/RebbecaSmith> .
      <http://example.org/Individuals/SaraSmith> <http://www.lbd.com/bold#active> "true" .
      <http://example.org/Individuals/RebbecaSmith> <http://purl.org/vocab/relationship/siblingOf> <http://example.org/Individuals/SaraSmith> .
      <http://example.org/Individuals/RebbecaSmith> <http://www.lbd.com/bold#active> "false" .
      <http://example.org/Individuals/PeterParker> <http://purl.org/vocab/relationship/worksWith> <http://example.org/Individuals/HarryMiller> .
      <http://example.org/Individuals/PeterParker> <http://purl.org/vocab/relationship/neighborOf> <http://example.org/Individuals/ClarkKent> .
      <http://example.org/Individuals/PeterParker> <http://www.lbd.com/bold#active> "true" .
      <http://example.org/Individuals/HarryMiller> <http://purl.org/vocab/relationship/parentOf> <http://example.org/Individuals/NatalieMiller> .
      <http://example.org/Individuals/HarryMiller> <http://www.lbd.com/bold#active> "true" .
      <http://example.org/Individuals/NatalieMiller> <http://purl.org/vocab/relationship/childOf> <http://example.org/Individuals/HarryMiller> .
      <http://example.org/Individuals/NatalieMiller> <http://www.lbd.com/bold#active> "true" .
      <http://example.org/Individuals/MartinOdersky> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/Individuals/RonaldKhun> .
      <http://example.org/Individuals/MartinOdersky> <http://www.lbd.com/bold#active> "true" .
      <http://example.org/Individuals/RonaldKhun> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/Individuals/MartinOdersky> .
      <http://example.org/Individuals/RonaldKhun> <http://www.lbd.com/bold#active> "true" .
      <http://example.org/Individuals/RonaldKhun> <http://www.lbd.com/bold#category> "deals" .
      <http://example.org/Individuals/RonaldKhun> <http://www.lbd.com/bold#category> "news" .

### Call ###

    curl -X POST "<cm-well-host>/_sp" --data-binary @curlInput.txt

### File Contents ###

    PATHS
    /example.org/Individuals?op=search&length=1000&with-data
    /example.org/Individuals/RonaldKhun
    /example.org/Individuals/JohnSmith?xg=3
    
    Gremlin
    g.v("http://example.org/Individuals/PeterParker").outE.inV.filter{it.id.matches(".*Miller.*")}.outE.inV

### Results ###

`v[http://example.org/Individuals/NatalieMiller]`    

## Notes ##

* When referring to field names in Gremlin queries, you must use the full URI notation.
* When applying a Gremlin query to CM-Well data, there is no option to choose the output format. Output is formatted according to the Gremlin engine's default.

## Related Topics ##
[Apply SPARQL to Query Results](API.Query.ApplySPARQLToQueryResults.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Query.ApplySPARQLToEntireGraph.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Query.DataStatistics.md)  

----