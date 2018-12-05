# Function: *Add Infotons and Fields* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Subscribe.Unsubscribe.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.ReplaceFieldValues.md)  

----

## Description ##
CM-Well allows you to create your own infotons and triples as well as read existing ones. You can create new infotons in CM-Well, and add their field values by adding triples with the new infoton as their subject. To do this, you send a POST command to the _in endpoint.entity

When you add a field and its value, you add a triple with this format:

    <infotonURI> <fieldID> <fieldValue>

If the first value does not refer to an existing infoton, the call will create both a new infoton and its field value. The fieldID must be in URI format (see [Field Name Formats](API.FieldNameFormats.md)).

>**Note:** 
To add more than one triple at a time (for instance, to add an infoton while at the same time adding some of its attributes), simply include all triples to add in the call's payload.

## Syntax ##

**URL:** \<cm-well-host\>/_in
**REST verb:** POST
**Mandatory parameters:** Triples to add.

----------

**Template:**

    curl -X POST "<cm-well-host>/_in?format=<format>" --data-binary <triples to add>

**URL example:** N/A

**Curl example (REST API):**

    curl -X POST "<cm-well-host>/_in?format=ttl" -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###
    <http://data.com/1-12345678> a <http://data.com/Person>; 
    <http://ont.com/bermuda/hasName> "Fred Fredson" .

## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Values&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example
:----------|:-------------|:--------|:---------
priority | If this flag appears in an update call, that call receives priority relative to non-priority calls. To use this flag, you must also pass the X-CM-WELL-TOKEN request header, with a token that grants access to the priority feature. This feature should be used with discretion so as not to flood the priority queue. | None | \<cm-well-host\>/_in?format=ttl&priority...

## Code Example ##

### Call ###

    curl -X POST "<cm-well-host>/_in?format=ttl" -H "Content-Type: text/plain" --data-binary @curlInput.txt

### File Contents ###
    <http://example.org/Individuals/FredFredson> a <http://data.com/Person>; 
    <http://ont.com/bermuda/hasName> "Fred Fredson" .

### Results ###

The result of the call above will simply be a success code. We can see the new infoton by running the following command:

    curl -X GET "<cm-well-host>/example.org/Individuals/FredFredson?format=ttl"
    
    @prefix nn:<cm-well-host>/meta/nn#> .
    @prefix bermuda: <http://ont.com/bermuda/> .
    @prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix sys:   <cm-well-host>/meta/sys#> .
    @prefix o: <http://example.org/Individuals/> .
    
    o:FredFredson  a  <http://data.com/Person> ;
    sys:dataCenter"dc1" ;
    sys:indexTime "1465231037915"^^xsd:long ;
    sys:lastModified  "2016-06-06T16:37:16.863Z"^^xsd:dateTime ;
    sys:parent"/example.org/Individuals" ;
    sys:path  "/example.org/Individuals/FredFredson" ;
    sys:type  "ObjectInfoton" ;
    sys:uuid  "ab8a75b12b158b4c305c3c2e52cf5758" ;
    bermuda:hasName   "Fred Fredson" .    

## Notes ##

* See [CM-Well Input and Output Formats](API.InputAndOutputFormats.md) to learn about the formats that CM-Well supports.
* If you are creating new infotons (which represent new entities), you will need to consult with the Information Architecture team about naming conventions. 
* If you add a value for a field that already exists, the first value is *not* overwritten. Instead, a second triple relationship is created, or in other word, another field with the same name and a different value is created. To learn how to overwrite a field value, see [Replace Field Values](API.Update.ReplaceFieldValues.md).
* When adding a new field value, you must use the full URI field name format, and not the prefix format (see [Field Name Formats](API.FieldNameFormats.md)).
* Some RDF formats (such as N3 and Turtle) support the special "a" type predicate, which indicates that the subject "is a" typed entity (as defined by the third element in the triple). The "a" predicate is shorthand for `<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>`.
* See [The Protocol System Field](API.ProtocolSystemField.md) to learn how the protocol value (**http** or **https**) of the infoton subject's URL is stored and handled.

## Related Topics ##
[Replace Field Values](API.Update.ReplaceFieldValues.md)
[CM-Well Input and Output Formats](API.InputAndOutputFormats.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Subscribe.Unsubscribe.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Update.ReplaceFieldValues.md)  

----