# CM-Well Dos and Don'ts #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](DevGuide.ManagingUsers.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](DevGuide.BestPractices.TOC.md)  

----

## Don'ts ##

### Don't Use Queues to Store Requests to CM-Well ###

We have encountered CM-Well clients that manage an internal queue for write requests to CM-Well. The internal queue often ends up being a performance bottleneck, especially for queue packages with persistent storage like Apache Kafka. Using a queue that's internal to the client is unnecessary, as CM-Well manages its own request queues. In the rare case where a write request receives a 504 HTTP error ("gateway timeout"), the best practice is to perform a brief sleep and retry the request.

Similarly, there is no need to use a client queue to store read requests, and again, doing so may create a performance bottleneck. For bulk read operations, the best practice is to use CM-Well's **consume** or **bulk-consume** APIs. In case of an error during the read process, to ensure that no data was lost, simply resume reading while supplying the position token you received from the last successful read.

### Don't Use the CM-Well Prefix in Triples/Quads Data ###

When you direct a query to CM-Well, you provide the HTTP address of the CM-Well environment you're targeting as the host name. For example, the following query is directed to the CM-Well pre-production environment:

    <cm-well-host>?op=search&qp=type.rdf:http://data.com/Person&recursive

However, when you provide triple/quad data to upload to CM-Well, neither the explicit host name (perhaps "cm-well-ppe.int.thomsonreuters.com", "cm-well-prod.int.thomsonreuters.com", etc.) nor the "virtual" host name ("cmwell://") should appear **in the data** (although it still appears as the host name parameter). CM-Well infoton URIs shold be provided in their "relative path" form, which is "http://" followed by the path under the CM-Well root.

For example:

    <http://example.org/Individuals/FredFredson> a <http://data.com/Person>.

The path `http://example.org/Individuals/FredFredson` is then mapped to the full path, as appropriate for the specific CM-Well host, e.g. `<cm-well-host>/example.org/Individuals/FredFredson` for the production environment, `<cm-well-host>/example.org/Individuals/FredFredson` for the PPE environment, and so on.

Using an explicit or virtual CM-Well path in uploaded data may result in the creation of unexpected path names.

**Don't** provide triples/quad data such as:

    <cmwell://example.org/Individuals/FredFredson> a <http://data.com/Person>.
    <cmwell/example.org/Individuals/FredFredson> a <http://data.com/Person>.

### Don't Mix URL-Encoded and Non-Encoded Characters in the Same SPARQL Query ###

If you need to include non-English characters in a SPARQL query (including the PATHS sections), add the `Content Type: text/plain;charset=utf-8` header to the HTTP request. If you choose to URL-encode the non-English characters, make sure to encode *all* of them (don't mix URL-encoded and non-encoded characters in the same query). This is because once CM-Well detects URL-encoding, it will assume that all characters are URL-encoded.

**Examples:**

Encode *all* non-English characters:

    curl -X POST <cm-well-host>/_sp?format=json -H "Content-Type:text/plain;charset=utf-8" --data-binary 'PATHS
    /data.com?op=search&qp=*[organizationNameLocalAka.fedapioa:%D0%9E%D0%A2%D0%9A%D0%A0%D0%AB%D0%A2%D0%9E%D0%95%20%D0%90%D0%9A%D0%A6%D0%98%D0%9E%D0%9D%D0%95%D0%A0%D0%9D%D0%9E%D0%95%20%D0%9E%D0%91%D0%A9%D0%95%D0%A1%D0%A2%D0%92%D0%9E]&xg=1&with-data&length=100

    SPARQL
    SELECT * WHERE { ?s ?p ?o }'

Encode *no* non-English characters:

    curl -X POST <cm-well-host>/_sp?format=json -H "Content-Type:text/plain;charset=utf-8" --data-binary 'PATHS
    /data.com?op=search&qp=*[organizationNameLocalAka.fedapioa:ОТКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО]&xg=1&with-data&length=100

    SPARQL
    SELECT * WHERE { ?s ?p ?o }'


### Don't Use Blank Nodes ###

Several types of RDF notation support **blank nodes** (also known as **bnodes**;  resources represented by blank nodes are also called **anonymous resources**). These are RDF nodes that have no defined URI. (see the [Wikipedia Blank Node entry](https://en.wikipedia.org/wiki/Blank_node) to learn more.) It's possible to define a temporary ID for the node such as **x**. However, such temporary identifiers are limited in scope only to a serialization of a particular RDF graph.

While supported in certain notations (RDF/XML, RDFa, Turtle, N3 and N-Triples) and providing short term convenience for defining complex structures and for protecting sensitive information from browser access, as a rule **using blank nodes is considered bad practice when working with large linked data stores like CM-Well, and is therefore discouraged**. Best practice for linked data is to produce reusable, permanent URIs for objects.

Disadvantages of using blank nodes include:

* They're limited to the document in which they appear.
* They reduce the potential for interlinking between different linked data sources.
* They make it much more difficult to merge data from different sources.

CM-Well does not support blank nodes in its own data, and therefore when it encounters one, it creates an arbitrary URI for it. Since this is the case, we recommend not using blank nodes in data you intend to upload to CM-Well, and instead creating an informative URI for the relevant node, such as one containing a hash of its field values.

## Dos ##

### Use Correct Syntax in Windows/Unix Command-Line ###

If you're using the Curl command-line utility to submit calls to CM-Well, you may encounter syntax errors that arise from differences between the command-line syntax for Windows and for Unix operating systems. See [Using the Curl Utility to Call CM-Well](DevGuide.CurlUtility.md) to learn more.

### Minimize the Number of Infoton Fields ###

Sometimes infoton fields contain links to other infotons, in order to express a relationship between two entities. For example, an infoton representing a company might refer to another company infoton as its parent company, or its supplier.

However, if you find you're creating many such infoton link fields in one infoton, you should consider creating intermediate objects that encapsulate several fields.

For example, instead of a company infoton that contains 5 SuppliesTo fields and 4 SuppliedBy fields, you can create a SupplierRelationship object that contains all 9 relationships, and refer to it in a single field of the company infoton.

This is better for CM-Well performance because:

* Multiple inbound/outbound links in one infoton can result in long processing times for queries that traverse these links. Such calls may even time out.
* Because of CM-Well's "immutable data" principle, every update of an infoton creates a new copy of the infoton. The more fields it has, the more storage these copies use. In the case of relationships that are relatively static over time, this is unnecessary and avoidable.

### Leave %50-%60 of Disk Space Free on a CM-Well Node ###

When using a CM-Well node in an operational or near-operational scenario, make sure that about %50-%60 of free disk space is maintained. This is because storage maintenance operations that CM-Well performs may temporarily use large amounts of disk space.

This recommendation is also relevant for installations of CM-Well Docker that are run in near-operational conditions.  

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](DevGuide.ManagingUsers.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](DevGuide.BestPractices.TOC.md)  

----
