# Tips for Testing and Debugging #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) 

----

**On this page:**

Here are some tips for facilitating testing and debugging of your CM-Well client application:

[Using the debug-info Flag in Search Operations](#hdr1)

[Using the verbose Flag in SPARQL Queries](#hdr2)

[Using the show-graph Flag in SPARQL Queries](#hdr3)

[Testing RDF Syntax with the dry-run Flag](#hdr4)

[Obtaining Processing Time from the X-CMWELL-RT Header](#hdr5)


<a name="hdr1"></a>
## Using the debug-info Flag in Search Operations ##

If a search operation is not behaving as expected, you may want to examine the precise query that CM-Well sends to the Elastic Search module (that is responsible for full-text search on infotons).

To do this, add the **debug-info** flag to the query, as follows:

    curl "<cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Disney&debug-info&format=json&pretty&length=1" 

>**Note:** The **debug-info** flag only works with the following output formats: json,jsonl,yaml.

The response then contains the **searchQueryStr** attribute, as follows:

    {
      "type" : "SearchResponse",
      "pagination" : {
    		"type" : "PaginationInfo",
    		"first" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-21T23%3A57%3A44.918Z&to=2016-07-21T23%3A57%3A44.918Z&qp=CommonName.mdaas%3ADisney&length=1&offset=0",
    		"self" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-21T23%3A57%3A44.918Z&to=2016-07-21T23%3A57%3A44.918Z&qp=CommonName.mdaas%3ADisney&length=1&offset=0",
    		"next" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-21T23%3A57%3A44.918Z&to=2016-07-21T23%3A57%3A44.918Z&qp=CommonName.mdaas%3ADisney&length=1&offset=1",
    		"last" : "http://cm-well-host.com/permid.org?format=json?&op=search&from=2016-07-21T23%3A57%3A44.918Z&to=2016-07-21T23%3A57%3A44.918Z&qp=CommonName.mdaas%3ADisney&length=1&offset=11005"
      },
      "results" : {
    		"type" : "SearchResults",
    		"fromDate" : "2016-07-21T23:57:44.918Z",
    		"toDate" : "2016-07-21T23:57:44.918Z",
    		"total" : 11005,
    		"offset" : 0,
    		"length" : 1,
    		"infotons" : [ {
      			"type" : "ObjectInfoton",
      			"system" : {
    				"uuid" : "2571c64bd1e86d4651448ea4105e9044",
    				"lastModified" : "2016-07-21T23:57:44.918Z",
    				"path" : "/permid.org/1-21592415660",
    				"dataCenter" : "dc1",
    				"indexTime" : 1469145465545,
    				"parent" : "/permid.org"
      			}
    		} ],
        "searchQueryStr" : "{\n  \"from\" : 0,\n  \"size\" : 1,\n  \"query\" : {\n\"filtered\" : {\n  \"query\" : {\n\"bool\" : {\n  \"must\" : {\n\"match\" : {\n  \"CommonName.mdaas\" : {\n\"query\" : \"Disney\",\n\"type\" : \"phrase\"\n  }\n}\n  }\n}\n  },\n  \"filter\" : {\n\"bool\" : {\n  \"must\" : [ {\n\"term\" : {\n  \"parent\" : \"/permid.org\"\n}\n  }, {\n\"range\" : {\n  \"lastModified\" : {\n\"from\" : null,\n\"to\" : null,\n\"include_lower\" : true,\n\"include_upper\" : true\n  }\n}\n  } ]\n}\n  }\n}\n  },\n  \"fields\" : [ \"type\", \"system.path\", \"system.uuid\", \"system.lastModified\", \"content.length\", \"content.mimeType\", \"linkTo\", \"linkType\", \"system.dc\", \"system.indexTime\", \"system.quad\" ],\n  \"sort\" : [ {\n\"system.lastModified\" : {\n  \"order\" : \"desc\"\n}\n  } ]\n}"
      }
    }
    
<a name="hdr2"></a>
## Using the verbose Flag in SPARQL Queries ##

You can add the **verbose** flag to SPARQL queries, to see a timed breakdown of the operations that the query performs.

For example, if you add the **verbose** flag to this query:

    curl -X POST "<cm-well-host>/_sp?format=ascii&verbose" -H "Content-Type:text/plain" --data-binary '
    PATHS
    /permid.org?op=search&qp=CommonName.mdaas:Marriott%20Ownership,organizationCity.mdaas:Orlando&with-data&xg=hasImmediateParent.mdaas>_

    SPARQL
    SELECT * WHERE {
    ?infoton <http://ont.thomsonreuters.com/mdaas/headquartersCommonAddress> ?Addr.
    }
    '

You get this response:

    Time metrics:
    Start        End          Duration     Type     Task                                                             # lines
    00:00:00.002 00:00:00.258 00:00:00.256 Subgraph /permid.org?op=search&qp=CommonName.mdaas:Marriott%20Ownershi... 141
    00:00:00.264 00:00:00.272 00:00:00.008 SPARQL   SELECT * WHERE { ?infoton <http://ont.thomsonreuters.com/mdaa... 3
    
    Results:
    ------------------------------------------------------------------------------------------------------------------
    | infoton                          | Addr                                                                        |
    ==================================================================================================================
    | <http://permid.org/1-4294969614> | "6649 Westwood Blvd Ste 300\nORLANDO\nFLORIDA\n32821-6066\nUnited States\n" |
    | <http://permid.org/1-5035948006> | "6649 Westwood Blvd\nORLANDO\nFLORIDA\n32821-8029\nUnited States\n"         | 
    ------------------------------------------------------------------------------------------------------------------

In the Time Metrics table, you can see the two query tasks: the search that collects infotons from CM-Well, and the SPARQL query that's applied to them. For each task, you see the following values:

* **Start** - the start time of the operation (in the format HH:MM:SS.MSC)
* **End** - the end time of the operation (in the format HH:MM:SS.MSC)
* **Duration** - the duration of the task's processing (in the format HH:MM:SS.MSC)
* **Type** - **Subgraph** for search on infotons; **SPARQL** for SPARQL queries
* **Task** - the query's text
* **# lines** - The number of lines (triples or quads) returned by matching the CM-Well query in the **Task** value.

<a name="hdr3"></a>
## Using the show-graph Flag in SPARQL Queries ##

You can add the **show-graph** parameter to SPARQL queries, in order to see the sub-graph that is produced by collecting the triples defined in the PATHS section. This can be helpful when you want to determine if there's a problem in the PATHS section or the SPARQL query.

>**Note:** You can only use this feature with the **format=ascii** flag and value.

For example, this call:

    curl -X POST "<cm-well-host>/_sp?format=ascii&show-graph" --data-binary '
    PATHS
    /example.org/Individuals?op=search&length=1000&with-data
    /example.org/Individuals/RonaldKhun
    /example.org/Individuals/JohnSmith?xg=3
    
    SPARQL
    SELECT DISTINCT ?name ?active WHERE { ?name <http://www.tr-lbd.com/bold#active> ?active . } ORDER BY DESC(?active)
    '

\- produces these results:

    Graph:
    <http://example.org/Individuals/DonaldDuck> <http://purl.org/vocab/relationship/knowsByReputation> <http://example.org/Individuals/MartinOdersky> .
    <http://example.org/Individuals/DonaldDuck> <http://www.tr-lbd.com/bold#active> "true" .
    <http://example.org/Individuals/DonaldDuck> <http://purl.org/vocab/relationship/mentorOf> <http://example.org/Individuals/JohnSmith> .
    <http://example.org/Individuals/RebbecaSmith> <http://www.tr-lbd.com/bold#active> "false" .
    <http://example.org/Individuals/RebbecaSmith> <http://purl.org/vocab/relationship/siblingOf> <http://example.org/Individuals/SaraSmith> .
    <http://example.org/Individuals/MartinOdersky> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/Individuals/RonaldKhun> .
    <http://example.org/Individuals/MartinOdersky> <http://www.tr-lbd.com/bold#active> "true" .
    <http://example.org/Individuals/BruceWayne> <http://purl.org/vocab/relationship/employedBy> <http://example.org/Individuals/DonaldDuck> .
    <http://example.org/Individuals/BruceWayne> <http://www.tr-lbd.com/bold#active> "true" .
    <http://example.org/Individuals/PeterParker> <http://www.tr-lbd.com/bold#active> "true" .
    <http://example.org/Individuals/PeterParker> <http://purl.org/vocab/relationship/worksWith> <http://example.org/Individuals/HarryMiller> .
    <http://example.org/Individuals/PeterParker> <http://purl.org/vocab/relationship/neighborOf> <http://example.org/Individuals/ClarkKent> .
    <http://example.org/Individuals/SaraSmith> <http://www.tr-lbd.com/bold#active> "true" .
    <http://example.org/Individuals/SaraSmith> <http://purl.org/vocab/relationship/siblingOf> <http://example.org/Individuals/RebbecaSmith> .
    <http://example.org/Individuals/RonaldKhun> <http://www.tr-lbd.com/bold#category> "deals" .
    <http://example.org/Individuals/RonaldKhun> <http://www.tr-lbd.com/bold#category> "news" .
    <http://example.org/Individuals/RonaldKhun> <http://www.tr-lbd.com/bold#active> "true" .
    <http://example.org/Individuals/RonaldKhun> <http://purl.org/vocab/relationship/collaboratesWith> <http://example.org/Individuals/MartinOdersky> .
    <http://example.org/Individuals/JohnSmith> <http://www.tr-lbd.com/bold#active> "true" .
    <http://example.org/Individuals/NatalieMiller> <http://www.tr-lbd.com/bold#active> "true" .
    <http://example.org/Individuals/NatalieMiller> <http://purl.org/vocab/relationship/childOf> <http://example.org/Individuals/HarryMiller> .
    <http://example.org/Individuals/HarryMiller> <http://purl.org/vocab/relationship/parentOf> <http://example.org/Individuals/NatalieMiller> .
    <http://example.org/Individuals/HarryMiller> <http://www.tr-lbd.com/bold#active> "true" .
    <http://example.org/Individuals/DaisyDuck> <http://purl.org/vocab/relationship/colleagueOf> <http://example.org/Individuals/BruceWayne> .
    <http://example.org/Individuals/DaisyDuck> <http://www.tr-lbd.com/bold#active> "false" .
    <http://example.org/Individuals/JohnSmith> <http://purl.org/vocab/relationship/parentOf> <http://example.org/Individuals/SaraSmith> .
    <http://example.org/Individuals/JohnSmith> <http://purl.org/vocab/relationship/friendOf> <http://example.org/Individuals/PeterParker> .
    
    Results:
    ------------------------------------------------------------
    | name   | active  |
    ============================================================
    | <http://example.org/Individuals/BruceWayne>| "true"  |
    | <http://example.org/Individuals/DonaldDuck>| "true"  |
    | <http://example.org/Individuals/HarryMiller>   | "true"  |
    | <http://example.org/Individuals/JohnSmith> | "true"  |
    | <http://example.org/Individuals/MartinOdersky> | "true"  |
    | <http://example.org/Individuals/NatalieMiller> | "true"  |
    | <http://example.org/Individuals/PeterParker>   | "true"  |
    | <http://example.org/Individuals/RonaldKhun>| "true"  |
    | <http://example.org/Individuals/SaraSmith> | "true"  |
    | <http://example.org/Individuals/DaisyDuck> | "false" |
    | <http://example.org/Individuals/RebbecaSmith>  | "false" |
    ------------------------------------------------------------

<a name="hdr4"></a>
## Testing RDF Syntax with the dry-run Flag ##

You can test the validity of your RDF input's syntax by adding the **dry-run** flag to an upload operation (directed to the _in endpoint). When this flag is added, the call only returns the validity status, without actually performing the upload.

For example, if you add the **dry-run** flag to this call:

    curl -X POST "<cm-well-host>/_in?format=ttl&dry-run" -H "Content-Type: text/plain" --data-binary "<http://example/Individuals/SantaClaus> a <http://data.com/Person>."
    
\- you get this response:

    {"success":true,"dry-run":true}

For this call:

    curl -X POST "<cm-well-host>/_in?format=ttl&dry-run" -H "Content-Type: text/plain" --data-binary "<http://example/Individuals/SantaClaus> is_a <http://data.com/Person>."

\- you get this response:

    {"success":false,"error":"[line: 1, col: 42] Unrecognized: is_a"}

<a name="hdr5"></a>
## Obtaining Processing Time from the X-CMWELL-RT Header ##

You can obtain the processing time for a call to CM-Well, by adding the **-v** flag to the curl command, and examining the value of the **X-CMWELL-RT** (RT = Run Time) response header. The time value is in milliseconds.

> **Note:** The **X-CMWELL-RT** value is the internal CM-Well processing time, without network latency. 

For example, this call:

    curl -v "<cm-well-host>/permid.org?op=search&qp=CommonName.mdaas:Coca%20Cola&length=1&format=ttl"

 \- returns this response:

    *   Trying 163.231.74.90...
    * Connected to cm-well-host.com (163.231.74.90) port 80 (#0)
    > GET /permid.org?op=search&qp=CommonName.mdaas:Coca%20Cola&length=1&format=ttl HTTP/1.1
    > Host: cm-well-host.com
    > User-Agent: curl/7.48.0
    > Accept: */*
    >
    < HTTP/1.1 200 OK
    < X-CMWELL-RT: 451
    < Content-Type: text/turtle;charset=UTF8
    < X-CMWELL-Version: 1.5.x-SNAPSHOT
    < X-CMWELL-Hostname: c053wmjcend06.int.thomsonreuters.com
    < Date: Wed, 20 Jul 2016 12:18:12 GMT
    < Content-Length: 2065
    <
    @prefix nn:<http://cm-well-host.com/meta/nn#> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix sys:   <http://cm-well-host.com/meta/sys#> .
    
    <http://permid.org/1-21570701963>
    sys:dataCenter"dc1" ;
    sys:indexTime "1469000214747"^^xsd:long ;
    sys:lastModified  "2016-07-20T07:36:53.295Z"^^xsd:dateTime ;
    sys:parent"/permid.org" ;
    sys:path  "/permid.org/1-21570701963" ;
    sys:type  "ObjectInfoton" ;
    sys:uuid  "043eaf8e1c751c285a08d1e2e89fb732" .
    
    [ sys:pagination  [ sys:first  <http://cm-well-host.com/permid.org?format=ttl?&op=search&from=2016-07-20T07%3A36%3A53.295Z&to=2016-07-20T07%3A36%3A53.295Z&qp=CommonName.mdaas%3ACoca+Cola&length=1&offset=0> ;
    sys:last   <http://cm-well-host.com/permid.org?format=ttl?&op=search&from=2016-07-20T07%3A36%3A53.295Z&to=2016-07-20T07%3A36%3A53.295Z&qp=CommonName.mdaas%3ACoca+Cola&length=1&offset=11943> ;
    sys:next   <http://cm-well-host.com/permid.org?format=ttl?&op=search&from=2016-07-20T07%3A36%3A53.295Z&to=2016-07-20T07%3A36%3A53.295Z&qp=CommonName.mdaas%3ACoca+Cola&length=1&offset=1> ;
    sys:self   <http://cm-well-host.com/permid.org?format=ttl?&op=search&from=2016-07-20T07%3A36%3A53.295Z&to=2016-07-20T07%3A36%3A53.295Z&qp=CommonName.mdaas%3ACoca+Cola&length=1&offset=0> ;
    sys:type   "PaginationInfo"
      ] ;
      sys:results [ sys:fromDate  "2016-07-20T07:36:53.295Z"^^xsd:dateTime ;
    sys:infotons  <http://permid.org/1-21570701963> ;
    sys:length"1"^^xsd:long ;
    sys:offset"0"^^xsd:long ;
    sys:toDate"2016-07-20T07:36:53.295Z"^^xsd:dateTime ;
    sys:total "11943"^^xsd:long ;
    sys:type  "SearchResults"
      ] ;
      sys:type"SearchResponse"
    ] .
    < Connection #0 to host cm-well-host.com left intact

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) 

----