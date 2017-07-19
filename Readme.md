# Introduction #
CM-Well is a writable Linked Data repository, developed by Thomson Reuters and used as its central Knowledge Graph database. CM-Well (Content Matrix Well) adheres to RDF principles, meaning that its data is in a [standard](https://www.w3.org/RDF/), machine-readable format.

The Thomson Reuters Knowledge Graph contains information about organizations, instruments and people in the world of finance, but you can use CM-Well for any kind of linked data. Thomson Reuters is now offering CM-Well as an Open Source platform for the developer community to use and enrich.

## Key features & technology ##
CM-Well is based on a clustered architecture, with durable storage into [Apache Cassandra](http://cassandra.apache.org) and indexing via [Elastic Search](https://github.com/elastic/elasticsearch). Key features include:
* Horizontal scaling to support billions of triples
* Cross site replication
* REST based APIs
* All writes treated as immutable, storing previous versions
* Named graph (quad) support
* Fast (~10ms by subject, ~50ms for search) retrieval by subject and predicate query
* Support for SPARQL on sub-graph and a beta of full graph SPARQL
* Subscription by query for downstream consumers

Other key technologies used under the covers include:
* [Angular](https://angular.io) for the UI
* [Akka](http://akka.io/) for cluster management
* [Jena](https://jena.apache.org/) for Sparql and RDF conversions
* [Netty](https://netty.io) for network comms
* [Zookeeper](https://zookeeper.apache.org) for cluster configuration


## Linked Data Repositories ##
You may be interested in exploring the following open-access linked data repositories and products:

* [Thomson Reuters PermID financial entities](https://permid.org/)
* [Thomson Reuters Intelligent Tagging](https://financial.thomsonreuters.com/en/products/infrastructure/trading-infrastructure/intelligent-tagging-text-analytics.html)
* [GeoNames geographical data](http://www.geonames.org)
* [WorldCat library catalogs](http://worldcat.org)
* [Data.gov U.S. government data](https://www.data.gov/developers/semantic-web)

Here are more dataset reference lists: 

* [https://en.wikipedia.org/wiki/Linked_data#Datasets](https://en.wikipedia.org/wiki/Linked_data#Datasets)
* [https://www.w3.org/wiki/DataSetRDFDumps](https://www.w3.org/wiki/DataSetRDFDumps)

## Further Reading ##

Here are some more resources to help you learn about linked data and CM-Well:

* [Linked Data](https://en.wikipedia.org/wiki/Linked_data)
* [The future is graph shaped](https://blogs.thomsonreuters.com/answerson/future-graph-shaped)
* [Introduction to CM-Well](server/cmwell-docs/cmwell-infodocs/Intro.IntroductionToCM-Well.md)
* [Overview of the CM-Well API](server/cmwell-docs/cmwell-infodocs/Intro.OverviewOfTheCM-WellAPI.md)

# Getting Started #

## Installing and Running CM-Well over Docker ##

*Coming soon.*

## Basic Write and Read Operations ##

You can get started with CM-Well by running some basic write and read operations. 

>**Note:** The Curl commands in the examples below assume that CM-Well is running on your local machine.

### Write Infotons and Fields ###

**Action**: Create 5 new infotons (an infoton is the basic unit of storage for RDF in CM-Well) under the path example/Individuals: MamaBear, PapaBear, BabyBear1, BabyBear2 and BabyBear3. Each bear has a hasName field with a name value.

**Curl command:**

    curl -X POST "http://localhost:8080/_in?format=ntriples" --data-binary @input.txt

**File contents:**

    <http://example/Individuals/MamaBear> <http://purl.org/vocab/relationship/spouseOf> <http://example/Individuals/PapaBear> .
    <http://example/Individuals/MamaBear> <http://xmlns.com/foaf/0.1/givenName> "Betty".
    <http://example/Individuals/PapaBear> <http://xmlns.com/foaf/0.1/givenName> "Barney".
    <http://example/Individuals/BabyBear1> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/MamaBear>.
    <http://example/Individuals/BabyBear1> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/PapaBear>.
    <http://example/Individuals/BabyBear1> <http://xmlns.com/foaf/0.1/givenName> "Barbara".
    <http://example/Individuals/BabyBear1> <http://purl.org/vocab/relationship/siblingOf> <http://example/Individuals/BabyBear2>.
    <http://example/Individuals/BabyBear1> <http://purl.org/vocab/relationship/siblingOf> <http://example/Individuals/BabyBear3>.
    <http://example/Individuals/BabyBear2> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/MamaBear>.
    <http://example/Individuals/BabyBear2> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/PapaBear>.
    <http://example/Individuals/BabyBear2> <http://xmlns.com/foaf/0.1/givenName> "Bobby".
    <http://example/Individuals/BabyBear3> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/MamaBear>.
    <http://example/Individuals/BabyBear3> <http://purl.org/vocab/relationship/childOf> <http://example/Individuals/PapaBear>.
    <http://example/Individuals/BabyBear3> <http://xmlns.com/foaf/0.1/givenName> "Bert".

**Response:**

    {"success":true}

### Read Infotons ###

**Action:** Read the infotons you created in the previous step under example/individuals, with associated predicates. 

**Curl command:**

    curl "http://localhost:8080/example/Individuals?op=search&format=ttl&recursive&with-data" 

**Response:** 

>**Note:** the sys namespace and predicates in the response which are metadata from CM-Well, not the data you stored.


    @prefix nn:    <http://localhost:8080/meta/nn#> .
    @prefix foaf:  <http://xmlns.com/foaf/0.1/> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix rel:   <http://purl.org/vocab/relationship/> .
    @prefix sys:   <http://localhost:8080/meta/sys#> .
    
    <http://localhost:8080/example/Individuals/MamaBear>
    sys:dataCenter"lh" ;
    sys:indexTime "1470058071114"^^xsd:long ;
    sys:lastModified  "2016-08-01T13:27:49.241Z"^^xsd:dateTime ;
    sys:parent"/example/Individuals" ;
    sys:path  "/example/Individuals/MamaBear" ;
    sys:type  "ObjectInfoton" ;
    sys:uuid  "e6f36b01bec464f9e2a8d8b690590e31" ;
    foaf:givenName   "Betty" ;
    rel:spouseOf  <http://example/Individuals/PapaBear> .
    
    <http://localhost:8080/example/Individuals/BabyBear2>
    sys:dataCenter    "lh" ;
    sys:indexTime     "1470058071125"^^xsd:long ;
    sys:lastModified  "2016-08-01T13:27:49.241Z"^^xsd:dateTime ;
    sys:parent        "/example/Individuals" ;
    sys:path          "/example/Individuals/BabyBear2" ;
    sys:type          "ObjectInfoton" ;
    sys:uuid          "284a5a2438db15b5f8d9ef87795c0945" ;
    foaf:givenName   "Bobby" ;
    rel:childOf       <http://example/Individuals/PapaBear> , <http://example/Individuals/MamaBear> .

    <http://localhost:8080/example/Individuals/BabyBear3>
    sys:dataCenter    "lh" ;
    sys:indexTime     "1470058071112"^^xsd:long ;
    sys:lastModified  "2016-08-01T13:27:49.239Z"^^xsd:dateTime ;
    sys:parent        "/example/Individuals" ;
    sys:path          "/example/Individuals/BabyBear3" ;
    sys:type          "ObjectInfoton" ;
    sys:uuid          "671d93482c72b51cef5afa18c71692a5" ;
    foaf:givenName   "Bert" ;
    rel:childOf       <http://example/Individuals/PapaBear> , <http://example/Individuals/MamaBear> .

    [ sys:pagination  [ sys:first  <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T13%3A27%3A49.239Z&to=2016-08-01T13%3A27%3A49.242Z&length=5&offset=0> ;
                sys:last   <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T13%3A27%3A49.239Z&to=2016-08-01T13%3A27%3A49.242Z&length=5&offset=5> ;
                sys:self   <http://localhost:8080/example/Individuals?format=ttl?&recursive=&op=search&from=2016-08-01T13%3A27%3A49.239Z&to=2016-08-01T13%3A27%3A49.242Z&length=5&offset=0> ;
                sys:type   "PaginationInfo"
              ] ;

    sys:results [ sys:fromDate  "2016-08-01T13:27:49.239Z"^^xsd:dateTime ;
                sys:infotons  <http://localhost:8080/example/Individuals/MamaBear> , <http://localhost:8080/example/Individuals/BabyBear1> , <http://localhost:8080/example/Individuals/PapaBear> , <http://localhost:8080/example/Individuals/BabyBear3> , <http://localhost:8080/example/Individuals/BabyBear2> ;
                sys:length    "5"^^xsd:long ;
                sys:offset    "0"^^xsd:long ;
                sys:toDate    "2016-08-01T13:27:49.242Z"^^xsd:dateTime ;
                sys:total     "5"^^xsd:long ;
                sys:type      "SearchResults"
              ] ;
      sys:type"SearchResponse"
    ] .
    
    <http://localhost:8080/example/Individuals/BabyBear1>
    sys:dataCenter    "lh" ;
    sys:indexTime     "1470058071125"^^xsd:long ;
    sys:lastModified  "2016-08-01T13:27:49.242Z"^^xsd:dateTime ;
    sys:parent        "/example/Individuals" ;
    sys:path          "/example/Individuals/BabyBear1" ;
    sys:type          "ObjectInfoton" ;
    sys:uuid          "1627fe787d44b5a4fff19f50181b585b" ;
    foaf:givenName   "Barbara" ;
    rel:childOf       <http://example/Individuals/MamaBear> , <http://example/Individuals/PapaBear> ;
    rel:siblingOf     <http://example/Individuals/BabyBear2> , <http://example/Individuals/BabyBear3> .

    <http://localhost:8080/example/Individuals/PapaBear>
    sys:dataCenter    "lh" ;
    sys:indexTime     "1470058071120"^^xsd:long ;
    sys:lastModified  "2016-08-01T13:27:49.241Z"^^xsd:dateTime ;
    sys:parent        "/example/Individuals" ;
    sys:path          "/example/Individuals/PapaBear" ;
    sys:type          "ObjectInfoton" ;
    sys:uuid          "6513a8d6395af8db932f49afb97cbfd1" ;
    foaf:givenName   "Barney" .

## CM-Well Tutorial ##

See [CM-Well Tutorials](server/cmwell-docs/cmwell-infodocs/Tutorial.HandsOnExercisesTOC.md) to learn how perform more CM-Well operations.

## Developer Resources ##

See the following resources to learn more about CM-Well workflows, APIs and architecture:

* [CM-Well API Reference](server/cmwell-docs/cmwell-infodocs/API.TOC.md)
* [CM-Well Developer Guide](server/cmwell-docs/cmwell-infodocs/DevGuide.TOC.md)
* [CM-Well Data Paradigms](server/cmwell-docs/cmwell-infodocs/Intro.CM-WellDataParadigms.md)
* [CM-Well High-Level Architecture](server/cmwell-docs/cmwell-infodocs/Intro.CM-WellHigh-LevelArchitecture.md)

# System Requirements #

## Build Requirements ##

Before building CM-Well, you will need to install the following software packages:

* Java 8
* Scala 2.11.11
* SBT(Scala Build Tool) version 0.13 or later

## Run Requirements ##

* Java 8
* Scala 2.11.11
* 8 GB RAM

<a name="buildcmw"></a>
# Downloading and Building CM-Well #

To build CM-Well from its code:

1.	Clone the CM-Well source branch from [CM-Well GitHub Code Page](https://github.com/thomsonreuters/CM-Well) to a local directory on your machine.
2.	On your machine, navigate to the `server` directory.
3.	To compile CM-Well, run the following Scala command: `sbt packageCmwell`
or preferably (for a complete rebuild and testing): `sbt ccft`

## Code Repository Structure ## 

The following tables describe CM-Well’s code directory structure:

**Top-level directories:**

Directory |	Description
:---------|:------------
cm-well	| Root directory
server	| Main CM-Well server directory


**Server directories:**

Directory |	Description
:---------|:------------
cmwell-batch | 	Old background indexing and persisting process
cmwell-bg	 | New background indexing and persisting process
cmwell-common	 | Common utility classes for several projects 
cmwell-cons | 	CM-Well install/upgrade/maintenance console
cmwell-controller | 	A wrapper around Akka Cluster library to orchestrate all components 
cmwell-dao | 	Cassandra driver
cmwell-data-tools | 	Auxiliary wrappers around CM-Well APIs, to be used as library by CM-Well client developers
cmwell-data-tools-app	 | cmwell-data-tools as a stand-alone app (can be used in Linux shell) 
cmwell-dc	 | Distributed Container. In charge of: health control, data center synchronization, etc.
cmwell-docs	 | CM-Well documentation
cmwell-domain	 | CM-Well basic entities (e.g. Infoton)
cmwell-formats	 | Output formatters for CM-Well data (e.g. to nquads or trig)
cmwell-fts	 | Full-text-search wrapper around ElasticSearch
cmwell-grid	 | ActorSystem wrapper library used to manage actors across a CM-Well cluster
cmwell-imp	 | Infoton merge process used by the background process for incremental updated support
cmwell-indexer	 | Indexes data in ElasticSearch using FTS (and used by the background process)
cmwell-irw	 | Infoton read from/write to Cassandra (using dao)
cmwell-it	 | Integration tests
cmwell-kafka-assigner | Kafka assigner utility
cmwell-plugin-gremlin | _sp plugin to support Gremlin (graph traversal language)
cmwell-rts	 | Real time search a.k.a pub-sub (real time update of changes in the data according to a certain query)
cmwell-spa	 | CM-Well default web-app
cmwell-sparql-agent	 | SPARQL Agent Processor
cmwell-stortill	 | Utilities for manual data fixes
cmwell-tlog	 | TransactionLog (ingest queue) file mechanism. Old mechanism of communication between WS and the old background process.
cmwell-tracking | 	Tracking capabilities
cmwell-util	 | Common utilities that are used by multiple projects
cmwell-ws	 | Web Service (Play! Framework application)
cmwell-zstore	 | A distributed key/value store, using Cassandra. Used by multiple projects.
project	 | Build instructions and mechanism (SBT)

# Running CM-Well #

To run your compiled version of CM-Well:

1. Build CM-Well as described in [Downloading and Building CM-Well](#buildcmw).
1. Navigate to the `server/cmwell-cons/app` directory.
1. To launch the CM-Well console, run the following command: ```./cmwell.sh```
1. To install a Personal Edition (single-node installation) instance of CM-Well, run the following commands:

    ```
    :load pe
    pe.install
    ```

>**Note**: Once CM-Well is installed, the CM-Well web UI is available at http://localhost:9000. See [CM-Well Web Interface](server/cmwell-docs/cmwell-infodocs/CM-WellWebInterface.md) to learn more.

# Support #

## Reporting Issues ##
You can report CM-Well issues at the [CM-Well GitHub Issues Page](https://github.com/thomsonreuters/CM-Well/issues).

## Discuss ##

We use [Gitter](https://gitter.im/thomsonreuters/CM-Well) to talk about CM-Well. Feel free to come join us!

# License #
CM-Well is Open Source and available under the Apache 2 License. See the [CM-Well License Page](https://github.com/thomsonreuters/CM-Well/blob/master/LICENSE) to learn more.
