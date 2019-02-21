# Quick Start

This page provides developers with a brief overview of how to build and run CM-Well, and how to get started with some basic CM-Well read and write operations.

## Basic Write and Read Operations

You can get started with CM-Well by running some basic write and read operations.

!!! note
	The Curl commands in the examples below assume that CM-Well is running on your local machine.

### Write Infotons and Fields

**Action**: Create 5 new infotons (an infoton is the basic unit of storage for RDF in CM-Well) under the path example/Individuals: MamaBear, PapaBear, BabyBear1, BabyBear2 and BabyBear3. Each bear has a hasName field with a name value.

**Curl command:**

```
curl -X POST "http://localhost:8080/_in?format=ntriples" --data-binary @input.txt
```

**File contents:**

```
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
```

**Response:**

```
{"success":true}
```

### Read Infotons

**Action:** Read the infotons you created in the previous step under example/individuals, with associated predicates.

**Curl command:**

```
curl "http://localhost:8080/example/Individuals?op=search&format=ttl&recursive&with-data"
```

**Response:**

!!! note
	The sys namespace and predicates in the response are metadata produced by CM-Well, not user-defined data.

```
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
```

## CM-Well Tutorials

See [CM-Well Tutorials](../Tutorials/Tutorial.Docker.md) to learn how perform more CM-Well operations.

## Developer Resources

See the following resources to learn more about CM-Well workflows, APIs and architecture:

* [CM-Well API Reference](../APIReference/Authorization/API.Auth.GeneratePassword.md)
* [CM-Well Developer Guide](DevGuide.QuickStart.md)
* [CM-Well Data Paradigms](../Introduction/Intro.CM-WellDataParadigms.md)
* [CM-Well High-Level Architecture](../Introduction/Intro.CM-WellHigh-LevelArchitecture.md)

## System Requirements

### Build Requirements

Before building CM-Well, you will need to install the following software packages:

* Java 8
* [Scala](http://www.scala-lang.org/download/) 2.12.4
* [SBT(Scala Build Tool)](https://github.com/sbt/sbt/releases/download/v1.1.4/sbt-1.1.4.zip) version 1.1.4 or later

>**Note** Currently, CM-Well only runs on Mac and Linux environments

### Run Requirements

* Java 8
* Scala 2.11.11
* 8 GB RAM

<a name="buildcmw"></a>
## Downloading and Building CM-Well

To build CM-Well from its code:

1.	Clone the CM-Well source branch from [CM-Well GitHub Code Page](https://github.com/CM-Well/CM-Well) to a local directory on your machine.
2.	On your machine, navigate to the `server` directory.
3.	To compile CM-Well, run the following Scala command: `sbt packageCmwell`
or preferably (for a complete rebuild and testing): `sbt ccft`

!!! note
	You may need to give sbt more heap to compile. A 2GB heap works, which you can set by specifying the following:

```
export SBT_OPTS="-Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M"
```

More detailed instructions can be found in the [Building CM-Well](../AdvancedTopics/Advanced.BuildingCM-Well.md).

## Code Repository Structure

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

## Running CM-Well

To run your compiled version of CM-Well:

1. Build CM-Well as described in [Downloading and Building CM-Well](#buildcmw).
1. Navigate to the `server/cmwell-cons/app` directory.
1. To launch the CM-Well console, run the following command: ```./cmwell.sh```
1. To install a Personal Edition (single-node installation) instance of CM-Well, run the following commands:

    ```
:load pe
    pe.install
    ```

>**Note**: Once CM-Well is installed, the CM-Well web UI is available at http://localhost:9000. See [CM-Well Web Interface](../WebInterface/CM-WellWebInterface.md) to learn more.



