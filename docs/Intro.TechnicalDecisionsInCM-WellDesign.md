# Technical Decisions in CM-Well's Design #

This page describes CM-Well's main technical features and design principles, the implementation options that were considered, and the decisions that were taken.

[Open, Linked Data and Web Oriented Architecture](#hdr1)

[RDF and Triple Stores](#hdr2)

[Information Object (Infoton)](#hdr3)

[Storage and Search Modules](#hdr4)

[Advanced Queries](#hdr5)

["Information in Motion"](#hdr6)

[Immutable Data and Deep History](#hdr7)


<a name="hdr1"></a>
## Open, Linked Data and Web Oriented Architecture ##
CM-Well is first and foremost a Linked Data repository, accessible via web service. Linked Data and Web Oriented Architecture (WOA) are global standards and design principles that model information so that it is extensible (by other data sets), easily machine-readable, and enables semantic queries and inference of implicit relationships among the modeled entities.

These are the most central principles that CM-Well implements, from which many other requirements are derived. (See [CM-Well Data Paradigms](Intro.CM-WellDataParadigms.md) to learn more.)

<a name="hdr2"></a>
## RDF and Triple Stores ##
The standard way of encoding Linked Data is in Resource Description Format (RDF), which is a way of describing *subject-predicate-object* triples. These triples represent either attributes belonging to entities (e.g. CompanyX - FoundedOnDate - 03/28/1998), or relationships between two entities (e.g. JohnSmith - WorksAt - Intel).

Since triples are the most basic unit of an RDF repository, the first requirement that CM-Well had to implement was to maintain a triple-store infrastructure that would be convenient, robust and scalable. 

Many triple-store products were assessed, including Oracle NoSQL, Apache JENA and BigData (now BlazeGraph). But none had the necessary scalability that was required for Thomson Reuters huge and ever-growing amount of data. It was decided to develop a thin proprietary layer to implement a scalable triple-store over 3rd-party storage packages (see [Storage and Search Modules](#hdr4)).

<a name="hdr3"></a>
## Information Object (Infoton) ##
Upon consideration of the type of data that would feed CM-Well (among other things, entities from the Thomson Reuters Knowledge Graph) it quickly became clear that a higher level of abstraction than the triple would be required. Most use cases focused on entities and their accompanying attributes and related entities. Thus the "information object" was born, and nicknamed "infoton". The infoton represents a single entity (such as an organization or person), and contains all the triples related to that entity.

As dictated by Linked Data principles, infotons were designed to be accessible via URI (Universal Resource Identifier).

Adding the infoton abstraction positioned CM-Well at the mid-point between key-value databases (the infoton's URI serves as a "key" for retrieving it) and document databases (the infoton itself is a kind of "document" containing all its related information).

<a name="hdr4"></a>
## Storage and Search Modules ##
CM-Well storage needed to be able to handle the magnitude of Big Data, while still enabling speedy retrieval and scalability. Again, several products were evaluated (including SONAR and Apache HBase) before deciding on the optimal tools.

CM-Well uses Apache Cassandra for table-based search (for direct access to infotons) and Elastic Search for full-text search capabilities. There is also a proprietary layer called FS-Logic, which hides implementation details from higher-level modules, allowing the infrastructure underneath to be changed if necessary.

<a name="hdr5"></a>
## Advanced Queries ##
Although the initial versions of CM-Well only supported direct URI access and traversing triples, later versions needed the added sophistication of complex queries. These were implemented using SPARQL - the standard query language for RDF stores. CM-Well uses the Jena SPARQL package.

<a name="hdr6"></a>
## "Information in Motion" ##
Many Linked Data repositories are geared to be "static", i.e. to represent a data set that hardly changes over time, which mostly serves read requests.

CM-Well is a dynamic data store, with capabilities for real-time updates and notifications. Users can create their own repository, modeling any data they choose. Users can also subscribe to get real-time notifications about changes to items of interest.

To support these functions, CM-Well has write functions in its API, which allow users to add, delete and update triples and infotons. It also supports PuSH notifications (an extension of RSS feeds that provides real-time notifications over HTTP).

<a name="hdr7"></a>
## Immutable Data and Deep History ##
CM-Well supports an "immutable data" paradigm, meaning that even though infotons may be updated over time, any version of an infoton that you retrieve will always exist in the CM-Well repository, possibly as a "historical" version. The "main" version (the one retrieved by the entity's URI) is always the most current version. 

This way, you have the option of both obtaining the up-to-the-minute information, while still being able to access historical versions for analytical purposes. 
Historical versions allow you to examine changes in attribute values over time.




