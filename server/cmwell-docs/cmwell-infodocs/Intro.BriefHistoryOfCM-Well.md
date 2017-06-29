# A Brief History of CM-Well #

This page briefly describes the conceptual and technical motivations that led to the development of the CM-Well platform.

## TMS - In the Beginning... ##

CM-Well was developed as an initiative of TR's TMS (Text Management Services) division. TMS was once a startup company called ClearForest, which specialized in Natural Language Processing, text mining, data analysis and knowledge representation. Reuters (this preceded the merger with Thomson) acquired ClearForest in 2007, incorporating its highly relevant capabilities into its own products. 

TMS already had expertise in tagging entities, events and relationships in free-form text, and had implemented various methods for storage and analysis of the tagged data. To begin with, these methods did not conform to any widely-accepted standard. The data was stored in standard relational databases, although conceptually it behaved more like a connected graph.

<img src="./_Images/Graph-database.jpg">

## The Rise of Semantic Web ##
The newly emerging standards of Semantic Web and Linked Data seemed like a natural choice for the data modeling standards TMS was seeking. These standards define a way to represent information that's conceptually similar to how people perceive it, but such that it's as easily accessible as a web page, and as easy to extend as it is to click on a link. Another key principle that Linked Data supports is interoperability - the ability for different data repositories to refer to and interact with each other.

## CM-Well High-Level Requirements ##
TMS wanted to create an information repository that would be:

* A container for TR's central entities and their attributes and relationships.
* A general, writable platform for data modeling.
* A Linked Data repository.
* Easily accessible to other TR applications.
* Updated in real-time as well as maintaining historical information. 
* Scalable for huge amounts of data.

CM-Well was developed to implement all of these requirements.

## CM-Well Architecture ##
According to Linked Data and Web Oriented Architecture (WOA) best practices, the TMS team determined that the CM-Well service would support a REST API over HTTP, and that its data would be represented as Resource Description Format (RDF) "triples". (See [CM-Well Data Paradigms](Intro.CM-WellDataParadigms.md) for more information.)

At the time, existing products for storing triples were not scalable to the degree required by CM-Well. NoSQL-type databases implemented an object-based design, as required by CM-Well, but did not perform well enough in terms of real-time consistency and supporting a high-level query language.

The TMS team decided to develop a new platform, relying on off-the-shelf packages where possible. CM-Well utilizes the following 3rd-party infrastructure modules:

* Cassandra - distributed database management.
* ElasticSearch - search capabilities.
* SPARQL query language - sophisticated queries over an RDF structure.

## The CM-Well Vision ##
CM-Well's vision is to be an eco-system of Linked Data objects, whose primary purpose is to describe the real-world entities that interest TR's customers - those that are significant in the business and financial domains. These entities are both inter-linked among themselves, and linkable to any application or platform that supports the RDF standard.

In addition to being a repository for TR's central financial information, CM-Well is a "technology stack" or general platform, which allows you to model any information domain you wish and create your own repository.




