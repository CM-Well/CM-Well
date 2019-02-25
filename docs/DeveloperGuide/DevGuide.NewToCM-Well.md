# New to CM-Well?

If you're new to CM-Well, this page is a good place to familiarize yourself with some basic concepts about CM-Well.

## What is CM-Well?

CM-Well is a writable Linked Data repository, developed by [Thomson Reuters](http://www.thomsonreuters.com) & [Refinitiv](http://www.refinitiv.com) and used as its central Knowledge Graph database. CM-Well (Content Matrix Well) adheres to RDF principles, meaning that data is in a [standard](https://www.w3.org/RDF/), machine-readable format.

CM-Well is _not_ a triple store! Our focus is on high scale, immutability of changes and downstream distribution of content. In BI terminology, CM-Well is a [data warehouse](https://en.wikipedia.org/wiki/Data_warehouse),  you might use a triple store downstream as a [data mart](https://en.wikipedia.org/wiki/Data_mart).

You can read more about the key differentiating features of CM-Well in our [paper](https://iswc2017.semanticweb.org/paper-518/) accepted to [2017 International Semantic Web Conference](https://iswc2017.semanticweb.org).

The Refinitiv Knowledge Graph contains information about organizations, instruments and people in the world of finance, but you can use CM-Well for any kind of linked data. Thomson Reuters & Refinitiv are now offering CM-Well as an Open Source platform for the developer community to use and enrich.

## Key Features and Technology
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
* [Angular](https://angular.io) and [React](https://facebook.github.io/react/) for the UIs
* [Akka](http://akka.io/) for cluster management
* [Jena](https://jena.apache.org/) for Sparql and RDF conversions
* [Netty](https://netty.io) for network comms
* [Zookeeper](https://zookeeper.apache.org) for cluster configuration

## Learn More About CM-Well

Here are some introductory topics you can read if you're new to CM-Well:

Topic | Description
:-----|:-------------
[Introduction to CM-Well](../Introduction/Intro.IntroductionToCM-Well.md) | An overview of CM-Well and the business need it meets
[CM-Well Data Paradigms](../Introduction/Intro.CM-WellDataParadigms.md) | RDF, graph databases and other data paradigms related to CM-Well
[Overview of the CM-Well API](../Introduction/Intro.OverviewOfTheCM-WellAPI.md) | A brief functional overview of the CM-Well API 
[Technical Aspects of CM-Well](../Introduction/Intro.TechnicalAspectsOfCM-Well.md) | A technical overview of the CM-Well API and platform, and what you'll need in order to develop a client application

To learn about the CM-Well API, read **at least** these chapters of the CM-Well Developer Guide: 

* [Using the Curl Utility to Call CM-Well](DevGuide.CurlUtility.md)
* [Basic Queries](DevGuide.BasicQueries.md) 
* [Advanced Queries](DevGuide.AdvancedQueries.md)

To start getting some hands-on experience with CM-Well, work through one of the [CM-Well Tutorials](../Tutorials/Tutorial.Docker.md). You can install CM-Well on your laptop.

!!! note 
	We recommend completing all introductory material described above, including working through tutorials, before attempting a proof-of-concept project.