# Overview of the CM-Well API #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Intro.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Intro.CM-WellDataParadigms.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Intro.TechnicalAspectsOfCM-Well.md)  

----

This page provides a brief, high-level overview of the CM-Well API's functionality.

For the detailed API documentation, please see [CM-Well API Reference](API.TOC.md).

Before diving into the API, we recommend reading the following introductory topics: 

* [Introduction to CM-Well](Intro.IntroductionToCM-Well.md)
* [Technical Aspects of CM-Well](Intro.TechnicalAspectsOfCM-Well.md)
* [CM-Well Data Paradigms](Intro.CM-WellDataParadigms.md)
* [CM-Well Input and Output Formats](API.InputAndOutputFormats.md)

CM-Well is a Linked Data repository that contains information from the TR Knowledge Graph and more. It is a software platform running in TR data centers and accessible to all TR developers. It supports read, write and query operations via a **REST API**.

CM-Well contains **RDF triples** with these components: **subject --> predicate --> object**. 
For example: CompanyA --> hasImmediateParent --> CompanyB. 

Since users are usually interested in a specific entity (a subject) and all of its accompanying attributes (predicates and objects), CM-Well supports a basic information unit called an **infoton** which contains exactly that (an entity and all of its accompanying attributes).

## CM-Well API Features and References ##

The following table provides a high-level summary of the CM-Well API's functionality, and where to read more about each feature:

Feature  | Reference
:---------|:----------
Get infoton | [Basic Queries](DevGuide.BasicQueries.md)
Search by field value | [Advanced Queries](DevGuide.AdvancedQueries.md)
Advanced search using SPARQL | [Using SPARQL on CM-Well Infotons](DevGuide.UsingSPARQLOnCM-WellInfotons.md)
Traverse inbound/outbound links | [Traversing Outbound and Inbound Links](API.Traversal.TOC.md)
Stream data | [Streaming Data from CM-Well](DevGuide.StreamingDataFromCM-Well.md)
Subscribe to real-time updates | [Subscribing to Real-Time Updates](DevGuide.SubscribingToReal-TimeUpdates.md)
CM-Well Downloader | [Using the CM-Well Downloader](Tools.UsingTheCM-WellDownloader.md)
CM-Well Ingester | [Using the CM-Well Ingester](Tools.UsingTheCM-WellIngester.md)
Create a new infoton | [Updating CM-Well](DevGuide.UpdatingCM-Well.md)
Add/update infoton fields | [Updating CM-Well](DevGuide.UpdatingCM-Well.md)
Delete infotons and fields | [Updating CM-Well](DevGuide.UpdatingCM-Well.md)
 
----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Intro.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Intro.CM-WellDataParadigms.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Intro.TechnicalAspectsOfCM-Well.md)  

----