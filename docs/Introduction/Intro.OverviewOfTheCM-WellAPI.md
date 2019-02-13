# API Overview 

This page provides a brief, high-level overview of the CM-Well API's functionality.

For the detailed API documentation, please see the CM-Well API Reference.

Before diving into the API, we recommend reading the following introductory topics: 

* [Introduction to CM-Well](Intro.IntroductionToCM-Well.md)
* [Technical Aspects of CM-Well](Intro.TechnicalAspectsOfCM-Well.md)
* [CM-Well Data Paradigms](Intro.CM-WellDataParadigms.md)
* [CM-Well Input and Output Formats](../APIReference/UsageTopics/API.InputAndOutputFormats.md)

CM-Well is a Linked Data repository that contains information from the TR Knowledge Graph and more. It is a software platform running in TR data centers and accessible to all TR developers. It supports read, write and query operations via a **REST API**.

CM-Well contains **RDF triples** with these components: **subject --> predicate --> object**. 
For example: CompanyA --> hasImmediateParent --> CompanyB. 

Since users are usually interested in a specific entity (a subject) and all of its accompanying attributes (predicates and objects), CM-Well supports a basic information unit called an **infoton** which contains exactly that (an entity and all of its accompanying attributes).

## CM-Well API Features

The following table provides a high-level summary of the CM-Well API's functionality, and where to read more about each feature:

Feature  | Reference
:---------|:----------
Get infoton | [Basic Queries](../DeveloperGuide/DevGuide.BasicQueries.md)
Search by field value | [Advanced Queries](../DeveloperGuide/DevGuide.AdvancedQueries.md)
Advanced search using SPARQL | [Using SPARQL on CM-Well Infotons](../DeveloperGuide/DevGuide.UsingSPARQLOnCM-WellInfotons.md)
Traverse inbound/outbound links | [Traversing Outbound and Inbound Links](../APIReference/Traversal/API.Traversal.Intro.md)
Stream data | [Streaming Data from CM-Well](../DeveloperGuide/DevGuide.StreamingDataFromCM-Well.md)
Subscribe to real-time updates | [Subscribing to Real-Time Updates](../DeveloperGuide/DevGuide.SubscribingToReal-TimeUpdates.md)
CM-Well Downloader | [Using the CM-Well Downloader](../AdvancedTopics/Tools/Tools.UsingTheCM-WellDownloader.md)
CM-Well Ingester | [Using the CM-Well Ingester](../AdvancedTopics/Tools/Tools.UsingTheCM-WellIngester.md)
Create a new infoton | [Updating CM-Well](../DeveloperGuide/DevGuide.UpdatingCM-Well.md)
Add/update infoton fields | [Updating CM-Well](../DeveloperGuide/DevGuide.UpdatingCM-Well.md)
Delete infotons and fields | [Updating CM-Well](../DeveloperGuide/DevGuide.UpdatingCM-Well.md)
 
