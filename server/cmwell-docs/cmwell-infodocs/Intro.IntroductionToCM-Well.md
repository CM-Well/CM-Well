# Introduction to CM-Well #

[What is CM-Well?](#hdr1)

[CM-Well as a Platform for Data Modeling](#hdr3)

[Why Should I Use CM-Well?](#hdr5)

[Getting Started with CM-Well](#hdr7)

<a name="hdr1"></a>
## What is CM-Well? ##
CM-Well is a writable [Linked Data](https://en.wikipedia.org/wiki/Linked_data) repository, which can be used to model data from various datasets.

CM-Well (Content Matrix Well) adheres to Open Data principles, meaning that its data is in a standard, machine-readable format. The CM-Well code is Open Source and available at [GitHub](https://github.com/thomsonreuters/CM-Well).

CM-Well's data is represented as a graph database, which means that it contains both entities and relationships between pairs of entities. For instance, it  contains facts such as "CompanyA is a subsidiary of CompanyB", or "PersonX works at CompanyA". Conceptually, a graph database is a structure composed of nodes representing entities, and directed arrows representing relationships among them. See the image below for an example.

<img src="./_Images/small-graph-database.png">

<a name="hdr3"></a>
## CM-Well as a Platform for Data Modeling ##
CM-Well can be used to build your own linked data repository. In addition to API calls for reading CM-Well information, there are calls for creating new entries and connections.

For example, you could create a data repository describing countries, their capitals, continents and currencies. For each of these entities, you would create a corresponding entity in CM-Well, with appropriate relationships between them (e.g. "Paris" - "is capital of" - "France").

<a name="hdr5"></a>
## Why Should I Use CM-Well? ##
CM-Well has many advantages and convenient features, including:

* Easy-to-use Linked Data sharing as a service.
* Direct access to entity info via URI.
* Does not require installation.
* Does not require downloading large files and subsequent processing and database insertion.
* Contains up-to-the-minute current info, as well as all historical versions of entities. 
* Supports retrieval, querying and filtering of content at different levels of granularity - from a single entity, to a stream of updates, to the entire repository.
* Enables navigation among related entities via the links returned within an entity's info.
* Allows you to create your own data repository.
* As opposed to standard triple-stores that focus only on reading relatively static data, CM-Well is specifically designed to handle data that is constantly updated. This includes features such as convenient upload, handling historical versions, subscribing to real-time updates, and more.

<a name="hdr7"></a>
## Getting Started with CM-Well ##

Here are some more topics to help you get started with CM-Well:

[Technical Aspects of CM-Well](Intro.TechnicalAspectsOfCM-Well.md)
[CM-Well Data Paradigms](Intro.CM-WellDataParadigms.md)
[CM-Well Input and Output Formats](API.InputAndOutputFormats.md)
[Overview of the CM-Well API](Intro.OverviewOfTheCM-WellAPI.md)
[Basic Queries](DevGuide.BasicQueries.md)



