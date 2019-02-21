# Zebra Update 2 (February 2019)

Title | Git Issue | Description 
:------|:----------|:------------
Modification to blank node rendering | [1014](https://github.com/thomsonreuters/CM-Well/pull/1014) | When ingesting RDF Data into CM-Well, previously blank nodes (nodes representing a resource for which a URI or literal is not given) were given a random ID and placed in ```http://blank_node/[id]``` for persistence. This meant that when rendering their RDF data, they were rendered as subjects of the "blank_node domain", and not as actual RDF Blank Nodes. This is now fixed. See more info and rendering examples at [Git Issue 1015](https://github.com/CM-Well/CM-Well/issues/1015).
yg chunk size enlarged for the GraphFeed application | [1044](https://github.com/thomsonreuters/CM-Well/pull/1044) | The GraphFeed application invokes CM-Well’s Consume API with yg. The maximum amount of infotons per chunk was previously 100, which lead to bad overall performance. The limit is now 3000, improving performance.


### Changes to API

None.


