# CM-Well Version Release Notes - Dec. 15 2016 #

## Change Summary ##

### New Features ###

Feature/Link | Description
:-------------|:-----------
[SPARQL Trigger Processor Is Now an Internal CM-Well Agent](#hdr1) | The SPARQL Trigger Processor, which previously was an external utility of CM-Well, is now part of the CM-Well server and runs automatically.
[Alternative Syntax for "Create Consumer"](#hdr2) | Alternative syntax for "create consumer" API (uses POST instead of GET to enable long query values).
[Cache in SPARQL](#hdr3) | Cache added to SPARQL queries.

### Notable Bug Fixes ###
N/A 

### Changes to API	 ###

Alternative syntax for "create consumer" API (uses POST instead of GET to enable long query values).

------------------------------

## Feature Descriptions ##

<a name="hdr1"></a>
### SPARQL Trigger Processor Is Now an Internal CM-Well Agent ###

**Description:**

The SPARQL Trigger Processor, which previously was an external utility of CM-Well, is now part of the CM-Well server and runs automatically.

In addition, the agent has been enhanced to adjust its consumption rate dynamically according to local network conditions.

The YAML configuration file remains the same, except for the addition of the Boolean **active** flag, which you can use to enable or disable the specific configuration.

**Documentation:** 
[Using the SPARQL Trigger Processor](Tools.UsingTheSPARQLTriggerProcessor.md)

----------

<a name="hdr2"></a>
### Alternative Syntax for Create Consumer ###

**Description:**

Previously, the CM-Well API only supported a GET command for creating a consumer object (for bulk streaming of infotons). Now there is a new alternative syntax for creating a consumer, which uses POST instead of GET. The purpose of the new syntax is to avoid the length limitation that some clients impose on GET commands, in case the user needs to define a very long **qp** value. In the new syntax, the query parameters are passed as data rather than as part of the URL.

**Example:**


    curl -vX POST http://cm-well/permid.org?op=create-consumer -H "Content-Type:application/x-www-form-urlencoded" --data-binary "index-time=123456789&qp=\*type.rdf::http://permid.org/ontology/organization/Organization,\*type.rdf::http://ont.thomsonreuters.com/mdaas/Organization"


**Documentation:** [Create Consumer](API.Stream.CreateConsumer.md)

----------

<a name="hdr3"></a>
### Cache in SPARQL ###

**Description:**

A cache has been added to the SPARQL query feature. Queries that recur within a short period of time return results from the cache instead of causing the query to be rerun. This is transparent to the user and improves performance of SPARQL queries.


**Documentation:** N/A.
