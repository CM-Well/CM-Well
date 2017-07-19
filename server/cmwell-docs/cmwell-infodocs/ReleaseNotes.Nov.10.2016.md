# CM-Well Version Release Notes - Nov. 10 2016 #

## Change Summary ##

### New Features ###

Feature/Link | Description
:-------------|:-----------
[slow-bulk Flag for Handling Slow Network Connections](#hdr1) | More robust handling of streaming over slow network connections.
[CM-Well Downloader Switches Automatically among Consume Modes](#hdr2) | Switch among consume, bulk-consume and slow-bulk, according to network conditions.
[Status of New JVM Processes Reported in Health Dashboard](#hdr3) | Status of new queue-related JVMs now reported in Health Dashboard.
[Enhanced SPARQL Trigger Processor](#hdr4) | The SPARQL Trigger Processor tool for creating materialized views now takes a user defined configuration file.
Cassandra version upgrade | The Cassandra database component in CM-Well was upgraded to a new minor version.
[cmwell-import Directive in SPARQL Stored Procedures](#hdr5) | SPARQL "stored procedures" now support the cmwell-import directive, for importing other stored procedures commands, while supporting nested imports.


### Notable Bug Fixes ###

* Various bugs were fixed in the "SPARQL on whole graph" feature. (The bugs were related to variable binding, non-string type mangling in SPARQL queries, and more.) The bugs used to cause duplicated query results (mentioned in last version's Known Issues), no results when the query included a start-date, etc. Additional bug fixes for this feature are expected for the next version.
* Rebooting a CM-Well node failed to restart some of the new JVM processes. This could result in failure to ingest data to CM-Well.
* Relevant to Ops personnel: Fixed an issue in self-healing of CM-Well nodes. Rolling patch reboots resulted in loss of connection with Elastic Search components, causing nodes to retain an error status.
* If a user used the **qstream** flavor for a streaming operation, this could case duplicate results to be returned.

### Changes to API	 ###

* The new parameter **slow-bulk** was added to the `_bulk-consume` API, to handle bulk streaming for slower network connections.
* A new cmwell-import directive is now supported for SPARQL stored procedures.

------------------------------

## Feature Descriptions ##

<a name="hdr1"></a>
### slow-bulk Flag for Handling Slow Network Connections ###

**Description:**

The _bulk-consume API, when run over certain slow network connections (usually WAN connections) would result in timeouts and loss of connection, as network speed became a bottleneck in the process. To address this issue, the **slow-bulk** optional flag was added to the `_bulk-consume` API.

When the **slow-bulk** flag is used, large bulk chunks are still streamed, but at a slower rate that is compatible with slower network connections. 

**Documentation:** [Consume Next Bulk](API.Stream.ConsumeNextBulk.md).

----------

<a name="hdr2"></a>
### CM-Well Downloader Switches Automatically among Consume Modes ###

**Description:**

The CM-Well Downloader standalone tool now switches automatically among the consume, bulk-consume and slow-bulk modes, depending on network conditions. This adjusts chunk size and/or streaming rate so that timeouts and loss of connection can be avoided, while still streaming at the highest rate possible.

**Documentation:** N/A.

----------

<a name="hdr3"></a>
### Status of New JVM Processes Reported in Health Dashboard ###

**Description:**

The status of new JVM processes that handle queues is now reported in the CM-Well Health Dashboard. This allows Ops personnel to see the complete picture of JVM process statuses.

**Documentation:** N/A.

----------

<a name="hdr4"></a>
### Enhanced SPARQL Trigger Processor ###

**Description:**

The SPARQL Trigger Processor tool for creating materialized views of CM-Well data has been improved. It now takes a user-defined YAML configuration file. The configuration file allows you to define "sensors" that detect changes in specified CM-Well paths, and create materialized views using the SPARQL CONSTRUCT commands that you define.  

**Documentation:** [Using the SPARQL Trigger Processor](Tools.UsingTheSPARQLTriggerProcessor.md).

<a name="hdr5"></a>
### cmwell-import Directive in SPARQL Stored Procedures ###

**Description:**
SPARQL "stored procedures" (SPARQL CONSTRUCT commands that are uploaded to CM-Well as file infotons) now support an import directive that allows you to define "nested" imports, i.e. constructs that import other constructs. 

To use the new directive, add `#cmwell-import` on a separate line at the beginning of the SPARQL construct, followed by one space character and a list of comma-separated absolute CM-Well paths of other CONSTRUCT queries.

Example:

    #cmwell-import /queries2/a1,/queries2/a2

**Documentation:** [Using SPARQL on CM-Well Infotons > Using the IMPORT Directive to Apply Uploaded CONSTRUCTs](DevGuide.UsingSPARQLOnCM-WellInfotons.md#hdr8)

