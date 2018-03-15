# CM-Well Version Release Notes - Quetzal (February 2018) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Piranha.January.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Rhino.March.2018.md)

----

## Change Summary ##


 Title | Git Issue | Description 
:------|:----------|:------------
New **parallelism** query parameter for sstream & bulk-consume | [338](https://github.com/thomsonreuters/CM-Well/pull/338) | Allows controlling the number of parallel processing threads during streaming. Default is 10.
Play upgrade | [402](https://github.com/thomsonreuters/CM-Well/pull/402) | Play minor version upgrade from 2.6.7 to 2.6.11
Akka upgrade | [402](https://github.com/thomsonreuters/CM-Well/pull/402) | Akka libraries minor version upgrade from 2.5.6 to 2.5.9
Full-text search improvements | [414](https://github.com/thomsonreuters/CM-Well/pull/414) | Infrastructure for improving FTS performance. Solves some cases of timeouts in Elastic Search client layer.
The SPARQL Triggered Processor Engine now uses **consume** rather than **bulk-consume** | [427](https://github.com/thomsonreuters/CM-Well/pull/427) | This improves STP's robustness.
New fields in STP Dashboard | [454](https://github.com/thomsonreuters/CM-Well/pull/454) | See \<cm-well-host\>/proc/stp.md to view the dashboard, and see [Using the SPARQL Triggered Processor](Tools.UsingTheSPARQLTriggerProcessor.md) to learn more about the details displayed.
Activated cluster sniffing for Elastic Search clients | [448](https://github.com/thomsonreuters/CM-Well/pull/448) | Previously full-text search might fail if the default ES instance didn't respond. Now the client will look for alternative ES instances.
Optional source configuration for STP | [480](https://github.com/thomsonreuters/CM-Well/pull/480) | Previously materialized views created by the SPARQL Triggered Processor were always from and to the same environment. This is still the default, but you can now specify a different source environment, using the **host-updates-source** parameter.
**Bug fix**: searching for **content** and **link** fields | [399](https://github.com/thomsonreuters/CM-Well/pull/399) | Fixed bug that prevented for searching for **content** and **link** metadata fields in file and link infotons respectively
**Bug fix**: Cassandra status in health dashboard | [411](https://github.com/thomsonreuters/CM-Well/pull/411) | The health dashboard failed to report Cassandra status in some cases
**Bug fix**: STP sensors were written to the wrong folder | [386](https://github.com/thomsonreuters/CM-Well/pull/386) | Didn't affect sensor operation, only the sensor file storage path.
**Bug fix**: health-detailed dashboard showed wrong metrics for Kafka partition status | [449](https://github.com/thomsonreuters/CM-Well/pull/449) | Showed the status of the (irrelevant) batch process rather than the bg process

### Changes to API ###

* New **parallelism** query parameter for [sstream](API.Stream.StreamInfotons.md) and [bulk-consume](API.Stream.ConsumeNextBulk.md).
* New **hostUpdatesSource** parameter in [SPARQL Triggered Processor](Tools.UsingTheSPARQLTriggerProcessor.md) configuration.

### Known Issues ###

Queries on values of all fields, using the **_all** wildcard, currently do not work. To be fixed.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Piranha.January.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Rhino.March.2018.md)


----