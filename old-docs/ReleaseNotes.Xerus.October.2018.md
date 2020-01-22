# CM-Well Version Release Notes - Xerus (October 2018) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Wombat.August.2018.md)

----

## Features and Upgrades ##


Title | Git Issue | Description 
:------|:----------|:------------
New GitHub release page | N/A | There is now a formal [CM-Well release page on GitHub](https://github.com/CM-Well/CM-Well/releases).
Set # of ES shards to 10 | [870](https://github.com/thomsonreuters/CM-Well/pull/870) | Set the number of ElasticSearch shards to 10 for new installations.
Change Cassandra strategy to NetworkTopologyStrategy | [871](https://github.com/thomsonreuters/CM-Well/pull/871) | This changes makes Cassandra aware of the physical machine topology, and prevents it from saving replicas of the same data in the same physical machine (which is a bad practice for data redundancy).
SBT build of CM-Well now produces a meaningful version label | [878](https://github.com/thomsonreuters/CM-Well/pull/878) | The version label now includes the Git tag.
Support versioning in SBT build | [897](https://github.com/thomsonreuters/CM-Well/pull/897) | SBT is now aware of the specific CM-Well build version. This allows performing customized actions during upgrades, according to the version.
Custom upgrade actions per version | [914](https://github.com/thomsonreuters/CM-Well/pull/914) | In the CM-Well installation package, support custom upgrade actions according to the specific version.
Support multiple parameters in the **sparqlToRoot** query | [940](https://github.com/thomsonreuters/CM-Well/pull/940) | The **sparqlToRoot** query in the STP sensor definition, which is run on the data that is input to the **sparqlMaterializer** command, can now query several parameters. Previously it could only query the **orgId** paramter. See [Using the SPARQL Triggered Processor](Tools.UsingTheSPARQLTriggerProcessor.md) for more details.


## Bug Fixes ##

Many of the bug fixes in this release are related to the CM-Well Data Consistency Crawler (DCC). This is a background service that "crawls" the recently written infotons and fixes data inconsistencies. It traverses the infoton that are stored for a certain period of time in a Kafka queue. Several bugs were fixed that were related to DCC false positives, i.e. infotons identified as inconsistent which actually weren't.


Title | Git Issue | Description 
:------|:----------|:------------
Kafka queue retention time enlarged to 1 week | [895](https://github.com/thomsonreuters/CM-Well/pull/895) | The Kafka queue storing updated infotons now stores them for 1 week instead of 24 hours. This prevents an issue of updates being deleted from the queue before the DCC can test them.
DCC false positive for null updates | [883](https://github.com/thomsonreuters/CM-Well/pull/883) | DCC would falsely identify an infoton as changed when in fact it was a null update (an update where no field values have changed).
DCC false positive if CAS version updated before ES | [888](https://github.com/thomsonreuters/CM-Well/pull/888) | If the DCC tested a new infoton that was updated in Cassandra but not in ElasticSearch yet, it would identify a false positive.
DCC false positive for deleted infotons | [900](https://github.com/thomsonreuters/CM-Well/pull/900) | Deletion of a non-existent infoton caused a false positive.
DCC false positive for 1ms difference in update time | [908](https://github.com/thomsonreuters/CM-Well/pull/908), [910](https://github.com/thomsonreuters/CM-Well/pull/910) | This is caused by an "artificial" update time that addresses the problem of two writes of the same path having reverse order in the Kafka queue. In order to solve this issue, BG adds 1ms to the time written by the WS and writes it to CAS and to ES. This case was previously identified as a data inconsistency, but should not be.
DCC false positive due to "quorum of 1" | [919](https://github.com/thomsonreuters/CM-Well/pull/919), [921](https://github.com/thomsonreuters/CM-Well/pull/921) | The CAS reads performed by DCC only required a "quorum of 1" (out of the 3 replicas). This could lead to false positives.
Cassandra Monitor parse error for "?" result | [916](https://github.com/thomsonreuters/CM-Well/pull/916) | If the Cassandra Monitoring service received a DN status of "?", this would cause an exception.
Graph traversal APIs modified to support https links | [924](https://github.com/thomsonreuters/CM-Well/pull/924) | The **xg**/**yg**/**gqp** APIs did not work for links with the **https** protocol.
Installation failure if another service was listening on port 9000 | [906](https://github.com/thomsonreuters/CM-Well/pull/906) | During CM-Well installation, the installer checks to see if the CM-Well Web Service is listening on port 9000. If a different service was listening on port 9000, the installer incorrectly assumed that the WS was installed and working, and proceeded with the installation. This caused the upload of the initial data to fail, and the whole system was not usable.

### Changes to API ###

None.


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Wombat.August.2018.md)

----