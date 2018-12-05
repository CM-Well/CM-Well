# CM-Well Version Release Notes - Wombat (August 2018) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Viper.June.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Xerus.October.2018.md)

----

## Features and Upgrades ##


Title | Git Issue | Description 
:------|:----------|:------------
Java version upgrade | [821](https://github.com/thomsonreuters/CM-Well/pull/821) | Upgraded Java8 version to 172
SBT version upgrade | [748](https://github.com/thomsonreuters/CM-Well/pull/748) | Upgraded SBT to version to 1.1.5
Scala version upgrade | [806](https://github.com/thomsonreuters/CM-Well/pull/806) | Scala version upgraded from 2.12.5 to 2.12.6
HTTPS prefix not saved in subject path | [857](https://github.com/thomsonreuters/CM-Well/pull/857) | Previously the http prefix in an infoton subject's path was dropped (e.g. **<http://example.org/abc>** to **/example.org/abc**), while https was retained in the path (e.g. **<http://example.org/abc>** to **https.example.org/abc**). Now the https prefix is omitted as well.
New release info file in CM-Well install package | [827](https://github.com/thomsonreuters/CM-Well/pull/827) | A new ```cmwell.properties``` file is now included in the CM-Well installation package. It contains the CM-Well release name and version, and the last Git commit ID added to the release.<br/>Example: `{"git_commit_version": "90a2e179a6b46351a0cab18e45000aad3c0d35c4", "cm-well_release": "Viper", "cm-well_version": "1.5.x-SNAPSHOT"}`
Minor change to Health Dashboard | [796](https://github.com/thomsonreuters/CM-Well/pull/796) | When a Cassandra node's status is Red, the relevant text will be in red bold font.
Data Consistency Crawler (DCC) now detects duplicate infotons | [863](https://github.com/thomsonreuters/CM-Well/pull/863) | DCC now detects situations where an Infoton has more than one version in ElasticSearch with the ```current=true``` property.
DCC performance improvement | [752](https://github.com/thomsonreuters/CM-Well/pull/752) | When fetching system fields for comparison, DCC now ignores the payload of FileInfotons (which is a system field). This improves runtime and lowers system stress.
DCC Detection Parallelism is now configurable | [780](https://github.com/thomsonreuters/CM-Well/pull/780) | The configuration key `cmwell.crawler.checkParallelism` was added. Default value is 1. This controls the DCC's level of parallel processing.
Data Consistency Tools added to code repository | [817](https://github.com/thomsonreuters/CM-Well/pull/817) | This is a set of tools that utlilizes SPARK to check consistency of Infotons in a CM-Well Cluster.
Calculate total agent runtime | [814](https://github.com/thomsonreuters/CM-Well/pull/814) | The total time an STP agent has been running for as well as the current uptime are now viewable in ```proc/stp.md```.
Search by POST documentation added | [866](https://github.com/thomsonreuters/CM-Well/pull/866) | This feature enables using the request body to pass all search terms and parameters. Good for long queries that can't be passed in the URL. See Syntax 2 in [Query for Infotons Using Field Conditions](API.Query.QueryForInfotonsUsingFieldConditions.md) to learn more.

## Bug Fixes ##


Title | Git Issue | Description 
:------|:----------|:------------
Search by POST bug | [843](https://github.com/thomsonreuters/CM-Well/pull/843) | Invoking a Search query with the POST verb didn't work on the root path.
Recovery from crash in DCC streaming | [762](https://github.com/thomsonreuters/CM-Well/pull/762) | The Data Consistency Crawler (DCC) is now robust when it crashes while streaming.
False positive for new Infotons | [787](https://github.com/thomsonreuters/CM-Well/pull/787) | Fixed false positive for new Infotons.
DCC crash for malformed history data | [766](https://github.com/thomsonreuters/CM-Well/pull/766) | Previously the DCC would crash if history data was malformed.
Large file infotons duplicated | [778](https://github.com/thomsonreuters/CM-Well/pull/778) | Previously file infotons larger than 512KB would be duplicated.
Don't force version=1 when updating ElasticSearch | [773](https://github.com/thomsonreuters/CM-Well/pull/773) | Previously, we always forced version-1 when writing to ES. This caused duplicates in some cases, e.g. when x-fix was used on the relevant path.
BG created duplicates after a crash | [801](https://github.com/thomsonreuters/CM-Well/pull/801) | When the BG process crashes, it restarts and may replay ingest commands that were already processed. This sometimes led to duplicate infotons. Now fixed by introducing Recovery Mode: when the BG process starts, it keeps the oldest existing Kafka offset. Until hitting that offset, it processes commands carefully, while not allowing duplicates to be created.
STP token time not always saved | [836](https://github.com/thomsonreuters/CM-Well/pull/836) | The token time was not always saved for the Sparql Triggered Processor. Would cause an empty sensor row to be displayed in CM-Well Health UI.

### Changes to API ###

None.


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Viper.June.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Xerus.October.2018.md)

----