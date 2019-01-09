# CM-Well Version Release Notes - Yak (November 2018) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Xerus.October.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Zebra.December.2018.md)

----

## Features and Upgrades ##


Title | Git Issue | Description 
:------|:----------|:------------
HTTPS protocol support | [951](https://github.com/thomsonreuters/CM-Well/pull/951) | The HTTPS protocol is now supported in RDF Subjects and preserved per infoton across all CM-Well APIs.
STP payloads now compressed | [952](https://github.com/thomsonreuters/CM-Well/pull/952) | The input to STP (retrieved with the **_in** API) is now compressed using GZip, for improved performance.
STP infoton rate now shown for last minute | [947](https://github.com/thomsonreuters/CM-Well/pull/947) | The infoton processing rate (displayed in the STP dashboard per sensor) now shows the latest per-minute rate rather than the overall mean value.

## Bug Fixes ##

Title | Git Issue | Description 
:------|:----------|:------------
STP can now handle empty results from sparqlToRoot query | [943](https://github.com/thomsonreuters/CM-Well/pull/943) | See title.
Bulk Consume API now respects the **with-history** parameter | [954](https://github.com/thomsonreuters/CM-Well/pull/954) | See title.


### Changes to API ###

None.


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Xerus.October.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Zebra.December.2018.md)

----