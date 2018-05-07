# CM-Well Version Release Notes - Swan (April 2018) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Rhino.March.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Turtle.May.2018.md)

----

## Change Summary ##


 Title | Git Issue | Description 
:------|:----------|:------------
SPARQL Triggered Processor (STP): "Bad data" logging | [613](https://github.com/thomsonreuters/CM-Well/pull/613) | New log message produced by the STP agent, in case an attempt is made to upload invalid data.
STP: Limit retries in case of a failure to retrieve data. | [624](https://github.com/thomsonreuters/CM-Well/pull/624), [616](https://github.com/thomsonreuters/CM-Well/pull/616) | Limit the number of retries in case of a failure to retrieve data, so that the STP doesn't enter an endless loop.
Modify JVM arguments to avoid lack of disk space problem | [615](https://github.com/thomsonreuters/CM-Well/pull/615) | Occasionally, some process core dumps were written to their working directories, causing the disk to fill to capacity. Now written to larger data disks.
SBT version update | [623](https://github.com/thomsonreuters/CM-Well/pull/623) | SBT version updated to 1.1.4.
Bug fix: Correct display of non-English characters in CM-Well web UI | [627](https://github.com/thomsonreuters/CM-Well/pull/627) | Added charset=utf8 when a textual format is requested, so that non-English characters are displayed correctly.

### Changes to API ###

None.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Rhino.March.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Turtle.May.2018.md)

----