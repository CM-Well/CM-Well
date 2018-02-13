# CM-Well Version Release Notes - Piranha (January 2018) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Octopus.December.2017.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Quetzal.February.2018.md)


----

## Change Summary ##


 Title | Git Issue | Description 
:------|:----------|:------------
Scala upgrade to 2.12.4 | [218](https://github.com/thomsonreuters/CM-Well/issues/218) | Scala package upgraded to 2.12.4
Added control mechanism to **bulk-consume**/**sstream** to limit parallel processing of ES indices | [337](https://github.com/thomsonreuters/CM-Well/issues/337) | The **sstream** operation (which is used by **bulk-consume**) performs parallel processing of several Elastic Search indices. When the parallelism was unlimited, this could cause performance issues. A mechanism has been added to allow limiting the number of parallel processing threads.
Improve DC-Sync robustness by ignoring indexTime errors | [339](https://github.com/thomsonreuters/CM-Well/issues/339) | Cases of faulty data, such that the indexTime value of an infoton in Cassandra is significantly different from the indexTime as it appears in Elastic Search, could cause gaps in the Data Center Synchronization (DC-Sync) operation. Now such faulty cases are written to an error log and skipped by the sync process.
New **use_auth** environment variable | [134](https://github.com/thomsonreuters/CM-Well/issues/134) | A new Boolean **use_auth** environment variable is now exposed under the **proc/node** folder. When its value is TRUE, this means that the CM-Well cluster is write-protected, and authorization is required to write to it.
Display of historical infoton versions is now supported in the new UI | [244](https://github.com/thomsonreuters/CM-Well/issues/244) | Several bugs were fixed in the /ii/\<uuid\> API that displays historical infoton versions in the new UI.
Performance boost to DC-Sync | [140](https://github.com/thomsonreuters/CM-Well/issues/140) | All DC-Sync requests and responses are now compressed in order to boost performance. 
Log improvements | Several | Noisy log messages were cleaned up, and more informative messages were added in several cases of runtime errors/warnings.
**Bug fix:** Don't allow writing to deleted infotons | [358](https://github.com/thomsonreuters/CM-Well/issues/358) | Previously, it was possible to update deleted infotons. An attempt to do this will now fail.
**Bug fix:** Lost iterator chunks | [328](https://github.com/thomsonreuters/CM-Well/issues/328) | A bug was fixed in the next-chunk operator, which could cause chunks to be lost in some cases.


### Changes to API ###

None.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Octopus.December.2017.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Quetzal.February.2018.md)


----