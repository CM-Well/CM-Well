# CM-Well Version Release Notes - C.Elegans (Mar. 2017) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Beowulf.Feb.2017.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Dolphin.Mar.2017.md)  

----

## Change Summary ##

GitLab Item # | Title | Description
:-------------|:------|:-----------
415 | Improve performance of ingestion to new data path. | In the standard method of ingestion, two separate queues are managed: one for writing to Cassandra storage and one for writing to Elastic Search indexing. In the case of small infotons, a single queue is managed for both these operations, allowing a cache to be utilized and making the ingestion faster.
422, 444 | Bug fixes and enhancements to the web service caching component. | See Title.

### Changes to API ###
N/A

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Beowulf.Feb.2017.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Dolphin.Mar.2017.md)  

----


