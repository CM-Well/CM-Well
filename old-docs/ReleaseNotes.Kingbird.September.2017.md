# CM-Well Version Release Notes - Kingbird (September 2017) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Jaguar.August.2017.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Lynx.September.2017.md)  

----

## Change Summary ##


 Title | Description 
:------|:-----------
Priority queue | Introducing the [Priority Queue](../blps/blp-700-priority-queue.md) feature. Adding the ```priority``` flag to an update request causes the request to receive priority in relation to other update requests. 
Bug fixes and improvements | Bug fixes in Consumer Tool, UI, Consume and Bulk-Consume APIs, bg (background) process. Consumer Tool optimizations.
Documentation | Additions and improvements.
Configuration refactoring | Many configuration parameters were moved from `-D` injections to configuration files. This is related to internal CM-Well launch mechanisms and doesn't affect the user. 
The **qp** parameter is now supported for DC-Sync | The Data Center synchronization mechanism allows you to automatically synchronize data among multiple CM-Well instances. Previously the synchronization could only be performed on the entire data repository. This feature has now been enhanced with the option to use `qp` parameter, thus controlling the subset of data that is synchronized. This can be useful, for example, when you want to provide access to "edge" instances with a limited dataset.

### Changes to API ###

The ```priority``` flag was added to **POST** calls and calls to the **_in** endpoint. See [Query Parameters](API.QueryParameters.md) to learn more.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Jaguar.August.2017.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Lynx.September.2017.md)  

----