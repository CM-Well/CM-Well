# CM-Well Version Release Notes - Nautilus (November 2017) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Mono.September.2017.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Octopus.December.2017.md)

----

## Change Summary ##


 Title | Git Issue | Description 
:------|:----------|:------------
Play minor version upgrade | [287](https://github.com/thomsonreuters/CM-Well/pull/287) | Play 3rd-party package upgraded to version 2.6.7
Integration test improvements | [275](https://github.com/thomsonreuters/CM-Well/pull/275) | Integration test improvements
**qstream** now supports **length** parameter | [285](https://github.com/thomsonreuters/CM-Well/pull/285) | The **qstream** streaming operator now supports the **length** parameter, which is the maximum number of infotons you want to receive.
Range filters now supported for **xg**/**yg**/**gqp** | [297](https://github.com/thomsonreuters/CM-Well/pull/297) | Range filters (<, <<, >, >>) for field values are now supported for **xg**/**yg**/**gqp** queries. See [Traversing Outbound and Inbound Links (*xg*, *yg* and *gqp*)](API.Traversal.TOC.md).
**Bug fix:** Removed unreliable missing data testing from **_sp** query | [296](https://github.com/thomsonreuters/CM-Well/pull/296) | The **_sp** API used to compare indexed infoton numbers with stored infoton numbers. In some cases of duplicate infotons, this would return erroneous "data missing" errors. This test has been disabled.
**Bug fix:** Hang in upgrade process | [279](https://github.com/thomsonreuters/CM-Well/pull/279) | A health test for Elastic Search would fail and the upgrade process would hang, due to a badly formed curl command (with no -X parameter). 
**Bug fix**: Broken pagination links in Atom format | [222](https://github.com/thomsonreuters/CM-Well/pull/222) | Pagination links in Atom-formatted results were broken because of redundant question marks.
 

### Changes to API ###

* **qstream** now takes the **length** parameter.
* Range filters now supported for **xg**/**yg**/**gqp**.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Mono.September.2017.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Octopus.December.2017.md)

----
