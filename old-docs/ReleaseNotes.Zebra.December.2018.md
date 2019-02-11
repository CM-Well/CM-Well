# CM-Well Version Release Notes - Zebra (December 2018) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Yak.November.2018.md)

----

## Features and Upgrades ##


Title | Git Issue | Description 
:------|:----------|:------------
New default RDF protocol parameter in cluster configuration | [961](https://github.com/thomsonreuters/CM-Well/pull/961) | You can now define the cluster's default RDF protocol (```http``` or ```https```) in the cluster's **conf** file.<br/><br/>**Example:** ```val cmw = GridSubDiv(... , defaultRdfProtocol = "https")```<br/><br/>The default protocol will be added to URIs of infotons for which the protocol is not provided in the ingested data.
Split data reads in case of failure in STP fetch of bulk data | [930](https://github.com/thomsonreuters/CM-Well/pull/930) | If the STP bulk read fails due to bad data, the entire **_out** request can fail (even for one bad infoton). Now in this case, STP splits **_out** requests into multiple single read (GET) requests so that the valid infotons in the bulk can be successfully read. Failed reads are written into a “red” log file.

 

## Bug Fixes ##

Title | Git Issue | Description 
:------|:----------|:------------
Fixed bug in handling response to heavy request | [972](https://github.com/thomsonreuters/CM-Well/pull/972) | Previously, when handling an expensive ingest request, the WS module would send newlines to the HA Proxy periodically, to prevent it from closing the connection. There was a bug related to this mechanism that caused an OK response to be sent erroneously after a failure to ingest. Now fixed.
Error reading timestamp in zStore caused false positive for Crawler | [980](https://github.com/thomsonreuters/CM-Well/pull/980) | In case of a timestamp artificially incremented with +1 in the case of very close updates, there was a bug in BG-Crawler coordination with regards to reading the timezone of the timestamps that are persisted in zStore. This caused false positive detections of updates in the Crawler’s report.
Support https in URIs with no domain | [968](https://github.com/thomsonreuters/CM-Well/pull/968) | Infoton subjects with the **https** protocol which didn't contain domain names (e.g. ```https://example/foo/bar```) were erroneously rendered as ```http://example/foo/bar```. Now fixed.

### Changes to API ###

None.


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Yak.November.2018.md)

----