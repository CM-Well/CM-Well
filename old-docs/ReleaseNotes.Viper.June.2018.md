# CM-Well Version Release Notes - Viper (June 2018) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Unicorn.June.2018.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Wombat.August.2018.md)


----

## Change Summary ##


 Title | Git Issue | Description 
:------|:----------|:------------
Data Consistency Crawler (DCC) | [736](https://github.com/thomsonreuters/CM-Well/pull/736) | A new module was added to the BG process. DCC checks for data consistency of infoton versions. Still in development; first phase will only test for and log inconsistencies without handling them.
New ```_kafka``` API | [743](https://github.com/thomsonreuters/CM-Well/pull/743) | The new ```_kafka``` API allows you to consume a Kafka queue via HTTP. Requires an Admin token for consuming system queues. (Enables the new DCC feature.)
SBT version upgrade | [748](https://github.com/thomsonreuters/CM-Well/pull/748) | From 1.1.4 to 1.1.5
New values for ```_backpressure``` API | [742]() | Rather than ```new\old\off\bar``` values for the ```_backpressure``` API, there are now ```enable\disable\block``` values.
STP performance improvement | [738](https://github.com/thomsonreuters/CM-Well/pull/738) | STP: adjusted fetch size for better performance.
STP Connection Pool | [747](https://github.com/thomsonreuters/CM-Well/pull/747) | Create a limited pool of connections to CM-Well that all STP sensors use. This limits the number of concurrent STP requests, thus preventing the STP from flooding CM-Well with requests.


### Changes to API ###

* New values for [```_backpressure``` Admin API](Admin.Backpressure.md)
* New [```_kafka``` Admin API](Admin.Kafka.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Unicorn.June.2018.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Wombat.August.2018.md)

----