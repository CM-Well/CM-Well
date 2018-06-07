# CM-Well Version Release Notes - Turtle (May 2018) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Swan.March.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Unicorn.June.2018.md)

----

## Change Summary ##


 Title | Git Issue | Description 
:------|:----------|:------------
SPARQL Triggered Processor (STP) performance improvements | [640](https://github.com/thomsonreuters/CM-Well/pull/640) | The values of the arguments for Consume and Bulk Consume (called by the STP) were fine-tuned. Should result in a major performance boost.
STP: New ```sp.path``` parameter | [652](https://github.com/thomsonreuters/CM-Well/pull/652) | A new ```sp.path``` parameter is supported for the **_sp** endpoint, allowing the currently processed path to be used in SPARQL queries. See more details below. 
New health view showing Elastic Search thread pool info | [643](https://github.com/thomsonreuters/CM-Well/pull/643) | Usage: ```GET /health/es_thread_pool```. (The result is equivalent to querying the internal ES cluster that CM-Well is using with ```/_cat/thread_pool?v```). This view shows the state of threads processing search and indexing requests to ES.
Improvements to robustness of the BG process | [651](https://github.com/thomsonreuters/CM-Well/pull/651) | Each Background (BG) Process can now host 0 to 3 BG Actors, rather than 1. Each BG Actor is in charge of one Partition, as before. If one BG Process crashes (e.g. due to a physical node failure), another BG Process automatically takes the lead on those Partitions. 
**Bug fix:** Fixed operation of ```_all``` field indicator | [656](https://github.com/thomsonreuters/CM-Well/pull/656) | The ```qp=_all:<value>``` usage to indicate a search in all fields didn't work; now fixed. (Also affected searches in the CM-Well UI.)
**Bug fix:** Font size in UI search box | [659](https://github.com/thomsonreuters/CM-Well/pull/659) | In the CM-Well UI, the search box's font was slightly larger than the font in the “search where” drop down. Now they are the same size. 

### Git Issue #652: New ```sp.path``` parameter for _sp endpoint ###

When the STP invokes the **_sp** endpoint, it previously provided only the ```sp.pid``` value.
It now provides an ```sp.path``` value as well.

Note that **_sp** supports parameter placeholders in SPARQL queries, and replaces any instance of ```%VARIABLE%``` in the post body with the value of ```sp.variable``` in the query parameters. The new feature enables you to use the currently processed path in SPARQL queries.

The PID and path parameters have the following values: 

For a given path P, the ```sp.pid``` value contains the substring following the last dash in P.
The ```sp.path``` contains the substring following the last slash in P. 

For example, if the STP is currently processing the path ```/example.org/123-xyz```, the invocation to **_sp** will include ```/_sp?sp.pid=xyz&sp.path=123-xyz```.


### Changes to API ###

New ```sp.path``` parameter for **_sp** endpoint.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Swan.March.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Unicorn.June.2018.md)

----