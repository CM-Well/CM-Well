# CM-Well Version Release Notes - Octopus (December 2017) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Nautilus.November.2017.md)

----

## Change Summary ##


 Title | Git Issue | Description 
:------|:----------|:------------
Upgrade SBT (Scala/Java build tool) to 0.13.16 and Scala to 2.11.12 | [302](https://github.com/thomsonreuters/CM-Well/pull/302) | See title
Support 2 private key values for authentication | [304](https://github.com/thomsonreuters/CM-Well/pull/304) | The CM-Well TR production environment now supports both legacy and new JWT authentication keys. Both keys are tested for during authentication. 
Improve authorization cache | [319](https://github.com/thomsonreuters/CM-Well/pull/319) | The authorization module occasionally produced false 403 errors. Authorization data is now stored in a simple in-memory map, and other improvements to robustness were made. There is no change to authorization APIs.
DC-Sync only for current data is now possible | [323](https://github.com/thomsonreuters/CM-Well/pull/323) | The DC-Sync feature enables synchronization of data across data centers. Adding ```"with-history": false``` to the DC-Sync configuration JSON causes the replication to apply only to current versions of all infotons, while historical versions are ignored.
Changes to SPARQL Triggered Processor configuration | N/A | Use X-CM-WELL-TOKEN header for authentication; configuration file path changed; start/stop flag no longer created automatically. See [Using the SPARQL Triggered Processor](Tools.UsingTheSPARQLTriggerProcessor.md) for more details.
**Bug fix:** Enable running ws/console code from the SBT console | [308](https://github.com/thomsonreuters/CM-Well/pull/308) | Previously there was a bug when trying to run ws/console code in Scala REPL mode. The related bug in configuration files was fixed.
**Bug fix:** Bug invoking a CM-Well host without the schema | [315](https://github.com/thomsonreuters/CM-Well/pull/315) | Previously if you tried invoking a CM-Well host without the schema (e.g. "localhost:9000" rather than "http://localhost:9000"), this caused an exception. Now fixed.


### Changes to API ###

Changes to SPARQL Triggered Processor configuration. See [Using the SPARQL Triggered Processor](Tools.UsingTheSPARQLTriggerProcessor.md) for more details.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Nautilus.November.2017.md)

----