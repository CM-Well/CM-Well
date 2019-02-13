# CM-Well Version Release Notes - Rhino (March 2018) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Quetzal.February.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Swan.April.2018.md)


----

## Change Summary ##


 Title | Git Issue | Description 
:------|:----------|:------------
Preventing namespace prefix ambiguity | [538](https://github.com/thomsonreuters/CM-Well/pull/538) | If an ingested field name has a namespace prefix (e.g. "common") that is already found in an existing namespace, the new prefix will be concatenated with its hash before being saved. This prevents the ambiguity that would arise if two namespaces had the same prefix.
For developers: More descriptive error messages for 400 Bad Request on _ow endpoint | [412](https://github.com/thomsonreuters/CM-Well/pull/412) | See title.
STP Dashboard: Display processing point-in-time per sensor | [458](https://github.com/thomsonreuters/CM-Well/pull/458) | The SPARQL Triggered Processor (STP) Dashboard now displays for each sensor the update time of the input infotons that it's currently processing. If a sensor has reached its processing "horizon" (i.e. it has processed all relevant infotons), instead of a time value, the string "Horizon" is displayed.
STP Dashboard: Display source value | [443](https://github.com/thomsonreuters/CM-Well/pull/443) | The STP Dashboard now displays the configured source data center, i.e. the data center from which infotons are taken in order to create a materialized view.
Bug fix: STP Dashboard rate calculations | [450](https://github.com/thomsonreuters/CM-Well/pull/450) | A bug was fixed regarding calculation of the rate of producing materialized views.
Bug fix: STP Dashboard statistics | [500](https://github.com/thomsonreuters/CM-Well/pull/500) | A bug was fixed regarding calculation of STP statistics.
Bug fix: STP Dashboard, erroneous data displayed in point-in-time field | [451](https://github.com/thomsonreuters/CM-Well/pull/451) | See title.
Bug fix: **yg**/**xg** filters for outbound links of non-string fields | [529](https://github.com/thomsonreuters/CM-Well/pull/529) | Previously, **yg**/**xg** graph traversal would fail for any filter applied to non-string fields.
Bug fix: Ingest of typed empty literal would fail | [544](https://github.com/thomsonreuters/CM-Well/pull/544) | See title.
Bug fix: Memory leak in Iterator API | [550](https://github.com/thomsonreuters/CM-Well/pull/550) | A memory leak in the Iterator API would cause the CM-Well Web Service component to reset frequently.

### Changes to API ###

None.

### Known Issues ###

* Queries on values of all fields, using the **_all** wildcard, currently do not work. To be fixed.
* If the Web Service instance that the STP Agent is working with restarts, all STP Dashboard Statistics will be reset.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Quetzal.February.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Swan.April.2018.md)

----