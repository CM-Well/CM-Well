# CM-Well Version Release Notes - Beowulf (Feb. 2017) #

## Change Summary ##

### New Features ###

[Support the '#' Character in Infoton UUIDs](#hdr1)

### Notable Bug Fixes ###

GitLab Item # | Title | Description
:-------------|:------|:-----------
370 | Empty subjects caused an error in **trig** format. | This bug caused an error to be produced when an attempt was made to ingest a triple with an empty subject (i.e. a the subject "<>"). Now supported. Reported by the Linked Data team.
389 | Failure to retrieve infotons whose timestamp has a UTC offset. | Infotons whose timestamp has a UTC offset would be ingested successfully, but trying to retrieve them would fail.

### Changes to API ###
The '#' is now supported as a valid character in infoton UUIDs.

------------------------------

## Feature Descriptions ##

<a name="hdr1"></a>
### Support the '#' Character in Infoton UUIDs ###

**GitLab Item No.:** 324.

**Description:**
CM-Well has been modified to support the '#' character in infoton UUIDs (previously not supported). This character is often used in ontology subjects, so this change comes to facilitate that usage. Both the CM-Well API and the web UI now support this change.

>**Note:** When using the '#' character as part of an infoton UUID, it must be escaped as `%23`.

**Documentation:** 
N/A