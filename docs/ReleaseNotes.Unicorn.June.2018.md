# CM-Well Version Release Notes - Unicorn (June 2018) #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Turtle.May.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Viper.June.2018.md)

----

## Change Summary ##


 Title | Git Issue | Description 
:------|:----------|:------------
STP input queue count | [691](https://github.com/thomsonreuters/CM-Well/pull/691) | In the SPARQL Triggered Processor dashboard, we now show the number of infotons waiting in the queue to be materialized. 
STP persistent sensor parameters | [692](https://github.com/thomsonreuters/CM-Well/pull/692) | STP sensor position token and statistics are now stored per sensor in Cassandra, to enable a robust restart from the same place in case of fault. 
STP fine tuning of consumption parameters | [697](https://github.com/thomsonreuters/CM-Well/pull/697) | Results in better control of consumption process.
**Bug Fix:** Fixed problem with JAVA_HOME setting when installing on AWS | [704](https://github.com/thomsonreuters/CM-Well/pull/704) | When installing CM-Well on Amazon Web Services (AWS), this bug required that the JAVA_HOME value be configured manually instead of automatically as part of installation.
Improvement in CM-Well integration tests | [706](https://github.com/thomsonreuters/CM-Well/pull/706) | Performance improvements to CM-Well integration tests.


### Changes to API ###

None

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](ReleaseNotes.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](ReleaseNotes.Turtle.May.2018.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](ReleaseNotes.Viper.June.2018.md)

----