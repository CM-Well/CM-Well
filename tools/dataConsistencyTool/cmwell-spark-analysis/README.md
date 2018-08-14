# CM-Well Spark Data Analytics

This project implements a collection of tools for analyzing the data integrity and consistency of data
within a CM-Well instance. The analysis done using Apache Spark.

## Installation

While these analytics are implemented using Apache Spark, it does not require a dedicated Spark cluster
to operate. The scripts provided to run the analytics will start up a suitable Spark instance that runs
in local mode (i.e., within a single JVM process). This could easily be adapted to run on a Spark cluster
if one was available.

This project requires direct access to the Cassandra and Elasticsearch nodes used by CM-Well,
so if the CM-Well instance being analyzed is running within an isolated virtual network, these components
must be installed within that virtual network. While you could use dedicated hardware, we have had good
results installing it on a CM-Well node. If you do install it on a CM-Well node, make sure that the
installation directory is on a device that has plenty of free space, since Spark temporary data and
extracted working datasets may be large.

Scripts in this project use the assembly from the *extract-index-from-es* project to extract data from ES
indexes. While this project includes use of the Spark ES connector, and that capability can be used, 
the Spark ES connector has proven to be unreliable, 
and the *extract-index-from-es* project appear to be both more reliable and faster.

The installation steps are:
* Create a directory that all the components will be installed in.
* If a JRE and Apache Spark are not already installed:
    * Download a JRE (currently, jre1.8.0_161 is assumed), and unpack it into installation directory.
    * Download Apache Spark (currently, spark-2.2.1-bin-hadoop2.7 is assumed), and unpack it into the 
    installation directory. Do _not_ use Spark 2.3.x as there is a known compatibility issue with
    the Cassandra Spark connector (see: https://datastax-oss.atlassian.net/browse/SPARKC-530)
    * Ensure that the names of the JRE and Spark directories are consistent with the *set-runtime.sh* script.
* Copy *src/main/resources/log4j.properties* to *spark-2.2.1-bin-hadoop2.7/conf*.
* Create an assembly for this project (sbt assembly). 
    * Copy the assembly *cmwell-spark-analysis-assembly-0.1.jar* to the installation directory.
* Create an assembly for the extract-index-from-es project (sbt assembly). 
    * Copy the assembly *extract-index-from-es-assembly-0.1.jar* to the installation directory.
* Copy the bash scripts in *src/scripts* to the installation directory (both from this project
and from the *extract-index-from-es project*) to the installation directory.
    * Make each of the scripts executable (chmod 777 *.sh).

If you have a pre-existing JRE and/or Spark installation, you can modify the script _set-runtime.sh_ to do nothing,
or to set the JAVA_HOME and/or SPARK_HOME variables accordingly. 

# Running the Analysis

Each analysis has an associated script, and in all cases, the script can be
invoked with a single parameter that is a CM-Well URL. For example:

`./find-infoton-inconsistencies "http://my.cmwell.host"`

In general, each script is designed to do some analysis, possibly extract some data into a given directory,
produce a file summarizing the results. The detailed data are typically retained for further analysis.
In general, bulk data extracted for further analysis will be stored in Parquet format, and smaller summary
data is stored in CSV format.

Some of the tools simply extract data for further analysis (e.g., within Spark). For example, to move to another
downstream system to compare the population of uuids (i.e., in an ad-hoc analysis via a Spark shell).

**internal-consistency.sh** - Runs a suite of internal consistency checks. This will be more efficient than
running individual scripts since it avoids repeating extracts.

**check-infoton-data-integrity.sh** - Reads from the Cassandra infoton table and recalculates the uuid.
If the calculated uuid is not the same as the stored uuid, the infoton is extracted for further study.
This analysis also does some structural checks on the infoton data (e.g., data content, duplicate system fields).
A summary of the failure counts, by date, is generated.

**dump-infoton-with-uuid-only.sh** - Extract the uuids for all infotons from the infoton table.

**dump-infoton with-key-fields.sh** - Extract the uuids, path and lastModified from the infoton table.

**dump-path-with-uuid-only.sh** - Extract the uuids for all infotons from the path table.

**dump-path-with-key-fields.sh** - Extract the uuids, path and lastModified from the path table.

**find-duplicate-system-fields.sh** - Looks for infotons that have duplicated system fields. A similar check is done by
*check-infoton-data-integrity*, but this analysis is much faster.

**find-infoton-index-inconsistencies.sh** - Compares the system fields in the infoton table and the index, and looks
for inconsistencies. This is mostly overlap with other analysis (esp. check-infoton-data-integrity), and is fairly
expensive to run (requires a join of index and infoton tables). The underlying spark analysis has some minor issues
that make it awkward to use (e.g., timestamp values are shown as numbers instead of formatted dates). This may
get resurrected if there is a need for it.

**set-difference-uuids.sh** - Gets the uuids from the index, infoton and path tables, and calculates the set
difference between each pair. This looks for uuids that are missing.

**find-duplicated-current-paths-in-index.sh** Gets the system fields for all index entries, and looks for any
paths that have more than one entry that is marked as current. Exactly one index entry (i.e., version, uuid) 
for a path should be current.
The result of the analysis will be a file called *duplicated-current.csv* in directory *duplicated-current-index*.

# Filtering

Most of the tools that extract data (or extract and analyze in a single step) implement options for filtering
the data. This is intended to reduce the time spent doing analysis. Filtering can be applied to both the
temporal dimension (lastModified and current) or the path dimension (path prefix).

As much as possible, the filtering is pushed down to the data source. 
For the infoton table, pushing filters down to Cassandra is not possible due to the schema. 
In the case of the infoton table, all rows still need to be read, but any down stream analysis (e.g., creating
a Parquet extract or shuffling the data) can still be eliminated by the filter.

Filtering is applied in multiple places, both on extract and for each analysis.
This is necessary since there are multiple filtering parameters implemented along the temporal dimension,
For example, we might want to do an extract of the Elasticsearch index and use it for both 
*find-duplicated-current-paths-in-index* and *set-difference-uuids*. For this case, we would specify both the
*--current-filter* and *--last-modified-gte-filter* parameters when extracting the data from the index, which would
*or* the results of the two filters together. When using the generated extract, the *--current-filter true* parameter
would be passed to the *find-duplicated-current-paths-in-index* analysis, and the *--last-modified-gte-filter*
parameter would be passed to the *set-difference-uuids* analysis to select the subset of the extract that is needed
for each analysis.

The filtering parameters are:

* **--current-filter \<boolean>** - Selects infotons that are either current or not current (i.e., the latest
infoton in the paths's history). 
* **--last-modified-gte-filter <ISO 8601 timestamp>** - Selects infotons that have a lastModified value that is
greater than or equal to a given timestamp.
* **--path-prefix-filter \<prefix>** - Selects infotons that have a path that starts with a given prefix.

