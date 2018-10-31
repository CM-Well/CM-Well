# CM-Well Elasticsearch Utilities

This project implements utilities for bulk analysis of data in Elasticsearch indexes, as well as utilities
for manipulating indexes. This project exists primarily because the Elasticsearch Spark connector works poorly
(at least with the old version of ES that CM-Well currently uses), and is not as fast as it might be. 

This implementation uses some of the same techniques as the ES Spark connector
in that it discovers the topology of the ES indexes, does partitioned retrieval by shard, and can distribute
requests evenly to ES nodes that actually hold the data. 

The core of the implementation is an Akka stream, where the source is a scroll over some index shard in ES, and the
sink either:
 * writes partitioned (by shard) data files in CSV or Parquet format, 
 * writes data back to another Elasticsearch index (index copy/reshard), or
 * calculates a hash of the contents of the index, which is useful as a fast check that two indexes 
 have an identical contents.

This project is a companion project to the CM-Well Spark Data Analytics project, and is used by the scripts in that
project to extract the data from ES, which is then used within the Spark analysis using that data. The installation
would typically be to copy the *extract-index-from-es-assembly-0.1.jar*, along with the scripts into the CM-Well
Spark Data Analytics installation directory. I'm not repeating the installation instructions here, other than to note
that if you are just doing operations from this project, you don't need to also provide a Spark runtime (just a JRE).

# Performance

On a system with ~2 billion infotons (using 24 threads in a single 31GB JVM), 
extracting index data to Parquet format performed as follows:

* *uuid only* - 18 bytes/infoton, 26 minutes.
* *system fields* - 52 bytes/infoton, 52 minutes.
* *complete document* - 185 bytes/infoton, 60 minutes.

Update: The implementation of the feature that selects fields to transfer over the network had to be disabled.
I found one CM-Well index that would consistently fail if you tried to select a subset of the fields. 
I turned that feature off as a workaround, but perhaps it should be re-enabled on an optional basis.

By doubling the parallelism level and the amount of JVM memory allocated, the time to extract uuids drop from 
26 minutes to 14 minutes, so the performance scales approximately linearly. While it might be desirable to run faster,
you should consider the impact on the node that this is running on (i.e., if it is a CM-Well node), as well as consider
the impact of the increased load on Elasticsearch.

## Scripts

*copy-index.sh* - Copies the data from one index into another. This has been used to create copies of indexes with
a different number of shards (ES does not allow the number of shards to be changed after index creation).
This script copies one index at a time from a given a source and target index name.

*copy-index-with-mapping.sh* - Copies data one set of indexes to another. A parameter provides the name of a file
containing a JSON mapping describing which indexes which source indexes are copied to which target indexes. For example,
this mapping might look like:

```
{ 
  "sourceIndex1": "targetIndex1", 
  "sourceIndex2": "targetIndex2"
}
```

The mapping may be 1:1, or might copy multiple indexes to a single target index. 
The indexes must already exist in the Elasticsearch instance.

*dump-complete-document-from-es.sh* - Extracts complete documents from the *cm_well_all* alias and writes it to
a Parquet file. The Parquet schema will have two columns: uuid and document, where the document column contains the
complete document in JSON format. Extracting complete documents is not supported with CSV format.

*dump-system-fields-from-es.sh* - Extracts system fields only from the *cm_well_all* alias and writes to a Parquet file.
The Parquet schema will include separate columns for each of the system fields.

*dump-uuids-from-es.sh* - Extracts uuids only from the *cm_well_all* alias and writes to a Parquet file.
The resulting schema will have a single uuid column.

All of the *dump-...-from-es.sh* scripts can be easily modified to extract only from a single named index.

*CalculateXORSummary* - (no script for this yet). This calculates a summary of hashes of all documents in an index.
This allows for a fast check that two indexes contain an identical set of documents. It does this by summarizing the
hash of each document using XOR, which works regardless of the order that the documents are processed. It also means
that we don't need to move any data around beyond retrieving the documents from ES.

## Filtering

The entry points for extracting from indexes implement options for filtering.
See the README.md for the companion *cmwell-spark-analysis* project.