#!/bin/bash

# Dump the contents of an index to a Parquet file, partitioned by shard.
# The resulting schema has two columns: uuid and document, where the document column contains the
# complete document stored in the index.

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1

WORKING_DIRECTORY="dump-complete-document"
EXTRACT_ES_UUIDS_JAR="extract-index-from-es-assembly-0.1.jar"

rm -rf "${WORKING_DIRECTORY}/complete-document"

# Specify the --read-index <index-name> parameter to extract data from a specific index

$JAVA_HOME/bin/java \
 -Xmx31G \
 -XX:+UseG1GC \
 -cp "${EXTRACT_ES_UUIDS_JAR}" cmwell.analytics.main.DumpCompleteDocumentFromEs \
 --out "${WORKING_DIRECTORY}/complete-document" \
 --format parquet \
 "${CMWELL_INSTANCE}"
