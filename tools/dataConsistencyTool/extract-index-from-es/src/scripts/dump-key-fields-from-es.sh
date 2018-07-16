#!/bin/bash

# Extract the CM-Well key fields (uuid, path, lastModified) from an index to a set of Parquet files (partitioned by shard).

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1

WORKING_DIRECTORY="dump-key-fields-from-es"
EXTRACT_DIRECTORY="index-key-fields"
EXTRACT_ES_UUIDS_JAR="extract-index-from-es-assembly-0.1.jar"

rm -rf "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY}"

# Specify the --read-index <index-name> parameter to extract data from a specific index

$JAVA_HOME/bin/java \
 -Xmx31G \
 -XX:+UseG1GC \
 -cp "${EXTRACT_ES_UUIDS_JAR}" cmwell.analytics.main.DumpKeyFieldsFromEs \
 --out "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY}" \
 --format parquet \
 "${CMWELL_INSTANCE}"
