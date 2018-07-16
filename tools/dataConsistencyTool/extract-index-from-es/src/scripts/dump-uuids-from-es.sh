#!/bin/bash

# Extract the CM-Well system fields from an index to a set of Parquet files (partitioned by shard).
# The resulting file will contain the single column: uuid.

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1

WORKING_DIRECTORY="dump-index-from-es"
EXTRACT_ES_UUIDS_JAR="extract-index-from-es-assembly-0.1.jar"

rm -rf "${WORKING_DIRECTORY}/es-uuids"

# Specify the --read-index <index-name> parameter to extract data from a specific index

$JAVA_HOME/bin/java \
 -Xmx31G \
 -XX:+UseG1GC \
 -cp "${EXTRACT_ES_UUIDS_JAR}" cmwell.analytics.main.DumpUuidOnlyFromEs \
 --out "${WORKING_DIRECTORY}/es-uuids" \
 --format parquet \
 "${CMWELL_INSTANCE}"
