#!/bin/bash

# Copy the documents from one index to another.

if [ -z $1 -o -z $2 -o -z $3 ]; then
 echo "usage: $0 <cmwell-url> <read-index-name> <write-index-name>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1
READ_INDEX=$2
WRITE_INDEX=$3

EXTRACT_ES_UUIDS_JAR="extract-index-from-es-assembly-0.1.jar"

$JAVA_HOME/bin/java \
 -Xmx31G \
 -XX:+UseG1GC \
 -cp "${EXTRACT_ES_UUIDS_JAR}" cmwell.analytics.main.CopyIndex \
 --read-index "${READ_INDEX}" \
 --write-index "${WRITE_INDEX}" \
 "${CMWELL_INSTANCE}"
