#!/bin/bash

# Copy the documents from one set of indexes to another.
# The mapping file is JSON in the form: { "sourceIndex1": "targetIndex1", "sourceIndex2": "targetIndex2", ... }
# There is no restriction on the mappings (e.g., could map multiple indexes to a single index).
# The indexes named in the mapping must already exist.

if [ -z $1 -o ! -s $2 ]; then
 echo "usage: $0 <cmwell-url> <mapping-file-name>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1
MAPPING=`cat $2`

EXTRACT_ES_UUIDS_JAR="extract-index-from-es-assembly-0.1.jar"

$JAVA_HOME/bin/java \
 -Xmx31G \
 -XX:+UseG1GC \
 -cp "${EXTRACT_ES_UUIDS_JAR}" cmwell.analytics.main.CopyIndexesWithMapping \
 --index-map "${MAPPING}" \
 "${CMWELL_INSTANCE}"
