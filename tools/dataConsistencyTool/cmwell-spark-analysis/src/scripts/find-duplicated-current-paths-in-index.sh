#!/bin/bash

# Look for index entries that have current=true, that have duplicated paths.
# Only one infoton with a given path should be indexed with current=true at a time.

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1

WORKING_DIRECTORY="duplicated-current-index"
ES_SYSTEM_FIELDS_TEMP="es-system-fields-current-only"

set -e # bail out if any command fails

rm -rf "${WORKING_DIRECTORY}"

$JAVA_HOME/bin/java \
 -XX:+UseG1GC \
 -Xmx31G \
 -cp "${EXTRACT_ES_UUIDS_JAR}" cmwell.analytics.main.DumpSystemFieldsFromEs \
 --current-filter true \
 --out "${WORKING_DIRECTORY}/${ES_SYSTEM_FIELDS_TEMP}" \
 --format parquet \
 "${CMWELL_INSTANCE}"

$SPARK_HOME/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "${SPARK_MASTER}" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class "cmwell.analytics.main.FindDuplicatedCurrentPathsInIndex" "${SPARK_ANALYSIS_JAR}" \
 --index "${WORKING_DIRECTORY}/${ES_SYSTEM_FIELDS_TEMP}" \
 --out "${WORKING_DIRECTORY}/duplicated-current" \
 "${CMWELL_INSTANCE}"

# Remove the extract from ES
rm -rf "${WORKING_DIRECTORY}/${ES_SYSTEM_FIELDS_TEMP}"

# Concatenate the output CSV files into a single file, and get rid of the enclosing directory.
# echo $'kind,uuid,lastModified,path,dc,indexName,indexTime,parent,current\n' > "${WORKING_DIRECTORY}/duplicated-current.csv"
cat "${WORKING_DIRECTORY}/duplicated-current"/*.csv >> "${WORKING_DIRECTORY}/duplicated-current.csv"
rm -rf "${WORKING_DIRECTORY}/duplicated-current"
