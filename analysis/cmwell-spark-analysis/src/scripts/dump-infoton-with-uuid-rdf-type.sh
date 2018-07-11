#!/bin/bash

# Dump uuids and rdf type from the infoton table.

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

OUTPUT_FILE="infoton-uuid-rdf-type"

rm -rf "${OUTPUT_FILE}"

$SPARK_HOME/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "${SPARK_MASTER}" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class "cmwell.analytics.main.DumpInfotonWithUuidAndRdfType" "${SPARK_ANALYSIS_JAR}" \
 --out "${OUTPUT_FILE}" \
 $@
