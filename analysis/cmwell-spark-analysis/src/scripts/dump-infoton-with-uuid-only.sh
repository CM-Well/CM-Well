#!/bin/bash

# Dump uuids from the infoton table.

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

WORKING_DIRECTORY="dump-uuids"

rm -rf "${WORKING_DIRECTORY}/infoton"

$SPARK_HOME/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "${SPARK_MASTER}" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class "cmwell.analytics.main.DumpInfotonWithUuidOnly" "${SPARK_ANALYSIS_JAR}" \
 --out "${WORKING_DIRECTORY}/infoton" \
 $@
