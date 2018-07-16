#!/bin/bash

# Check that the contents of the infoton table match the infoton's uuid.

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1

WORKING_DIRECTORY="infoton-data-integrity"

set -e # bail out if any command fails

rm -rf "${WORKING_DIRECTORY}"

$SPARK_HOME/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "${SPARK_MASTER}" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class "cmwell.analytics.main.CheckInfotonDataIntegrity" "${SPARK_ANALYSIS_JAR}" \
 --out "${WORKING_DIRECTORY}/infoton-data-integrity" \
 "${CMWELL_INSTANCE}"

# Move the single detail Parquet up to the working directory.
mv "${WORKING_DIRECTORY}"/infoton-data-integrity/part*.csv "${WORKING_DIRECTORY}"/infoton-data-integrity.csv
rm -rf "${WORKING_DIRECTORY}"/infoton-data-integrity
