#!/bin/bash

# Find inconsistencies between the infoton table and the ES index.

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1

WORKING_DIRECTORY="infoton-index-inconsistencies"
ES_SYSTEM_FIELDS_TEMP="index-system-fields"

CONSISTENCY_THRESHOLD="1d"

set -e # bail out if any command fails

rm -rf "${WORKING_DIRECTORY}"

$JAVA_HOME/bin/java \
 -XX:+UseG1GC \
 -Xmx31G \
 -cp "${EXTRACT_ES_UUIDS_JAR}" cmwell.analytics.main.DumpSystemFieldsFromEs \
 --out "${WORKING_DIRECTORY}/${ES_SYSTEM_FIELDS_TEMP}" \
 --format parquet \
 "${CMWELL_INSTANCE}"

$SPARK_HOME/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "$SPARK_MASTER" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=$SPARK_TMP" \
 --class "cmwell.analytics.main.FindInfotonIndexInconsistencies" "${SPARK_ANALYSIS_JAR}" \
 --current-threshold="${CONSISTENCY_THRESHOLD}" \
 --es "${WORKING_DIRECTORY}/${ES_SYSTEM_FIELDS_TEMP}" \
 --out-parquet "${WORKING_DIRECTORY}/detail" \
 --out-csv "${WORKING_DIRECTORY}/detail_csv" \
 "${CMWELL_INSTANCE}"

# Get rid of the source index data since it may be large.
rm -rf "${WORKING_DIRECTORY}"/${ES_SYSTEM_FIELDS_TEMP}

# Extract the detail data in CSV form for easier access.
mv "${WORKING_DIRECTORY}"/detail_csv/part*.csv "${WORKING_DIRECTORY}"/detail.csv
rm -rf "${WORKING_DIRECTORY}"/detail_csv

$SPARK_HOME/bin/spark-submit \
 --master "$SPARK_MASTER" -Xmx${SPARK_MEMORY} --driver-memory "$SPARK_MEMORY" --conf "spark.local.dir=$SPARK_TMP" \
 --class "cmwell.analytics.main.AnalyzeInconsistenciesResult" "${SPARK_ANALYSIS_JAR}" \
 --in "${WORKING_DIRECTORY}/detail" \
 --out "${WORKING_DIRECTORY}/summary.csv"