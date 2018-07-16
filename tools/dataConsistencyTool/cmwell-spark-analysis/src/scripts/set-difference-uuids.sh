#!/bin/bash

# Run an set difference analysis of the uuids in the infoton/path/index within a CM-Well instance.
# This uses the separate extract-index-from-es assembly to extract the data from es.

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1

WORKING_DIRECTORY="uuid-set-comparison"
EXTRACT_DIRECTORY_INDEX="index-key-fields"
EXTRACT_DIRECTORY_INFOTON="infoton-key-fields"
EXTRACT_DIRECTORY_PATH="path-key-fields"

CONSISTENCY_THRESHOLD="1d"

set -e # bail out if any command fails

rm -rf "${WORKING_DIRECTORY}"

# Extract key fields from index, path and infoton.

${JAVA_HOME}/bin/java \
 -Xmx31G \
 -XX:+UseG1GC \
 -cp "${EXTRACT_ES_UUIDS_JAR}" cmwell.analytics.main.DumpKeyFieldsFromEs \
 --out "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INDEX}" \
 --format parquet \
 "${CMWELL_INSTANCE}"

${SPARK_HOME}/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "$SPARK_MASTER" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class cmwell.analytics.main.DumpInfotonWithKeyFields "${SPARK_ANALYSIS_JAR}" \
 --out "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INFOTON}" \
 "${CMWELL_INSTANCE}"

 ${SPARK_HOME}/bin/spark-submit \
  --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "$SPARK_MASTER" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class cmwell.analytics.main.DumpPathWithKeyFields "${SPARK_ANALYSIS_JAR}" \
 --out "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_PATH}" \
 "${CMWELL_INSTANCE}"

# Do the set difference operations

${SPARK_HOME}/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master ${SPARK_MASTER} --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class cmwell.analytics.main.SetDifferenceUuids "${SPARK_ANALYSIS_JAR}" \
 --current-threshold="${CONSISTENCY_THRESHOLD}" \
 --infoton "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INFOTON}" \
 --path "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_PATH}" \
 --index "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INDEX}" \
 --out "${WORKING_DIRECTORY}" \
 "${CMWELL_INSTANCE}"

# Remove the extract directories since they may be large
rm -rf "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INFOTON}"
rm -rf "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_PATH}"
rm -rf "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INDEX}"

# Extract the csv files for each set difference from the hadoop-format directory.
# They will be in hadoop format (one file per partition), so concatenate them all.
for x in "infoton-except-path" "infoton-except-index" "index-except-path" "index-except-infoton" "path-except-infoton" "path-except-index"
do
 cat "${WORKING_DIRECTORY}/${x}"/*.csv > "${WORKING_DIRECTORY}/${x}.csv"
 rm -rf "${WORKING_DIRECTORY}/${x}"
done
