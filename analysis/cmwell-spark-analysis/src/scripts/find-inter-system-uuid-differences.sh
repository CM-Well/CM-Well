#!/bin/bash

# Run an set difference analysis of the uuids between two systems.
# This script assumes that the extract of key fields from the the sites being compared are in the working directory.
#

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1

WORKING_DIRECTORY="inter-system-uuid-differences"

# The directories where the extracts from the two sites are expected to be.
EXTRACT_DIRECTORY_SITE1="site1"
EXTRACT_DIRECTORY_SITE2="site2"

# Logical names for the sites. TODO: Change these to something meaningful.
SITE1_NAME="site1"
SITE2_NAME="site2"

if [ ! -d "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_SITE1}" ]; then
  echo "The key fields extract from site1 must be placed in ${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_SITE1}"
  exit 1
fi

if [ ! -d "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_SITE2}" ]; then
  echo "The key fields extract from site1 must be placed in ${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_SITE2}"
  exit 1
fi

CONSISTENCY_THRESHOLD="1d"

set -e # bail out if any command fails

rm -rf "${WORKING_DIRECTORY}/${SITE1_NAME}-except-${SITE2_NAME}*"
rm -rf "${WORKING_DIRECTORY}/${SITE2_NAME}-except-${SITE1_NAME}*"
rm -f "${WORKING_DIRECTORY}/_SUCCESS"

# Do the set difference operations

${SPARK_HOME}/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master ${SPARK_MASTER} --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class cmwell.analytics.main.InterSystemSetDifference "${SPARK_ANALYSIS_JAR}" \
 --current-threshold="${CONSISTENCY_THRESHOLD}" \
 --site1data "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_SITE1}" \
 --site2data "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_SITE2}" \
 --site1name "${SITE1_NAME}" \
 --site2name "${SITE2_NAME}" \
 --out "${WORKING_DIRECTORY}"

# Extract the csv files for each set difference from the hadoop-format directory.
# They will be in hadoop format (one file per partition), so concatenate them all.
for x in "${SITE1_NAME}-except-${SITE2_NAME}" "${SITE2_NAME}-except-${SITE1_NAME}"
do
 cat "${WORKING_DIRECTORY}/${x}"/*.csv > "${WORKING_DIRECTORY}/${x}.csv"
 rm -rf "${WORKING_DIRECTORY}/${x}"
done

# Remove the extract directories since they may be large
rm -rf "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_SITE1}"
rm -rf "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_SITE2}"

touch "${WORKING_DIRECTORY}/_SUCCESS"
