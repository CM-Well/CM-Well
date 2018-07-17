#!/bin/bash

# Run a suite of internal data consistency checks on a CM-Well instance.

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url> [--no-source-filter]"
 exit 1
fi

CMWELL_INSTANCE=$1

export SOURCE_FILTER="--source-filter"
while test $# -gt 0; do
    case "$1" in
        -h|--help)
            echo "usage: $0 <cmwell-url> [--no-source-filter]"
            exit 1
            ;;
        -nsf|--no-source-filter)
            shift
            export SOURCE_FILTER="--no-source-filter"
            ;;
        *)
            shift
            ;;
    esac
done

source ./set-runtime.sh

# Do all our work in this directory
WORKING_DIRECTORY="internal-consistency"

EXTRACT_DIRECTORY_INDEX="index-system-fields"
EXTRACT_DIRECTORY_INFOTON="infoton-key-fields"
EXTRACT_DIRECTORY_PATH="path-key-fields"

# If an inconsistency is detected, it is filtered out if it falls within this period.
# This is a matter of quality of service policy, and might vary according to the system.
# For cases where there are multiple extracts, the consistency threshold will be this value
# plus the total time to extract all the data.
CONSISTENCY_THRESHOLD=`expr 10 \* 60 \* 1000`  # 10 minutes in milliseconds

set -e # bail out if any command fails

rm -rf "${WORKING_DIRECTORY}"
rm -rf log


# The first round of analysis is to check the infoton table for data consistency.
# This requires an extract of the infoton table that can't easily be combined with the copy that is read and
# cached below, so we just do it separately up front.

$SPARK_HOME/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "${SPARK_MASTER}" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class "cmwell.analytics.main.CheckInfotonDataIntegrity" "${SPARK_ANALYSIS_JAR}" \
 --out "${WORKING_DIRECTORY}/infoton-data-integrity" \
 "${CMWELL_INSTANCE}"

# Move the single detail CSV up to the working  directory.
cat "${WORKING_DIRECTORY}"/infoton-data-integrity/part*.csv > "${WORKING_DIRECTORY}"/infoton-data-integrity.csv
rm -rf "${WORKING_DIRECTORY}"/infoton-data-integrity


# Extract key fields from index, path and infoton.
# For the index, we do additional analysis, so extract all system fields.
# We want all of the retrieval operations to be grouped together do minimize the inconsistency window.
# Disable source filtering because of Elastic index bug returning random 500 internal server errors.

extract_start=`date +%s`

${JAVA_HOME}/bin/java \
 -Xmx31G \
 -XX:+UseG1GC \
 -cp "${EXTRACT_ES_UUIDS_JAR}" cmwell.analytics.main.DumpSystemFieldsFromEs \
 --out "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INDEX}" \
 "${SOURCE_FILTER}" \
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

 extract_end=`date +%s`

 # Calculate a new consistency threshold that CONSISTENCY_THRESHOLD plus the total extract time.
 OVERALL_CONSISTENCY_THRESHOLD=$[CONSISTENCY_THRESHOLD + (extract_end - extract_start) ]


# Exactly one version (uuid) for a path should be marked as current.
# Use the saved copy of the index data for this analysis.
$SPARK_HOME/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "${SPARK_MASTER}" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class "cmwell.analytics.main.FindDuplicatedCurrentPathsInIndex" "${SPARK_ANALYSIS_JAR}" \
 --current-threshold "${CONSISTENCY_THRESHOLD}ms" \
 --index "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INDEX}" \
 --out "${WORKING_DIRECTORY}/duplicated-current-index" \
 "${CMWELL_INSTANCE}"

 # Move the single detail CSV up to the working directory.
cat "${WORKING_DIRECTORY}"/duplicated-current-index/part*.csv > "${WORKING_DIRECTORY}"/duplicated-current-index.csv
rm -rf "${WORKING_DIRECTORY}"/duplicated-current-index


# Do the set difference operations - compare the set of uuids in the path and infoton tables and the index.

${SPARK_HOME}/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master ${SPARK_MASTER} --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class cmwell.analytics.main.SetDifferenceUuids "${SPARK_ANALYSIS_JAR}" \
 --current-threshold "${OVERALL_CONSISTENCY_THRESHOLD}ms" \
 --infoton "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INFOTON}" \
 --path "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_PATH}" \
 --index "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INDEX}" \
 --out "${WORKING_DIRECTORY}" \
 "${CMWELL_INSTANCE}"

# Extract the csv files for each set difference from the hadoop-format directory.
# They will be in hadoop format (one file per partition), so concatenate them all.
for x in "infoton-except-path" "infoton-except-index" "index-except-path" "index-except-infoton" "path-except-infoton" "path-except-index"
do
 cat "${WORKING_DIRECTORY}/${x}"/*.csv > "${WORKING_DIRECTORY}/${x}.csv"
 rm -rf "${WORKING_DIRECTORY}/${x}"
done


# Remove the extract directories since they may be large.
# The infoton data is specifically not removed since it might be needed for comparisons between systems.
#rm -rf "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INFOTON}"
rm -rf "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_PATH}"
rm -rf "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_INDEX}"


# Touch the _SUCCESS file to indicate successful completion of the internal consistency checks
touch "${WORKING_DIRECTORY}/_SUCCESS"
