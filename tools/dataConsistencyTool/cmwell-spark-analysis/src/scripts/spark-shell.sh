#!/bin/bash

# Start a Spark shell (e.g., for ad-hoc analysis on extracted data).

source ./set-runtime.sh

$SPARK_HOME/bin/spark-shell \
 --master "${SPARK_MASTER}" --driver-memory "${SPARK_MEMORY}" --conf "spark.local.dir=${SPARK_TMP}"
