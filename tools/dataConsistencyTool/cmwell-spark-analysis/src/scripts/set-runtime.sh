#!/bin/bash

# All the scripts in this project call this script first to ensure that JAVA_HOME and SPARK_HOME
# environment variables are exported.
#
# This implementation assumes that specific versions of the JRE and Spark are unpacked in the installation directory.
# This is implemented this way since these tools will typically be run in an environment where we cannot install
# a JRE or Spark runtime.
#
# If running on an environment that already has a suitable JAVA_HOME and SPARK_HOME, you can just comment out
# the export statements in this file, and that way all the scripts don't need to be modified.
#
# When setting up the Spark environment, you will want to ensure that an appropriate log4j.properties is copied
# to $SPARK_HOME/conf (there is a simple version in src/main/resources/log4j.properties).

export JAVA_HOME=jre1.8.0_161

export SPARK_HOME=spark-2.2.1-bin-hadoop2.7


# Ideally, this should be changed to distribute the tmp space over as many local drives as possible.
# If you do not do that, operations like a join will likely cause complete I/O saturation, and the analysis
# will run very slowly.
#SPARK_TMP="/disk1/tmp,/disk2/tmp,/disk3/tmp,/disk4/tmp"
export SPARK_TMP="tmp"

# We are running Spark in a single local process (as opposed to an actual cluster), so we need to tell it how many
# executors to use. With the machines that we have been using, there are typically 48 cores, so we use half the cores
# for Spark to avoid consuming all the CPU resources on that machine. You might adjust this configuration to use
# more or less CPU resources.
export SPARK_MASTER="local[24]"

# All these analysis tools should work well in 31g of JVM heap. 31g is the largest heap where the JVM can use
# compressed pointers, so don't go beyond 31g.
export SPARK_MEMORY="31g"

# The assemblies that are used by the various analysis tasks.
export EXTRACT_ES_UUIDS_JAR="extract-index-from-es-assembly-0.1.jar"
export SPARK_ANALYSIS_JAR="cmwell-spark-analysis-assembly-0.1.jar"


