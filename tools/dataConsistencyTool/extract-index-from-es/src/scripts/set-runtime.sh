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
