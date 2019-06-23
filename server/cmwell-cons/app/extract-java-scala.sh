#!/bin/sh
cd $(dirname -- "$0")
bash -c "
cd components-extras;
tar -xf scala-2.12.8.tgz;
tar -xf OpenJDK8U-jdk_x64_linux_hotspot_8u212b04.tar.gz;
mv jdk8u212-b04 java;
mv scala-2.12.8 scala;

"
