#!/bin/sh
cd $(dirname -- "$0")
bash -c "
cd components-extras;
tar -xf scala-2.12.8.tgz;
tar -xf OpenJDK8U-jdk_x64_linux_hotspot_8u232b09.tar.gz;
mv jdk8u232-b09 java;
mv scala-2.12.8 scala;

"
