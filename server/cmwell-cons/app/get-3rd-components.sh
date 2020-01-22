#!/bin/sh
bash -c "

mkdir -p components-extras;
cd components-extras;
rm -rf scala-2.*.tgz;
rm -rf jdk*;
rm -rf OpenJDK*;
wget https://downloads.lightbend.com/scala/2.13.1/scala-2.13.1.tgz;
wget https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u232-b09/OpenJDK8U-jdk_x64_linux_hotspot_8u232b09.tar.gz

cd -"
