#!/bin/sh
bash -c "

mkdir -p components-extras;
cd components-extras;
rm -rf scala-2.*.tgz;
rm -rf jdk*;
rm -rf OpenJDK*;
wget https://downloads.lightbend.com/scala/2.12.8/scala-2.12.8.tgz;
wget https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u212-b04/OpenJDK8U-jdk_x64_linux_hotspot_8u212b04.tar.gz

cd -"
