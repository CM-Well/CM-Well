#!/bin/sh
bash -c "

mkdir -p components-extras;
cd components-extras;
rm -rf scala-2.11.*.tgz;
rm -rf jdk*;
rm -rf OpenJDK*;
wget https://downloads.lightbend.com/scala/2.12.6/scala-2.12.6.tgz;
wget https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u192-b12/OpenJDK8U-jdk_x64_linux_hotspot_8u192b12.tar.gz

cd -"
