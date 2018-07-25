#!/bin/sh
bash -c "

mkdir -p components-extras;
cd components-extras;
rm -rf scala-2.11.*.tgz;
rm -rf jdk*;

wget https://downloads.lightbend.com/scala/2.12.6/scala-2.12.6.tgz;
wget http://builder/java/jdk-8u172-linux-x64.tar.gz

cd -"
