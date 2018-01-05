#!/bin/sh
bash -c "

mkdir -p components-extras;
cd components-extras;
rm logstash-2.1.1.tar.gz;
rm -rf scala-2.11.*.tgz;
rm -rf jdk*;
rm kibana-4.1.1-linux-x64.tar.gz;

wget http://builder/logstash/logstash-2.1.1.tar.gz;
wget http://builder/logstash/kibana-4.1.1-linux-x64.tar.gz;
wget http://downloads.lightbend.com/scala/2.12.4/scala-2.12.4.tgz;
wget http://builder/java/jdk-8u66-linux-x64.tar.gz;

cd -"
