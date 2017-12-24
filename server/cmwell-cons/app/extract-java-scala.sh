#!/bin/sh
cd $(dirname -- "$0")
bash -c "
cd components-extras;
tar -xf scala-2.11.12.tgz;
tar -xf jdk-8u66-linux-x64.tar.gz;
mv jdk1.8.0_66 java;
mv scala-2.11.12 scala;

rm scala/lib/akka*

" 2> /dev/null
