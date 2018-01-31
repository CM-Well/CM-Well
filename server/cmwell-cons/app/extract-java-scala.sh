#!/bin/sh
cd $(dirname -- "$0")
bash -c "
cd components-extras;
tar -xf scala-2.12.4.tgz;
tar -xf jdk-8u66-linux-x64.tar.gz;
mv jdk1.8.0_66 java;
mv scala-2.12.4 scala;

" 2> /dev/null
