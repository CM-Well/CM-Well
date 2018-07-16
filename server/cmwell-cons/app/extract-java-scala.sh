#!/bin/sh
cd $(dirname -- "$0")
bash -c "
cd components-extras;
tar -xf scala-2.12.6.tgz;
tar -xf jre-8u172-linux-x64.tar.gz;
mv jre1.8.0_172 java;
mv scala-2.12.6 scala;

" 2> /dev/null
