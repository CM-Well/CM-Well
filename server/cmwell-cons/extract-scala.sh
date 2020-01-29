#!/bin/sh
cd $(dirname -- "$0")
bash -c "
cd /home/u/cmwell/components-extras;
tar -xf scala-2.12.6.tgz;
mv scala-2.12.6 scala;

" 2> /dev/null
