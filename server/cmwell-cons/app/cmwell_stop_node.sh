#!/bin/sh
cd $(dirname -- "$0")
#echo $1
if [ -z $1 ]; then
    echo "Node IP is missing, cannot continue!"
    exit 1
fi
ping -c 1 -q $1 > /dev/null
if [ $? != "0" ] ; then
   echo No ping to $1, cannot continue!
   exit 1
fi

echo "Stopping CM-Well on node $1"
source ./cmw.env
scala -nc -cp 'cons-lib/*' -I cmw.conf -e "cmw.shutdown(List(\"$1\")); sys.exit(0)" 2>&1 | bash -c 'tee >( grep -w -v "^#.*#" > out.log )' | grep -w -v "^#.*#"

