#!/bin/sh
cd $(dirname -- "$0")

if [ -z $1 ]; then
    echo "Node IP is missing, cannot continue!"
    exit 1
fi
ping -c 1 -q $1 > /dev/null
if [ $? != "0" ] ; then
   echo No ping to $1, cannot continue!
   exit 1
fi

while true ; 
do
    echo "WARNING: $1 Will be removed from CM-Well grid!!!"
    echo "All data on this node will be wiped. Continue?"
    echo "If YES, write: YeS and press Enter" 
    read answer
    if [ $answer == 'YeS' ]; then
		break
	else
		echo "Wise choice. Exiting."
        exit 0
	fi
done

source ./cmw.env
scala -nc -cp 'cons-lib/*' -I cmw.conf -e "cmw.removeNode(\"$1\"); sys.exit(0)" 2>&1 | bash -c 'tee out.log'

echo "Please backup and send to TMS Operation team following files:"
ls -d -1 `pwd`/cmw-mappings*
