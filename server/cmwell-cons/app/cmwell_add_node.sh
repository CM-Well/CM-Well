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
map_file="cmw-mappings_$1"
if [ ! -f $map_file ] ; then
   echo "$map_file for $1 not found, cannot continue!"
   exit 1
fi 

while true ; 
do
    echo "WARNING: $1 Will be added to CM-Well grid!!!"
    echo "Continue?"
    echo "If YES, write: YeS and press Enter" 
    read answer
    if [ $answer == 'YeS' ]; then
		break
	else
		echo "Wise choice. Exiting"
        exit 0
	fi
done

source ./cmw.env
scala -cp 'cons-lib/*' -I cmw.conf -e "cmw.addNodes(\"$map_file\"); sys.exit(0)" 2>&1 | bash -c 'tee out.log'
