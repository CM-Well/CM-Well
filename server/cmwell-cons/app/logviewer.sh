#!/bin/sh

CLUSTER=$1
MACHINE=$2
LOGPATH=$3

cd $(dirname -- "$0")
source ./cmw.env
bash -c "scala -cp components/cmwell-controller-assembly-1.2.1-SNAPSHOT.jar:components/cmwell-cons_2.11-1.2.1-SNAPSHOT.jar -I $CLUSTER -e $CLUSTER.getLogViewer(\"$MACHINE\", \"$LOGPATH\") 2> >( grep -v '^#.*#' )"
