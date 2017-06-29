#!/bin/sh
cd $(dirname -- "$0")
if [ -e "cmw.env" ]
then
  . ./cmw.env
fi

echo "########################################################################"
echo "# PLEASE NOTICE: THIS IS cmwell-cons DEBUGGER AND NOT CM-WELL DEBUGGER #"
echo "########################################################################"

bash -c "scala -J-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -nc -cp 'cons-lib/*' 2> >( grep -v '^#.*#' )"
