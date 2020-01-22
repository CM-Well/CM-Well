#!/bin/sh
cd $(dirname -- "$0")
source ./cmw.env

scala -cp 'cons-lib/*' -I cmw.conf -e "cmw.prepareMachines(); sys.exit(0)" 2>&1 | bash -c 'tee out.log'
