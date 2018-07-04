#!/bin/sh
cd $(dirname -- "$0")
source ./cmw.env
scala -nc -cp 'cons-lib/*' -I cmw.conf -e "cmw.purge; sys.exit(0)" 2>&1 | bash -c 'tee > out.log'