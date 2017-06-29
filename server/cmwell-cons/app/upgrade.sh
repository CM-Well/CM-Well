#!/bin/sh
cd $(dirname -- "$0")
. ./cmw.env

scala -nc -cp 'cons-lib/*' -I cmw.conf -e "cmw.upgrade() ; cmw.shutDownDataInitializer(); sys.exit(0)" 2>&1 | bash -c 'tee >( grep -w -v "^#.*#" > out.log )' | grep -w -v "^#.*#"
