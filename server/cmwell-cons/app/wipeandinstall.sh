#!/bin/sh
./wipewarning.sh && {

cd $(dirname -- "$0")
source ./cmw.env
scala -cp 'cons-lib/*' -I cmw.conf -e "cmw.install; cmw.shutDownDataInitializer(); sys.exit(0)" 2>&1 | bash -c 'tee >( egrep -w -v "^#.*#|^\*.*\*|^[[:space:]]*$" > out.log )' | egrep -w -v "^#.*#|^\*.*\*|^[[:space:]]*$"

}
