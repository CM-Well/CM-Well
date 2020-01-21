#!/bin/sh
cd $(dirname -- "$0")
source ./cmw.env

scala -cp 'cons-lib/*' -I cmw.conf -e "cmw.genResources(); cmw.restartCassandra ; sys.exit(0)"
