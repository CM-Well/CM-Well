#!/bin/sh

INST_DIR=$1
USE_AUTHORIZATION=${2-true}
DEPLOY_JAVA=${3-false}
SYN_LINK_LIB=${4-true}
cd $(dirname -- "$0")
MSG='"in script after install"'

bash -c """scala "-Dlocation=$INST_DIR" -nc -cp 'cons-lib/*' -I pe -e 'val newPe = pe.copy(nbg = true, oldBg = false,newBg = true,useAuthorization = $USE_AUTHORIZATION, deployJava = $DEPLOY_JAVA, symLinkLib = $SYN_LINK_LIB); newPe.LogLevel.debug ; newPe.install; sys.exit(0)'"""
