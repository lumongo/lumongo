#!/bin/bash

WORKING_DIR="${BASH_SOURCE[0]}";
if ([ -h "${WORKING_DIR}" ]) then
while([ -h "${WORKING_DIR}" ]) do WORKING_DIR=`readlink "${WORKING_DIR}"`; done
fi
pushd . > /dev/null
cd `dirname ${WORKING_DIR}` > /dev/null
WORKING_DIR=`pwd`;
popd > /dev/null

if [ -z $LUMONGO_CLIENT_JAVA_SETTINGS ]
then
	export LUMONGO_CLIENT_JAVA_SETTINGS="-Xmx256m"
fi

java $LUMONGO_CLIENT_JAVA_SETTINGS -cp $WORKING_DIR/@project@-@version@.jar org.lumongo.admin.IndexAdmin "$@"
