#!/bin/bash

WORKING_DIR=$(dirname $(readlink -f ${BASH_SOURCE[0]}))

if [ -z $LUMONGO_JAVA_SETTINGS ]
then
	export LUMONGO_JAVA_SETTINGS="-Xmx2048m"
fi

STARTCMD="java $LUMONGO_JAVA_SETTINGS -cp $WORKING_DIR/@project@-@version@.jar org.lumongo.admin.StartNode $@"
$STARTCMD
