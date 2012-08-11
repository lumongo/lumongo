#!/bin/bash

WORKING_DIR=$(dirname $(readlink -f ${BASH_SOURCE[0]}))

if [ -z $LUMONGO_CLIENT_JAVA_SETTINGS ]
then
	export LUMONGO_CLIENT_JAVA_SETTINGS="-Xmx256m"
fi

java $LUMONGO_CLIENT_JAVA_SETTINGS -cp $WORKING_DIR/@project@-@version@.jar org.lumongo.admin.ClusterAdmin "$@"
