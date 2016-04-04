#!/bin/sh

MAIN_CLASS=munwin.tsv_kafka_producer.KafkaProducer

pushd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null
PROJECT_DIR="$( pwd )"

CONF_DIR=$PROJECT_DIR/conf

#Get the location of the tnsnames.ora and wallet files
USER="$(whoami)"
HOME="$(getent passwd $user | awk -F ':' '{print $6}')"

JAVA=/usr/bin/java
MAX_MEMORY="-Xmx256M"
MAX_PERM_GEN="-XX:MaxPermSize=256m"
MEMORY_SETTINGS="$max_memory $max_perm_gen"

OPTIONS="-d64"

LIB_JARS=; for i in $PROJECT_DIR/lib/*.jar; do LIB_JARS=$LIB_JARS:$i; done;
LIB_DEP_JARS=; for i in $PROJECT_DIR/lib/dependencies/*.jar; do LIB_DEP_JARS=$LIB_DEP_JARS:$i; done;
CLASSPATH=$CONF_DIR:$LIB_JARS:$LIB_DEP_JARS

#java -Xms10m -Xmx256m -cp $CLASSPATH $MAIN_CLASS $@
COMMAND="$JAVA $MEMORY_SETTINGS $OPTIONS -cp $CLASSPATH $MAIN_CLASS $@"
exec $COMMAND

popd >/dev/null
