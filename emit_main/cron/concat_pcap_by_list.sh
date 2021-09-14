#!/bin/bash

# This script concatenates PCAP files in a given list

# $1 is the deployment environment (dev, test, ops)
# $2 is the input file list

echo "$(date +"%F %T,%3N"): Executing concat_pcap_by_list.sh in '$1' environment on file list '$2' ."

BASE_DATA_DIR=/store/emit/$1/tvac/testbed/emit-sim3
INPUT_LIST=$2
GROUP=emit-${1}

# Get the timestamp of first and last PCAPs in the list in order to name the output file
START_FILE=$(basename `cat $INPUT_LIST | head -n1`)
START_TIME=${START_FILE:9:15}
STOP_FILE=$(basename `cat $INPUT_LIST | tail -n1`)
STOP_TIME=${STOP_FILE:9:15}

cd $BASE_DATA_DIR
CONCAT_FILE=emit-edd-${START_TIME}-${STOP_TIME}.pcap

# Remove file if it already exists
if [[ -f "$CONCAT_FILE" ]]; then
    rm -f $CONCAT_FILE
fi
cat $INPUT_LIST | while read line; do cat $line >> $CONCAT_FILE; done;

# Update permissions
chgrp $GROUP $CONCAT_FILE
chmod ug+rw $CONCAT_FILE

# mv $CONCAT_FILE /store/emit/$1/tvac/$CONCAT_FILE
# rm -f $TMP_FILE_LIST