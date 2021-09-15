#!/bin/bash

# This script copies a set of files from the testbed server based on the modification date.  Optionally, it
# concatenates them into a single PCAP for further processing.

# $1 is the deployment environment (dev, test, ops)
# $2 is the remote server hostname (emit-sim3, etc.)
# $3 is how many minutes back to check for modified files
# $4 Flag to execute concatenation (0 = False, 1 = True)

echo "$(date +"%F %T,%3N"): Executing copy_testbed_files.sh cron job in '$1' environment from '$2' using '$3' minutes \
for '-mmin' and concat flag set to '$4'."

SOURCE_DIR=/proj/emit/dev/data/$2
TARGET_DIR=/store/emit/$1/tvac/testbed/$2
GROUP=emit-${1}

# Create tmp file list of remote files that were modified recently
TIMESTAMP=`date "+%Y%m%dT%H%M%S"`
TMP_FILE_LIST=$TARGET_DIR/rsync_filelist_${TIMESTAMP}.txt
ssh ${2}.jpl.nasa.gov \
"find $SOURCE_DIR -mmin -$3 -name 'emit-edd*pcap' -exec realpath --relative-to=$SOURCE_DIR {} \;" | sort > $TMP_FILE_LIST

# Perform the rsync
rsync -rtz --progress --partial-dir=$TARGET_DIR/rsync_partial --log-file=$TARGET_DIR/rsync.log \
--files-from=$TMP_FILE_LIST ${2}.jpl.nasa.gov:$SOURCE_DIR $TARGET_DIR

# Update permissions so files are passed into SDS with proper permissions
chgrp -R $GROUP $TARGET_DIR
chmod -R ug+rw $TARGET_DIR

# Perform concatenation of PCAP files
if [[ $4 -eq 1 ]]; then
    echo "Concatenation flag set to 1, concatenating files in $TMP_FILE_LIST..."
    ./concat_pcap_by_list.sh  $1 $2 $TMP_FILE_LIST
fi