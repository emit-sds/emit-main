#!/bin/bash

# This script copies a set of files from the testbed server based on the modification date.  Optionally, it
# concatenates them into a single PCAP for further processing.

# $1 is the deployment environment (dev, test, ops)
# $2 is how many minutes back to check for modified files
# $3 Flag to execute concatenation (0 = False, 1 = True)

echo "$(date +"%F %T,%3N"): Executing copy_testbed_files.sh cron job in '$1' environment using '$2' minutes for '-mmin'."

SOURCE_DIR=/proj/emit/dev/data/emit-sim3
TARGET_DIR=/store/emit/$1/tvac/testbed/emit-sim3
GROUP=emit-${1}

# Create tmp file list of remote files that were modified recently
TIMESTAMP=`date "+%Y%m%dT%H%M%S"`
TMP_FILE_LIST=$TARGET_DIR/rsync_filelist_${TIMESTAMP}.txt
ssh emit-sim3.jpl.nasa.gov \
"find $SOURCE_DIR -mmin -$2 -name 'emit-edd*pcap' -exec realpath --relative-to=$SOURCE_DIR {} \;" | sort > $TMP_FILE_LIST

# Perform the rsync
rsync -rtz --progress --partial-dir=$TARGET_DIR/rsync_partial --log-file=$TARGET_DIR/rsync.log \
--files-from=$TMP_FILE_LIST emit-sim3.jpl.nasa.gov:$SOURCE_DIR $TARGET_DIR

# Update permissions so files are passed into SDS with proper permissions
chgrp -R $GROUP $TARGET_DIR
chmod -R ug+rw $TARGET_DIR

# Perform concatenation of PCAP files
if [[ $3 -eq 1 ]]; then
    echo "Concatenation flag set to 1, concatenating files in $TMP_FILE_LIST..."
    ./concat_pcap_by_list.sh  $1 $TMP_FILE_LIST
fi