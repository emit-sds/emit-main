#!/bin/bash

# This script is for the ops environment only and copies APID 1675 science data from the EGSE server, then kicks off
# ingestion into the SDS using the cron partition

# $1 is the date to copy ("YYYYMMDD")

DATE=""
if [[ -z "$1" ]]; then
  DATE=$(date -d "1 day ago" "+%Y%m%d")
else
  DATE=${1}
fi

DATE_DASHES=$(date -d ${DATE} +%Y-%m-%d)

echo "$(date +"%F %T,%3N"): Executing copy_ingest_1675_science_data.sh cron job for date ${DATE}."

OPS_CONDA_ENV=emit-main-20220606-ops
echo "Activating conda environment ${OPS_CONDA_ENV}"
source /beegfs/store/shared/anaconda3/etc/profile.d/conda.sh
conda activate ${OPS_CONDA_ENV}

REMOTE_SERVER=emit-egse1.jpl.nasa.gov
REMOTE_DIR=/proj/emit/ops/data/emit-egse1/APID_1675
# Marker file is a log file like this remote-epc-2022-11-09.log
MARKER_FILE=${REMOTE_DIR}/remote-epc-${DATE_DASHES}.log
EGSE_DIR=/store/emit/ops/ingest/egse1
INGEST_DATE_DIR=${EGSE_DIR}/${DATE}

if [[ -d ${INGEST_DATE_DIR} ]]; then
  echo "Directory ${INGEST_DATE_DIR} already exists. Assuming ingest already ran or started. Exiting..."
  exit 0
fi

# If the directory doesn't exist then check the remote server for the log file to see if the data has been
# transferred.  Log file will say "total size is" if so.
if $(ssh ${REMOTE_SERVER} "cat ${MARKER_FILE} | grep -q 'total size is'"); then
  echo "The file ${MARKER_FILE} on ${REMOTE_SERVER} says that files have completed transfer. Ready to copy files."
  mkdir ${INGEST_DATE_DIR}
  cd ${INGEST_DATE_DIR}

  echo "Copying files from ${REMOTE_SERVER}:${REMOTE_DIR}/1675_${DATE}* to ${INGEST_DATE_DIR}"
  scp -p ${REMOTE_SERVER}:${REMOTE_DIR}/1675_${DATE}* .

  if [ -z "$(ls -A ${INGEST_DATE_DIR})" ]; then
    echo "${INGEST_DATE_DIR} is empty after copying files. Exiting..."
    exit 1
  fi

  # If not empty, then assume we have some files including .rpsm files
  echo "Moving rpsm files to subdirectory"
  mkdir rpsm
  mv *.rpsm rpsm/

  HOSC_BASE=1675_${DATE}000000_
  if [[ -f ${HOSC_BASE} ]]; then
    echo "Renaming ${HOSC_BASE} to ${HOSC_BASE}hsc.bin"
    mv ${HOSC_BASE} ${HOSC_BASE}hsc.bin
  fi
  for file in ${HOSC_BASE}-*; do
    if [[ -f $file ]]; then
      echo "Renaming $file to ${file}_hsc.bin"
      mv $file ${file}_hsc.bin
    fi
  done

  echo "Updating permissions on ${INGEST_DATE_DIR}"
  chgrp -R emit-ops ${INGEST_DATE_DIR}
  chmod -R g+w ${INGEST_DATE_DIR}

  echo "Starting workflow jobs"
  cd /store/emit/ops/repos/emit-main/emit_main
  for file in $(ls ${INGEST_DATE_DIR}/1675* | sort -V); do
    echo "Running workflow with $file"
    # python run_workflow.py -c config/ops_sds_config.json -p l1aframe --partition cron -s $file
  done
else
  echo "Either ${MARKER_FILE} did not exist on ${REMOTE_SERVER} or the files have not yet transferred for this date. Exiting..."
  exit 0
fi
