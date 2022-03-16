#!/bin/bash

# $1 is the environment, either "test" or "ops"
# $2 is the conda environment

echo "$(date +"%F %T,%3N"): Executing copy_sftp.sh cron job with '$1' environment and '$2' conda environment."

source /beegfs/store/shared/anaconda3/etc/profile.d/conda.sh
conda activate $2

INGEST_DIR=/store/emit/$1/ingest
SFTP_DIR=${INGEST_DIR}/sftp
RPSM_DIR=${SFTP_DIR}/rpsm_archive
DATE_DIR=${RPSM_DIR}/$(date "+%Y%m%d")
ERROR_DIR=${SFTP_DIR}/errors

GROUP=emit-${1}

if [[ ! -e ${SFTP_DIR} ]]; then
    mkdir -p ${SFTP_DIR}
    chgrp $GROUP ${SFTP_DIR}
    chmod ug+rw ${SFTP_DIR}
fi

if [[ ! -e ${RPSM_DIR} ]]; then
    mkdir -p ${RPSM_DIR}
    chgrp $GROUP ${RPSM_DIR}
    chmod ug+rw ${RPSM_DIR}
fi

if [[ ! -e ${ERROR_DIR} ]]; then
    mkdir -p ${ERROR_DIR}
    chgrp $GROUP ${ERROR_DIR}
    chmod ug+rw ${ERROR_DIR}
fi

export AIT_ROOT=/store/emit/$1/repos/emit-ios
export AIT_CONFIG=/store/emit/$1/repos/emit-ios/config/config.yaml
export AIT_ISS_CONFIG=/store/emit/$1/repos/emit-ios/config/sim.yaml

# Copy data from remote /sftp/emitops/sds folder to local sftp folder
echo "$(date +"%F %T,%3N"): Attempting to copy data from sftp.jpl.nasa.gov..."
python /store/emit/$1/repos/emit-ios/emit/bin/emit_sftp_copy.py ${SFTP_DIR} --sftp_path /sftp/emitops/sds --rpsm
echo "$(date +"%F %T,%3N"): emit_sftp_copy.py script completed."

for file in ${SFTP_DIR}/*; do
    if [[ -f "$file" && $file != *rpsm ]]; then
        if [[ -e ${file}.rpsm ]]; then
            echo "$(date +"%F %T,%3N"): Found ${file} and corresponding ${file}.rpsm"
            chgrp $GROUP ${file} ${file}.rpsm
            chmod ug+rw ${file} ${file}.rpsm
            # Add a date folder if it doesn't exist
            if [[ ! -e ${DATE_DIR} ]]; then
                mkdir -p ${DATE_DIR}
                chgrp $GROUP ${DATE_DIR}
                chmod ug+rw ${DATE_DIR}
            fi
            mv $file ${INGEST_DIR}/
            mv ${file}.rpsm ${DATE_DIR}/
            echo "$(date +"%F %T,%3N"): Moved ${file} to ${INGEST_DIR}/ and ${file}.rpsm to ${DATE_DIR}/"
        else
            echo "$(date +"%F %T,%3N"): Found ${file}, but no corresponding .rpsm file. Moving ${file} to ${ERROR_DIR}/"
            chgrp $GROUP ${file}
            chmod ug+rw ${file}
            mv ${file} ${ERROR_DIR}/
        fi
    fi
done
