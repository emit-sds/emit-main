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
echo "$(date +"%F %T,%3N"): Attempting to copy data from sftp.jpl.nasa.gov from /sftp/emitops/sds (using --rpsm)"
python /store/emit/$1/repos/emit-ios/emit/bin/emit_sftp_copy.py ${SFTP_DIR} --sftp_path /sftp/emitops/sds --rpsm
echo "$(date +"%F %T,%3N"): Copy from /sftp/emitops/sds completed."

# Copy data from remote /sftp/emitops/iss-bad folder to local sftp folder
echo "$(date +"%F %T,%3N"): Attempting to copy data from sftp.jpl.nasa.gov from /sftp/emitops/iss-bad"
python /store/emit/$1/repos/emit-ios/emit/bin/emit_sftp_copy.py ${SFTP_DIR} --sftp_path /sftp/emitops/iss-bad
echo "$(date +"%F %T,%3N"): Copy from /sftp/emitops/iss-bad completed."

for file in ${SFTP_DIR}/*; do
    fname=$(basename $file)
    if [[ $fname == 167*gz || $fname == emit_167*gz || $fname == EMIT_167*gz ]]; then
        fname_noext=$(basename $file .gz)
        unzipped_path="${SFTP_DIR}/${fname_noext}"
        rpsm_path="${unzipped_path}.rpsm"
        if [[ -e ${rpsm_path} ]]; then
            echo "$(date +"%F %T,%3N"): Found ${file} and corresponding ${rpsm_path}"
            gzip -d $file
            echo "$(date +"%F %T,%3N"): Unzipped ${file}"

            chgrp $GROUP ${unzipped_path} ${rpsm_path}
            chmod ug+rw ${unzipped_path} ${rpsm_path}

            # Add a date folder if it doesn't exist
            if [[ ! -e ${DATE_DIR} ]]; then
                echo "$(date +"%F %T,%3N"): Adding folder ${DATE_DIR}"
                mkdir -p ${DATE_DIR}
                chgrp $GROUP ${DATE_DIR}
                chmod ug+rw ${DATE_DIR}
            fi

            # Move rpsm file to archive
            mv ${rpsm_path} ${DATE_DIR}/
            echo "$(date +"%F %T,%3N"): Moved ${rpsm_path} to ${DATE_DIR}/"

            # Add _hsc.bin suffix to HOSC files to play nice with existing ingest code and move to ingest folder
            mv ${unzipped_path} ${INGEST_DIR}/${fname_noext}_hsc.bin
            echo "$(date +"%F %T,%3N"): Moved ${unzipped_path} to ${INGEST_DIR}/${fname_noext}_hsc.bin"
        else
            echo "$(date +"%F %T,%3N"): Found ${file}, but no corresponding .rpsm file. Moving ${file} to ${ERROR_DIR}/"
            chgrp $GROUP ${file}
            chmod ug+rw ${file}
            mv ${file} ${ERROR_DIR}/
        fi

    elif [[ $fname == bad*gz ]]; then
        gzip -d $file
        echo "$(date +"%F %T,%3N"): Unzipped ${file}"
        fname_noext=$(basename $file .gz)
        unzipped_path="${SFTP_DIR}/${fname_noext}"
        chgrp $GROUP ${unzipped_path}
        chmod ug+rw ${unzipped_path}
        mv ${unzipped_path} ${INGEST_DIR}/${fname_noext}.sto
        echo "$(date +"%F %T,%3N"): Moved ${SFTP_DIR}/${fname_noext} to ${INGEST_DIR}/${fname_noext}.sto"
    fi
done
