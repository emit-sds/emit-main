#!/bin/bash

# $1 is the environment, either "test" or "ops"
# $2 is the conda environment

echo "$(date +"%F %T,%3N"): Executing ingest_pcap_files.sh cron job with '$1' environment and '$2' conda environment."

source /shared/anaconda3/etc/profile.d/conda.sh
conda activate $2

TVAC_DIR=/store/emit/$1/tvac
mkdir -p ${TVAC_DIR}/processed
mkdir -p ${TVAC_DIR}/tmp

export AIT_ROOT=/store/emit/$1/repos/emit-ios
export AIT_CONFIG=/store/emit/$1/repos/emit-ios/config/config.yaml
export AIT_ISS_CONFIG=/store/emit/$1/repos/emit-ios/config/sim.yaml

for file in ${TVAC_DIR}/*.pcap; do
    if [[ -f "$file" ]]; then
        echo "$(date +"%F %T,%3N"): Processing ${file}..."
        python /store/emit/$1/repos/emit-sds-l0/emit_pcap_to_hosc_raw.py --input-file ${file} --output-dir ${TVAC_DIR}/tmp
        mv ${file} ${TVAC_DIR}/processed/
        mv ${TVAC_DIR}/tmp/* /store/emit/$1/ingest/
        echo "$(date +"%F %T,%3N"): Moved ${file} to ${TVAC_DIR}/processed/ and output HOSC files to /store/emit/$1/ingest/"
    else
        echo "$(date +"%F %T,%3N"): Did not find any PCAP files to process."
    fi
done
