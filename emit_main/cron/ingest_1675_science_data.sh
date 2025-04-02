#!/bin/bash

# This script performs ingestion of 1675 science data into the SDS using the cron partition

# $1 is the environment, either "dev", "test", or "ops"
# $2 is the conda environment

echo "$(date +"%F %T,%3N"): Executing ingest_1675_science_data.sh cron job for $1 environment with $2 conda environment"

echo "Activating conda environment $2"
source /local/miniconda3/etc/profile.d/conda.sh
conda activate ${2}

cd /store/emit/${1}/repos/emit-main/emit_main
# for file in `ls -1tr /store/emit/ops/ingest/1675*`; do
for file in $(find /store/emit/${1}/ingest -maxdepth 1 -type f -name "1675*hsc.bin" | sort -t '_' -k2,2V -k3,3V); do
  echo "Ingesting HOSC file $file"
  python run_workflow.py -c config/${1}_sds_config.json -p l1aframe --partition cron --miss_pkt_thresh 0.1 -s $file
done
