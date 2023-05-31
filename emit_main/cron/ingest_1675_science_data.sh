#!/bin/bash

# This script performs ingestion of 1675 science data into the SDS using the cron partition

# $1 is the environment, either "dev", "test", or "ops"
# $2 is the conda environment

echo "$(date +"%F %T,%3N"): Executing ingest_1675_science_data.sh cron job for $1 environment with $2 conda environment"

echo "Activating conda environment $2"
source /beegfs/store/shared/anaconda3/etc/profile.d/conda.sh
conda activate ${2}

cd /store/emit/${1}/repos/emit-main/emit_main
for file in $(ls -1tr /store/emit/${1}/ingest/1675*); do
  echo "Ingesting HOSC file $file"
  python run_workflow.py -c config/${1}_sds_config.json -p l1aframe --partition cron --miss_pkt_thresh 0.1 -s $file
done
