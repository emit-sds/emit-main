#!/bin/bash

# $1 is the environment, either "test" or "ops"
# $2 is the conda environment

T=$(date)

echo
echo "$T: Executing ingest_hosc_files.sh cron job with '$1' argument"
echo

source /shared/anaconda3/etc/profile.d/conda.sh
conda activate $2

cd /store/emit/$1/repos/emit-main/emit_main
python run_file_monitor.py -c config/test_sds_config.json
