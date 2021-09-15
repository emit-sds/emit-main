#!/bin/bash

# $1 is the environment, either "dev", "test", or "ops"
# $2 is the conda environment
# $3 Optional args to pass into run_file_monitor (like --test_mode)

T=$(date)

echo
echo "$T: Executing ingest_hosc_files.sh cron job with '$1' environment, '$2' conda environment, and '$3' optional args"
echo

source /shared/anaconda3/etc/profile.d/conda.sh
conda activate $2

cd /store/emit/$1/repos/emit-main/emit_main

python run_file_monitor.py -c config/${1}_sds_config.json $3
