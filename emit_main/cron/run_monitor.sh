#!/bin/bash

# $1 is the environment, either "dev", "test", or "ops"
# $2 is the conda environment
# $3 is the monitor to run ("ingest", "frames", "bad", "geo", "email")
# $4 Optional args to pass into run_workflow (like --test_mode)

T=$(date)

echo
echo "$T: Executing run_monitor.sh cron job with '$1' environment, '$2' conda environment, and '$3' monitor and '$4' optional args"
echo

source /store/local/miniforge3/etc/profile.d/conda.sh
conda activate $2

cd /store/emit/$1/repos/emit-main/emit_main

python run_workflow.py -c config/${1}_sds_config.json -m $3 $4
