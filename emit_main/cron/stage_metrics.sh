#!/bin/bash

# This script gets recent metrics and stages them for ingest by IOS into Grafana

# $1 is the environment, either "dev", "test", or "ops"
# $2 is the conda environment
# $3 is number of days back to check 

echo "$(date +"%F %T,%3N"): Executing stage_metrics.sh cron job for $1 environment with $2 conda environment for the last $3 days"

source /local/miniforge3/etc/profile.d/conda.sh
conda activate $2

# Move current CSV files to bak folder
mv /store/emit/$1/reports/grafana/*.csv /store/emit/$1/reports/grafana/bak/

cd /store/emit/${1}/repos/emit-main/emit_main
# Generate metrics
srun -p emit -N 1 -c 64 --mem=320G --job-name=metrics --dependency=singleton python cron/compile_metrics.py --dates $(date -u -d "$3 days ago" "+%Y%m%d"),$(date -u "+%Y%m%d") >> /store/emit/$1/logs/cron_compile_metrics.log
# Export to CSV
srun -p emit -N 1 -c 64 --mem=320G --job-name=metrics --dependency=singleton python cron/compile_metrics.py --dates $(date -u -d "$3 days ago" "+%Y%m%d"),$(date -u "+%Y%m%d") --export_to_dir /store/emit/$1/reports/grafana/
# Generate task runtime CSV (just get last 7 days)
python cron/slurm_metrics.py --dates $(date -u -d "7 days ago" "+%Y-%m-%d"),$(date -u "+%Y-%m-%d") --out_dir /store/emit/$1/reports/grafana/