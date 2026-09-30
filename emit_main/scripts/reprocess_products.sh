#!/bin/bash

# $1 is start time YYYY-MM-DDT00:00:00
# $2 is stop time YYYY-MM-DDT00:00:00
# $3 is step to begin on
# $4 is the slurm partition
# $5 is --dry-run flag (optional)

T=$(date)
START=$1
STOP=$2
STEP=${3:-1}
PARTITION=$4

DRYRUN=""
if [ "$5" == "--dry-run" ]; then
  DRYRUN="--dry-run | grep -A3 flag"
fi

echo -e "\n$T: Executing reprocess_products.sh with start '$START' and stop '$STOP' on partition '$PARTITION' beginning with step '$STEP'\n"

cd /store/emit/ops/repos/emit-main/emit_main

export PYTHONUNBUFFERED=1

if [ "$STEP" -le 1 ]; then
  CUR_DAY=$(date -d "$START" +%Y-%m-%d)
  STOP_DAY=$(date -d "$STOP" +%Y-%m-%d)
  while [ "$(date -d "$CUR_DAY" +%s)" -lt "$(date -d "$STOP_DAY" +%s)" ]; do
    NEXT_DAY=$(date -d "$CUR_DAY + 1 day" +%Y-%m-%d)
    DAY_START="${CUR_DAY}T00:00:00"
    DAY_STOP="${NEXT_DAY}T00:00:00"
    echo -e "\n### 1 - Starting cal for $DAY_START to $DAY_STOP at $(date)\n"
    eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $DAY_START --stop_time $DAY_STOP -m cal -w 57 --retry-failed --partition $PARTITION $DRYRUN"
    CUR_DAY=$NEXT_DAY

    # Run geo on the whole date range to catch any new orbits to geolocate
    echo -e "\n### 2 - Starting geo at $(date)\n"
    eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m geo -w 57 --retry-failed --partition $PARTITION $DRYRUN"

  done
fi

# if [ "$STEP" -le 2 ]; then
#   echo -e "\n### 2 - Starting geo at $(date)\n"
#   eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m geo -w 57 --retry-failed --partition $PARTITION $DRYRUN"
# fi

if [ "$STEP" -le 3 ]; then
  echo -e "\n### 3 - Starting l2 at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m l2 -w 57 --retry-failed --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 4 ]; then
  echo -e "\n### 4 - Starting maskTf at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m maskTf -w 57 --retry-failed --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 5 ]; then
  echo -e "\n### 5 - Starting frcov at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m frcov -w 57 --retry-failed --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 6 ]; then
  echo -e "\n### 6 - Starting l2b at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m l2b -w 570 --retry-failed --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 7 ]; then
  echo -e "\n### 7 - Starting l3rfl at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m l3rfl -w 228 --retry-failed --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 8 ]; then
  echo -e "\n### 8 - Starting ch4 at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m ch4 -w 570 --retry-failed --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 9 ]; then
  echo -e "\n### 9 - Starting co2 at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m co2 -w 570 --retry-failed --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 10 ]; then
  echo -e "\n### 10 - Starting mch4 at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m mch4 -w 65 --retry-failed --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 11 ]; then
  echo -e "\n### 11 - Starting mco2 at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m mco2 -w 65 --retry-failed --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 12 ]; then
  echo -e "\n### 12 - Starting ml1b at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m ml1b -w 65 --retry-failed --partition $PARTITION $DRYRUN"
fi
