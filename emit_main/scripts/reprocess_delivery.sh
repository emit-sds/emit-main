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

echo -e "\n$T: Executing reprocess_delivery.sh with start '$START' and stop '$STOP' on partition '$PARTITION' beginning with step '$STEP'\n"

cd /store/emit/ops/repos/emit-main/emit_main

export PYTHONUNBUFFERED=1

if [ "$STEP" -le 1 ]; then
  echo -e "\n### 1 - Starting dl1brdn at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m dl1brdn -w 65 --retry-failed --daac-ingest-queue backward --partition $PARTITION $DRYRUN" 
fi

if [ "$STEP" -le 2 ]; then
  echo -e "\n### 2 - Starting dl1batt at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m dl1batt -w 65 --retry-failed --daac-ingest-queue backward --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 3 ]; then
  echo -e "\n### 3 - Starting dl2a at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m dl2a -w 65 --retry-failed --daac-ingest-queue backward --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 4 ]; then
  echo -e "\n### 4 - Starting dmaskTf at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m dmaskTf -w 65 --retry-failed --daac-ingest-queue backward --partition $PARTITION $DRYRUN" 
fi

if [ "$STEP" -le 5 ]; then
  echo -e "\n### 5 - Starting dfrcov at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m dfrcov -w 65 --retry-failed --daac-ingest-queue backward --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 6 ]; then
  echo -e "\n### 6 - Starting dl2b at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m dl2b -w 65 --retry-failed --daac-ingest-queue backward --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 7 ]; then
  echo -e "\n### 7 - Starting dl3rfl at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m dl3rfl -w 65 --retry-failed --daac-ingest-queue backward --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 8 ]; then
  echo -e "\n### 8 - Starting dch4 at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m dch4 -w 65 --retry-failed --daac-ingest-queue backward --partition $PARTITION $DRYRUN"
fi

if [ "$STEP" -le 9 ]; then
  echo -e "\n### 9 - Starting dco2 at $(date)\n"
  eval "python run_workflow.py -c config/ops_sds_config.json --date_field start_time --start_time $START --stop_time $STOP -m dco2 -w 65 --retry-failed --daac-ingest-queue backward --partition $PARTITION $DRYRUN"
fi
