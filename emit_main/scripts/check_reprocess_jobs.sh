#!/bin/bash
#
# Checks whether any of the EMIT catalog reprocessing/delivery/backup jobs
# are currently running under a given user (default: emit-cron-ops) and
# summarizes the date range / steps each one is using.
#
# Usage: ./check_reprocess_jobs.sh [username]

USER_NAME="${1:-emit-cron-ops}"

ROWS=$(ps -u "$USER_NAME" -ww -o pid=,etime=,args= 2>/dev/null | \
  awk '{pid=$1; etime=$2; cmd=""; for (i=3; i<=NF; i++) cmd = cmd $i " "; print pid "|" etime "|" cmd}')

if [ -z "$ROWS" ]; then
  echo "No processes found for user '$USER_NAME'."
  exit 0
fi

FOUND=0

while IFS='|' read -r PID ETIME CMD; do
  [ -z "$PID" ] && continue

  if [[ "$CMD" == *reprocess_products.sh* ]]; then
    FOUND=1
    ARGS=$(echo "$CMD" | sed -E 's#.*reprocess_products\.sh[[:space:]]*##')
    read -r START STOP STEP END_STEP PARTITION _ <<< "$ARGS"
    echo "[reprocess_products.sh] PID $PID, running $ETIME"
    echo "  Date range : ${START:-?} -> ${STOP:-?}"
    echo "  Steps      : ${STEP:-1} to ${END_STEP:-13}"
    echo "  Partition  : ${PARTITION:-?}"
    echo

  elif [[ "$CMD" == *reprocess_delivery.sh* ]]; then
    FOUND=1
    ARGS=$(echo "$CMD" | sed -E 's#.*reprocess_delivery\.sh[[:space:]]*##')
    read -r START STOP STEP PARTITION _ <<< "$ARGS"
    echo "[reprocess_delivery.sh] PID $PID, running $ETIME"
    echo "  Date range : ${START:-?} -> ${STOP:-?}"
    echo "  Step       : ${STEP:-1}"
    echo "  Partition  : ${PARTITION:-?}"
    echo

  elif [[ "$CMD" == *"rclone copy"* && "$CMD" == *ngis-data* ]]; then
    FOUND=1
    DIR=$(echo "$CMD" | grep -oE 'acquisitions/[0-9]{8}' | sed 's#acquisitions/##' | head -1)
    echo "[rclone backup -> ngis-data] PID $PID, running $ETIME"
    echo "  Acquisition dir: ${DIR:-unknown}"
    echo
  fi
done <<< "$ROWS"

if [ "$FOUND" -eq 0 ]; then
  echo "No reprocessing/delivery/backup jobs currently running for user '$USER_NAME'."
fi
