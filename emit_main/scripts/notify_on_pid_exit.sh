#!/bin/bash
#
# Waits for a given PID to stop running, then emails the user.
#
# Usage: ./notify_on_pid_exit.sh <pid> [poll_interval_seconds]

PID="$1"
POLL_INTERVAL="${2:-30}"
EMAIL="winston.olson-duvall@jpl.nasa.gov,emit-sds-notification-aaaarlpim3nfmcknfd7wzv5le4@jpl.org.slack.com"

if [ -z "$PID" ]; then
  echo "Usage: $0 <pid> [poll_interval_seconds]" >&2
  exit 1
fi

if ! [[ "$PID" =~ ^[0-9]+$ ]]; then
  echo "Error: pid must be a positive integer, got '$PID'" >&2
  exit 1
fi

if ! kill -0 "$PID" 2>/dev/null; then
  echo "PID $PID is not currently running."
  exit 0
fi

CMD=$(ps -p "$PID" -o args= 2>/dev/null)
echo "Watching PID $PID ($CMD), checking every ${POLL_INTERVAL}s..."

while kill -0 "$PID" 2>/dev/null; do
  sleep "$POLL_INTERVAL"
done

HOST=$(hostname)
STOP_TIME=$(date)

mail -s "PID $PID has stopped running on $HOST" "$EMAIL" <<EOF
PID $PID has stopped running.

Host    : $HOST
Command : ${CMD:-unknown}
Stopped : $STOP_TIME
EOF

echo "PID $PID has stopped. Notification email sent to $EMAIL."
