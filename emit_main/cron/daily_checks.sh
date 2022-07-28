#!/bin/bash

# $1 is the environment, either "dev", "test", or "ops"
# $2 date to check in YYYYMMDD

DATE=$(date -u "+%Y%m%d")

if [[ ! -z "$2" ]]; then
    DATE=$2
fi

echo "$(date +"%F %T,%3N"): Executing daily_checks.sh cron job with '$1' environment and '${DATE}' date."

#REPORT_FILE=/store/emit/${1}/reports/daily_${DATE}.yml
REPORT_FILE=/store/emit/dev/reports/daily_${DATE}.yml
echo "Writing to file ${REPORT_FILE}..."

echo "title: \"Daily Summary Report for ${DATE}\"" > $REPORT_FILE

# Check streams
total_packets=0
total_missing=0
total_gaps=0

echo "streams:" >> $REPORT_FILE
for apid in "1674" "1675" "1676"; do

    echo "  - ${apid}:" >> $REPORT_FILE
    packets=""
    missing=""
    gaps=""

    match=/store/emit/${1}/data/streams/${apid}/${DATE}/l0/*report.txt

    if compgen -G ${match} > /dev/null; then
        packets=$(grep -s "Packet Count" ${match} | awk '{sum+=$3} END {print sum}')
        missing=$(grep -s "Missing PSC Count" ${match} | awk '{sum+=$4} END {print sum}')
        missing_files=$(grep -s "Missing PSC Count" ${match} | grep -v ": 0")
        gaps=$(grep -s "PSC Errors" ${match} | awk '{sum+=$4} END {print sum}')
    fi

    if [[ -z $packets ]]; then packets=0; fi
    if [[ -z $missing ]]; then missing=0; fi
    if [[ -z $gaps ]]; then gaps=0; fi

    echo "      total_packets: ${packets}" >> $REPORT_FILE
    echo "      missing_packets: ${missing}" >> $REPORT_FILE
    echo "      psc_gaps: ${gaps}" >> $REPORT_FILE
    for file in "${missing_files[@]}"; do
        echo "$file" >> $REPORT_FILE
    done

    total_packets=$(($total_packets + $packets))
    total_missing=$(($total_missing + $missing))
    total_gaps=$(($total_gaps + $gaps))
done

echo "stream_totals:" >> $REPORT_FILE
echo "  total_packets: ${total_packets}" >> $REPORT_FILE
echo "  missing_packets: ${total_missing}" >> $REPORT_FILE
echo "  psc_gaps: ${total_gaps}" >> $REPORT_FILE