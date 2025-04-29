"""
A script that generates task runtimes from the slurm log

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import os
import pandas as pd

# To get CSV file, query sacct like this:
# sacct --starttime=now-9days --format=User%15,JobID%8,JobName%30,State,ElapsedRaw,Submit,Start,End | grep -v batch | grep -v "\-\-" | awk '{print $1","$2","$3","$4","$5","$6","$7","$8}' > /home/emit-cron-test/runtimes.csv


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Generate task runtimes")
    parser.add_argument("input", help="Slurm job completion log")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    df["end_time"] = pd.to_datetime(df["End"], format="%Y-%m-%dT%H:%M:%S", errors="coerce")
    df["start_time"] = pd.to_datetime(df["Start"], format="%Y-%m-%dT%H:%M:%S", errors="coerce")
    df["submission_time"] = pd.to_datetime(df["Submit"], format="%Y-%m-%dT%H:%M:%S", errors="coerce")

    df_valid = df.dropna(subset=['start_time'])

    # Extract hour from submission_time
    df_valid['hour'] = df_valid['start_time'].dt.hour

    # Group by user and hour, count jobs
    job_counts = df_valid.groupby(['User', 'hour']).size().reset_index(name='job_count')

    # Pivot to get users as rows and hours as columns
    pivot_table = job_counts.pivot(index='User', columns='hour', values='job_count').fillna(0).astype(int)

    print(pivot_table)



if __name__ == '__main__':
    main()
