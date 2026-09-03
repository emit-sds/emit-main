import argparse
import datetime as dt
import io
import pandas as pd
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", type=str, help="", default="emit-cron-ops")
    parser.add_argument("--days", type=int, help="Number of days back to look (default: 3), or specify --dates to override", default=3)
    parser.add_argument("--dates", help="Comma separated dates (YYYY-MM-DD,YYYY-MM-DD)")
    parser.add_argument("--out_dir", type=str, help="Slurm metrics CSV file", default=".")
    args = parser.parse_args()

    date_range_arg = f"--starttime=now-{args.days}days"

    # If --dates is used, then override the date range
    if args.dates is not None:
        start, stop = args.dates.split(",")
        date_range_arg = f"--starttime={start} --endtime={stop}"

    print(f"Using date range argument(s): {date_range_arg}")

    cmd = f"""sacct -u {args.user} {date_range_arg} \
    --noheader \
    --format=JobID,JobName%30,State,ElapsedRaw,Submit,Start,End \
    | grep -v batch | grep -v "\\-\\-" \
    | awk '{{print $1","$2","$3","$4","$5","$6","$7}}'"""

    out = subprocess.check_output(cmd, shell=True, text=True)
    df = pd.read_csv(io.StringIO(out), header=None, names=["job_id","task_name","state","elapsed_time_raw","submit","start","end"])

    df["start_time"] = pd.to_datetime(df["start"], errors="coerce")
    df["end_time"] = pd.to_datetime(df["end"], errors="coerce")
    df["submit_time"] = pd.to_datetime(df["submit"], errors="coerce")

    df["wait_time_sec"] = (df["start_time"] - df["submit_time"]).dt.total_seconds()
    df["compute_time_sec"] = (df["end_time"] - df["start_time"]).dt.total_seconds()
    df = df[df["task_name"].str.startswith("emit.", na=False)]
    df["task_name"] = df["task_name"].str.replace("^emit\\.", "", regex=True)
    
    # Add job_id as nanoseconds to submit time
    df["job_id"] = df["job_id"].astype(int)
    df["job_id_mod"] = df["job_id"] % 10**9
    df["job_id_mod"] = df["job_id_mod"].astype(str).str.zfill(9)
    df["submit_time_nano"] = df['submit'].astype(str) + "." + df['job_id_mod']

    # Fix up columns for CSV
    df = df.drop(columns=["submit_time","start_time","end_time","end","start","submit","elapsed_time_raw","job_id_mod"])
    df = df[["submit_time_nano","job_id","task_name","state","wait_time_sec","compute_time_sec"]]

    # Generate output file name based on dates used to query
    if args.dates is None:
        stop_time = dt.datetime.now(dt.timezone.utc)
        stop = stop_time.strftime("%Y%m%d")
        start_time = stop_time - dt.timedelta(days=args.days)
        start = start_time.strftime("%Y%m%d")
    else:
        stop = stop.replace("-", "")
        start = start.replace ("-", "")
    outfile = f"{args.out_dir}/metrics_tasks_{start}_{stop}.csv"
    df.to_csv(outfile, index=False)

if __name__ == "__main__":
    main()
