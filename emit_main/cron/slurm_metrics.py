import pandas as pd
import subprocess
import io
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", type=str, help="", default="emit-cron-ops")
    parser.add_argument("--days", type=int, help="Number of days back to look", default=3)
    parser.add_argument("--out", type=str, help="Slurm metrics CSV file", default="slurm_metrics.csv")
    args = parser.parse_args()

    cmd = f"""sacct -u {args.user} --starttime=now-{args.days}days \
    --noheader \
    --format=JobName%30,State,ElapsedRaw,Submit,Start,End \
    | grep -v batch | grep -v "\\-\\-" \
    | awk '{{print $1","$2","$3","$4","$5","$6}}'"""

    out = subprocess.check_output(cmd, shell=True, text=True)
    df = pd.read_csv(io.StringIO(out), header=None, names=["task_name","state","elapsed_time_raw","submit","start","end"])

    df["start_time"] = pd.to_datetime(df["start"], errors="coerce")
    df["end_time"] = pd.to_datetime(df["end"], errors="coerce")
    df["submit_time"] = pd.to_datetime(df["submit"], errors="coerce")

    df["wait_time_sec"] = (df["start_time"] - df["submit_time"]).dt.total_seconds()
    df["compute_time_sec"] = (df["end_time"] - df["start_time"]).dt.total_seconds()
    df = df[df["task_name"].str.startswith("emit.", na=False)]
    df["task_name"] = df["task_name"].str.replace("^emit\\.", "", regex=True)

    df = df.drop(columns=["start_time","end_time","end","start","submit","elapsed_time_raw"])

    df.to_csv(args.out, index=False)

if __name__ == "__main__":
    main()
