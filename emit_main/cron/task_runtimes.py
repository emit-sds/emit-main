"""
A script that generates task runtimes from the slurm log

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import os
import pandas as pd


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Generate task runtimes")
    parser.add_argument("input", help="Slurm job completion log")
    parser.add_argument("--start_time", help="Start time (YYYY-MM-DDTHH:MM:SS)", default="2010-01-01T00:00:00")
    parser.add_argument("--stop_time", help="Stop time (YYYY-MM-DDTHH:MM:SS)", default="2030-01-01T00:00:00")
    args = parser.parse_args()

    start = dt.datetime.strptime(args.start_time, "%Y-%m-%dT%H:%M:%S")
    stop = dt.datetime.strptime(args.stop_time, "%Y-%m-%dT%H:%M:%S")

    print(f"Analyzing {args.input} for jobs between {args.start_time} and {args.stop_time}")

    tasks = {
        "name": [],
        "state": [],
        "start": [],
        "stop": [],
        "submit": [],
        "exit_code": []
    }
    count = 1
    with open(args.input, "r") as f:
        for line in f.readlines():
            if count % 100000 == 0:
                print(f"Read in {count} lines...")
            count += 1
            props = line.rstrip("\n").split(" ")
            props = props[:-1]
            kv = {}
            for p in props:
                if len(p.split("=")) == 1 or (len(p.split("=")) > 1 and p.split("=")[1] == ""):
                    kv[p.split("=")[0]] = None
                else:
                    kv[p.split("=")[0]] = p.split("=")[1]
            # {p.split("=")[0]: p.split("=")[1] for p in props}
            tt = 1
            if start < dt.datetime.strptime(kv["StartTime"], "%Y-%m-%dT%H:%M:%S") <= stop and \
                    kv["Name"] is not None and kv["Name"].startswith("emit."):
                tasks["name"].append(kv["Name"])
                tasks["state"].append(kv["JobState"])
                tasks["start"].append(dt.datetime.strptime(kv["StartTime"], "%Y-%m-%dT%H:%M:%S"))
                tasks["stop"].append(dt.datetime.strptime(kv["EndTime"], "%Y-%m-%dT%H:%M:%S"))
                tasks["submit"].append(dt.datetime.strptime(kv["SubmitTime"], "%Y-%m-%dT%H:%M:%S"))
                tasks["exit_code"].append(kv["ExitCode"])

    df = pd.DataFrame(tasks)
    df["runtime"] = (df["stop"] - df["start"])
    df["runtime_seconds"] = df["runtime"].map(lambda delta: delta.total_seconds())
    df["completion_time"] = (df["stop"] - df["submit"])
    df["completion_time_seconds"] = df["completion_time"].map(lambda delta: delta.total_seconds())
    names = df["name"].unique()

    names.sort()
    output = {
        "task": [],
        "num_jobs": [],
        "avg_runtime": [],
        "avg_completion_time": []
    }
    for name in names:
        runtimes = df.loc[(df["name"] == name) & (df["state"] == "COMPLETED")]
        avg_runtime = runtimes["runtime"].mean()
        avg_comp_time = runtimes["completion_time"].mean()
        # print(f"{name}: {runtimes.shape[0]} tasks with average runtime of {str(avg).split('.')[0]}")
        output["task"].append(name)
        output["num_jobs"].append(runtimes.shape[0])
        if "days" in str(avg_runtime):
            output["avg_runtime"].append(str(avg_runtime).split(".")[0].split(" ")[2])
        else:
            output["avg_runtime"].append(None)

        if "days" in str(avg_comp_time):
            output["avg_completion_time"].append(str(avg_comp_time).split(".")[0].split(" ")[2])
        else:
            output["avg_completion_time"].append(None)

    output_df = pd.DataFrame(output)
    print(output_df.to_string())

    # out_dir = os.path.dirname(args.input)
    out_dir = os.getcwd()
    out_file = os.path.join(out_dir, "tasks.csv")
    df.to_csv(out_file)


if __name__ == '__main__':
    main()
