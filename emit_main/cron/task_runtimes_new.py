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
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    df["EndDate"] = pd.to_datetime(df["End"], format="%Y-%m-%dT%H:%M:%S")
    df["StartDate"] = pd.to_datetime(df["Start"], format="%Y-%m-%dT%H:%M:%S")
    df["SubmitDate"] = pd.to_datetime(df["Submit"], format="%Y-%m-%dT%H:%M:%S")

    df["runtime"] = (df["EndDate"] - df["StartDate"])
    df["runtime_seconds"] = df["runtime"].map(lambda delta: delta.total_seconds())
    df["completion_time"] = (df["EndDate"] - df["SubmitDate"])
    df["completion_time_seconds"] = df["completion_time"].map(lambda delta: delta.total_seconds())
    names = df["JobName"].unique()

    names.sort()
    output = {
        "task": [],
        "num_jobs": [],
        "avg_runtime": [],
        "avg_completion_time": []
    }
    for name in names:
        runtimes = df.loc[(df["JobName"] == name) & (df["State"] == "COMPLETED")]
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
    # out_dir = os.getcwd()
    # out_file = os.path.join(out_dir, "tasks.csv")
    # df.to_csv(out_file)


if __name__ == '__main__':
    main()
