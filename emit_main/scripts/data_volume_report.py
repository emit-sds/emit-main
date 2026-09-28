"""
sbatch -p standard -J date_volume -o /dev/null -c 64 --mem=320G -t 04:00:00 \
--wrap="/store/local/miniforge3/envs/emit-main-20250705-dev/bin/python /store/emit/dev/repos/emit-main/emit_main/scripts/data_volume_report.py \
-e ops -o /tmp/data_volume.csv"
"""
import argparse
import csv
import glob
import os
from concurrent.futures import ThreadPoolExecutor

from emit_main.database.database_manager import DatabaseManager

DATA_ROOT = None
MAX_WORKERS = 64


def get_dcid_dates(dcid_coll):

    records = dcid_coll.find(
        {"dcid": {"$ne": None}, "start_time": {"$ne": None}},
        {"_id": 0, "dcid": 1, "start_time": 1}
    )
    return {r["dcid"]: r["start_time"].strftime("%Y%m%d") for r in records}


def valid_date(date):
    return len(date) == 8 and date.isdigit()


def dir_size(path):
    if os.path.islink(path.rstrip('/')):
        return 0
    total = 0
    stack = [path]
    while stack:
        with os.scandir(stack.pop()) as it:
            for entry in it:
                if entry.is_dir(follow_symlinks=False):
                    stack.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
    return total


def add_acquisitions(dates, jobs):
    for date_dir in glob.glob(f'{DATA_ROOT}/acquisitions/*'):
        date = os.path.basename(date_dir)

        if not valid_date(date):
            continue

        dates.add(date)

        for acq_dir in glob.glob(f'{date_dir}/*/'):
            for prod_dir in glob.glob(f'{acq_dir}/*/'):
                prod = 'acq_' + os.path.basename(prod_dir[:-1])
                jobs.append((date, prod, prod_dir))


def add_streams(dates, jobs):
    for streams_dir in glob.glob(f'{DATA_ROOT}/streams/*/'):
        for date_stream_dir in glob.glob(streams_dir + '*'):
            date = os.path.basename(date_stream_dir)

            if not valid_date(date):
                continue

            dates.add(date)

            for prod_dir in glob.glob(f'{date_stream_dir}/*/'):
                prod = 'stream_' + os.path.basename(prod_dir[:-1])
                jobs.append((date, prod, prod_dir))


def add_orbits(dates, jobs):
    for date_orbit_dir in glob.glob(f'{DATA_ROOT}/orbits/*'):
        date = os.path.basename(date_orbit_dir)

        if not valid_date(date):
            continue

        dates.add(date)

        for orbit_dir in glob.glob(f'{date_orbit_dir}/*/'):
            for prod_dir in glob.glob(f'{orbit_dir}/*/'):
                prod = 'orbit_' + os.path.basename(prod_dir[:-1])
                jobs.append((date, prod, prod_dir))


def add_dcids(dates, jobs, dcid_dates):
    for dcid_supdir in glob.glob(f'{DATA_ROOT}/data_collections/by_dcid/*'):
        for dcid_dir in glob.glob(f'{dcid_supdir}/*'):
            dcid = os.path.basename(dcid_dir)
            date = dcid_dates.get(dcid)

            if date is None or not valid_date(date):
                print(f'No valid date for {dcid}')
                continue

            dates.add(date)

            for prod_dir in glob.glob(f'{dcid_dir}/*/'):
                name = os.path.basename(prod_dir[:-1])
                for dprod in ['acquisitions', 'decomp', 'frames']:
                    if dprod in name:
                        prod = f'dcid_{dprod}'
                        break
                else:
                    prod = 'dcid_' + name
                jobs.append((date, prod, prod_dir))


def compute_volumes(dates, jobs):
    date_volume = {date: {} for date in dates}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        sizes = list(ex.map(lambda j: dir_size(j[2]), jobs))

    for (date, prod, _), size in zip(jobs, sizes):
        date_volume[date][prod] = date_volume[date].get(prod, 0) + size / 1E6

    return date_volume


def collect(dcid_coll):
    dcid_dates = get_dcid_dates(dcid_coll)

    dates = set()
    jobs = []

    add_acquisitions(dates, jobs)
    add_streams(dates, jobs)
    add_orbits(dates, jobs)
    add_dcids(dates, jobs, dcid_dates)

    return compute_volumes(dates, jobs)


def save(data, path):
    prods = sorted({prod for v in data.values() for prod in v})
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date'] + prods)
        for date in sorted(data):
            w.writerow([date] + [data[date].get(prod, 0) for prod in prods])


def parse_args():
    parser = argparse.ArgumentParser(description='Compute EMIT data volume (MB) per date and product.')
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    parser.add_argument('-r', '--data-root', default=None, help='default: /store/emit/{env}/data')
    parser.add_argument('-w', '--max-workers', type=int, default=MAX_WORKERS, help=f'default: {MAX_WORKERS}')
    parser.add_argument('-o', '--output', default='date_volume.csv', help='default: date_volume.csv')
    return parser.parse_args()


def main():
    global DATA_ROOT, MAX_WORKERS

    args = parse_args()
    DATA_ROOT = args.data_root or f'/store/emit/{args.env}/data'
    MAX_WORKERS = args.max_workers

    config_path = f"/store/emit/{args.env}/repos/emit-main/emit_main/config/{args.env}_sds_config.json"
    print(f"Using config_path {config_path}")

    dm = DatabaseManager(config_path)
    dcid_coll = dm.db.data_collections


    date_volume = collect(dcid_coll)
    save(date_volume, args.output)


if __name__ == '__main__':
    main()