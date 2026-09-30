import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Lock
import requests
from tqdm import tqdm
from emit_main.database.database_manager import DatabaseManager

collection_concept_ids = { '01' : {'l1b':'C2408009906-LPCLOUD',
                                   'l2a':'C2408750690-LPCLOUD',
                                   'l2b':'C2408034484-LPCLOUD',
                                   'frcov':'C3911089796-LPCLOUD'},
                           '02' : {'l1b':'C4079829720-LPCLOUD',
                                   'l2a':'C4079844428-LPCLOUD',
                                   'ch4':'C3242680113-LPCLOUD',
                                   'co2':'C3243477145-LPCLOUD',
                                   'mask':'C3882545269-LPCLOUD',
                                   'l3rfl':'C4284742737-LPCLOUD',
                                   'frcov':'C4303752957-LPCLOUD',
                                   'l2b':'C4079846859-LPCLOUD'},
                           '03' : {'mask':'C4279547358-LPCLOUD',
                                   'ch4':'C4303752968-LPCLOUD',
                                   'co2':'C4303752974-LPCLOUD'}}

daac_submissions = {'l1b':'rdn',
                    'l2a':'rfl',
                    'ch4':'ch4',
                    'co2':'co2',
                    'mask':'maskTf',
                    'frcov':'frc',
                    'l3rfl':'rfl',
                    'l2b':'min'}


url = "https://cmr.earthdata.nasa.gov/search/granules.json"
fmt = "%Y-%m-%dT%H:%M:%SZ"

def parse_time(value):
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def get_temporal_ranges(concept_id, n_ranges, start_time=None, end_time=None):
    if not start_time or not end_time:
        response = requests.get(
            "https://cmr.earthdata.nasa.gov/search/collections.json",
            params={"concept_id": concept_id},
        )
        response.raise_for_status()
        entry = response.json()["feed"]["entry"][0]

    if start_time:
        start = parse_time(start_time)
    else:
        start = parse_time(entry["time_start"])
    if end_time:
        end = parse_time(end_time)
    elif entry.get("time_end"):
        end = parse_time(entry["time_end"])
    else:
        end = datetime.now(timezone.utc)

    if start >= end:
        raise ValueError("start_time must be before end_time")

    step = (end - start) / n_ranges
    temporals = []
    for i in range(n_ranges):
        if i == 0:
            start_str = start.strftime(fmt) if start_time else ""
        else:
            start_str = (start + step * i).strftime(fmt)
        if i == n_ranges - 1:
            end_str = end.strftime(fmt) if end_time else ""
        else:
            end_str = (start + step * (i + 1)).strftime(fmt)
        temporals.append(f"{start_str},{end_str}" if start_str or end_str else None)
    return temporals


def get_granules(concept_id, temporal, progress):
    granules = []
    headers = {"Accept": "application/json"}
    session = requests.Session()
    try:
        while True:
            response = session.get(
                url,
                params={"collection_concept_id": concept_id, "temporal": temporal, "page_size": 2000},
                headers=headers,
            )
            response.raise_for_status()

            data = response.json()
            entries = data.get("feed", {}).get("entry", [])
            if entries:
                granules.extend(entry["title"] for entry in entries)

            with progress["lock"]:
                progress["bar"].update(len(entries))

            search_after = response.headers.get("CMR-Search-After")
            if len(entries) < 2000 or not search_after:
                break
            headers["CMR-Search-After"] = search_after

    except requests.exceptions.RequestException as e:
        tqdm.write(f"Request error: {e}")
    except ValueError as e:
        tqdm.write(f"Data error: {e}")
    return granules


def parallel_granules(concept_id, start_time=None, end_time=None, n_ranges=50, workers=5):
    temporal = None
    if start_time or end_time:
        start_str = parse_time(start_time).strftime(fmt) if start_time else ""
        end_str = parse_time(end_time).strftime(fmt) if end_time else ""
        temporal = f"{start_str},{end_str}"

    response = requests.get(url, params={"collection_concept_id": concept_id, "temporal": temporal, "page_size": 0})
    response.raise_for_status()
    hits = response.headers.get("CMR-Hits")
    progress = {"bar": tqdm(total=int(hits) if hits else None, desc="Retrieving granules", unit="granule"), "lock": Lock()}

    temporals = get_temporal_ranges(concept_id, n_ranges, start_time, end_time)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = executor.map(lambda t: get_granules(concept_id, t, progress), temporals)

    progress["bar"].close()

    return list(dict.fromkeys(g for r in results for g in r))


def main():

    description = "List acquisitions submitted to the DAAC that are missing from CMR"

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("product")
    parser.add_argument("version", type=str)
    parser.add_argument("--start_time", default=None)
    parser.add_argument("--end_time", default=None)
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    parser.add_argument('-o', '--output', default=None, help='optional output file, prints missing if not provided')

    args = parser.parse_args()
    
    config_path = f"/store/emit/{args.env}/repos/emit-main/emit_main/config/{args.env}_sds_config.json"
    print(f"Using config_path {config_path}")

    dm = DatabaseManager(config_path)
    acq_coll = dm.db.acquisitions

    query = {f"products.{args.product}.{args.version}.{daac_submissions[args.product]}_daac_submissions": {"$exists": True}}

    time_filter = {}
    if args.start_time:
        time_filter["$gte"] = parse_time(args.start_time)
    if args.end_time:
        time_filter["$lte"] = parse_time(args.end_time)
    if time_filter:
        query["start_time"] = time_filter

    records = acq_coll.find(
        query,
        {"acquisition_id": 1, "_id": 0}
        )
    acq = [x["acquisition_id"] for x in records]

    if len(acq) == 0:
        print(f"No delivered {args.version} products found in database.")
        return

    concept_id = collection_concept_ids[args.version][args.product]

    granules = parallel_granules(concept_id, start_time=args.start_time, end_time=args.end_time)
    granules_acq_ids = {f'emit{x.split("_")[4].lower()}' for x in granules}

    missing = [a for a in acq if a not in granules_acq_ids]

    if args.output:
        print(f"{len(missing)}/{len(acq)} v{args.version} {args.product} acquisitions missing from CMR, written to {args.output}")
        
        with open(args.output, "w") as f:
            f.writelines(f"{a}\n" for a in missing)
    else:
        print(f"{len(missing)}/{len(acq)} v{args.version} {args.product} acquisitions missing from CMR")
        
        for a in missing:
            print(a)


if __name__ == "__main__":
    main()