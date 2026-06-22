"""
This script is meant to be run one time to prepare the database for v2 reprocessing. 
It adds product version tracking to products and statuses (e.g. products->l1a->01)

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import pdb
import sys

from pymongo import UpdateOne

from emit_main.workflow.workflow_manager import WorkflowManager


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Prep database for v2 reprocessing")
    parser.add_argument("-e", "--env", default="test", help="Where to run the report")
    parser.add_argument("-d", "--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    parser.add_argument("-c", "--collections", default="", help="Comma separated list of collections to update, choices include \"s,dc,o,a\"")
    args = parser.parse_args()

    env = args.env

    if len(args.collections) < 1:
        print("No collection specified. Exiting.")
        sys.exit(1)

    start = None
    stop = None
    if args.dates is not None:
        start, stop = args.dates.split(",")
        start = dt.datetime.strptime(start, "%Y%m%d")
        stop = dt.datetime.strptime(stop, "%Y%m%d")
    else:
        print("You need to provide dates to run this script")
        sys.exit(1)

    # Get workflow manager and db collections
    config_path = f"/store/emit/{env}/repos/emit-main/emit_main/config/{env}_sds_config.json"
    print(f"Using config path {config_path} and start/stop dates of {start} and {stop}")
    wm = WorkflowManager(config_path=config_path)
    db = wm.database_manager.db

    # TODO: Check all product fields - can I put "raw" and "daac" here or handle them specially?
    prod_versions_v1 = {
        "raw": "01",
        "daac": "01",
        "l0": "01",
        "l1a": "01",
        "l1b": "01",
        "l2a": "01",
        "l2b": "01",
        "ch4": "02",
        "co2": "02",
        "mask": "02",
        "frcov": "01",
    }

    # Update streams collection
    if "s" in args.collections:
        updates = []
        streams_coll = db.streams
        query = {
            "build_num": "0106",
            "start_time": {"$gte": start, "$lt": stop},
        }
        docs = list(streams_coll.find(query))
        print(f"Found {len(docs)} matching documents in streams collection for query:\n{query}")

        # Loop through documents and populate new products dictionary
        for d in docs:
            # Check for products_bak and exit if exists
            if d.get("products_bak"):
                print("Found existing products_bak. Exiting without making updates...")
                sys.exit(1)

            products_old = d.get("products")
            if products_old:
                products_new = {}
                for p in products_old.keys():
                    products_new[p] = {
                        prod_versions_v1[p]: products_old[p]
                    }

                # Append to updates for bulk write
                updated_fields = {
                    "products": products_new,
                    "products_bak": products_old
                }
                # pdb.set_trace()
                updates.append(
                    UpdateOne(
                        {"_id": d["_id"]},
                        {"$set": updated_fields}
                    )
                )
        
        # Bulk write
        if updates:
            result = streams_coll.bulk_write(updates)
            print(f"Updated streams collection:\n{result}")

    # Update data_collections collection
    if "dc" in args.collections:
        updates = []
        dc_coll = db.data_collections
        query = {
            "build_num": "0106",
            "start_time": {"$gte": start, "$lt": stop},
        }
        docs = list(dc_coll.find(query))
        print(f"Found {len(docs)} matching documents in data_collections collection for query:\n{query}")

        # Loop through documents and populate new products dictionary
        for d in docs:
            # Check for products_bak and exit if exists
            if d.get("products_bak"):
                print("Found existing products_bak. Exiting without making updates...")
                sys.exit(1)

            updated_fields = {}
            # Add product versions to "products" dictionary
            products_old = d.get("products")
            if products_old:
                products_new = {}
                for p in products_old.keys():
                    # Handle the nested ghg scenario
                    if p == "ghg":
                        # Loop through gases
                        for g in products_old[p].keys():
                            products_new[g] = {
                                prod_versions_v1[g]: products_old[p][g]
                            }
                        # Continue to next iteration
                        continue

                    # If not ghg, then handle the standard l1a and l1b scenario
                    products_new[p] = {
                        prod_versions_v1[p]: products_old[p]
                    }

                # Append to updates for bulk write
                updated_fields["products"] = products_new
                updated_fields["products_bak"] = products_old

            # Add product versions to frames_status, ch4_status, co2_status, and ready_for_l1b_mosaic
            status_versions = {
                "frames_status": prod_versions_v1["l1a"],
                "ch4_status": prod_versions_v1["ch4"],
                "co2_status": prod_versions_v1["co2"],
                "ready_for_l1b_mosaic": prod_versions_v1["l1b"]
            }
            for status in status_versions.keys():
                status_old = d.get(status)
                if status_old:
                    status_new = {
                        status_versions[status]: status_old
                    }
                    updated_fields[status] = status_new
                    updated_fields[f"{status}_bak"] = status_old
                
            if len(updated_fields) > 0:
                # pdb.set_trace()
                updates.append(
                    UpdateOne(
                        {"_id": d["_id"]},
                        {"$set": updated_fields}
                    )
                )
        
        # Bulk write
        if updates:
            result = dc_coll.bulk_write(updates)
            print(f"Updated data_collections collection:\n{result}")

    # TODO: Move cloud_fraction and nodata_fraction to products dictionary
    
    print("Finished updating DB")

    # Fix a bad update:
    # db.streams.updateMany({products_bak: {$exists: 1}}, {$unset: {"products": 1}})
    # db.streams.updateMany({products_bak: {$exists: 1}}, {$rename: {"products_bak": "products"}})

    # db.data_collections.updateMany({products_bak: {$exists: 1}}, {$unset: {"products": 1}})
    # db.data_collections.updateMany({products_bak: {$exists: 1}}, {$rename: {"products_bak": "products"}})
    # db.data_collections.updateMany({frames_status_bak: {$exists: 1}}, {$unset: {"frames_status": 1}})
    # db.data_collections.updateMany({frames_status_bak: {$exists: 1}}, {$rename: {"frames_status_bak": "frames_status"}})
    # db.data_collections.updateMany({ch4_status_bak: {$exists: 1}}, {$unset: {"ch4_status": 1}})
    # db.data_collections.updateMany({ch4_status_bak: {$exists: 1}}, {$rename: {"ch4_status_bak": "ch4_status"}})
    # db.data_collections.updateMany({co2_status_bak: {$exists: 1}}, {$unset: {"co2_status": 1}})
    # db.data_collections.updateMany({co2_status_bak: {$exists: 1}}, {$rename: {"co2_status_bak": "co2_status"}})
    # db.data_collections.updateMany({ready_for_l1b_mosaic_bak: {$exists: 1}}, {$unset: {"ready_for_l1b_mosaic": 1}})
    # db.data_collections.updateMany({ready_for_l1b_mosaic_bak: {$exists: 1}}, {$rename: {"ready_for_l1b_mosaic_bak": "ready_for_l1b_mosaic"}})

if __name__ == '__main__':
    main()
