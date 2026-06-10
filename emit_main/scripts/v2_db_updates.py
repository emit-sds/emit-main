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
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    parser.add_argument("-d", "--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    parser.add_argument("-c", "--collections", default="s,dc,o,a", help="Comma separated list of collections to update")
    args = parser.parse_args()

    env = args.env

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
            products_old = d.get("products")
            if products_old:
                products_new = {}
                for p in products_old.keys():
                    if p in wm.config["prod_versions"]:
                        prod_version = wm.config["prod_versions"][p]
                    else: 
                        prod_version = "01"
                    products_new[p] = {
                        prod_version: products_old[p]
                    }

                # Append to updates for bulk write
                updated_products = {
                    "products": products_new,
                    "products_bak": products_old
                }
                # pdb.set_trace()
                updates.append(
                    UpdateOne(
                        {"_id": d["_id"]},
                        {"$set": updated_products}
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
            updated_fields = {}
            products_old = d.get("products")
            if products_old:
                products_new = {}
                for p in products_old.keys():
                    if p in wm.config["prod_versions"]:
                        prod_version = wm.config["prod_versions"][p]
                    else: 
                        prod_version = "01"
                    products_new[p] = {
                        prod_version: products_old[p]
                    }

                # Append to updates for bulk write
                updated_products = {
                    "products": products_new,
                    "products_bak": products_old
                }
                # pdb.set_trace()
                updates.append(
                    UpdateOne(
                        {"_id": d["_id"]},
                        {"$set": updated_products}
                    )
                )
        
        # Bulk write
        if updates:
            result = dc_coll.bulk_write(updates)
            print(f"Updated streams collection:\n{result}")

    # TODO: Move cloud_fraction and nodata_fraction to products dictionary
    
    print("Finished updating DB")

    # Fix a bad update:
    # db.streams.updateMany({products_bak: {$exists: 1}}, {$unset: {"products": 1}})
    # db.streams.updateMany({products_bak: {$exists: 1}}, {$rename: {"products_bak": "products"}})


if __name__ == '__main__':
    main()
