"""
This script is meant to be run one time to prepare the database for v2 reprocessing. 
It adds product version tracking to products and statuses (e.g. products->l1a->01)

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import copy
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
    parser.add_argument("--update-db",  dest="update_db", action="store_true", help="Must add this flag to perform the bulk writes")
    args = parser.parse_args()

    env = args.env

    if len(args.collections) < 1:
        print("No collection specified. Exiting.")
        sys.exit(1)

    if not args.update_db:
        print("The --update-db flag is not set.  Script will run but no updates will be made to the DB.")

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

    prod_versions_v1 = {
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
                    # Handle product keys that don't have product versions
                    if p in ["raw", "daac"]:
                        prod_version = "01"
                    else:
                        prod_version =  prod_versions_v1[p]
                    products_new[p] = {
                        prod_version: products_old[p]
                    }

                # Append to updates for bulk write
                updated_fields = {
                    "products": products_new,
                }
                # pdb.set_trace()
                updates.append(
                    UpdateOne(
                        {"_id": d["_id"]},
                        {"$set": updated_fields}
                    )
                )
        
        # Bulk write
        if updates and args.update_db:
            chunk_size = 5000
            total_modified = 0
            total_matched = 0
            total_upserted = 0

            for i in range(0, len(updates), chunk_size):
                chunk = updates[i : i + chunk_size]
                result = streams_coll.bulk_write(chunk, ordered=False)
                
                total_modified += result.modified_count
                total_matched += result.matched_count
                total_upserted += result.upserted_count

            print(f"Updated streams collection:")
            print(f"Matched: {total_matched}, Modified: {total_modified}, Upserted: {total_upserted}")

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
                
            if len(updated_fields) > 0:
                # pdb.set_trace()
                updates.append(
                    UpdateOne(
                        {"_id": d["_id"]},
                        {"$set": updated_fields}
                    )
                )
        
        # Bulk write
        if updates and args.update_db:
            chunk_size = 3000
            total_modified = 0
            total_matched = 0
            total_upserted = 0

            for i in range(0, len(updates), chunk_size):
                chunk = updates[i : i + chunk_size]
                result = dc_coll.bulk_write(chunk, ordered=False)
                
                total_modified += result.modified_count
                total_matched += result.matched_count
                total_upserted += result.upserted_count

            print(f"Updated data_collections collection:")
            print(f"Matched: {total_matched}, Modified: {total_modified}, Upserted: {total_upserted}")

    # Update orbits collection
    if "o" in args.collections:
        updates = []
        orbits_coll = db.orbits
        query = {
            "build_num": "0106",
            "start_time": {"$gte": start, "$lt": stop},
        }
        docs = list(orbits_coll.find(query))
        print(f"Found {len(docs)} matching documents in orbits collection for query:\n{query}")

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
                    # Handle the standard l1a and l1b scenario
                    products_new[p] = {
                        prod_versions_v1[p]: products_old[p]
                    }

                # Append to updates for bulk write
                updated_fields["products"] = products_new

            # Add product versions to bad_status, raw_status, and radiance_status
            status_versions = {
                "bad_status": prod_versions_v1["l1a"],
                "raw_status": prod_versions_v1["l1a"],
                "radiance_status": prod_versions_v1["l1b"]
            }
            for status in status_versions.keys():
                status_old = d.get(status)
                if status_old:
                    status_new = {
                        status_versions[status]: status_old
                    }
                    updated_fields[status] = status_new
                
            if len(updated_fields) > 0:
                # pdb.set_trace()
                updates.append(
                    UpdateOne(
                        {"_id": d["_id"]},
                        {"$set": updated_fields}
                    )
                )
        
        # Bulk write
        if updates and args.update_db:
            chunk_size = 5000
            total_modified = 0
            total_matched = 0
            total_upserted = 0

            for i in range(0, len(updates), chunk_size):
                chunk = updates[i : i + chunk_size]
                result = orbits_coll.bulk_write(chunk, ordered=False)
                
                total_modified += result.modified_count
                total_matched += result.matched_count
                total_upserted += result.upserted_count

            print(f"Updated orbits collection:")
            print(f"Matched: {total_matched}, Modified: {total_modified}, Upserted: {total_upserted}")

    # Update acquisitions collection
    if "a" in args.collections:
        updates = []
        acqs_coll = db.acquisitions
        query = {
            "build_num": "0106",
            "start_time": {"$gte": start, "$lt": stop},
        }
        docs = list(acqs_coll.find(query))
        print(f"Found {len(docs)} matching documents in acquisitions collection for query:\n{query}")

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
                    # Merge the l3 and frcov dictionaries to remove "l3"
                    if p in ("l3", "frcov"):
                        # frcov_prods = products_new.get("frcov")
                        # if frcov_prods:
                        #     # If we have the "frcov" key already, then just add the old products
                        #     frcov_prods[prod_versions_v1["frcov"]].update(products_old[p])
                        if "frcov" in products_new.keys():
                            # If we have the "frcov" key already, then just add the old products
                            products_new["frcov"][prod_versions_v1["frcov"]].update(copy.deepcopy(products_old[p]))
                        else:
                            products_new["frcov"] = {
                                prod_versions_v1["frcov"]: copy.deepcopy(products_old[p])
                            }
                        # Continue to the next iteration
                        continue

                    # Handle the standard scenarios (l1a, l1b, l2a, l2b, mask) if we made it this far
                    products_new[p] = {
                        prod_versions_v1[p]: products_old[p]
                    }
                    
                    # Copy the cloud fraction values to the new products dictionary
                    # Leave the old ones in place (instead of renaming to _bak).  I will delete them after
                    cloud_fraction_old = d.get("cloud_fraction")
                    if p == "l2a" and cloud_fraction_old:
                        products_new[p][prod_versions_v1[p]]["mask"]["cloud_fraction"] = cloud_fraction_old
                    # cloud_fraction_02 and nodata_fraction are always together
                    # rename cloud_fraction_02 to cloud_fraction on copy
                    cloud_fraction_02_old = d.get("cloud_fraction_02")
                    nodata_fraction_old = d.get("nodata_fraction")
                    if p == "mask" and cloud_fraction_02_old and nodata_fraction_old:
                        products_new[p][prod_versions_v1[p]]["maskTf"]["cloud_fraction"] = cloud_fraction_02_old
                        products_new[p][prod_versions_v1[p]]["maskTf"]["nodata_fraction"] = nodata_fraction_old

                    # Copy the mean_solar_azimuth and mean_solar_zenith values to the new products dictionary
                    mean_solar_azimuth_old = d.get("mean_solar_azimuth")
                    mean_solar_zenith_old = d.get("mean_solar_zenith")
                    if p == "l1b" and mean_solar_azimuth_old and mean_solar_zenith_old:
                        products_new[p][prod_versions_v1[p]]["obs"]["band_means"] = {
                            "solar_azimuth": mean_solar_azimuth_old,
                            "solar_zenith": mean_solar_zenith_old
                            }

                    # Copy the gring values to the new products dictionary
                    # Leave the old ones in place (instead of renaming to _bak).  I will delete them after
                    gring_old = d.get("gring")
                    if p == "l1b" and gring_old:
                        products_new[p][prod_versions_v1[p]]["glt"]["gring"] = gring_old

                # Append to updates for bulk write
                updated_fields["products"] = products_new
                
            if len(updated_fields) > 0:
                # if updated_fields["products_bak"].get("frcov") and updated_fields["products_bak"].get("l3"):
                # if updated_fields["products"].get("l1b", {}).get("01", {}).get("glt", {}).get("gring", None) is not None:
                #     pdb.set_trace()
                updates.append(
                    UpdateOne(
                        {"_id": d["_id"]},
                        {"$set": updated_fields}
                    )
                )
        
        # Bulk write
        if updates and args.update_db:
            chunk_size = 3000
            total_modified = 0
            total_matched = 0
            total_upserted = 0

            for i in range(0, len(updates), chunk_size):
                chunk = updates[i : i + chunk_size]
                result = acqs_coll.bulk_write(chunk, ordered=False)
                
                total_modified += result.modified_count
                total_matched += result.matched_count
                total_upserted += result.upserted_count

            print(f"Updated acquisitions collection:")
            print(f"Matched: {total_matched}, Modified: {total_modified}, Upserted: {total_upserted}")

    
    print("Finished executing script")


if __name__ == '__main__':
    main()
