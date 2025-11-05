"""
A script to compile metrics from various places and record them in a file or database table

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime
import datetime as dt
import glob
import json
import os
import requests
import sys

import pandas as pd

import spectral.io.envi as envi

from dateutil.relativedelta import relativedelta

from emit_main.database.database_manager import DatabaseManager


def update_collection(db_collection, query, data_frame):
    # Check if documents have changes before updating
    utc_now = dt.datetime.now(tz=dt.timezone.utc)
    result = db_collection.find_one(query)

    if result is None:
        data_frame["last_modified"] = utc_now
        db_collection.insert_one(data_frame)
        # print(f"Inserted DB with {data_frame}")
        return

    # If result is not none then compare new values with existing and only update if different
    if "_id" in result:
        del result["_id"]
    if "last_modified" in result:
        del result["last_modified"]

    needs_update = False
    for k,v in data_frame.items():
        if k not in result or result[k] != data_frame[k]:
            needs_update = True

    if needs_update:
        data_frame["last_modified"] = utc_now
        db_collection.update_one(query, {"$set": data_frame})
        # print(f"Updated DB with {data_frame}")
    else:
        pass
        # print("DB already contains matching document, so no update.")


def export_apid(apid, dm, query, out_dir, start, stop):
    if apid == "1674":
        apid_coll = dm.db.trending_1674
    elif apid == "1675":
        apid_coll = dm.db.trending_1675
    elif apid == "1676":
        apid_coll = dm.db.trending_1676
    results = list(apid_coll.find(query).sort("timestamp", 1))
    if len(results) > 0:
        utc_now_str = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        apid_outfile = f"{out_dir}/metrics_{apid}_{start}_{stop}.csv"
        df = pd.DataFrame(results)
        df = df.drop(["_id"], axis=1)
        df["timestamp"] = df["timestamp"].apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"))
        df["last_modified"] = df["last_modified"].apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"))
        df.to_csv(apid_outfile, sep=",", index=False)


def export_scene(dm, query, out_dir, start, stop):
    trending_acqs_coll = dm.db.trending_acquisitions
    results = list(trending_acqs_coll.find(query).sort("timestamp", 1))
    if len(results) > 0:
        utc_now_str = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        outfile = f"{out_dir}/metrics_scenes_{start}_{stop}.csv"
        df = pd.DataFrame(results)
        df = df.drop(["_id"], axis=1)
        df["timestamp"] = df["timestamp"].apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"))
        df["last_modified"] = df["last_modified"].apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"))
        df.to_csv(outfile, sep=",", index=False)


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Compile metrics for tracking")
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    parser.add_argument("--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    parser.add_argument("--date", help="A single date - YYYYMMDD")
    parser.add_argument("--month", help="Start date of month - YYYYMMDD")
    parser.add_argument("--metrics", default="streams,scenes,cmr", help="Which metrics to collect (streams,scenes,cmr)")
    parser.add_argument("--tracking_json", default="/store/brodrick/emit/emit-visuals/track_coverage.json",
                        help="JSON containing scene metrics")
    parser.add_argument("--export_to_dir", default=None)
    args = parser.parse_args()

    env = args.env

    if args.dates is None and args.date is None and args.month is None:
        print("You must specify either --date or --dates")

    start, stop = None, None

    if args.date is not None:
        start = args.date

    if args.month is not None:
        start = args.month

    if args.dates is not None:
        start, stop = args.dates.split(",")

    start_date = dt.datetime.strptime(start, "%Y%m%d")
    if stop is None:
        if args.month is not None:
            stop_date = start_date + relativedelta(months=1)
            stop = stop_date.strftime("%Y%m%d")
        if args.date is not None:
            stop_date = start_date + datetime.timedelta(days=1)
            stop = stop_date.strftime("%Y%m%d")
    else:
        stop_date = dt.datetime.strptime(stop, "%Y%m%d")

    print(f"Using start and stop of {start} and {stop}, and start_date and stop_date of {start_date} and {stop_date}")

    metrics_flags = args.metrics.split(",")

    config_path = f"/store/emit/{args.env}/repos/emit-main/emit_main/config/{args.env}_sds_config.json"
    print(f"Using config_path {config_path}")
    dm = DatabaseManager(config_path)

    # If exporting, just export CSVs based on date range
    if args.export_to_dir:
        print(f"Exporting to directory {args.export_to_dir}")
        if not os.path.exists(args.export_to_dir):
            os.makedirs(args.export_to_dir)
        query = {
            "timestamp": {"$gte": start_date, "$lt": stop_date}
        }

        if "streams" in metrics_flags:
            for apid in ["1674", "1675", "1676"]:
                print(f"Exporting apid {apid} to CSV")
                export_apid(apid, dm, query, args.export_to_dir, start, stop)

        if "scenes" in metrics_flags:
            print("Exporting scene data to CSV")
            export_scene(dm, query, args.export_to_dir, start, stop)
        sys.exit()

    if "streams" in metrics_flags:
        # Collect metrics on APID streams
        for apid in ["1674", "1675", "1676"]:
            print(f"Collecting metrics on apid {apid}")
            date_dirs = glob.glob(f"/store/emit/{env}/data/streams/{apid}/*")
            start_dir = f"/store/emit/{env}/data/streams/{apid}/{start}"
            stop_dir = f"/store/emit/{env}/data/streams/{apid}/{stop}"
            date_dirs = [dir for dir in date_dirs if start_dir <= dir < stop_dir]
            date_dirs.sort()
            print(f"The filtered list of stream dirs to check is {date_dirs}")

            for dir in date_dirs:
                l0_reports = glob.glob(f"{dir}/l0/*report.txt")
                for report in l0_reports:
                    packet_count, missing_packets, psc_gaps, duplicate_packets, timing_errors = 0, 0, 0, 0, 0
                    with open(report, "r") as f:
                        for line in f.readlines():
                            if "Packet Count" in line and "Duplicate" not in line:
                                packet_count = int(line.rstrip("\n").split(" ")[-1])
                            if "Missing PSC Count" in line:
                                missing_packets = int(line.rstrip("\n").split(" ")[-1])
                            if "PSC Errors" in line:
                                psc_gaps = int(line.rstrip("\n").split(" ")[-1])
                            if "Duplicate Packet Count" in line:
                                duplicate_packets = int(line.rstrip("\n").split(" ")[-1])
                            if "Timing Errors Count" in line:
                                timing_errors = int(line.rstrip("\n").split(" ")[-1])
                    timestamp_str = os.path.basename(report).split("_")[2]
                    timestamp = dt.datetime.strptime(timestamp_str, "%Y%m%dt%H%M%S")
                    # timestamp_utc = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
                    df = {
                        "timestamp": timestamp,
                        f"{apid}_packets": packet_count,
                        f"{apid}_psc_gaps": psc_gaps,
                        f"{apid}_missing_packets": missing_packets,
                        f"{apid}_duplicate_packets": duplicate_packets,
                        f"{apid}_timing_errors": timing_errors
                    }
                    ccsds_file = report.replace("_report.txt", ".bin")
                    start_time = os.path.basename(ccsds_file).split("_")[2].upper()
                    hosc_files = glob.glob(f"{dir}/raw/{apid}_{start_time}_*hsc.bin")
                    if os.path.exists(ccsds_file):
                        df[f"{apid}_ccsds_size_bytes"] = os.path.getsize(ccsds_file)
                    if len(hosc_files) > 0:
                        df[f"{apid}_hosc_size_bytes"] = os.path.getsize(hosc_files[0])

                    if apid == "1674":
                        apid_coll = dm.db.trending_1674
                    elif apid == "1675":
                        apid_coll = dm.db.trending_1675
                    elif apid == "1676":
                        apid_coll = dm.db.trending_1676
                    query = {"timestamp": timestamp}
                    update_collection(apid_coll, query, df)

    if "scenes" in metrics_flags:
        # Collect metrics on acquisitions/scenes
        print(f"Collecting metrics on acquisitions/scenes")
        date_dirs = glob.glob(f"/store/emit/{env}/data/acquisitions/*")
        start_dir = f"/store/emit/{env}/data/acquisitions/{start}"
        stop_dir = f"/store/emit/{env}/data/acquisitions/{stop}"
        date_dirs = [dir for dir in date_dirs if start_dir <= dir < stop_dir]
        date_dirs.sort()
        print(f"The filtered list of acquisition dirs to check is {date_dirs}")

        # First, get data from acquisition directories
        print("Checking acquisition directories")
        for dir in date_dirs:
            acqs = [os.path.basename(a) for a in glob.glob(f"{dir}/*")]
            # print(f"Found acqs: {acqs}")
            for acq in acqs:
                timestamp_str = os.path.basename(acq).split("_")[0][4:]
                timestamp = dt.datetime.strptime(timestamp_str, "%Y%m%dt%H%M%S")
                df = {
                    "timestamp": timestamp,
                    "acquisition_id": acq
                }
                raw_img_files = glob.glob(f"{dir}/{acq}/l1a/{acq}*_raw_*img")
                reassembly_reports = glob.glob(f"{dir}/{acq}/l1a/{acq}*_raw_*report.txt")
                rdn_img_files = glob.glob(f"{dir}/{acq}/l1b/{acq}*_rdn_*img")
                rdn_hdr_files = glob.glob(f"{dir}/{acq}/l1b/{acq}*_rdn_*hdr")
                rdn_nc_files = glob.glob(f"{dir}/{acq}/l1b/{acq}*_rdn_*nc")
                glt_img_files = glob.glob(f"{dir}/{acq}/l1b/{acq}*_glt_*img")
                loc_img_files = glob.glob(f"{dir}/{acq}/l1b/{acq}*_loc_*img")
                obs_img_files = glob.glob(f"{dir}/{acq}/l1b/{acq}*_obs_*img")
                obs_nc_files = glob.glob(f"{dir}/{acq}/l1b/{acq}*_obs_*nc")
                rfl_img_files = glob.glob(f"{dir}/{acq}/l2a/{acq}*_rfl_*img")
                rfl_nc_files = glob.glob(f"{dir}/{acq}/l2a/{acq}*_rfl_*nc")
                rflunc_img_files = glob.glob(f"{dir}/{acq}/l2a/{acq}*_rfluncert_*img")
                rflunc_nc_files = glob.glob(f"{dir}/{acq}/l2a/{acq}*_rfluncert_*nc")
                mask_img_files = glob.glob(f"{dir}/{acq}/l2a/{acq}*_mask_*img")
                mask_nc_files = glob.glob(f"{dir}/{acq}/l2a/{acq}*_mask_*nc")
                min_img_files = glob.glob(f"{dir}/{acq}/l2b/{acq}*_abun_*img")
                min_nc_files = glob.glob(f"{dir}/{acq}/l2b/{acq}*_abun_*nc")
                minunc_img_files = glob.glob(f"{dir}/{acq}/l2b/{acq}*_abununcert_*img")
                minunc_nc_files = glob.glob(f"{dir}/{acq}/l2b/{acq}*_abununcert_*nc")
                ch4_img_files = glob.glob(f"{dir}/{acq}/ghg/ch4/{acq}*_ch4_*img")
                ch4_tif_files = glob.glob(f"{dir}/{acq}/ghg/ch4/{acq}*_ortch4_*tif")
                sensch4_img_files = glob.glob(f"{dir}/{acq}/ghg/ch4/{acq}*_sensch4_*img")
                sensch4_tif_files = glob.glob(f"{dir}/{acq}/ghg/ch4/{acq}*_ortsensch4_*tif")
                uncertch4_img_files = glob.glob(f"{dir}/{acq}/ghg/ch4/{acq}*_uncertch4_*img")
                uncertch4_tif_files = glob.glob(f"{dir}/{acq}/ghg/ch4/{acq}*_ortuncertch4_*tif")
                co2_img_files = glob.glob(f"{dir}/{acq}/ghg/co2/{acq}*_co2_*img")
                co2_tif_files = glob.glob(f"{dir}/{acq}/ghg/co2/{acq}*_ortco2_*tif")
                sensco2_img_files = glob.glob(f"{dir}/{acq}/ghg/co2/{acq}*_sensco2_*img")
                sensco2_tif_files = glob.glob(f"{dir}/{acq}/ghg/co2/{acq}*_ortsensco2_*tif")
                uncertco2_img_files = glob.glob(f"{dir}/{acq}/ghg/co2/{acq}*_uncertco2_*img")
                uncertco2_tif_files = glob.glob(f"{dir}/{acq}/ghg/co2/{acq}*_ortuncertco2_*tif")
                df["l1a_raw_exists"], df["l1b_radiance_exists"] = False, False
                df["l2a_reflectance_exists"], df["l2b_mineral_id_exists"] = False, False

                if len(raw_img_files) > 0:
                    df["raw_img_size_bytes"] = os.path.getsize(raw_img_files[0])
                    df["l1a_raw_exists"] = True
                if len(rdn_img_files) > 0:
                    df["rdn_img_size_bytes"] = os.path.getsize(rdn_img_files[0])
                    df["l1b_radiance_exists"] = True
                if len(rdn_nc_files) > 0:
                    df["rdn_nc_size_bytes"] = os.path.getsize(rdn_nc_files[0])
                if len(glt_img_files) > 0:
                    df["glt_img_size_bytes"] = os.path.getsize(glt_img_files[0])
                if len(loc_img_files) > 0:
                    df["loc_img_size_bytes"] = os.path.getsize(loc_img_files[0])
                if len(obs_img_files) > 0:
                    df["obs_img_size_bytes"] = os.path.getsize(obs_img_files[0])
                if len(obs_nc_files) > 0:
                    df["obs_nc_size_bytes"] = os.path.getsize(obs_nc_files[0])
                if len(rfl_img_files) > 0:
                    df["rfl_img_size_bytes"] = os.path.getsize(rfl_img_files[0])
                    df["l2a_reflectance_exists"] = True
                if len(rfl_nc_files) > 0:
                    df["rfl_nc_size_bytes"] = os.path.getsize(rfl_nc_files[0])
                if len(rflunc_img_files) > 0:
                    df["rflunc_img_size_bytes"] = os.path.getsize(rflunc_img_files[0])
                if len(rflunc_nc_files) > 0:
                    df["rflunc_nc_size_bytes"] = os.path.getsize(rflunc_nc_files[0])
                if len(mask_img_files) > 0:
                    df["mask_img_size_bytes"] = os.path.getsize(mask_img_files[0])
                if len(mask_nc_files) > 0:
                    df["mask_nc_size_bytes"] = os.path.getsize(mask_nc_files[0])
                if len(min_img_files) > 0:
                    df["min_img_size_bytes"] = os.path.getsize(min_img_files[0])
                    df["l2b_mineral_id_exists"] = True
                if len(min_nc_files) > 0:
                    df["min_nc_size_bytes"] = os.path.getsize(min_nc_files[0])
                if len(minunc_img_files) > 0:
                    df["minunc_img_size_bytes"] = os.path.getsize(minunc_img_files[0])
                if len(minunc_nc_files) > 0:
                    df["minunc_nc_size_bytes"] = os.path.getsize(minunc_nc_files[0])
                if len(ch4_img_files) > 0:
                    df["ch4_img_size_bytes"] = os.path.getsize(ch4_img_files[0])
                if len(ch4_tif_files) > 0:
                    df["ch4_tif_size_bytes"] = os.path.getsize(ch4_tif_files[0])
                if len(sensch4_img_files) > 0:
                    df["sensch4_img_size_bytes"] = os.path.getsize(sensch4_img_files[0])
                if len(sensch4_tif_files) > 0:
                    df["sensch4_tif_size_bytes"] = os.path.getsize(sensch4_tif_files[0])
                if len(uncertch4_img_files) > 0:
                    df["uncertch4_img_size_bytes"] = os.path.getsize(uncertch4_img_files[0])
                if len(uncertch4_tif_files) > 0:
                    df["uncertch4_tif_size_bytes"] = os.path.getsize(uncertch4_tif_files[0])
                if len(co2_img_files) > 0:
                    df["co2_img_size_bytes"] = os.path.getsize(co2_img_files[0])
                if len(co2_tif_files) > 0:
                    df["co2_tif_size_bytes"] = os.path.getsize(co2_tif_files[0])
                if len(sensco2_img_files) > 0:
                    df["sensco2_img_size_bytes"] = os.path.getsize(sensco2_img_files[0])
                if len(sensco2_tif_files) > 0:
                    df["sensco2_tif_size_bytes"] = os.path.getsize(sensco2_tif_files[0])
                if len(uncertco2_img_files) > 0:
                    df["uncertco2_img_size_bytes"] = os.path.getsize(uncertco2_img_files[0])
                if len(uncertco2_tif_files) > 0:
                    df["uncertco2_tif_size_bytes"] = os.path.getsize(uncertco2_tif_files[0])
                # Get reassembly report info
                if len(reassembly_reports) > 0:
                    num_lines, corrupt_lines, cloudy_frames = 0, 0, 0
                    lines_per_frame = 32  # default it but also get it below
                    missing_frames, corrupt_frames, decompression_errors = 0, 0, 0
                    submode = ""
                    with open(reassembly_reports[0], "r") as f:
                        for line in f.readlines():
                            if "Submode:" in line:
                                submode = line.rstrip("\n").split(" ")[-1]
                            if "Instrument mode:" in line:
                                instrument_mode = line.rstrip("\n").split(" ")[-1]
                            if "Number of lines:" in line:
                                num_lines = int(line.rstrip("\n").split(" ")[-1])
                            if "Total corrupt lines" in line:
                                corrupt_lines = int(line.rstrip("\n").split(" ")[-1])
                            if "Number of lines per frame" in line:
                                lines_per_frame = int(line.rstrip("\n").split(" ")[-1])
                            if "Total cloudy frames" in line:
                                cloudy_frames = int(line.rstrip("\n").split(" ")[-1])
                            if "Total missing frames" in line:
                                missing_frames = int(line.rstrip("\n").split(" ")[-1])
                            if "Total corrupt frames" in line:
                                corrupt_frames = int(line.rstrip("\n").split(" ")[-1])
                            if "Total decompression errors" in line:
                                decompression_errors = int(line.rstrip("\n").split(" ")[-1])

                    df.update({
                        "submode": submode,
                        "instrument_mode": instrument_mode,
                        "lines": num_lines,
                        "corrupt_lines": corrupt_lines,
                        "frames": num_lines // lines_per_frame,
                        "cloudy_frames": cloudy_frames,
                        "missing_frames": missing_frames,
                        "corrupt_frames": corrupt_frames,
                        "decompression_errors": decompression_errors
                    })

                # Get masked pixel noise from radiance header
                if len(rdn_hdr_files) > 0:
                    hdr = envi.read_envi_header(rdn_hdr_files[0])
                    if "masked pixel noise" in hdr:
                        df["masked_pixel_noise"] = float(hdr["masked pixel noise"])

                # Update the DB
                trending_acqs_coll = dm.db.trending_acquisitions
                query = {"timestamp": timestamp}
                update_collection(trending_acqs_coll, query, df)

        # Get metrics from tracking json
        print(f"Checking tracking json file at {args.tracking_json}")
        with open(args.tracking_json, "r") as f:
            scenes = json.load(f)["features"]
            for s in scenes:
                p = s["properties"]
                timestamp_str = os.path.basename(p["fid"]).split("_")[0][4:]
                timestamp = dt.datetime.strptime(timestamp_str, "%Y%m%dt%H%M%S")
                # Only process scenes in the argument time range
                if start_date < timestamp < stop_date:
                    df = {
                        "timestamp": timestamp,
                        "aspect": p["Aspect (local surface aspect 0 to 360 degrees clockwise from N)"],
                        "cosine_i": p["Cosine(i) (apparent local illumination factor based on DEM slope and aspect and to sun vector)"],
                        "earth_sun_distance": p["Earth-sun distance (AU)"],
                        "path_length": p["Path length (sensor-to-ground in meters)"],
                        "slope": p["Slope (local surface slope as derived from DEM in degrees)"],
                        "solar_phase": p["Solar phase (degrees between to-sensor and to-sun vectors in principal plane)"],
                        "to_sensor_azimuth": p["To-sensor azimuth (0 to 360 degrees CW from N)"],
                        "to_sensor_zenith": p["To-sensor zenith (0 to 90 degrees from zenith)"],
                        "to_sun_azimuth": p["To-sun azimuth (0 to 360 degrees CW from N)"],
                        "to_sun_zenith": p["To-sun zenith (0 to 90 degrees from zenith)"],
                        "utc_time_decimal_hours": p["UTC Time (decimal hours for mid-line pixels)"]
                    }
                    if "Cloud + Cirrus Fraction" in p:
                        df["cloud_plus_cirrus_fraction"] = p["Cloud + Cirrus Fraction"]
                    if "Cloud Fraction" in p:
                        df["cloud_fraction"] = p["Cloud Fraction"]
                    if "Clouds & Buffer Fraction" in p:
                        df["clouds_and_buffer_fraction"] = p["Clouds & Buffer Fraction"]
                    if "Screened Onboard Fraction" in p:
                        df["screened_onboard_fraction"] = p["Screened Onboard Fraction"]
                    if "Total Cloud Fraction" in p:
                        df["total_cloud_fraction"] = p["Total Cloud Fraction"]
                    if "Retrieved AOT Median" in p:
                        df["retrieved_aot_median"] = p["Retrieved AOT Median"]
                    if "Retrieved Ele. Median" in p:
                        df["retrieved_ele_median"] = p["Retrieved Ele. Median"]
                    if "Retrieved WV Median" in p:
                        df["retrieved_wavelength_median"] = p["Retrieved WV Median"]

                    trending_acqs_coll = dm.db.trending_acquisitions
                    query = {"timestamp": timestamp}
                    update_collection(trending_acqs_coll, query, df)

    if "cmr" in metrics_flags:
        CMR_OPS = 'https://cmr.earthdata.nasa.gov/search'  # CMR API Endpoint
        url = f'{CMR_OPS}/granules.umm_json'
        collections = {
            # "l1a": "C2407975601-LPCLOUD",
            "l1b": "C2408009906-LPCLOUD",
            "l2a": "C2408750690-LPCLOUD",
            "l2b": "C2408034484-LPCLOUD"
        }
        # token = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6IndpbnN0b25vbHNvbiIsImV4cCI6MTcxODU3MDg3NywiaWF0IjoxNzEzMzg2ODc3LCJpc3MiOiJFYXJ0aGRhdGEgTG9naW4ifQ.3HAXsV3fzMYMY7LqztJa71Z_vfyXbleR3JZGkXv57uMfekYRHN_Y9rw38dX3twtccBiTsohlFXXprFnTpiBKMgqDxcS55eis8G46SF8Y7Nt4qikY8k4RkF7szMmfqcOJYpj1v1INONBhF9W7Pjew9DUpLJiMQgyKbC90gEmCADbPVZVgL0_rlzT7oL__w_vQTQMauwz7Exr3T63BeISJzAj1jz7zeDo3ROuGZrqtfSX9z-ngLRpCyO9CjCF_ABtL0Y6ZRBoZPiYE43sfm-YnSiKhAfy46ju5CpHomPxbAWrfzDqkN52OXX9tLFGkrK8ucW4snOWW8_LZn5rwGOVKFA"
        cur_date = start_date
        while cur_date < stop_date:
            cur_plus_one = cur_date + datetime.timedelta(days=1)
            prod_range = f"{cur_date.strftime('%Y-%m-%dT%H:%M:%SZ')},{cur_plus_one.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            print(f"### CMR prod_range: {prod_range}")
            # Loop through collections
            for level, coll in collections.items():
                response = requests.get(url, params={'concept_id': coll, 'page_size': 500, 'temporal': prod_range},
                                        headers={'Accept': 'application/json'
                                                 #'Authorization':f'Bearer {token}'
                                                 })
                out_json_dir = f"/store/emit/{env}/reports/cmr/{cur_date.strftime('%Y%m%d')}"
                out_json_path = f"{out_json_dir}/cmr_{cur_date.strftime('%Y%m%d')}_{level}.json"
                if not os.path.exists(out_json_dir):
                    os.makedirs(out_json_dir)
                with open(out_json_path, "w") as f:
                    f.write(json.dumps(response.json(), indent=4))

                # Loop through json and get granules
                for g in response.json()["items"]:
                    timestamp = dt.datetime.strptime(g["meta"]["native-id"].split("_")[4], "%Y%m%dT%H%M%S")
                    last_publish_date = dt.datetime.strptime(g["meta"]["revision-date"], "%Y-%m-%dT%H:%M:%S.%fZ")

                    level_pub_date = f"{level}_publish_date"
                    acq_to_level_pub_seconds = f"acquisition_to_{level}_publish_seconds"
                    df = {
                        "timestamp": timestamp,
                        level_pub_date: last_publish_date,
                        acq_to_level_pub_seconds: (last_publish_date - timestamp).total_seconds()
                    }
                    trending_acqs_coll = dm.db.trending_acquisitions
                    query = {"timestamp": timestamp}
                    update_collection(trending_acqs_coll, query, df)

            cur_date = cur_plus_one


if __name__ == '__main__':
    main()
