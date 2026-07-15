"""
A script to compile metrics from various places and record them in a file or database table

"""

import argparse
import datetime as dt
import json

import pandas as pd
from dateutil.relativedelta import relativedelta

from emit_main.database.database_manager import DatabaseManager

OBS_VARS = {
                "path_length": "Path length (sensor-to-ground in meters)",
                "sensor_azimuth": "To-sensor azimuth (0 to 360 degrees CW from N)",
                "sensor_zenith":  "To-sensor zenith (0 to 90 degrees from zenith)",
                "solar_azimuth": "To-sun azimuth (0 to 360 degrees CW from N)",
                "solar_zenith": "To-sun zenith (0 to - 90 degrees from zenith)",
                "solar_phase": "Solar phase (degrees between to-sensor and to-sun vectors in principal - plane)",
                "slope": "Slope (local surface slope as derived from DEM in degrees)",
                "aspect": "Aspect (local surface aspect 0 to 360 degrees clockwise from N)",
                "cosine_i": "Cosine(i) (apparent local illumination factor based on DEM slope and - aspect and to sun vector)",
                "utc_time": "UTC Time (decimal hours for mid-line pixels)",
                "earth_sun_dist": "Earth-sun distance (AU)",
            }

STATE_VARS = {
                "aot": "Retrieved AOT Median",
                "surface_elevation_km": "Retrieved Ele. Median",
                "h2o":  "Retrieved WV Median"
            }

MASK_VARS = {
                "cloudratio_fraction" : "Cloud Cover with ratio",
                "cloud_fraction" : "Cloud Fraction Spectf",
                "nodata_fraction" : "Screened Onboard Fraction",
            }

def flatten_dict(d, parent_key="", sep="."):
    flattened = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            flattened.update(flatten_dict(v, new_key, sep=sep))
        else:
            flattened[new_key] = v
    return flattened

def make_feature(record, l1b_v, l2a_v, mask_v):

    record = flatten_dict(record)
    st = record.get('start_time')
    et = record.get('stop_time')

    fid = record.get('acquisition_id')

    time_string = fid[4:].upper()

    feature = {
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": gring_to_polygon(record['gring'])},
        "properties": {
            "fid": fid,
            "dcid": record.get('associated_dcid'),
            "Orbit": record.get('orbit'),
            "Orbit Segment": record.get('scene'),
            "start_time": st.strftime("%Y-%m-%dT%H:%M:%SZ") if st else None,
            "end_time": et.strftime("%Y-%m-%dT%H:%M:%SZ") if et else None,
            'style': {"weight":1,
                      "opacity":1,
                      "fillColor": "#0000FF",
                      "color": "#0000FF"}
        },
    }

    for key, name in OBS_VARS.items():
        obs_val = record.get(f'products.l1b.{l1b_v}.obs.band_means.{key}')
        if obs_val:
            feature["properties"][name] = round(obs_val, 2)
    for key, name in STATE_VARS.items():
        state_val = record.get(f'products.l2a.{l2a_v}.state.band_medians.{key}')
        if state_val:
            feature["properties"][name] = round(state_val, 2)

    for key, name in MASK_VARS.items():
        mask_val = record.get(f'products.mask.{mask_v}.maskTf.{key}')
        if mask_val:
            feature["properties"][name] = mask_val

    screened = feature["properties"].get("Screened Onboard Fraction", False)
    cloud_fraction = feature["properties"].get("Cloud Fraction Spectf", False)

    if screened & cloud_fraction:
        feature["properties"]["Cloud Cover"] = screened + cloud_fraction

    on_daac = False

    if record.get(f'products.l1b.{l1b_v}.rdn_ummg'):
        l1b_base = f'https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/EMITL1BRAD.0{l1b_v}'
        l1b_rad = f'EMIT_L1B_RAD_0{l1b_v}_{time_string}'
        l1b_obs = f'EMIT_L1B_OBS_0{l1b_v}_{time_string}'
        feature['properties']['L1B Radiance Download'] = f'{l1b_base}/{l1b_rad}/{l1b_rad}.nc'
        feature['properties']['L1B Observation Download'] = f'{l1b_base}/{l1b_rad}/{l1b_obs}.nc'
        on_daac = True

    if record.get(f'products.l2a.{l2a_v}.rfl_ummg'):
        l2a_base = f'https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/EMITL2ARFL.0{l2a_v}'
        l2a_rfl = f'EMIT_L2A_RFL_0{l2a_v}_{time_string}'
        l2a_rflunc = f'EMIT_L2A_RFLUNCERT_0{l2a_v}_{time_string}'
        feature['properties']['L2A Reflectance Download'] = f'{l2a_base}/{l2a_rfl}/{l2a_rfl}.nc'
        feature['properties']['L2A Reflectance Uncertainty Download'] = f'{l2a_base}/{l2a_rfl}/{l2a_rflunc}.nc'
        on_daac = True

    if record.get(f'products.mask.{mask_v}.maskTf_ummg'):
        mask_base = f'https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/EMITL2AMASK.0{mask_v}'
        l2a_mask = f'EMIT_L2A_MASK_0{mask_v}_{time_string}'
        feature['properties']['L2A Mask Download'] = f'{mask_base}/{l2a_mask}/{l2a_mask}.nc'
        on_daac = True

    if on_daac == False:
        feature['properties']['style'] = {"weight":1,"opacity":1,"fillColor": "#f6c409", "color": "#f6c409"}

    return feature

def gring_to_polygon(gring):
    ring = [list(pt) for pt in gring]
    if ring[0] != ring[-1]:
        ring.append(ring[0])
    return [ring]

def open_chunk(n, out_file_chunk_base):
    fh = open(f"{out_file_chunk_base}_{n}.json", "w")
    fh.write('{\n    "type": "FeatureCollection",\n    "features": [\n')
    return fh

def close_chunk(fh):
    fh.write("\n    ]\n}\n")
    fh.close()

def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Compile metrics for tracking")
    parser.add_argument('out_base', help="Ouput coverage json file")
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    parser.add_argument("--chunk", action="store_true", default=False, help="Split output into chunked JSON files")
    parser.add_argument("--dataframe", action="store_true", default=False, help="Write features to CSV")
    parser.add_argument("--chunksize",type = int, default=5000, help="JSON splitting size")
    parser.add_argument("--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    args = parser.parse_args()

    out_file = f'{args.out_base}_pub.json'
    out_file_db = f'{args.out_base}_db.csv'
    out_file_chunk_base = f'{args.out_base}_pub_chunk'

    config_path = f"/store/emit/{args.env}/repos/emit-main/emit_main/config/{args.env}_sds_config.json"
    print(f"Using config_path {config_path}")

    dm = DatabaseManager(config_path)
    acq_coll = dm.db.acquisitions

    query = {"gring": {"$exists": True}}

    if args.dates is not None:
        parts = args.dates.split(",")
        start_date = dt.datetime.strptime(parts[0], "%Y%m%d")
        stop_date = dt.datetime.strptime(parts[1], "%Y%m%d")

        query["start_time"] = {"$gte": start_date, "$lt": stop_date}

    # TODO: add dm.
    l1b_v = config["product_config"]["prod_versions"]["l1b"]
    l2a_v = config["product_config"]["prod_versions"]["l2a"]
    mask_v = config["product_config"]["prod_versions"]["mask"]

    projection = {
        "gring": 1,
        "acquisition_id": 1,
        "associated_dcid": 1,
        "orbit": 1,
        "scene": 1,
        "start_time": 1,
        "stop_time": 1,
        f"products.l1b.{l1b_v}.obs.band_means": 1,
        f"products.l2a.{l2a_v}.state.band_medians": 1,
        f"products.mask.{mask_v}.maskTf": 1,
        f"products.l1b.{l1b_v}.rdn_ummg": 1,
        f"products.l2a.{l2a_v}.rfl_ummg": 1,
        f"products.mask.{mask_v}.maskTf_ummg": 1,
        "_id": 0,
    }

    features = []
    chunk_n = 0
    chunk_count = 0
    cf = None

    cursor = acq_coll.find(query, projection, batch_size=1000).sort("acquisition_id", 1)

    daac_index = 1
    fid_index = 1
    with open(out_file, "w") as f:
        f.write('{\n    "type": "FeatureCollection",\n    "features": [\n')
        first = True
        for record in cursor:
            feat = make_feature(record, l1b_v, l2a_v, mask_v)

            feat['properties']['FID_index'] = fid_index
            feat['properties']['DAAC_index'] = daac_index
            fid_index +=1
            if 'L1B Radiance Download' in list(feat['properties'].keys()):
              daac_index += 1

            if args.dataframe:
                features.append(feat['properties'])
            line = "        " + json.dumps(feat)
            if not first:
                f.write(",\n")
            f.write(line)
            first = False
            if args.chunk:
                if cf is None:
                    cf = open_chunk(chunk_n, out_file_chunk_base)
                    chunk_first = True
                if not chunk_first:
                    cf.write(",\n")
                cf.write(line)
                chunk_first = False
                chunk_count += 1
                if chunk_count >= args.chunk_size:
                    close_chunk(cf)
                    cf = None
                    chunk_n += 1
                    chunk_count = 0
        f.write("\n    ]\n}\n")

    if cf is not None:
        close_chunk(cf)

    if args.dataframe:
        pd.DataFrame(features).to_csv(out_file_db, index=False)

if __name__ == '__main__':
    main()
