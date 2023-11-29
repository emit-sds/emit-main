"""
This code contains tasks for executing EMIT Level 1B PGEs and helper utilities.

Authors: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
         Philip G. Brodrick, philip.brodrick@jpl.nasa.gov
"""

import datetime
import glob
import json
import logging
import os

import h5netcdf.legacyapi as netCDF4
import luigi
import spectral.io.envi as envi
# import netCDF4

from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.output_targets import AcquisitionTarget, OrbitTarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.slurm import SlurmJobTask
from emit_utils import daac_converter
from emit_utils.file_checks import envi_header, get_gring_boundary_points, get_band_mean

logger = logging.getLogger("emit-main")


class L1BCalibrate(SlurmJobTask):
    """
    Performs calibration of raw data to produce radiance
    :returns: Spectrally calibrated radiance
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    dark_path = luigi.Parameter(default="")
    use_future_flat = luigi.BoolParameter(default=False)

    n_cores = 40
    memory = 180000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        dm = wm.database_manager
        build_nums = wm.config["product_versions"]["l1b"]["compatible_input_builds"]
        # Insert new acquisition if it doesn't exist
        if wm.acquisition is None and len(build_nums) > 1:
            compat_acq = dm.find_acquisition_by_id_and_product(self.acquisition_id, "products.l1a.raw.img_path",
                                                               build_nums=build_nums)
            if compat_acq:
                compat_acq = wm.remove_keys_from_dict(["_id", "gring", "mean_solar_azimuth", "mean_solar_zenith",
                                                      "cloud_fraction"], compat_acq)
                compat_build_num = compat_acq["build_num"]
                compat_acq["build_num"] = wm.config["build_num"]
                # TODO: Remove processing_version everywhere
                compat_acq["product_versions"] = wm.config["product_versions"]
                compat_acq["processing_log"] = []
                products = compat_acq["products"]
                compat_acq["products"] = {
                    "l1a": products["l1a"]
                }
                compat_acq["copied_from"] = compat_build_num
                if not dm.find_acquisition_by_id(self.acquisition_id):
                    dm.insert_acquisition(compat_acq)
                    wm.print(__name__, f"Inserted acquisition from build {compat_build_num} as {compat_acq}")
                    wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
            else:
                raise RuntimeError(f"Unable to find compatible acquisition for {self.acquisition_id} using build_nums "
                                   f"{build_nums}")

        acq = wm.acquisition
        pge = wm.pges["emit-sds-l1b"]

        # PGE writes to tmp folder
        tmp_dir = self.local_tmp_dir
        tmp_output_dir = os.path.join(tmp_dir, "output")
        wm.makedirs(tmp_output_dir)
        tmp_rdn_img_path = os.path.join(tmp_output_dir, os.path.basename(acq.rdn_img_path))
        tmp_rdndestripe_img_path = tmp_rdn_img_path.replace("rdn", "rdndestripe")
        tmp_bandmask_img_path = os.path.join(tmp_output_dir, os.path.basename(acq.bandmask_img_path))
        tmp_ffupdate_img_path = os.path.join(tmp_output_dir, os.path.basename(acq.ffupdate_img_path))
        tmp_ffmedian_img_path = os.path.join(tmp_output_dir, os.path.basename(acq.ffmedian_img_path))
        log_name = os.path.basename(acq.rdn_img_path.replace(".img", "_pge.log"))
        runconfig_name = os.path.basename(acq.rdn_img_path.replace(".img", "_runconfig.json"))
        tmp_log_path = os.path.join(tmp_output_dir, log_name)
        tmp_runconfig_path = os.path.join(tmp_dir, runconfig_name)
        l1b_config_path = wm.config["l1b_config_path"]

        # Set instrument mode
        instrument_mode = "default"
        if acq.instrument_mode == "cold_img_mid" or acq.instrument_mode == "cold_img_mid_vdda":
            instrument_mode = "half"

        # Update config file values with absolute paths and store all input files for logging later
        with open(l1b_config_path, "r") as f:
            l1b_config = json.load(f)
        input_files = {}
        for key, value in l1b_config.items():
            if "_file" in key:
                if not value.startswith("/"):
                    l1b_config[key] = os.path.abspath(os.path.join(os.path.dirname(l1b_config_path), value))
                input_files[key] = l1b_config[key]

        # Also update the nested "modes"
        if "modes" in l1b_config:
            for key, value in l1b_config["modes"].items():
                for k, v in value.items():
                    if "_file" in k:
                        if not v.startswith("/"):
                            l1b_config["modes"][key][k] = os.path.abspath(os.path.join(os.path.dirname(l1b_config_path), v))
                        if key == instrument_mode:
                            input_files[k] = l1b_config["modes"][key][k]

        # Get raw input path
        raw_img_path = acq.products["l1a"]["raw"]["img_path"]

        # Get nearby darks
        dark_img_path = ""
        if len(self.dark_path) > 0:
            dark_img_path = self.dark_path
        else:
            # Find dark image - Get nearest dark image, but throw error if not within last 800 minutes
            recent_darks = dm.find_acquisitions_touching_date_range(
                "dark",
                "stop_time",
                acq.start_time - datetime.timedelta(minutes=800),
                acq.start_time,
                instrument_mode=acq.instrument_mode,
                min_valid_lines=512,
                sort=-1,
                build_nums=build_nums)
            future_darks = dm.find_acquisitions_touching_date_range(
                "dark",
                "start_time",
                acq.stop_time,
                acq.stop_time + datetime.timedelta(minutes=800),
                instrument_mode=acq.instrument_mode,
                min_valid_lines=512,
                sort=1,
                build_nums=build_nums)

            # Trim out any cases where darks were not processed to l1a
            for dark_ind in range(len(future_darks) - 1, -1, -1):
                if 'products' not in list(future_darks[dark_ind].keys()):
                    future_darks.pop(dark_ind)
            for dark_ind in range(len(recent_darks) - 1, -1, -1):
                if 'products' not in list(recent_darks[dark_ind].keys()):
                    recent_darks.pop(dark_ind)

            if (recent_darks is None or len(recent_darks) == 0) and (future_darks is None or len(future_darks) == 0):
                raise RuntimeError(f"Unable to find any darks for acquisition {acq.acquisition_id} within 800 minutes.")

            # Set dark image path to recent or future depending on which is nearest
            recent_dark_offset = None
            future_dark_offset = None
            if recent_darks is not None and len(recent_darks) > 0:
                dark_img_path = recent_darks[0]["products"]["l1a"]["raw"]["img_path"]
                recent_dark_offset = abs((acq.start_time - recent_darks[0]["stop_time"]).total_seconds())
                wm.print(__name__, f"Recent dark offset: {recent_dark_offset}")

            if future_darks is not None and len(future_darks) > 0:
                dark_img_path = future_darks[0]["products"]["l1a"]["raw"]["img_path"]
                future_dark_offset = abs((future_darks[0]["start_time"] - acq.stop_time).total_seconds())
                wm.print(__name__, f"Future dark offset: {future_dark_offset}")

            # If we have both, then set the dark image path back to recent dark if the offset is smaller
            if recent_dark_offset and future_dark_offset and recent_dark_offset <= future_dark_offset:
                dark_img_path = recent_darks[0]["products"]["l1a"]["raw"]["img_path"]
                wm.print(__name__, "Recent dark offset is less than future dark offset")

        # Add to input files
        input_files["dark_file"] = dark_img_path
        input_files["raw_file"] = raw_img_path

        # Get recent ffupdate paths (returns the 350 most recent in descending order)
        ffupdate_acqs = dm.find_nearby_acquisitions_with_ffupdate(acq.start_time, self.use_future_flat, 350)
        if not self.use_future_flat:
            ffupdate_acqs.reverse()
        flat_field_update_paths = []
        for acq_obj in ffupdate_acqs:
            if os.path.exists(acq_obj["products"]["l1b"]["ffupdate"]["img_path"]):
                flat_field_update_paths.append(acq_obj["products"]["l1b"]["ffupdate"]["img_path"])
        # If the number of paths is less than 100, then set to empty because we can't use them
        if len(flat_field_update_paths) < 100:
            flat_field_update_paths = []

        # Create runconfig
        runconfig = {
            "repository_dir": pge.repo_dir,
            "tmp_dir": tmp_dir,
            "level": self.level,
            "instrument_mode": instrument_mode,
            "raw_img_path": raw_img_path,
            "dark_img_path": dark_img_path,
            "l1b_config": l1b_config,
            "flat_field_update_paths": flat_field_update_paths,
            "rdn_img_path": tmp_rdn_img_path
        }
        with open(tmp_runconfig_path, "w") as f:
            f.write(json.dumps(runconfig, indent=4))

        emitrdn_wrapper_exe = os.path.join(pge.repo_dir, "emitrdn_wrapper.py")
        utils_path = os.path.join(pge.repo_dir, "utils")
        env = os.environ.copy()
        env["PYTHONPATH"] = f"$PYTHONPATH:{utils_path}"
        env["RAY_worker_register_timeout_seconds"] = "600"
        cmd = ["python", emitrdn_wrapper_exe, tmp_runconfig_path]
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Copy output files to l1b dir
        # Copy flat field update if it exists
        if os.path.exists(tmp_ffupdate_img_path):
            wm.copy(tmp_ffupdate_img_path, acq.ffupdate_img_path)
            wm.copy(envi_header(tmp_ffupdate_img_path), acq.ffupdate_hdr_path)
        # Copy the median flat field and the destriped radiance if they exist, otherwise copy the undestriped radiance
        if os.path.exists(tmp_ffmedian_img_path):
            wm.copy(tmp_rdndestripe_img_path, acq.rdn_img_path)
            wm.copy(envi_header(tmp_rdndestripe_img_path), acq.rdn_hdr_path)
            wm.copy(tmp_ffmedian_img_path, acq.ffmedian_img_path)
            wm.copy(envi_header(tmp_ffmedian_img_path), acq.ffmedian_hdr_path)
        else:
            wm.copy(tmp_rdn_img_path, acq.rdn_img_path)
            wm.copy(envi_header(tmp_rdn_img_path), acq.rdn_hdr_path)
        # Copy the rest of the files
        wm.copy(tmp_bandmask_img_path, acq.bandmask_img_path)
        wm.copy(envi_header(tmp_bandmask_img_path), acq.bandmask_hdr_path)
        wm.copy(tmp_runconfig_path, os.path.join(acq.l1b_data_dir, runconfig_name))
        wm.copy(tmp_log_path, os.path.join(acq.l1b_data_dir, os.path.basename(tmp_log_path)))

        # Update hdr files
        input_files["runconfig"] = tmp_runconfig_path
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        doc_version = "EMIT SDS L1B JPL-D 104187, Initial"
        hdr = envi.read_envi_header(acq.rdn_hdr_path)
        hdr["emit acquisition start time"] = acq.start_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit acquisition stop time"] = acq.stop_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit pge name"] = pge.repo_url
        hdr["emit pge version"] = pge.version_tag
        hdr["emit pge input files"] = input_files_arr
        hdr["emit pge run command"] = " ".join(cmd)
        hdr["emit software build version"] = wm.config["extended_build_num"]
        hdr["emit documentation version"] = doc_version
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.rdn_img_path), tz=datetime.timezone.utc)
        hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit data product version"] = wm.config["product_versions"]["l1b"]["processing_version"]
        hdr["emit acquisition daynight"] = acq.daynight
        if os.path.exists(tmp_ffmedian_img_path):
            hdr["emit flat field median-based destriping"] = 1
        else:
            hdr["emit flat field median-based destriping"] = 0
        hdr["data ignore value"] = -9999

        envi.write_envi_header(acq.rdn_hdr_path, hdr)

        # PGE writes metadata to db
        product_dict = {
            "img_path": acq.rdn_img_path,
            "hdr_path": acq.rdn_hdr_path,
            "created": creation_time,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.rdn": product_dict})

        product_dict_bm = {
            "img_path": acq.bandmask_img_path,
            "hdr_path": acq.bandmask_hdr_path,
            "created": creation_time
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.bandmask": product_dict_bm})

        if os.path.exists(acq.ffupdate_img_path):
            product_dict_ffupdate = {
                "img_path": acq.ffupdate_img_path,
                "hdr_path": acq.ffupdate_hdr_path,
                "created": creation_time,
            }
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.ffupdate": product_dict_ffupdate})

        if os.path.exists(acq.ffmedian_img_path):
            product_dict_ffmedian = {
                "img_path": acq.ffmedian_img_path,
                "hdr_path": acq.ffmedian_hdr_path,
                "created": creation_time,
            }
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.ffmedian": product_dict_ffmedian})

        # Check if orbit now has complete set of radiance files and update orbit metadata
        wm_orbit = WorkflowManager(config_path=self.config_path, orbit_id=acq.orbit)
        # Insert new orbit if it doesn't exist
        if wm_orbit.orbit is None and len(build_nums) > 1:
            compat_orbit = dm.find_orbit_by_id_and_product(acq.orbit, "products.l1a.uncorr_att_eph_path",
                                                           build_nums=build_nums)
            if compat_orbit:
                compat_orbit = wm.remove_keys_from_dict(["_id", "radiance_status"],  compat_orbit)
                compat_build_num = compat_orbit["build_num"]
                compat_orbit["build_num"] = wm.config["build_num"]
                # TODO: Remove processing version everywhere
                compat_orbit["product_versions"] = wm.config["product_versions"]
                compat_orbit["processing_log"] = []
                products = compat_orbit["products"]
                compat_orbit["products"] = {
                    "l1a": products["l1a"]
                }
                compat_orbit["copied_from"] = compat_build_num
                if not dm.find_orbit_by_id(acq.orbit):
                    dm.insert_orbit(compat_orbit)
                    wm.print(__name__, f"Inserted orbit from build {compat_build_num} as {compat_orbit}")
                    wm_orbit = WorkflowManager(config_path=self.config_path, orbit_id=acq.orbit)
            else:
                raise RuntimeError(f"Unable to find compatible orbit for {acq.orbit} using build_nums {build_nums}")

        orbit = wm_orbit.orbit
        if orbit.has_complete_radiance():
            dm.update_orbit_metadata(orbit.orbit_id, {"radiance_status": "complete"})
        else:
            dm.update_orbit_metadata(orbit.orbit_id, {"radiance_status": "incomplete"})

        # Add flat field paths here for database log entry
        if len(flat_field_update_paths) >= 100:
            input_files["flat_field_update_paths"] = flat_field_update_paths

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1b_rdn_img_path": acq.rdn_img_path,
                "l1b_rdn_hdr_path:": acq.rdn_hdr_path,
                "l1b_bandmask_img_path:": acq.bandmask_img_path,
                "l1b_bandmask_hdr_path:": acq.bandmask_hdr_path,
            }
        }
        if os.path.exists(acq.ffupdate_img_path):
            log_entry["output"]["l1b_ffupdate_img_path"] = acq.ffupdate_img_path
            log_entry["output"]["l1b_ffupdate_hdr_path"] = acq.ffupdate_hdr_path

        if os.path.exists(acq.ffmedian_img_path):
            log_entry["output"]["l1b_ffmedian_img_path"] = acq.ffmedian_img_path
            log_entry["output"]["l1b_ffmedian_hdr_path"] = acq.ffmedian_hdr_path

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L1BGeolocate(SlurmJobTask):
    """
    Performs geolocation using BAD telemetry and counter-OS time pair file
    :returns: Geolocation files including GLT, OBS, LOC, corrected attitude and ephemeris
    """

    config_path = luigi.Parameter()
    orbit_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    ignore_missing_radiance = luigi.BoolParameter(default=False)

    n_cores = 40
    memory = 180000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.orbit_id}")
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.orbit_id}")
        wm = WorkflowManager(config_path=self.config_path, orbit_id=self.orbit_id)
        return OrbitTarget(orbit=wm.orbit, task_family=self.task_family)
        return None

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, orbit_id=self.orbit_id)
        orbit = wm.orbit
        dm = wm.database_manager

        # Check for missing radiance files before proceeding. Override with --ignore_missing_radiance arg
        if self.ignore_missing_radiance is False and orbit.has_complete_radiance() is False:
            raise RuntimeError(f"Unable to run {self.task_family} on {self.orbit_id} due to missing radiance files in "
                               f"orbit.")

        # Get acquisitions in orbit (only science with at least 1 valid line, not dark) - radiance and line timestamps
        acquisitions_in_orbit = dm.find_acquisitions_by_orbit_id(orbit.orbit_id, "science", min_valid_lines=2)

        # Build input_files dictionary
        input_files = {
            "attitude_ephemeris_file": orbit.products["l1a"]["uncorr_att_eph_path"],
            "timestamp_radiance_pairs": []
        }
        for acq in acquisitions_in_orbit:
            try:
                line_timestamps_path = acq["products"]["l1a"]["raw_line_timestamps"]["txt_path"]
                rdn_img_path = acq["products"]["l1b"]["rdn"]["img_path"]
            except KeyError:
                wm.print(__name__, f"Could not find a radiance image path for {acq['acquisition_id']} in DB.")
                continue
            file_pair = {
                "timestamps_file": line_timestamps_path,
                "radiance_file": rdn_img_path
            }
            input_files["timestamp_radiance_pairs"].append(file_pair)

        # Build run command
        pge = wm.pges["emit-sds-l1b-geo"]
        l1b_geo_install_dir = wm.config["l1b_geo_install_dir"]
        l1b_geo_pge_exe = os.path.join(l1b_geo_install_dir, "l1b_geo_pge")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        wm.makedirs(tmp_output_dir)
        l1b_osp_dir = wm.config["l1b_geo_osp_dir"]

        tmp_input_files_path = os.path.join(tmp_output_dir, "l1b_geo_run_config.json")
        with open(tmp_input_files_path, "w") as f:
            f.write(json.dumps(input_files, indent=4))

        cmd = [l1b_geo_pge_exe, tmp_output_dir, l1b_osp_dir, tmp_input_files_path]
        pge.run(cmd, tmp_dir=self.tmp_dir, use_conda_run=False)

        # Get unique acquisitions_ids
        output_prods = [os.path.basename(path) for path in glob.glob(os.path.join(tmp_output_dir, "emit*img"))]
        output_prods.sort()
        acquisition_ids = list(set([prod.split("_")[0] for prod in output_prods]))
        acquisition_ids.sort()
        wm.print(__name__, f"Found acquisition ids: {acquisition_ids}")
        output_prods = {
            "l1b_glt_img_paths": [],
            "l1b_glt_hdr_paths": [],
            "l1b_loc_img_paths": [],
            "l1b_loc_hdr_paths": [],
            "l1b_obs_img_paths": [],
            "l1b_obs_hdr_paths": [],
            "l1b_rdn_kmz_paths": [],
            "l1b_rdn_png_paths": []
        }
        acq_prod_map = {}
        for id in acquisition_ids:
            wm_acq = WorkflowManager(config_path=self.config_path, acquisition_id=id)
            acq = wm_acq.acquisition
            # Find all tmp paths
            tmp_glt_img_path = glob.glob(os.path.join(tmp_output_dir, f"{id}*glt*img"))[0]
            tmp_glt_hdr_path = glob.glob(os.path.join(tmp_output_dir, f"{id}*glt*hdr"))[0]
            tmp_loc_img_path = glob.glob(os.path.join(tmp_output_dir, f"{id}*loc*img"))[0]
            tmp_loc_hdr_path = glob.glob(os.path.join(tmp_output_dir, f"{id}*loc*hdr"))[0]
            tmp_obs_img_path = glob.glob(os.path.join(tmp_output_dir, f"{id}*obs*img"))[0]
            tmp_obs_hdr_path = glob.glob(os.path.join(tmp_output_dir, f"{id}*obs*hdr"))[0]
            tmp_rdn_kmz_path = glob.glob(os.path.join(tmp_output_dir, f"{id}*rdn*kmz"))[0]
            # Get png paths and then identify non-projected vs projected
            tmp_rdn_png_paths = glob.glob(os.path.join(tmp_output_dir, f"{id}*rdn*png"))
            tmp_rdn_png_path = None
            tmp_rdn_proj_png_path = None
            for png_path in tmp_rdn_png_paths:
                if "_proj" in png_path:
                    tmp_rdn_proj_png_path = png_path
                else:
                    tmp_rdn_png_path = png_path

            # Copy tmp paths to /store
            wm.print(__name__, f"Copying {tmp_glt_img_path} to {acq.glt_img_path}")
            wm.copy(tmp_glt_img_path, acq.glt_img_path)
            wm.copy(tmp_glt_hdr_path, acq.glt_hdr_path)
            wm.copy(tmp_loc_img_path, acq.loc_img_path)
            wm.copy(tmp_loc_hdr_path, acq.loc_hdr_path)
            wm.copy(tmp_obs_img_path, acq.obs_img_path)
            wm.copy(tmp_obs_hdr_path, acq.obs_hdr_path)
            wm.copy(tmp_rdn_kmz_path, acq.rdn_kmz_path)
            wm.copy(tmp_rdn_png_path, acq.rdn_png_path)
            wm.copy(tmp_rdn_proj_png_path, acq.rdn_png_path.replace(".png", "_proj.png"))
            # Symlink from orbits l1b dir to acquisitions l1b dir
            wm.symlink(acq.glt_img_path, os.path.join(orbit.l1b_dir, os.path.basename(acq.glt_img_path)))
            wm.symlink(acq.glt_hdr_path, os.path.join(orbit.l1b_dir, os.path.basename(acq.glt_hdr_path)))
            wm.symlink(acq.loc_img_path, os.path.join(orbit.l1b_dir, os.path.basename(acq.loc_img_path)))
            wm.symlink(acq.loc_hdr_path, os.path.join(orbit.l1b_dir, os.path.basename(acq.loc_hdr_path)))
            wm.symlink(acq.obs_img_path, os.path.join(orbit.l1b_dir, os.path.basename(acq.obs_img_path)))
            wm.symlink(acq.obs_hdr_path, os.path.join(orbit.l1b_dir, os.path.basename(acq.obs_hdr_path)))
            wm.symlink(acq.rdn_kmz_path, os.path.join(orbit.l1b_dir, os.path.basename(acq.rdn_kmz_path)))
            wm.symlink(acq.rdn_png_path, os.path.join(orbit.l1b_dir, os.path.basename(acq.rdn_png_path)))
            # Keep track of output paths for processing log
            output_prods["l1b_glt_img_paths"].append(acq.glt_img_path)
            output_prods["l1b_glt_hdr_paths"].append(acq.glt_hdr_path)
            output_prods["l1b_loc_img_paths"].append(acq.loc_img_path)
            output_prods["l1b_loc_hdr_paths"].append(acq.loc_hdr_path)
            output_prods["l1b_obs_img_paths"].append(acq.loc_img_path)
            output_prods["l1b_obs_hdr_paths"].append(acq.loc_hdr_path)
            output_prods["l1b_rdn_kmz_paths"].append(acq.rdn_kmz_path)
            output_prods["l1b_rdn_png_paths"].append(acq.rdn_png_path)
            # Keep track of acquisition product map for products
            acq_prod_map[id] = {
                "glt": {
                    "img_path": acq.glt_img_path,
                    "hdr_path": acq.glt_hdr_path,
                    "created": datetime.datetime.fromtimestamp(os.path.getmtime(acq.glt_img_path),
                                                               tz=datetime.timezone.utc)
                },
                "loc": {
                    "img_path": acq.loc_img_path,
                    "hdr_path": acq.loc_hdr_path,
                    "created": datetime.datetime.fromtimestamp(os.path.getmtime(acq.loc_img_path),
                                                               tz=datetime.timezone.utc)
                },
                "obs": {
                    "img_path": acq.obs_img_path,
                    "hdr_path": acq.obs_hdr_path,
                    "created": datetime.datetime.fromtimestamp(os.path.getmtime(acq.obs_img_path),
                                                               tz=datetime.timezone.utc)
                },
                "rdn_kmz": {
                    "kmz_path": acq.rdn_kmz_path,
                    "created": datetime.datetime.fromtimestamp(os.path.getmtime(acq.rdn_kmz_path),
                                                               tz=datetime.timezone.utc)
                },
                "rdn_png": {
                    "png_path": acq.rdn_png_path,
                    "created": datetime.datetime.fromtimestamp(os.path.getmtime(acq.rdn_png_path),
                                                               tz=datetime.timezone.utc)
                }
            }

            # Update acquisition header files and DB
            # Update hdr files
            acq_input_files = {
                "attitude_ephemeris_file": orbit.products["l1a"]["uncorr_att_eph_path"],
                "timestamp_file": acq.products["l1a"]["raw_line_timestamps"]["txt_path"],
                "radiance_file": acq.products["l1b"]["rdn"]["img_path"]
            }
            input_files_arr = ["{}={}".format(key, value) for key, value in acq_input_files.items()]
            doc_version = "EMIT SDS L1B JPL-D 104187, Initial"
            for img_path, hdr_path in [(acq.glt_img_path, acq.glt_hdr_path),
                                       (acq.loc_img_path, acq.loc_hdr_path),
                                       (acq.obs_img_path, acq.obs_hdr_path)]:

                hdr = envi.read_envi_header(hdr_path)
                hdr["emit acquisition start time"] = acq.start_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
                hdr["emit acquisition stop time"] = acq.stop_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
                hdr["emit pge name"] = pge.repo_url
                hdr["emit pge version"] = pge.version_tag
                hdr["emit pge input files"] = input_files_arr
                hdr["emit pge run command"] = " ".join(cmd)
                hdr["emit software build version"] = wm.config["extended_build_num"]
                hdr["emit documentation version"] = doc_version
                if "_glt_" in img_path:
                    creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.glt_img_path),
                                                                    tz=datetime.timezone.utc)
                if "_loc_" in img_path:
                    creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.loc_img_path),
                                                                    tz=datetime.timezone.utc)
                if "_obs_" in img_path:
                    creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.obs_img_path),
                                                                    tz=datetime.timezone.utc)
                hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
                hdr["emit data product version"] = wm.config["product_versions"]["l1b"]["processing_version"]
                hdr["emit acquisition daynight"] = acq.daynight

                envi.write_envi_header(hdr_path, hdr)

            # Write product dict
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.glt": acq_prod_map[id]["glt"]})
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.loc": acq_prod_map[id]["loc"]})
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.obs": acq_prod_map[id]["obs"]})
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.rdn_kmz": acq_prod_map[id]["rdn_kmz"]})
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.rdn_png": acq_prod_map[id]["rdn_png"]})

            # Get additional attributes and add to DB
            glt_gring = get_gring_boundary_points(acq.glt_hdr_path)
            mean_solar_azimuth = get_band_mean(acq.obs_img_path, 3)
            mean_solar_zenith = get_band_mean(acq.obs_img_path, 4)
            meta = {
                "gring": glt_gring,
                "mean_solar_azimuth": mean_solar_azimuth,
                "mean_solar_zenith": mean_solar_zenith
            }
            dm.update_acquisition_metadata(acq.acquisition_id, meta)

        # Finish updating orbit level properties
        # Copy back corrected att/eph
        tmp_corr_att_eph_path = glob.glob(os.path.join(tmp_output_dir, "*l1b_att*nc"))[0]

        # Update attitude/ephemeris netcdf metadata before copy
        l1b_processing_version = wm.config["product_versions"]["l1b"]["processing_version"]
        ae_nc = netCDF4.Dataset(tmp_corr_att_eph_path, 'r+')
        daac_converter.makeGlobalAttrBase(ae_nc)
        ae_nc.title = f"EMIT L1B Corrected Spacecraft Attitude and Ephemeris V0{l1b_processing_version}"
        ae_nc.summary = ae_nc.summary + \
            f"\\n\\nThis collection contains L1B Corrected Spacecraft Attitude and Ephemeris (ATT). \
ATT contains the uncorrected Broadcast Ancillary Data (BAD) ephemeris and attitude quaternions \
from the ISS, and the data after correction by the geolocation process. \
This product is generated at the orbit level."
        ae_nc.time_coverage_start = orbit.start_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        ae_nc.time_coverage_end = orbit.stop_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        ae_nc.software_build_version = wm.config["extended_build_num"]
        ae_nc.product_version = "V0" + l1b_processing_version
        run_command = "PGE Run Command: {" + " ".join(cmd) + "}"
        nc_input_files = "PGE Input Files: {" + orbit.products["l1a"]["uncorr_att_eph_path"] + "}"
        ae_nc.history = run_command + ", " + nc_input_files

        ae_nc.sync()
        ae_nc.close()

        wm.copy(tmp_corr_att_eph_path, orbit.corr_att_eph_path)
        output_prods["l1b_corr_att_eph_path"] = orbit.corr_att_eph_path

        # Make work directory
        wm.makedirs(orbit.l1b_geo_work_dir)
        # Copy back geoqa path but use corr att/eph naming as basis
        tmp_geoqa_path = glob.glob(os.path.join(tmp_output_dir, "*_geoqa_*"))[0]
        geoqa_name = os.path.basename(orbit.corr_att_eph_path).replace("_att_", "_geoqa_")
        wm.copy(tmp_geoqa_path, os.path.join(orbit.l1b_geo_work_dir, geoqa_name))
        # Copy back remainder of work directory
        ancillary_workdir_paths = glob.glob(os.path.join(tmp_output_dir, "l1b_geo*"))
        ancillary_workdir_paths += glob.glob(os.path.join(tmp_output_dir, "map*"))
        ancillary_workdir_paths += glob.glob(os.path.join(tmp_output_dir, "extra*"))
        for path in ancillary_workdir_paths:
            wm.copy(path, os.path.join(orbit.l1b_geo_work_dir, os.path.basename(path)))

        # Update metadata and product dictionary
        corr_att_product_dict = {
            "nc_path": orbit.corr_att_eph_path,
            "created": datetime.datetime.fromtimestamp(os.path.getmtime(orbit.corr_att_eph_path),
                                                       tz=datetime.timezone.utc)
        }
        dm.update_orbit_metadata(orbit.orbit_id, {"products.l1b.corr_att_eph": corr_att_product_dict})
        dm.update_orbit_metadata(orbit.orbit_id, {"products.l1b.acquisitions": acq_prod_map})

        # Add processing log
        doc_version = "EMIT SDS L1B JPL-D 104187, Initial"
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": output_prods
        }
        dm.insert_orbit_log_entry(orbit.orbit_id, log_entry)


class L1BRdnFormat(SlurmJobTask):
    """
    Converts L1B (geolocation and radiance) to netcdf files
    :returns: L1B netcdf output for delivery
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    memory = 18000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=acq, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l1b"]

        output_generator_exe = os.path.join(pge.repo_dir, "output_conversion.py")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        wm.makedirs(tmp_output_dir)
        tmp_daac_rdn_nc_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l1b_rdn.nc")
        tmp_daac_obs_nc_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l1b_obs.nc")
        tmp_log_path = os.path.join(self.local_tmp_dir, "output_conversion_pge.log")
        l1b_processing_version = wm.config["product_versions"]["l1b"]["processing_version"]
        cmd = ["python", output_generator_exe, tmp_daac_rdn_nc_path, tmp_daac_obs_nc_path, acq.rdn_img_path, acq.obs_img_path, acq.loc_img_path,
               acq.glt_img_path, "V0" + str(l1b_processing_version), wm.config["extended_build_num"],
               "--log_file", tmp_log_path]
        # If we have the flat field update median file, then add it to NetCDF
        if os.path.exists(acq.ffmedian_img_path):
            cmd.extend(["--flat_field_update", acq.ffmedian_img_path])

        # Run this inside the emit-main conda environment to include emit-utils and other requirements
        main_pge = wm.pges["emit-main"]
        main_pge.run(cmd, tmp_dir=self.tmp_dir)

        # Copy and rename output files back to /store
        log_path = acq.rdn_nc_path.replace(".nc", "_nc_pge.log")
        wm.copy(tmp_daac_rdn_nc_path, acq.rdn_nc_path)
        wm.copy(tmp_daac_obs_nc_path, acq.obs_nc_path)
        wm.copy(tmp_log_path, log_path)

        # PGE writes metadata to db
        dm = wm.database_manager
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.rdn_nc_path), tz=datetime.timezone.utc)
        product_dict_netcdf = {
            "netcdf_rdn_path": acq.rdn_nc_path,
            "netcdf_obs_path": acq.obs_nc_path,
            "created": nc_creation_time
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.rdn_netcdf": product_dict_netcdf})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "rdn_img_path": acq.rdn_img_path,
                "obs_img_path": acq.obs_img_path,
                "loc_img_path": acq.loc_img_path,
                "glt_img_path": acq.glt_img_path
            },
            "pge_run_command": " ".join(cmd),
            "documentation_version": "TBD",
            "product_creation_time": nc_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1b_rdn_netcdf_path": acq.rdn_nc_path,
                "l1b_obs_netcdf_path": acq.obs_nc_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L1BRdnDeliver(SlurmJobTask):
    """
    Stages NetCDF and UMM-G files and submits notification to DAAC interface
    :returns: Staged L1B files
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    daac_ingest_queue = luigi.Parameter(default="forward")
    override_output = luigi.BoolParameter(default=False)

    memory = 18000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return L1BRdnFormat(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                            partition=self.partition)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")

        if self.override_output:
            return None

        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=acq, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-main"]

        # Get local SDS names
        ummg_path = acq.rdn_nc_path.replace(".nc", ".cmr.json")

        # Create local/tmp daac names and paths
        daac_rdn_nc_name = f"{acq.rdn_granule_ur}.nc"
        daac_obs_nc_name = f"{acq.obs_granule_ur}.nc"
        daac_browse_name = f"{acq.rdn_granule_ur}.png"
        daac_ummg_name = f"{acq.rdn_granule_ur}.cmr.json"
        daac_rdn_nc_path = os.path.join(self.tmp_dir, daac_rdn_nc_name)
        daac_obs_nc_path = os.path.join(self.tmp_dir, daac_obs_nc_name)
        daac_browse_path = os.path.join(self.tmp_dir, daac_browse_name)
        daac_ummg_path = os.path.join(self.tmp_dir, daac_ummg_name)

        # Copy files to tmp dir and rename
        wm.copy(acq.rdn_nc_path, daac_rdn_nc_path)
        wm.copy(acq.obs_nc_path, daac_obs_nc_path)
        wm.copy(acq.rdn_png_path, daac_browse_path)

        # Get the software_build_version (extended build num when product was created)
        hdr = envi.read_envi_header(acq.rdn_hdr_path)
        software_build_version = hdr["emit software build version"]

        # Create the UMM-G file
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.rdn_nc_path), tz=datetime.timezone.utc)
        daynight = "Day" if acq.submode == "science" else "Night"
        l1b_pge = wm.pges["emit-sds-l1b"]
        cloud_fraction = acq.cloud_fraction if "cloud_fraction" in acq.metadata else None
        ummg = daac_converter.initialize_ummg(acq.rdn_granule_ur, nc_creation_time, "EMITL1BRAD",
                                              acq.collection_version, acq.start_time,
                                              acq.stop_time, l1b_pge.repo_name, l1b_pge.version_tag,
                                              software_build_version=software_build_version,
                                              software_delivery_version=wm.config["extended_build_num"],
                                              doi=wm.config["dois"]["EMITL1BRAD"], orbit=int(acq.orbit),
                                              orbit_segment=int(acq.scene), scene=int(acq.daac_scene),
                                              solar_zenith=acq.mean_solar_zenith,
                                              solar_azimuth=acq.mean_solar_azimuth,
                                              cloud_fraction=cloud_fraction)

        ummg = daac_converter.add_data_files_ummg(ummg, [daac_rdn_nc_path, daac_obs_nc_path, daac_browse_path],
                                                  daynight, ["NETCDF-4", "NETCDF-4", "PNG"])
        # ummg = daac_converter.add_related_url(ummg, l1b_pge.repo_url, "DOWNLOAD SOFTWARE")
        ummg = daac_converter.add_boundary_ummg(ummg, acq.gring)
        daac_converter.dump_json(ummg, ummg_path)
        wm.change_group_ownership(ummg_path)

        # Copy ummg file to tmp dir and rename
        wm.copy(ummg_path, daac_ummg_path)

        # Copy files to S3 for staging
        for path in (daac_rdn_nc_path, daac_obs_nc_path, daac_browse_path, daac_ummg_path):
            cmd_aws_s3 = [wm.config["aws_cli_exe"], "s3", "cp", path, acq.aws_s3_uri_base, "--profile",
                          wm.config["aws_profile"]]
            pge.run(cmd_aws_s3, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = f"{acq.rdn_granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
        cnm_submission_path = os.path.join(acq.l1b_data_dir, cnm_submission_id + "_cnm.json")
        target_src_map = {
            daac_rdn_nc_name: os.path.basename(acq.rdn_nc_path),
            daac_obs_nc_name: os.path.basename(acq.obs_nc_path),
            daac_browse_name: os.path.basename(acq.rdn_png_path),
            daac_ummg_name: os.path.basename(ummg_path)
        }
        provider = wm.config["daac_provider_forward"]
        queue_url = wm.config["daac_submission_url_forward"]
        if self.daac_ingest_queue == "backward":
            provider = wm.config["daac_provider_backward"]
            queue_url = wm.config["daac_submission_url_backward"]
        notification = {
            "collection": "EMITL1BRAD",
            "provider": provider,
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": acq.rdn_granule_ur,
                "dataVersion": acq.collection_version,
                "files": [
                    {
                        "name": daac_rdn_nc_name,
                        "uri": acq.aws_s3_uri_base + daac_rdn_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_rdn_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_rdn_nc_path, "sha512")
                    },
                    {
                        "name": daac_obs_nc_name,
                        "uri": acq.aws_s3_uri_base + daac_obs_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_obs_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_obs_nc_path, "sha512")
                    },
                    {
                        "name": daac_browse_name,
                        "uri": acq.aws_s3_uri_base + daac_browse_name,
                        "type": "browse",
                        "size": os.path.getsize(daac_browse_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_browse_path, "sha512")
                    },
                    {
                        "name": daac_ummg_name,
                        "uri": acq.aws_s3_uri_base + daac_ummg_name,
                        "type": "metadata",
                        "size": os.path.getsize(daac_ummg_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_ummg_path, "sha512")
                    }
                ]
            }
        }

        # Write notification submission to file
        with open(cnm_submission_path, "w") as f:
            f.write(json.dumps(notification, indent=4))
        wm.change_group_ownership(cnm_submission_path)

        # Submit notification via AWS SQS
        cnm_submission_output = cnm_submission_path.replace(".json", ".out")
        cmd_aws = [wm.config["aws_cli_exe"], "sqs", "send-message", "--queue-url", queue_url, "--message-body",
                   f"file://{cnm_submission_path}", "--profile", wm.config["aws_profile"], ">", cnm_submission_output]
        pge.run(cmd_aws, tmp_dir=self.tmp_dir)
        wm.change_group_ownership(cnm_submission_output)
        cnm_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(cnm_submission_path),
                                                            tz=datetime.timezone.utc)

        # Record delivery details in DB for reconciliation report
        dm = wm.database_manager
        for file in notification["product"]["files"]:
            delivery_report = {
                "timestamp": utc_now,
                "extended_build_num": wm.config["extended_build_num"],
                "collection": notification["collection"],
                "collection_version": notification["product"]["dataVersion"],
                "granule_ur": acq.rdn_granule_ur,
                "sds_filename": target_src_map[file["name"]],
                "daac_filename": file["name"],
                "uri": file["uri"],
                "type": file["type"],
                "size": file["size"],
                "checksum": file["checksum"],
                "checksum_type": file["checksumType"],
                "submission_id": cnm_submission_id,
                "submission_queue": queue_url,
                "submission_status": "submitted"
            }
            dm.insert_granule_report(delivery_report)

        # Update db with log entry
        product_dict_ummg = {
            "ummg_json_path": ummg_path,
            "created": datetime.datetime.fromtimestamp(os.path.getmtime(ummg_path), tz=datetime.timezone.utc)
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.rdn_ummg": product_dict_ummg})

        if "rdn_daac_submissions" in acq.metadata["products"]["l1b"] and \
                acq.metadata["products"]["l1b"]["rdn_daac_submissions"] is not None:
            acq.metadata["products"]["l1b"]["rdn_daac_submissions"].append(cnm_submission_path)
        else:
            acq.metadata["products"]["l1b"]["rdn_daac_submissions"] = [cnm_submission_path]
        dm.update_acquisition_metadata(
            acq.acquisition_id,
            {"products.l1b.rdn_daac_submissions": acq.metadata["products"]["l1b"]["rdn_daac_submissions"]})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "rdn_netcdf_path": acq.rdn_nc_path,
                "obs_netcdf_path": acq.obs_nc_path,
                "rdn_png_path": acq.rdn_png_path
            },
            "pge_run_command": " ".join(cmd_aws),
            "documentation_version": "TBD",
            "product_creation_time": cnm_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1b_rdn_ummg_path:": ummg_path,
                "l1b_rdn_cnm_submission_path": cnm_submission_path
            }
        }
        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L1BAttDeliver(SlurmJobTask):
    """
    Stages NetCDF and UMM-G files and submits notification to DAAC interface
    :returns: Staged L1B files
    """

    config_path = luigi.Parameter()
    orbit_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    daac_ingest_queue = luigi.Parameter(default="forward")
    override_output = luigi.BoolParameter(default=False)

    memory = 18000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.orbit_id}")
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.orbit_id}")

        if self.override_output:
            return None

        wm = WorkflowManager(config_path=self.config_path, orbit_id=self.orbit_id)
        return OrbitTarget(orbit=wm.orbit, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, orbit_id=self.orbit_id)
        orbit = wm.orbit
        pge = wm.pges["emit-main"]

        # Get local SDS names
        nc_path = orbit.corr_att_eph_path
        ummg_path = nc_path.replace(".nc", ".cmr.json")

        # Update NetCDF with software_delivery_version and get software_build_version
        nc_ds = netCDF4.Dataset(nc_path, 'r+')
        software_build_version = nc_ds.software_build_version
        if 'software_delivery_version' in nc_ds.ncattrs() and nc_ds.software_delivery_version == wm.config["extended_build_num"]:
            logging.info('Skipping software_delivery_version assignment, because it already exists and matches')
        else:
            nc_ds.software_delivery_version = wm.config["extended_build_num"]
        nc_ds.sync()
        nc_ds.close()

        # Create local/tmp daac names and paths
        l1b_processing_version = wm.config["product_versions"]["l1b"]["processing_version"]
        collection_version = f"0{l1b_processing_version}"
        start_time_str = orbit.start_time.strftime("%Y%m%dT%H%M%S")
        granule_ur = f"EMIT_L1B_ATT_{collection_version}_{start_time_str}_{orbit.orbit_id}"
        daac_nc_name = f"{granule_ur}.nc"
        daac_ummg_name = f"{granule_ur}.cmr.json"
        daac_nc_path = os.path.join(self.tmp_dir, daac_nc_name)
        daac_ummg_path = os.path.join(self.tmp_dir, daac_ummg_name)

        # Copy files to tmp dir and rename
        wm.copy(nc_path, daac_nc_path)

        # Create the UMM-G file
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(nc_path), tz=datetime.timezone.utc)
        l1bgeo_pge = wm.pges["emit-sds-l1b-geo"]
        ummg = daac_converter.initialize_ummg(granule_ur, nc_creation_time, "EMITL1BATT", collection_version,
                                              orbit.start_time, orbit.stop_time, l1bgeo_pge.repo_name,
                                              l1bgeo_pge.version_tag,
                                              software_build_version=software_build_version,
                                              software_delivery_version=wm.config["extended_build_num"],
                                              doi=wm.config["dois"]["EMITL1BATT"], orbit=int(orbit.orbit_id))
        ummg = daac_converter.add_data_files_ummg(ummg, [daac_nc_path], "Both", ["NETCDF-4"])
        # ummg = daac_converter.add_related_url(ummg, l1bgeo_pge.repo_url, "DOWNLOAD SOFTWARE")

        daac_converter.dump_json(ummg, ummg_path)
        wm.change_group_ownership(ummg_path)

        # Copy ummg file to tmp dir and rename
        wm.copy(ummg_path, daac_ummg_path)

        # Copy files to S3 for staging
        for path in (daac_nc_path, daac_ummg_path):
            cmd_aws_s3 = [wm.config["aws_cli_exe"], "s3", "cp", path, orbit.aws_s3_uri_base, "--profile",
                          wm.config["aws_profile"]]
            pge.run(cmd_aws_s3, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = f"{granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
        cnm_submission_path = os.path.join(orbit.l1b_dir, cnm_submission_id + "_cnm.json")
        target_src_map = {
            daac_nc_name: os.path.basename(nc_path),
            daac_ummg_name: os.path.basename(ummg_path)
        }
        provider = wm.config["daac_provider_forward"]
        queue_url = wm.config["daac_submission_url_forward"]
        if self.daac_ingest_queue == "backward":
            provider = wm.config["daac_provider_backward"]
            queue_url = wm.config["daac_submission_url_backward"]
        notification = {
            "collection": "EMITL1BATT",
            "provider": provider,
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": granule_ur,
                "dataVersion": collection_version,
                "files": [
                    {
                        "name": daac_nc_name,
                        "uri": orbit.aws_s3_uri_base + daac_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_nc_path, "sha512")
                    },
                    {
                        "name": daac_ummg_name,
                        "uri": orbit.aws_s3_uri_base + daac_ummg_name,
                        "type": "metadata",
                        "size": os.path.getsize(daac_ummg_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_ummg_path, "sha512")
                    }
                ]
            }
        }

        # Write notification submission to file
        with open(cnm_submission_path, "w") as f:
            f.write(json.dumps(notification, indent=4))
        wm.change_group_ownership(cnm_submission_path)

        # Submit notification via AWS SQS
        cnm_submission_output = cnm_submission_path.replace(".json", ".out")
        cmd_aws = [wm.config["aws_cli_exe"], "sqs", "send-message", "--queue-url", queue_url, "--message-body",
                   f"file://{cnm_submission_path}", "--profile", wm.config["aws_profile"], ">", cnm_submission_output]
        pge.run(cmd_aws, tmp_dir=self.tmp_dir)
        wm.change_group_ownership(cnm_submission_output)
        cnm_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(cnm_submission_path),
                                                            tz=datetime.timezone.utc)

        # Record delivery details in DB for reconciliation report
        dm = wm.database_manager
        for file in notification["product"]["files"]:
            delivery_report = {
                "timestamp": utc_now,
                "extended_build_num": wm.config["extended_build_num"],
                "collection": notification["collection"],
                "collection_version": notification["product"]["dataVersion"],
                "granule_ur": granule_ur,
                "sds_filename": target_src_map[file["name"]],
                "daac_filename": file["name"],
                "uri": file["uri"],
                "type": file["type"],
                "size": file["size"],
                "checksum": file["checksum"],
                "checksum_type": file["checksumType"],
                "submission_id": cnm_submission_id,
                "submission_queue": queue_url,
                "submission_status": "submitted"
            }
            dm.insert_granule_report(delivery_report)

        # Update db with log entry
        product_dict_ummg = {
            "ummg_json_path": ummg_path,
            "created": datetime.datetime.fromtimestamp(os.path.getmtime(ummg_path), tz=datetime.timezone.utc)
        }
        dm.update_orbit_metadata(orbit.orbit_id, {"products.l1b.att_ummg": product_dict_ummg})

        if "att_daac_submissions" in orbit.metadata["products"]["l1b"] and \
                orbit.metadata["products"]["l1b"]["att_daac_submissions"] is not None:
            orbit.metadata["products"]["l1b"]["att_daac_submissions"].append(cnm_submission_path)
        else:
            orbit.metadata["products"]["l1b"]["att_daac_submissions"] = [cnm_submission_path]
        dm.update_orbit_metadata(
            orbit.orbit_id,
            {"products.l1b.att_daac_submissions": orbit.metadata["products"]["l1b"]["att_daac_submissions"]})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "netcdf_path": nc_path
            },
            "pge_run_command": " ".join(cmd_aws),
            "documentation_version": "TBD",
            "product_creation_time": cnm_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1b_att_ummg_path:": ummg_path,
                "l1b_att_cnm_submission_path": cnm_submission_path
            }
        }
        dm.insert_orbit_log_entry(self.orbit_id, log_entry)
