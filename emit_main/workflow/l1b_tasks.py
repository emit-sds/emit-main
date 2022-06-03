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

import luigi
import spectral.io.envi as envi
import netCDF4

from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.output_targets import AcquisitionTarget, OrbitTarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.slurm import SlurmJobTask
from emit_utils import daac_converter

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
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l1b"]

        # PGE writes to tmp folder
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        wm.makedirs(tmp_output_dir)
        tmp_rdn_img_path = os.path.join(tmp_output_dir, os.path.basename(acq.rdn_img_path))
        log_name = os.path.basename(acq.rdn_img_path.replace(".img", "_pge.log"))
        tmp_log_path = os.path.join(tmp_output_dir, log_name)
        l1b_config_path = wm.config["l1b_config_path"]
        with open(l1b_config_path, "r") as f:
            config = json.load(f)

        # Update config file values with absolute paths and store all input files for logging later
        input_files = {}
        for key, value in config.items():
            if "_file" in key:
                if not value.startswith("/"):
                    config[key] = os.path.abspath(os.path.join(os.path.dirname(l1b_config_path), value))
                input_files[key] = config[key]

        # Also update the nested "modes"
        if "modes" in config:
            for key, value in config["modes"].items():
                for k, v in value.items():
                    if "_file" in k:
                        if not v.startswith("/"):
                            config["modes"][key][k] = os.path.abspath(os.path.join(os.path.dirname(l1b_config_path), v))
                        input_files[k] = config["modes"][key][k]

        tmp_config_path = os.path.join(self.local_tmp_dir, "l1b_config.json")
        with open(tmp_config_path, "w") as outfile:
            json.dump(config, outfile)

        dm = wm.database_manager

        if len(self.dark_path) > 0:
            dark_img_path = self.dark_path
        else:
            # Find dark image - Get most recent dark image, but throw error if not within last 400 minutes
            recent_darks = dm.find_acquisitions_touching_date_range(
                "dark",
                "stop_time",
                acq.start_time - datetime.timedelta(minutes=400),
                acq.start_time,
                instrument_mode=acq.instrument_mode,
                min_valid_lines=256,
                sort=-1)
            if recent_darks is None or len(recent_darks) == 0:
                raise RuntimeError(f"Unable to find any darks for acquisition {acq.acquisition_id} within last 400 "
                                   f"minutes.")

            dark_img_path = recent_darks[0]["products"]["l1a"]["raw"]["img_path"]

        input_files["dark_file"] = dark_img_path

        emitrdn_exe = os.path.join(pge.repo_dir, "emitrdn.py")
        utils_path = os.path.join(pge.repo_dir, "utils")
        env = os.environ.copy()
        env["PYTHONPATH"] = f"$PYTHONPATH:{utils_path}"
        env["RAY_worker_register_timeout_seconds"]="600"
        instrument_mode = "default"
        if acq.instrument_mode == "cold_img_mid" or acq.instrument_mode == "cold_img_mid_vdda":
            instrument_mode = "half"
        cmd = ["python", emitrdn_exe,
               "--mode", instrument_mode,
               "--level", self.level,
               "--log_file", tmp_log_path,
               acq.raw_img_path,
               dark_img_path,
               tmp_config_path,
               tmp_rdn_img_path]
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Copy output files to l1b dir
        for file in glob.glob(os.path.join(tmp_output_dir, "*")):
            if file.endswith(".hdr"):
                wm.copy(file, acq.rdn_hdr_path)
            else:
                wm.copy(file, os.path.join(acq.l1b_data_dir, os.path.basename(file)))

        # Update hdr files
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
        hdr["emit data product version"] = wm.config["processing_version"]
        hdr["emit acquisition daynight"] = acq.daynight

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

        # Check if orbit now has complete set of radiance files and update orbit metadata
        wm_orbit = WorkflowManager(config_path=self.config_path, orbit_id=acq.orbit)
        orbit = wm_orbit.orbit
        if orbit.has_complete_radiance():
            dm.update_orbit_metadata(orbit.orbit_id, {"radiance_status": "complete"})
        else:
            dm.update_orbit_metadata(orbit.orbit_id, {"radiance_status": "incomplete"})

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
                "l1b_rdn_hdr_path:": acq.rdn_hdr_path
            }
        }

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
        acquisitions_in_orbit = dm.find_acquisitions_by_orbit_id(orbit.orbit_id, "science", min_valid_lines=1)

        # Build input_files dictionary
        input_files = {
            "attitude_ephemeris_file": orbit.uncorr_att_eph_path,
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
        output_prods = [os.path.basename(path) for path in glob.glob(os.path.join(tmp_output_dir, "emit*"))]
        output_prods.sort()
        acquisition_ids = set([prod.split("_")[0] for prod in output_prods])
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
            tmp_rdn_png_path = glob.glob(os.path.join(tmp_output_dir, f"{id}*rdn*png"))[0]
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
                "attitude_ephemeris_file": orbit.uncorr_att_eph_path,
                "timestamp_file": acq.raw_img_path.replace(".img", "_line_timestamps.txt"),
                "radiance_file": acq.rdn_img_path
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
                hdr["emit data product version"] = wm.config["processing_version"]
                hdr["emit acquisition daynight"] = acq.daynight

                envi.write_envi_header(hdr_path, hdr)

            # Write product dict
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.glt": acq_prod_map[id]["glt"]})
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.loc": acq_prod_map[id]["loc"]})
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.obs": acq_prod_map[id]["obs"]})
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.rdn_kmz": acq_prod_map[id]["rdn_kmz"]})
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.rdn_png": acq_prod_map[id]["rdn_png"]})

        # Finish updating orbit level properties
        # Copy back corrected att/eph
        tmp_corr_att_eph_path = glob.glob(os.path.join(tmp_output_dir, "*l1b_att*nc"))[0]

        # Update attitude/ephemeris netcdf metadata before copy
        ae_nc = netCDF4.Dataset(tmp_corr_att_eph_path, 'r+')
        daac_converter.makeGlobalAttrBase(ae_nc)
        ae_nc.title = "EMIT L1B Corrected Spacecraft Attitude and Ephemeris V001"
        ae_nc.summary = ae_nc.summary + \
            f"\\n\\nThis collection contains L1B Corrected Spacecraft Attitude and Ephemeris (ATT).\
            ATT contains the uncorrected Broadcast Ancillary Data (BAD) ephemeris and attitude quaternions \
            from the ISS, and the data after correction by the geolocation process. \
            This product is generated at the orbit level.\
            "
        ae_nc.product_version = wm.config["extended_build_num"]
        ae_nc.time_coverage_start = orbit.start_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        ae_nc.time_coverage_end = orbit.stop_time.strftime("%Y-%m-%dT%H:%M:%S%z")

        ae_nc.sync()
        ae_nc.close()

        wm.copy(tmp_corr_att_eph_path, orbit.corr_att_eph_path)

        # Copy back remainder of work directory
        wm.makedirs(orbit.l1b_geo_work_dir)
        ancillary_workdir_paths = glob.glob(os.path.join(tmp_output_dir, "l1b_geo*"))
        ancillary_workdir_paths += glob.glob(os.path.join(tmp_output_dir, "map*"))
        ancillary_workdir_paths += glob.glob(os.path.join(tmp_output_dir, "*_geoqa_*"))
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

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)

        return (L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                             partition=self.partition),
                L1BGeolocate(config_path=self.config_path, orbit_id=acq.orbit, level=self.level,
                             partition=self.partition))

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
        cmd = ["python", output_generator_exe, tmp_daac_rdn_nc_path, tmp_daac_obs_nc_path, acq.rdn_img_path, acq.obs_img_path, acq.loc_img_path,
               acq.glt_img_path, "V0" + str(wm.config["processing_version"]), "--log_file", tmp_log_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)

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

    task_namespace = "emit"
    n_cores = 1
    memory = 30000

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return L1BRdnFormat(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                            partition=self.partition)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
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

        # Create the UMM-G file
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.rdn_nc_path), tz=datetime.timezone.utc)
        daynight = "Day" if acq.submode == "science" else "Night"
        l1b_pge = wm.pges["emit-sds-l1b"]
        ummg = daac_converter.initialize_ummg(acq.rdn_granule_ur, nc_creation_time, "EMITL1BRAD",
                                              acq.collection_version, wm.config["extended_build_num"],
                                              l1b_pge.repo_name, l1b_pge.version_tag, cloud_fraction = acq.cloud_fraction)
        ummg = daac_converter.add_data_files_ummg(
            ummg,
            [daac_rdn_nc_path, daac_obs_nc_path, daac_browse_path],
            daynight,
            ["NETCDF-4", "NETCDF-4", "PNG"])
        ummg = daac_converter.add_related_url(ummg, l1b_pge.repo_url, "DOWNLOAD SOFTWARE")
        # TODO: replace w/ database read or read from L1B Geolocate PGE
        tmp_boundary_points_list = [[-118.53, 35.85], [-118.53, 35.659], [-118.397, 35.659], [-118.397, 35.85]]
        ummg = daac_converter.add_boundary_ummg(ummg, tmp_boundary_points_list)
        daac_converter.dump_json(ummg, ummg_path)
        wm.change_group_ownership(ummg_path)

        # Copy ummg file to tmp dir and rename
        wm.copy(ummg_path, daac_ummg_path)

        # Copy files to staging server
        partial_dir_arg = f"--partial-dir={acq.daac_partial_dir}"
        log_file_arg = f"--log-file={os.path.join(self.tmp_dir, 'rsync.log')}"
        target = f"{wm.config['daac_server_internal']}:{acq.daac_staging_dir}/"
        group = f"emit-{wm.config['environment']}" if wm.config["environment"] in ("test", "ops") else "emit-dev"
        # This command only makes the directory and changes ownership if the directory doesn't exist
        cmd_make_target = ["ssh", wm.config["daac_server_internal"], "\"if", "[", "!", "-d",
                           f"'{acq.daac_staging_dir}'", "];", "then", "mkdir", f"{acq.daac_staging_dir};", "chgrp",
                           group, f"{acq.daac_staging_dir};", "fi\""]
        pge.run(cmd_make_target, tmp_dir=self.tmp_dir)

        for path in (daac_rdn_nc_path, daac_obs_nc_path, daac_browse_path, daac_ummg_path):
            cmd_rsync = ["rsync", "-azv", partial_dir_arg, log_file_arg, path, target]
            pge.run(cmd_rsync, tmp_dir=self.tmp_dir)

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
        notification = {
            "collection": "EMITL1BRAD",
            "provider": wm.config["daac_provider"],
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": acq.rdn_granule_ur,
                "dataVersion": acq.collection_version,
                "files": [
                    {
                        "name": daac_rdn_nc_name,
                        "uri": acq.daac_uri_base + daac_rdn_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_rdn_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_rdn_nc_path, "sha512")
                    },
                    {
                        "name": daac_obs_nc_name,
                        "uri": acq.daac_uri_base + daac_obs_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_obs_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_obs_nc_path, "sha512")
                    },
                    {
                        "name": daac_browse_name,
                        "uri": acq.daac_uri_base + daac_browse_name,
                        "type": "browse",
                        "size": os.path.getsize(daac_browse_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_browse_path, "sha512")
                    },
                    {
                        "name": daac_ummg_name,
                        "uri": acq.daac_uri_base + daac_ummg_name,
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
        cmd_aws = [wm.config["aws_cli_exe"], "sqs", "send-message", "--queue-url", wm.config["daac_submission_url"], "--message-body",
                   f"file://{cnm_submission_path}", "--profile", wm.config["aws_profile"]]
        pge.run(cmd_aws, tmp_dir=self.tmp_dir)
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
                "sds_filename": target_src_map[file["name"]],
                "daac_filename": file["name"],
                "uri": file["uri"],
                "type": file["type"],
                "size": file["size"],
                "checksum": file["checksum"],
                "checksum_type": file["checksumType"],
                "submission_id": cnm_submission_id,
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

    task_namespace = "emit"
    n_cores = 1
    memory = 30000

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.orbit_id}")
        return L1BGeolocate(config_path=self.config_path, orbit_id=self.orbit_id, level=self.level,
                            partition=self.partition)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.orbit_id}")
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

        # Create local/tmp daac names and paths
        collection_version = f"0{wm.config['processing_version']}"
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
                                              wm.config["extended_build_num"], l1bgeo_pge.repo_name,
                                              l1bgeo_pge.version_tag)
        ummg = daac_converter.add_data_files_ummg(ummg, [daac_nc_path], "Both", ["NETCDF-4"])
        ummg = daac_converter.add_related_url(ummg, l1bgeo_pge.repo_url, "DOWNLOAD SOFTWARE")
        # TODO: Remove boundary for orbit?
        # TODO: replace w/ database read or read from L1B Geolocate PGE
        # tmp_boundary_points_list = [[-118.53, 35.85], [-118.53, 35.659], [-118.397, 35.659], [-118.397, 35.85]]
        # ummg = daac_converter.add_boundary_ummg(ummg, tmp_boundary_points_list)
        daac_converter.dump_json(ummg, ummg_path)
        wm.change_group_ownership(ummg_path)

        # Copy ummg file to tmp dir and rename
        wm.copy(ummg_path, daac_ummg_path)

        # Copy files to staging server
        partial_dir_arg = f"--partial-dir={orbit.daac_partial_dir}"
        log_file_arg = f"--log-file={os.path.join(self.tmp_dir, 'rsync.log')}"
        target = f"{wm.config['daac_server_internal']}:{orbit.daac_staging_dir}/"
        group = f"emit-{wm.config['environment']}" if wm.config["environment"] in ("test", "ops") else "emit-dev"
        # This command only makes the directory and changes ownership if the directory doesn't exist
        cmd_make_target = ["ssh", wm.config["daac_server_internal"], "\"if", "[", "!", "-d",
                           f"'{orbit.daac_staging_dir}'", "];", "then", "mkdir", f"{orbit.daac_staging_dir};", "chgrp",
                           group, f"{orbit.daac_staging_dir};", "fi\""]
        pge.run(cmd_make_target, tmp_dir=self.tmp_dir)

        for path in (daac_nc_path, daac_ummg_path):
            cmd_rsync = ["rsync", "-azv", partial_dir_arg, log_file_arg, path, target]
            pge.run(cmd_rsync, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = f"{granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
        cnm_submission_path = os.path.join(orbit.l1b_dir, cnm_submission_id + "_cnm.json")
        target_src_map = {
            daac_nc_name: os.path.basename(nc_path),
            daac_ummg_name: os.path.basename(ummg_path)
        }
        notification = {
            "collection": "EMITL1BATT",
            "provider": wm.config["daac_provider"],
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": granule_ur,
                "dataVersion": collection_version,
                "files": [
                    {
                        "name": daac_nc_name,
                        "uri": orbit.daac_uri_base + daac_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_nc_path, "sha512")
                    },
                    {
                        "name": daac_ummg_name,
                        "uri": orbit.daac_uri_base + daac_ummg_name,
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
        cmd_aws = [wm.config["aws_cli_exe"], "sqs", "send-message", "--queue-url", wm.config["daac_submission_url"], "--message-body",
                   f"file://{cnm_submission_path}", "--profile", wm.config["aws_profile"]]
        pge.run(cmd_aws, tmp_dir=self.tmp_dir)
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
                "sds_filename": target_src_map[file["name"]],
                "daac_filename": file["name"],
                "uri": file["uri"],
                "type": file["type"],
                "size": file["size"],
                "checksum": file["checksum"],
                "checksum_type": file["checksumType"],
                "submission_id": cnm_submission_id,
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
