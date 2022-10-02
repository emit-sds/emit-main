"""
This code contains tasks for executing EMIT Methane Detection.

Authors: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
         Philip G. Brodrick, philip.brodrick@jpl.nasa.gov
         John Chapman, john.chapman@jpl.nasa.gov
         Andrew Thorpe, andrew.thorpe@jpl.nasa.gov
"""

import datetime
import glob
import json
import logging
import os

import luigi
import spectral.io.envi as envi

from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.output_targets import AcquisitionTarget, OrbitTarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.l1b_tasks import L1BCalibrate, L1BGeolocate


class GHG(SlurmJobTask):
    """
    Performs greenhouse gas mapping on the EMIT SDS
    :returns: Matched filter form greenhouse gas estimation
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

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        return (L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                             partition=self.partition),
                L1BGeolocate(config_path=self.config_path, orbit_id=acq.orbit, level=self.level,
                             partition=self.partition))

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-methane"]

        # PGE writes to tmp folder
        tmp_output_dir = os.path.join(self.local_tmp_dir, "ghg")
        wm.makedirs(tmp_output_dir)

        env = os.environ.copy()

        # Generate dynamic unit absorption sepctrum for the scene (set for both methane and CO2)
        surf_cmd = ["python" ]
        pge.run(surf_cmd, tmp_dir=self.tmp_dir, env=env)

        surf_cmd = ["python" ]
        pge.run(surf_cmd, tmp_dir=self.tmp_dir, env=env)

        # Run matched filter
        surf_cmd = ["python" ]
        pge.run(surf_cmd, tmp_dir=self.tmp_dir, env=env)

        surf_cmd = ["python" ]
        pge.run(surf_cmd, tmp_dir=self.tmp_dir, env=env)

        # Run local surface / masking
        surf_cmd = ["python" ]
        pge.run(surf_cmd, tmp_dir=self.tmp_dir, env=env)

        surf_cmd = ["python" ]
        pge.run(surf_cmd, tmp_dir=self.tmp_dir, env=env)

        # Build KMZs and scaled Geotiffs
        surf_cmd = ["python" ]
        pge.run(surf_cmd, tmp_dir=self.tmp_dir, env=env)

        surf_cmd = ["python" ]
        pge.run(surf_cmd, tmp_dir=self.tmp_dir, env=env)


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
        env["RAY_worker_register_timeout_seconds"] = "600"
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