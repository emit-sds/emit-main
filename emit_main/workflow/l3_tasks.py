"""
This code contains tasks for executing EMIT Level 3 PGEs and helper utilities.

Author: Philip G. Brodrick, philip.brodrick@jpl.nasa.gov
"""

import datetime
import logging
import os
import time

import luigi
import spectral.io.envi as envi

from emit_main.workflow.output_targets import AcquisitionTarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l1b_tasks import L1BGeolocate
from emit_main.workflow.l2a_tasks import L2AMask, L2AReflectance
from emit_main.workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-main")


class L3Unmix(SlurmJobTask):
    """
    Creates L3 fractional cover estimates
    :returns: Fractional cover file and uncertainties
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    n_cores = 64
    memory = 360000

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return (L2AReflectance(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                               partition=self.partition),
                L2AMask(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                        partition=self.partition))

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        start_time = time.time()
        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["SpectralUnmixing"]

        # Build PGE commands for run_tetracorder_pge.sh
        unmix_exe = os.path.join(pge.repo_dir, "unmix.jl")
        endmember_key = "level_1"
        tmp_log_path = os.path.join(self.local_tmp_dir,
                                    os.path.basename(acq.cover_img_path).replace(".img", "_pge.log"))
        output_base = os.path.join(self.local_tmp_dir, "unmixing_output")

        # Set up environment variables
        env = os.environ.copy()
        env["PATH"] = "/store/shared/julia-1.9.1/bin:${PATH}"
        env["JULIA_DEPOT_PATH"] = "/store/shared/.julia_191_shared"
        env["JULIA_PROJECT"] = pge.repo_dir

        # Build command
        cmd_unmix = ['julia', '-p', str(self.n_cores), unmix_exe, acq.rfl_img_path, wm.config["unmixing_library"],
                     endmember_key, output_base, "--normalization", "brightness", "--mode", "sma-best",
                     "--n_mc", "20", "--reflectance_uncertainty_file", acq.rfluncert_img_path,
                     "--spectral_starting_column", "8", "--num_endmembers", "30", "--log_file", tmp_log_path]

        pge.run(cmd_unmix, tmp_dir=self.tmp_dir, env=env, use_conda_run=False)

        wm.copy(f'{output_base}_fractional_cover', acq.cover_img_path)
        wm.copy(f'{output_base}_fractional_cover.hdr', acq.cover_hdr_path)
        wm.copy(f'{output_base}_fractional_cover_uncertainty', acq.coveruncert_img_path)
        wm.copy(f'{output_base}_fractional_cover_uncertainty.hdr', acq.coveruncert_hdr_path)
        wm.copy(tmp_log_path, acq.cover_img_path.replace(".img", "_pge.log"))

        input_files = {
            "reflectance_file": acq.rfl_img_path,
            "reflectance_uncertainty_file": acq.rfluncert_img_path,
            "endmember_path": wm.config["unmixing_library"],
        }

        # Update hdr files
        for header_to_update in [acq.cover_hdr_path, acq.coveruncert_hdr_path]:
            input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
            doc_version = "EMIT SDS L3 JPL-D 104238, Rev A"  # \todo check
            hdr = envi.read_envi_header(header_to_update)
            hdr["emit acquisition start time"] = acq.start_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit acquisition stop time"] = acq.stop_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit pge name"] = pge.repo_url
            hdr["emit pge version"] = pge.version_tag
            hdr["emit pge input files"] = input_files_arr
            hdr["emit pge run command"] = " ".join(cmd_unmix)
            hdr["emit software build version"] = wm.config["extended_build_num"]
            hdr["emit documentation version"] = doc_version
            creation_time = datetime.datetime.fromtimestamp(
                os.path.getmtime(acq.cover_img_path), tz=datetime.timezone.utc)
            hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit data product version"] = wm.config["processing_version"]
            hdr["emit acquisition daynight"] = acq.daynight
            envi.write_envi_header(header_to_update, hdr)

        # PGE writes metadata to db
        dm = wm.database_manager
        product_dict_cover = {
            "img_path": acq.cover_img_path,
            "hdr_path": acq.cover_hdr_path,
            "created": creation_time,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l3.cover": product_dict_cover})

        product_dict_cover_uncert = {
            "img_path": acq.coveruncert_img_path,
            "hdr_path": acq.coveruncert_hdr_path,
            "created": creation_time,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l3.coveruncert": product_dict_cover_uncert})

        total_time = time.time() - start_time
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd_unmix),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "pge_runtime_seconds": total_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l3_cover_img_path": acq.cover_img_path,
                "l3_cover_hdr_path:": acq.cover_hdr_path,
                "l3_coveruncert_img_path": acq.coveruncert_img_path,
                "l3_coveruncert_hdr_path:": acq.coveruncert_hdr_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
