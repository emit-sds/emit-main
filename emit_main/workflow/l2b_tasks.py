"""
This code contains tasks for executing EMIT Level 2B PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import os

import luigi
import spectral.io.envi as envi

from emit_main.workflow.envi_target import ENVITarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l1b_tasks import L1BGeolocate
from emit_main.workflow.l2a_tasks import L2AMask, L2AReflectance
from emit_main.workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-main")


class L2BAbundance(SlurmJobTask):
    """
    Creates L2B mineral spectral abundance estimate file
    :returns: Mineral abundance file and uncertainties
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    memory = 30000
    local_tmp_space = 125000

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
        return ENVITarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l2b"]

        # Build PGE commands for run_tetracorder_pge.sh
        run_tetra_exe = os.path.join(pge.repo_dir, "run_tetracorder_pge.sh")
        cmd_tetra = [run_tetra_exe, self.local_tmp_dir, acq.rfl_img_path]
        env = os.environ.copy()
        env["SP_LOCAL"] = wm.config["specpr_path"]
        env["SP_BIN"] = "${SP_LOCAL}/bin"
        env["TETRA"] = wm.config["tetracorder_path"]
        env["PATH"] = "${PATH}:${SP_LOCAL}/bin:${TETRA}/bin:/usr/bin"
        pge.run(cmd_tetra, tmp_dir=self.tmp_dir, env=env)

        # Build aggregator cmd
        aggregator_exe = os.path.join(pge.repo_dir, "aggregator.py")
        tetra_out_dir = os.path.join(self.local_tmp_dir, "tetra_out")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        os.makedirs(tmp_output_dir)
        tmp_abun_path = os.path.join(tmp_output_dir, os.path.basename(acq.abun_img_path))
        tmp_abun_hdr_path = tmp_abun_path + ".hdr"
        input_files = {
            "reflectance_file": acq.rfl_img_path,
            "reflectance_uncertainty_file": acq.uncert_img_path
        }
        cmd = ["python", aggregator_exe, tetra_out_dir, tmp_abun_path,
               "-calculate_uncertainty", "0",
               "-reflectance_file", acq.rfl_img_path,
               "-reflectance_uncertainty_file", acq.uncert_img_path]
        pge.run(cmd, cwd=pge.repo_dir, tmp_dir=self.tmp_dir)

        # Copy mask files to l2a dir
        wm.copy(tmp_abun_path, acq.abun_img_path)
        wm.copy(tmp_abun_hdr_path, acq.abun_hdr_path)

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        doc_version = "EMIT SDS L2B JPL-D 104237, Rev A"
        hdr = envi.read_envi_header(acq.abun_hdr_path)
        hdr["emit acquisition start time"] = acq.start_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit acquisition stop time"] = acq.stop_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit pge name"] = pge.repo_url
        hdr["emit pge version"] = pge.version_tag
        hdr["emit pge input files"] = input_files_arr
        hdr["emit pge run command"] = " ".join(cmd)
        hdr["emit software build version"] = wm.config["build_num"]
        hdr["emit documentation version"] = doc_version
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.abun_img_path), tz=datetime.timezone.utc)
        hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit data product version"] = wm.config["processing_version"]
        envi.write_envi_header(acq.abun_hdr_path, hdr)

        # PGE writes metadata to db
        dm = wm.database_manager
        product_dict = {
            "img_path": acq.abun_img_path,
            "hdr_path": acq.abun_hdr_path,
            "created": creation_time,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2b.abun": product_dict})

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
                "l2b_abun_img_path": acq.abun_img_path,
                "l2b_abun_hdr_path:": acq.abun_hdr_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
