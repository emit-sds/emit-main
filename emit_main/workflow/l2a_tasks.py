"""
This code contains tasks for executing EMIT Level 2A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os
import shutil

import luigi

from emit_main.workflow.envi_target import ENVITarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l1b_tasks import L1BCalibrate, L1BGeolocate
from emit_main.workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-main")


# TODO: Full implementation TBD
class L2AReflectance(SlurmJobTask):
    """
    Performs atmospheric correction on radiance
    :returns: Surface reflectance and uncertainties
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()

    n_cores = 40
    memory = 180000
    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id)

        # TODO: Add L1BGeolocate(config_path=self.config_path, acquisition_id=self.acquisition_id) when ready

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return ENVITarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l2a"]

        # Build PGE cmd
        tmp_log_path = os.path.join(self.tmp_dir, "isofit.log")
        wavelength_path = os.path.join(pge.repo_dir, "surface", "emit_wavelengths.txt")
        wavelength_path = "/home/brodrick/src/isofit/examples/20171108_Pasadena/remote/20170320_ang20170228_wavelength_fit.txt" 
        surface_path = os.path.join(pge.repo_dir, "surface", "EMIT_surface_test.mat")
        surface_path = "/beegfs/scratch/brodrick/emit/sonoran_desert/support/basic_surface.mat" 
        apply_oe_exe = os.path.join(wm.pges["isofit"].repo_dir, "isofit", "utils", "apply_oe.py")
        emulator_base = "/shared/sRTMnet_v100"
        cmd = ["python", apply_oe_exe, acq.rdn_img_path, acq.loc_img_path, acq.obs_img_path, self.tmp_dir, "ang",
               "--presolve=1", "--empirical_line=1", "--emulator_base=" + emulator_base,
               "--n_cores", "40",
               "--wavelength_path", wavelength_path,
               "--surface_path", surface_path,
               "--log_file", tmp_log_path]

        env = os.environ.copy()
        env["SIXS_DIR"] = "/shared/sixs"
        env["EMULATOR_DIR"] = emulator_base
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Copy output files to l2a dir and rename
        tmp_rfl_path = os.path.join(self.tmp_dir, "output", self.acquisition_id + "_rfl")
        tmp_rfl_hdr_path = tmp_rfl_path + ".hdr"
        tmp_uncert_path = os.path.join(self.tmp_dir, "output", self.acquisition_id + "_uncert")
        tmp_uncert_hdr_path = tmp_uncert_path + ".hdr"
        shutil.copy2(tmp_rfl_path, acq.rfl_img_path)
        shutil.copy2(tmp_rfl_hdr_path, acq.rfl_hdr_path)
        shutil.copy2(tmp_uncert_path, acq.uncert_img_path)
        shutil.copy2(tmp_uncert_hdr_path, acq.uncert_hdr_path)
        # Copy log file and rename
        log_path = acq.rfl_img_path.replace(".img", "_pge.log")
        shutil.copy2(tmp_log_path, log_path)
