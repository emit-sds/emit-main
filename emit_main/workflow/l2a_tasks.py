"""
This code contains tasks for executing EMIT Level 2A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging

import luigi

from emit_main.workflow.acquisition import Acquisition
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
        return (L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id),
                L1BGeolocate(config_path=self.config_path, acquisition_id=self.acquisition_id))

    def output(self):

        logger.debug(self.task_family + " output")
        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return (ENVITarget(acq.rfl_img_path),
                ENVITarget(acq.uncert_img_path),
                ENVITarget(acq.mask_img_path))

    def work(self):

        logger.debug(self.task_family + " run")
        logger.debug("task tmp_dir is %s" % self.tmp_dir)

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["isofit"]

        cmd = ["python", pge.apply_oe_exe, acq.rdn_img_path, acq.loc_img_path, acq.obs_img_path, self.tmp_dir, "ang",
               "--presolve=0", "--empirical_line=1", "--copy_input_files=0"
               "--surface_path=/beegfs/scratch/brodrick/emit/sonoran_desert/support/basic_surface.mat",
               "--log_file=" + self.tmp_dir + "/isofit.log",
               "--n_cores=40",
               "--channelized_uncertainty=/home/brodrick/src/isofit/data/avirisng_systematic_error.txt",
               "--wavelength_path=/home/brodrick/src/isofit/examples/20171108_Pasadena/remote/20170320_ang20170228_wavelength_fit.txt",
               "--lut_config_file=/beegfs/scratch/winstono/emit/sonoran_desert/support/lut_config.json"]
        pge.run(cmd, tmp_dir=self.tmp_dir)