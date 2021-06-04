"""
This code contains tasks for executing EMIT Level 2A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import os
import shutil

import luigi
import spectral.io.envi as envi

from emit_main.workflow.envi_target import ENVITarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l1b_tasks import L1BCalibrate, L1BGeolocate
from emit_main.workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-main")


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
        apply_oe_exe = os.path.join(wm.pges["isofit"].repo_dir, "isofit", "utils", "apply_oe.py")
        tmp_log_path = os.path.join(self.tmp_dir, "isofit.log")
        wavelength_path = wm.config["isofit_wavelength_path"]
        surface_path = wm.config["isofit_surface_path"]
        emulator_base = wm.config["isofit_emulator_base"]
        input_files = {
            "radiance_file": acq.rdn_img_path,
            "pixel_locations_file": acq.loc_img_path,
            "observation_parameters_file": acq.obs_img_path,
            "wavelength_file": wavelength_path,
            "surface_file": surface_path
        }
        cmd = ["python", apply_oe_exe, acq.rdn_img_path, acq.loc_img_path, acq.obs_img_path, self.tmp_dir, "emit",
               "--presolve=1", "--empirical_line=1", "--emulator_base=" + emulator_base,
               "--n_cores", "40",
               "--wavelength_path", wavelength_path,
               "--surface_path", surface_path,
               "--ray_temp_dir", "/tmp/ray-" + os.path.basename(self.tmp_dir),
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
        tmp_lbl_path = os.path.join(self.tmp_dir, "output", self.acquisition_id + "_lbl")
        tmp_lbl_hdr_path = tmp_lbl_path + ".hdr"
        tmp_statesubs_path = os.path.join(self.tmp_dir, "output", self.acquisition_id + "_subs_state")
        tmp_statesubs_hdr_path = tmp_statesubs_path + ".hdr"
        shutil.copy2(tmp_rfl_path, acq.rfl_img_path)
        shutil.copy2(tmp_rfl_hdr_path, acq.rfl_hdr_path)
        shutil.copy2(tmp_uncert_path, acq.uncert_img_path)
        shutil.copy2(tmp_uncert_hdr_path, acq.uncert_hdr_path)
        shutil.copy2(tmp_lbl_path, acq.lbl_img_path)
        shutil.copy2(tmp_lbl_hdr_path, acq.lbl_hdr_path)
        shutil.copy2(tmp_statesubs_path, acq.statesubs_img_path)
        shutil.copy2(tmp_statesubs_hdr_path, acq.statesubs_hdr_path)
        # TODO: Remove symlinks when possible
        os.symlink(acq.rfl_hdr_path, acq.rfl_img_path + ".hdr")
        os.symlink(acq.uncert_hdr_path, acq.uncert_img_path + ".hdr")
        # Copy log file and rename
        log_path = acq.rfl_img_path.replace(".img", "_pge.log")
        shutil.copy2(tmp_log_path, log_path)

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        doc_version = "EMIT SDS L2A JPL-D 104236, Rev B"
        dm = wm.database_manager
        for img_path, hdr_path in [(acq.rfl_img_path, acq.rfl_hdr_path), (acq.uncert_img_path, acq.uncert_hdr_path)]:
            hdr = envi.read_envi_header(hdr_path)
            hdr["emit pge name"] = pge.repo_url
            hdr["emit pge version"] = pge.version_tag
            hdr["emit pge input files"] = input_files_arr
            hdr["emit pge run command"] = " ".join(cmd)
            hdr["emit software build version"] = wm.config["build_num"]
            hdr["emit documentation version"] = doc_version
            creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(img_path))
            hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S")
            hdr["emit data product version"] = wm.config["processing_version"]
            envi.write_envi_header(hdr_path, hdr)

            # Update product dictionary in DB
            product_dict = {
                "img_path": img_path,
                "hdr_path": hdr_path,
                "dimensions": {
                    "lines": hdr["lines"],
                    "samples": hdr["samples"],
                    "bands": hdr["bands"]
                }
            }
            if "_rfl_" in img_path:
                dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2a.rfl": product_dict})
            elif "_uncert_" in img_path:
                dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2a.uncert": product_dict})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(),
            "completion_status": "SUCCESS",
            "output": {
                "l2a_rfl_img_path": acq.rfl_img_path,
                "l2a_rfl_hdr_path:": acq.rfl_hdr_path,
                "l2a_uncert_img_path": acq.uncert_img_path,
                "l2a_uncert_hdr_path:": acq.uncert_hdr_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L2AMask(SlurmJobTask):
    """
    Creates masks
    :returns: Mask file
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return (L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id),
                L2AReflectance(config_path=self.config_path, acquisition_id=self.acquisition_id))

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return ENVITarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l2a"]

        # Build PGE commands for apply_glt.py
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        os.makedirs(tmp_output_dir)
        tmp_rdnort_path = os.path.join(tmp_output_dir, os.path.basename(acq.rdnort_img_path))
        tmp_locort_path = os.path.join(tmp_output_dir, os.path.basename(acq.locort_img_path))
        tmp_lblort_path = os.path.join(tmp_output_dir, os.path.basename(acq.lblort_img_path))
        apply_glt_exe = os.path.join(pge.repo_dir, "apply_glt.py")

        cmd_rdn = ["python", apply_glt_exe, acq.rdn_img_path, acq.glt_img_path, tmp_rdnort_path]
        cmd_loc = ["python", apply_glt_exe, acq.loc_img_path, acq.glt_img_path, tmp_locort_path]
        cmd_lbl = ["python", apply_glt_exe, acq.lbl_img_path, acq.glt_img_path, tmp_lblort_path]
        pge.run(cmd_rdn, tmp_dir=self.tmp_dir)
        pge.run(cmd_loc, tmp_dir=self.tmp_dir)
        pge.run(cmd_lbl, tmp_dir=self.tmp_dir)

        # Build PGE command for make_masks.py
        tmp_rho_path = os.path.join(tmp_output_dir, self.acquisition_id + "_rho")
        tmp_mask_path = os.path.join(tmp_output_dir, os.path.basename(acq.mask_img_path))
        tmp_mask_hdr_path = tmp_mask_path + ".hdr"
        solar_irradiance_path = os.path.join(pge.repo_dir, "data", "kurudz_0.1nm.dat")
        make_masks_exe = os.path.join(pge.repo_dir, "make_emit_masks.py")
        input_files = {
            "ortho_radiance_file": tmp_rdnort_path,
            "ortho_pixel_locations_file": tmp_locort_path,
            "ortho_subset_labels_file": tmp_lblort_path,
            "state_subset_file": acq.statesubs_img_path,
            "solar_irradiance_file": solar_irradiance_path

        }
        cmd = ["python", make_masks_exe, tmp_rdnort_path, tmp_locort_path, tmp_lblort_path, acq.statesubs_img_path,
               solar_irradiance_path, tmp_rho_path, tmp_mask_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)

        # Copy mask files to l2a dir
        shutil.copy2(tmp_mask_path, acq.mask_img_path)
        shutil.copy2(tmp_mask_hdr_path, acq.mask_hdr_path)

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        doc_version = "EMIT SDS L2A JPL-D 104236, Rev B"
        hdr = envi.read_envi_header(acq.mask_hdr_path)
        hdr["emit pge name"] = pge.repo_url
        hdr["emit pge version"] = pge.version_tag
        hdr["emit pge input files"] = input_files_arr
        hdr["emit pge run command"] = " ".join(cmd)
        hdr["emit software build version"] = wm.config["build_num"]
        hdr["emit documentation version"] = doc_version
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.mask_img_path))
        hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S")
        hdr["emit data product version"] = wm.config["processing_version"]
        envi.write_envi_header(acq.mask_hdr_path, hdr)

        # PGE writes metadata to db
        dm = wm.database_manager
        product_dict = {
            "img_path": acq.mask_img_path,
            "hdr_path": acq.mask_hdr_path,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2a.mask": product_dict})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(),
            "completion_status": "SUCCESS",
            "output": {
                "l2a_mask_img_path": acq.mask_img_path,
                "l2a_mask_hdr_path:": acq.mask_hdr_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
