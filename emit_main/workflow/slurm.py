import datetime
import logging
import os
import pickle
import shutil
import subprocess
import time

import luigi

from emit_main.workflow.workflow_manager import WorkflowManager

logger = logging.getLogger("emit-main")


def _build_sbatch_script(tmp_dir, cmd, job_name, outfile, errfile, n_nodes, n_tasks, n_cores, memory):
    """Create shell script to submit to Slurm queue via `sbatch`

    Returns path to sbatch script

    """

    conda_exe = os.getenv("CONDA_EXE")
    conda_env = os.getenv("CONDA_DEFAULT_ENV")

    sbatch_template = """#!/bin/bash
#SBATCH -J {job_name}
#SBATCH --output={outfile}
#SBATCH --error={errfile}
#SBATCH -N{n_nodes}
#SBATCH -n{n_tasks}
#SBATCH --cpus-per-task={n_cores}
#SBATCH --mem={memory}
{conda_exe} run -n {conda_env} {cmd}
    """
    sbatch_script = os.path.join(tmp_dir, job_name+".sh")
    with open(sbatch_script, "w") as f:
        f.write(
            sbatch_template.format(
                cmd=cmd,
                job_name=job_name, 
                outfile=outfile, 
                errfile=errfile,
                n_nodes=n_nodes,
                n_tasks=n_tasks,
                n_cores=n_cores,
                memory=memory,
                conda_exe=conda_exe,
                conda_env=conda_env)
            )
    return sbatch_script


def _parse_squeue_state(squeue_out, job_id):
    """Parse "state" column from squeue output for given job_id

    Returns state for the *first* job matching job_id. Returns 'u' if
    `squeue` output is empty or job_id is not found.

    """

    invalid_job_str = "Invalid job id specified"
    if invalid_job_str in squeue_out:
        return "u"

    lines = squeue_out.split('\n')

    for line in lines:
        if "JOBID" in line:
            continue
        elif len(line.strip()) == 0:
            continue
        else:
            returned_id = line.split()[0]
            state = line.split()[4]
            logger.debug("Squeue for job %i returned ID: %s, State: %s" % (job_id, returned_id, state))
            return state

    return "u"


def _get_sbatch_errors(errfile):
    """Checks error file for errors and returns result

    Returns contents of error file.  Returns empty string if empty.

    """
    errors = ""
    if not os.path.exists(errfile):
        logger.info("No error file found at %s" % errfile)
    with open(errfile, "r") as f:
        errors = f.read()
    return errors


class SlurmJobTask(luigi.Task):

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter(default="")
    stream_path = luigi.Parameter(default="")

    n_nodes = luigi.IntParameter(default=1)
    n_tasks = luigi.IntParameter(default=1)
    n_cores = luigi.IntParameter(default=1)
    memory = luigi.IntParameter(default=4000)

    tmp_dir = ""
    task_tmp_id = ""

    def _dump(self, out_dir=''):
        """Dump instance to file."""
        with self.no_unpicklable_properties():
            self.job_file = os.path.join(out_dir, 'job-instance.pickle')
            logger.debug("Pickling to file: %s" % self.job_file)
            pickle.dump(self, open(self.job_file, "wb"), protocol=2)

    def _init_local(self):

        if len(self.acquisition_id) > 0:
            self.task_tmp_id = self.acquisition_id
        else:
            self.task_tmp_id = os.path.basename(self.stream_path)
        wm = WorkflowManager(self.config_path)
        # Create tmp folder
        base_tmp_dir = wm.scratch_tmp_dir
        timestamp = datetime.datetime.now().strftime("%Y%m%dt%H%M%S")
#        timestamp = datetime.datetime.now().strftime('%Y%m%dt%H%M%S_%f') # Use this for microseconds
        folder_name = self.task_tmp_id + "_" + self.task_family + "_" + timestamp

        for b,a in [(' ',''),('(','_'),(')','_'),(',','_'),('/','_')]:
          folder_name = folder_name.replace(b,a)
        self.tmp_dir = os.path.join(base_tmp_dir, folder_name)
#        max_filename_length = os.fstatvfs(0).f_namemax
#        self.tmp_dir = self.tmp_dir[:max_filename_length]
        logger.info("Created tmp dir: %s", self.tmp_dir)
        os.makedirs(self.tmp_dir)

        # If config file is relative path, copy config file to tmp dir
        if not self.config_path.startswith("/"):
            rel_config_dir = os.path.dirname(self.config_path)
            tmp_config_dir = os.path.join(self.tmp_dir, rel_config_dir)
            os.makedirs(tmp_config_dir)
            shutil.copy(self.config_path, tmp_config_dir)

        # Dump the code to be run into a pickle file
        logger.debug("Dumping pickled class")
        self._dump(self.tmp_dir)

    def _run_job(self):

        # Build an sbatch script  that will run slurm_runner.py on the directory we've specified
        cwd = os.getcwd()
        # Remove "test" directory if this is a unit test run
        if os.path.basename(cwd) == "test":
            cwd = os.path.dirname(cwd)
        runner_path = os.path.join(os.path.abspath(cwd), "slurm_runner.py")
        # enclose tmp_dir in quotes to protect from special escape chars
        job_str = 'python {0} "{1}"'.format(runner_path, self.tmp_dir)

        # Build sbatch script
        self.outfile = os.path.join(self.tmp_dir, 'job.out')
        self.errfile = os.path.join(self.tmp_dir, 'job.err')
        sbatch_script = _build_sbatch_script(self.tmp_dir, job_str, self.task_family, self.outfile, self.errfile,
                                             self.n_nodes, self.n_tasks, self.n_cores, self.memory)
        logger.debug('sbatch script: ' + sbatch_script)

        # Submit the job and grab job ID
        output = subprocess.check_output("sbatch " + sbatch_script, shell=True)
        self.job_id = int(output.decode("utf-8").split(" ")[-1])
        logger.info("%s %s submitted with job id %i" % (self.task_tmp_id, self.task_family, self.job_id))

        self._track_job()

        # Now delete the temporaries, if they're there.
       #if self.tmp_dir and os.path.exists(self.tmp_dir):
       #    logger.info('Removing temporary directory %s' % self.tmp_dir)
       #    shutil.rmtree(self.tmp_dir)

    def _track_job(self):
        while True:
            # Sleep for a little bit
#            time.sleep(random.randint(POLL_TIME_RANGE[0], POLL_TIME_RANGE[1]))
            time.sleep(30)

            # See what the job's up to
            logger.info("Checking status of job %i..." % self.job_id)
            squeue_out = subprocess.check_output(["squeue", "-j", str(self.job_id)]).decode("utf-8")
            logger.debug("squeue_out is\n %s" % squeue_out)
            slurm_status = _parse_squeue_state(squeue_out, self.job_id)
            if slurm_status == "PD":
                logger.info("%s %s with job id %i is PENDING..." % (self.task_tmp_id, self.task_family, self.job_id))
            if slurm_status == "R":
                logger.info("%s %s with job id %i is RUNNING..." % (self.task_tmp_id, self.task_family, self.job_id))
            if slurm_status == "S":
                logger.info("%s %s with job id %i is SUSPENDED..." % (self.task_tmp_id, self.task_family, self.job_id))
            if slurm_status == "u":
                errors = _get_sbatch_errors(self.errfile)
                # If no errors, then must be finished
                if not errors:
                    logger.info("%s %s with job id %i has COMPLETED WITH NO ERRORS " % (self.task_tmp_id, self.task_family, self.job_id))
                else: # then we have completed with errors
                    logger.info("%s %s with job id %i has COMPLETED WITH ERRORS/WARNINGS:\n%s" % (self.task_tmp_id, self.task_family, self.job_id, errors))
                break
            #TODO: Add the rest of the states from https://slurm.schedmd.com/squeue.html

    def run(self):

        wm = WorkflowManager(self.config_path)

        if wm.luigi_local_scheduler:
            # Run job locally without Slurm scheduler
            logger.debug("Running task locally: %s" % self.task_family)
            self._init_local()
            self.work()
        else:
            # Run the job
            logger.debug("Running task with Slurm: %s" % self.task_family)
            self._init_local()
            self._run_job()

    def work(self):
        """Override this method, rather than ``run()``,  for your actual work."""
        pass
