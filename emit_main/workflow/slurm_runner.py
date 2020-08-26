try:
    import cPickle as pickle
except ImportError:
    import pickle

import os
import sys

from emit_main.workflow.l2a_tasks import *

def _do_work_on_compute_node(work_dir):

    # Open up the pickle file with the work to be done
    os.chdir(work_dir)
    sys.path.insert(0,work_dir)
    print(sys.path)
    with open("job-instance.pickle", "rb") as f:
        job = pickle.load(f)

    # Do the work contained
    job.work()

def main(args=sys.argv):
    """Run the work() method from the class instance in the file "job-instance.pickle".
    """
    try:
        # Set up logging.
        logging.basicConfig(level=logging.WARN)
        work_dir = sys.argv[1]
        assert os.path.exists(work_dir), "First argument to sge_runner.py must be a directory that exists"
        _do_work_on_compute_node(work_dir)
    except Exception as e:
        # Dump encoded data that we will try to fetch using mechanize
        print(e)
        raise

if __name__ == '__main__':
    main()
