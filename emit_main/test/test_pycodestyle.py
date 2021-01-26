"""
This code contains a test function for PEP8 compliance

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os
import pycodestyle


def test_pep8_conformance():
    """
    Test that we conform to PEP8.
    """

    test_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(test_dir, "pycodestyle_config.txt")
    repo_dir = os.path.dirname(os.path.dirname(test_dir))

    print("\nRunning test_pep8_conformance with config: %s" % config_path)

    paths = []
    for root, dirs, files in os.walk(repo_dir):
        for file in files:
            if file.endswith(".py"):
                paths.append(os.path.join(root, file))

    # After acceptance, this will be uncommented
    pep8style = pycodestyle.StyleGuide(config_file=config_path, quiet=False)
    result = pep8style.check_files(paths)
    if result.total_errors != 0:
        print("Found PEP8 conformance error.")
        print("Please fix your style with autopep8.")
    assert result.total_errors == 0
