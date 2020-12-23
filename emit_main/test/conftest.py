"""
This config file allows pytest to pass arguments into tests

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

from pytest import fixture


def pytest_addoption(parser):
    parser.addoption(
        "--config_path",
        action="store"
    )


@fixture()
def config_path(request):
    return request.config.getoption("--config_path")