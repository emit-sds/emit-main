"""
This code contains the Acquisition class that manages acquisitions and their metadata

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""


class Acquisition:

    def __init__(self, acquisition_id, metadata=None):
        """
        :param acquisition_id: The name of the acquisition with timestamp (eg. "emit20200519t140035")
        """

        self._id = acquisition_id

        # TODO: Define and initialize acquisition metadata

        if metadata is not None:
            self.__dict__.update(metadata)
