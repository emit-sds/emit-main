"""
This code overrides the luigi.LocalTarget class and returns true if both and ENVI file and its .hdr file exist

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import luigi

class ENVITarget(luigi.LocalTarget):
    def exists(self):
        """
        Returns ``True`` if the both the path for this FileSystemTarget exists along with its associated .hdr file;
        ``False`` otherwise.
        """
        path = self.path
        if '*' in path or '?' in path or '[' in path or '{' in path:
            logger.warning("Using wildcards in path %s might lead to processing of an incomplete dataset; "
                           "override exists() to suppress the warning.", path)
        return self.fs.exists(path) and self.fs.exists(path.replace("img", "hdr"))