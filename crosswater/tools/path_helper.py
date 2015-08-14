"""Tools for working with paths.
"""

import os


class ChDir(object):
    """Context manager for changing into a dir and back.
    """
    def __init__(self, new_path):
        self.new_path = new_path

    def __enter__(self):
        self.old_path = os.getcwd()
        os.chdir(self.new_path)

    def __exit__(self, exc_type, exc_value, traceback):
        os.chdir(self.old_path)
