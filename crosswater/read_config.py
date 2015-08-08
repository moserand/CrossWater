# coding: utf-8

"""Read the config.ini.
"""

import configparser
import os


def get_file_content(file_name):
    with open(file_name) as fobj:
        try:
            return fobj.read()
        except UnicodeDecodeError:
            print(file_name)
            raise


def read_config(file_name_or_fobj):
    """Read the config file
    """
    config = configparser.ConfigParser()
    if isinstance(file_name_or_fobj, str):
        config.read(file_name_or_fobj)
    else:
        config.read_string(file_name_or_fobj.read())
    base_path = os.getcwd()

    preprocessing = dict(config['preprocessing'].items())
    for key, value in preprocessing.items():
        if key.endswith('_path') and not os.path.isabs(value):
            preprocessing[key] = os.path.normpath(os.path.join(base_path,
                                                               value))
    return {'preprocessing': preprocessing}

