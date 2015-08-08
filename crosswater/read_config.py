# coding: utf-8

"""Read the config.ini.
"""

import configparser
from pathlib import Path


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
    base_path = Path().absolute().cwd()

    preprocessing = dict(config['preprocessing'].items())
    for key, value in preprocessing.items():
        if key.endswith('_path') and not Path(value).is_absolute():
            preprocessing[key] = str((base_path / Path(value)).resolve())
    return {'preprocessing': preprocessing}

