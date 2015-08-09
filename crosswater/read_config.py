# coding: utf-8

"""Read the config.ini.
"""

import configparser
import os


def make_abs_paths(config, section_name):
    """Make all paths absoulte and turn section into dict.
    """
    sec_dict = dict(config[section_name].items())
    base_path = os.getcwd()
    for key, value in sec_dict.items():
        if key.endswith('_path') and not os.path.isabs(value):
            sec_dict[key] = os.path.normpath(os.path.join(base_path, value))
    return sec_dict


def read_config(file_name_or_fobj):
    """Read the config file
    """
    config = configparser.ConfigParser()
    if isinstance(file_name_or_fobj, str):
        config.read(file_name_or_fobj)
    else:
        config.read_string(file_name_or_fobj.read())
    res = {sec_name: make_abs_paths(config, sec_name) for
           sec_name in config.sections()}
    res['catchment_model']['number_of_workers'] = int(
        res['catchment_model']['number_of_workers'])
    return res
