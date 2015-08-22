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
    max_ids = config['preprocessing'].getint('max_ids', fallback=None)
    batch_size = config['preprocessing'].getint('batch_size', fallback=None)
    strahler_limit = config['preprocessing'].getint('strahler_limit',
                                                    fallback=None)
    res = {sec_name: make_abs_paths(config, sec_name) for
           sec_name in config.sections()}
    res['catchment_model']['number_of_workers'] = int(
        res['catchment_model']['number_of_workers'])
    res['preprocessing']['max_ids'] = max_ids
    res['preprocessing']['batch_size'] = batch_size
    res['preprocessing']['strahler_limit'] = strahler_limit
    return res
