from io import StringIO

import os

import pytest

from crosswater import read_config


@pytest.fixture
def config_file():
    return StringIO("""
[preprocessing]
discharge_path = ../Daten/Discharge/WA_Q2010.txt
temperature_path = ../Daten/Temperature/E-OBS_T2010.txt
precipitation_path = ../Daten/Precipitation/RADOLAN_P2010.txt
landuse_path = ../Daten/GIS/AL2000_catchment.dbf
catchment_path = /tmp/Daten/GIS/CATCHMENTS_Rhine.dbf


[catchment_model]
number_of_workers = 1

[routing_model]

[postprocessing]
""")


def test_pre_config_relative(config_file):
    base_path = os.getcwd()
    pre = read_config.read_config(config_file)
    assert pre['preprocessing']['discharge_path'] == os.path.normpath(
        os.path.join(base_path,'../Daten/Discharge/WA_Q2010.txt'))


def test_pre_config_absolut(config_file):
    pre = read_config.read_config(config_file)
    path = '/tmp/Daten/GIS/CATCHMENTS_Rhine.dbf'
    assert pre['preprocessing']['catchment_path'] == path


