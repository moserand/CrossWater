# coding: utf-8

"""Generate HDF5 file with all catchment model input data.

Sources are DBF and some large text files.
"""

import sys

import numpy
import pandas
import tables

from crosswater.read_config import read_config
from crosswater.tools import dbflib
from crosswater.tools.hdf5_helpers import find_ids
from crosswater.tools.time_helper import ProgressDisplay


def read_dbf_cols(dbf_file_name, col_names=None):
    """Returns a dictionary with column names as keys and lists as values.
    
    Returns dict with all columns if `col_names` is false.
    
    dbf_file_name is a string
    col_names is a list containing strings
    """
    dbf_file = dbflib.DbfReader(dbf_file_name)
    dbf_file.read_all()
    if not col_names:
        return dbf_file.data
    res = {key: dbf_file.data[key] for key in col_names}
    return res


def read_dbf_col(dbf_file_name, col_name):
    """Retruns all column entries for a given column name.
    """
    return read_dbf_cols(dbf_file_name, [col_name])[col_name]


def get_value_by_id(dbf_file_name, col_name, converter=1, ids=None):
    """Returns a dict catchment-id: value
    
    converter for units with default value 1
    ids to filter (e.g. only Strahler)
    """
    data = read_dbf_cols(dbf_file_name, ['WSO1_ID', col_name])
    res = {id_: value * converter for id_, value in
           zip(data['WSO1_ID'], data[col_name])}
    if ids:
        res = {id_: value for id_, value in res.items() if id_ in ids}
    return res


def get_tot_areas(dbf_file_name, ids=None):
    """Returns a dict with catchment ids as keys and areas as values."""
    return get_value_by_id(dbf_file_name, 'AREA', ids=ids)


def get_strahler(dbf_file_name, ids=None):
    """Returns a dict with catchment ids as keys and strahler as values."""
    return get_value_by_id(dbf_file_name, 'STRAHLER', ids=ids)


def get_appl_areas(dbf_file_name, crops, ids=None):
    """Returns a dict with catchment ids as keys and total areas as values.

    Crop areas are added to total area.    
    """
    res = get_value_by_id(dbf_file_name, crops[0], converter=1e6, ids=ids)
    for crop in crops[1:]:
        res_crop = get_value_by_id(dbf_file_name, crop, converter=1e6, ids=ids)
        res = {id_: res.get(id_, 0) + res_crop.get(id_, 0) for id_ in set(res) & set(res_crop) }
    return res
    

def get_appl_rates(dbf_file_name, ids=None):
    """Returns a dict with catchments ids as keys and application rate as 
    values.
    """
    return get_value_by_id(dbf_file_name, 'APPL_RATES', ids=ids)


def filter_strahler_lessthan(strahler, tot_areas, appl_areas, appl_rates, strahler_limit=3):
    """Use only catchments where STRAHLER is <= limit.
    """

    def apply_filter(old_values):
        """Filter for ids.
        """
        return {id_: value for id_, value in old_values.items() if id_ in ids}
    
    ids = {id_ for id_, value in strahler.items() if value <= strahler_limit}
    return (apply_filter(strahler), apply_filter(tot_areas),
            apply_filter(appl_areas), apply_filter(appl_rates))


class Parameters(tables.IsDescription):
    # pylint: disable=too-few-public-methods
    """Table layout for parameters."""
    name = tables.StringCol(100)
    value = tables.Float64Col()
    unit = tables.StringCol(20)


def create_hdf_file(file_name, tot_areas, appl_areas, appl_rates):
    """Create HDF5 file and add areas as parameters."""
    ids = sorted(tot_areas.keys())
    h5_file = tables.open_file(file_name, mode='w',
                               title='Input data for catchment models.')
    for id_ in ids:
        # create new group (where, name, title)        
        group = h5_file.create_group('/', 'catch_{}'.format(id_),
                                     'catchment {}'.format(id_))
        # create new table (where, name, description, title)
        table = h5_file.create_table(group, 'parameters', Parameters,
                                     'constant parameters')
        tot_area = tot_areas[id_]
        appl_area = appl_areas[id_]
        appl_rate = appl_rates[id_]
        
        # fill parameter table by rows
        row = table.row
        row['name'] = 'A_tot'
        row['value'] = tot_area
        row['unit'] = 'm**2'
        row.append()
        row['name'] = 'A_appl'
        row['value'] = appl_area
        row['unit'] = 'm**2'
        row.append()
        row['name'] = 'R_appl'
        row['value'] = appl_rate
        row['unit'] = 'g/m**2'
        row.append()
    h5_file.close()


def add_input_tables(h5_file_name, t_file_name, p_file_name, q_file_name,
                     batch_size=None, total=365 * 24):
    """Add input with pandas.
    """
    # pylint: disable=too-many-locals
    filters = tables.Filters(complevel=5, complib='zlib')
    h5_file = tables.open_file(h5_file_name, mode='a')
    get_child = h5_file.root._f_get_child  # pylint: disable=protected-access
    all_ids = ids = find_ids(h5_file)
    usecols = None
    if batch_size is None:
        batch_size = sys.maxsize
    if batch_size < len(all_ids):
        usecols = True
    counter = 0
    total_ids = len(all_ids)
    prog = ProgressDisplay(total_ids)
    # pylint: disable=undefined-loop-variable
    while all_ids:
        ids = all_ids[-batch_size:]
        all_ids = all_ids[:-batch_size]
        if usecols:
            usecols = ids
        temp = pandas.read_csv(t_file_name, sep=';', parse_dates=True,
                               usecols=usecols)
        precip = pandas.read_csv(p_file_name, sep=';', parse_dates=True,
                                 usecols=usecols)
        dis = pandas.read_csv(q_file_name, sep=';', parse_dates=True,
                              usecols=usecols)
        temp_hourly = temp.reindex(dis.index, method='ffill')
        for id_ in ids:
            counter += 1
            inputs = pandas.concat([temp_hourly[id_], precip[id_], dis[id_]],
                                   axis=1)
            inputs.columns = ['temperature', 'precipitation', 'discharge']
            input_table = inputs.to_records(index=False)
            name = 'catch_{}'.format(id_)
            group = get_child(name)
            h5_file.create_table(group, 'inputs', input_table,
                                 'time varying inputs', expectedrows=total,
                                 filters=filters)
            prog.show_progress(counter, additional=id_)
    prog.show_progress(counter, additional=id_, force=True)
    int_steps = pandas.DataFrame(dis.index.to_series()).astype(numpy.int64)
    int_steps.columns = ['timesteps']
    time_steps = int_steps.to_records(index=False)
    h5_file.create_table('/', 'time_steps', time_steps,
                         'time steps for all catchments')
    h5_file.close()


def get_first_ids(q_file_name, max_ids):
    """Get first `max_ids` from the dicharge file.
    """
    with open(q_file_name) as fobj:
        header = next(fobj).strip().split(';')
    return {int(entry[1:-1]) for entry in header[:max_ids]}


def preprocess(config_file):
    """Do the preprocessing.
    """
    config = read_config(config_file)
    h5_file_name = config['preprocessing']['hdf_input_path']
    t_file_name = config['preprocessing']['temperature_path']
    p_file_name = config['preprocessing']['precipitation_path']
    q_file_name = config['preprocessing']['discharge_path']
    max_ids = config['preprocessing']['max_ids']
    ids = None
    if max_ids:
        ids = get_first_ids(q_file_name, max_ids)
    batch_size = config['preprocessing']['batch_size']
    strahler = get_strahler(config['preprocessing']['catchment_path'], ids)
    tot_areas = get_tot_areas(config['preprocessing']['catchment_path'], ids)
    crops = config['preprocessing']['crops'].split(', ')    
    appl_areas = get_appl_areas(config['preprocessing']['landuse_path'], crops, ids)
    appl_rates = get_appl_rates(config['preprocessing']['micropollutant_path'], ids)
    strahler_limit = config['preprocessing']['strahler_limit'] 
    strahler, tot_areas, appl_areas, appl_rates = filter_strahler_lessthan(
        strahler, tot_areas, appl_areas, appl_rates, strahler_limit)
    create_hdf_file(h5_file_name, tot_areas, appl_areas, appl_rates)
    add_input_tables(h5_file_name, t_file_name, p_file_name, q_file_name,
                     batch_size=batch_size)


if __name__ == '__main__':

    from crosswater.tools.time_helper import show_used_time

    @show_used_time
    def test():
        """Try it out.
        """
        config = sys.argv[1]
        print('processing with {} ...'.format(config))
        preprocess(config)

    test()
