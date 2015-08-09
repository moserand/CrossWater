# coding: utf-8

import tables

from crosswater.read_config import read_config
from crosswater.tools.csv_reader import CsvReader
from crosswater.tools import dbflib


def read_dbf_cols(dbf_file_name, col_names=None):
    """Returns a dictionary with column names as keys and lists as values.

    Returns dict with all columns if `col_names` is false.
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


def get_value_by_id(dbf_file_name, col_name):
    """Returns a dict catchment-id: value
    """
    data = read_dbf_cols(dbf_file_name, ['WSO1_ID', col_name])
    return {id_: value for id_, value in zip(data['WSO1_ID'],
                                                   data[col_name])}


def get_tot_areas(dbf_file_name):
    """Returns a dict with catchment ids as keys and areas as values."""
    return get_value_by_id(dbf_file_name, 'AREA')


def get_strahler(dbf_file_name):
    """Returns a dict with catchment ids as keys and strahler as values."""
    return get_value_by_id(dbf_file_name, 'STRAHLER')


def get_appl_areas(dbf_file_name):
    """Returns a dict with catchment ids as keys and maiz areas as values."""
    return get_value_by_id(dbf_file_name, 'LMAIZ')


def filter_strahler_lessthan_three(strahler, tot_areas, appl_areas):
    """Use only catchments where STRAHLER is <= 3.
    """

    def apply_filter(old_values):
        return {id_: value for id_, value in old_values.items() if id_ in ids}

    ids = {id_ for id_, value in strahler.items() if value <= 3}
    return (apply_filter(strahler), apply_filter(tot_areas),
            apply_filter(appl_areas))


class Parameters(tables.IsDescription):
    """Table layout for parameters."""
    name = tables.StringCol(100)
    value = tables.Float64Col()
    unit = tables.StringCol(20)


class Input(tables.IsDescription):
    """Table layout for time dependent input."""
    datetime = tables.Time64Col()
    temperature = tables.Float64Col()
    precipitation = tables.Float64Col()
    discharge = tables.Float64Col()


def create_hdf_file(file_name, tot_areas, appl_areas):
    """Create HDF5 file and add areas as parameters."""
    ids = sorted(tot_areas.keys())
    assert ids == sorted(appl_areas.keys())
    h5_file = tables.open_file(file_name, mode='w',
                                title='Input data for catchment models.')
    for id_ in ids:
        group = h5_file.create_group('/', 'catch_{}'.format(id_), 'catchment {}'.format(id_))
        table = h5_file.create_table(group, 'parameters', Parameters,
                                     'constant parameters')
        row = table.row
        row['name'] = 'A_tot'
        row['value'] = tot_areas[id_]
        row['unit'] = 'm**2'
        row.append()
        row['name'] = 'A_appl'
        row['value'] = appl_areas[id_]
        row['unit'] = 'm**2'
        row.append()
    h5_file.close()


def add_input_tables(h5_file_name, t_file_name, p_file_name, q_file_name):
    h5_file = tables.open_file(h5_file_name, mode='a')
    reader = CsvReader(t_file_name, p_file_name, q_file_name)
    table_name = 'input'
    counter = 0
    total = 365 * 24
    start = timeit.default_timer()
    fraction = 0
    for catchments in reader:
        duration = timeit.default_timer() - start
        total_time = duration * fraction
        counter += 1
        fraction = total / counter
        print(counter, 'total:', total_time,
              'elapsed', duration, 'remaining', total_time - duration)
        for group in h5_file.walk_nodes('/', 'Group'):
            name = group._v_name
            if name.startswith('catch_'):
                id_ = int(name.split('_')[1])
                row_data = catchments[id_]
                try:
                    table = group._f_get_child(table_name)
                except tables.NoSuchNodeError:
                    table = h5_file.create_table(group, table_name, Input,
                                               'time varying inputs')
                row = table.row
                row_names = ['datetime', 'temperature', 'precipitation',
                             'discharge']
                for row_name, row_value in zip(row_names, row_data):
                    row[row_name] = row_value
                row.append()
    h5_file.close()




def preprocess(config_file):
    config = read_config(config_file)
    h5_file_name = config['preprocessing']['hdf_input_path']
    t_file_name = config['preprocessing']['temperature_path']
    p_file_name = config['preprocessing']['precipitation_path']
    q_file_name = config['preprocessing']['discharge_path']

    strahler = get_strahler(config['preprocessing']['catchment_path'])
    tot_areas = get_tot_areas(config['preprocessing']['catchment_path'])
    appl_areas = get_appl_areas(config['preprocessing']['landuse_path'])
    strahler, tot_areas, appl_areas = filter_strahler_lessthan_three(
        strahler, tot_areas, appl_areas)
    create_hdf_file(h5_file_name, tot_areas, appl_areas)
    add_input_tables(h5_file_name, t_file_name, p_file_name, q_file_name)





if __name__ == '__main__':

    import sys
    import timeit

    start = timeit.default_timer()
    preprocess(sys.argv[1])
    print('run time:', timeit.default_timer() - start)

