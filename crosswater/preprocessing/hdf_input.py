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


def create_hdf_file(file_name, tot_areas, appl_areas, skip_missing_ids=False,
                    ids=None):
    """Create HDF5 file and add areas as parameters."""
    if not ids:
        skip_missing_ids = False
        ids = sorted(tot_areas.keys())
        assert ids == sorted(appl_areas.keys())
    h5_file = tables.open_file(file_name, mode='w',
                                title='Input data for catchment models.')
    for id_ in ids:
        group = h5_file.create_group('/', 'catch_{}'.format(id_),
                                     'catchment {}'.format(id_))
        table = h5_file.create_table(group, 'parameters', Parameters,
                                     'constant parameters')
        try:
            tot_area = tot_areas[id_]
            appl_area = appl_areas[id_]
        except KeyError:
            if not skip_missing_ids:
                raise
        row = table.row
        row['name'] = 'A_tot'
        row['value'] = tot_area
        row['unit'] = 'm**2'
        row.append()
        row['name'] = 'A_appl'
        row['value'] = appl_area
        row['unit'] = 'm**2'
        row.append()
    h5_file.close()


def add_input_tables_batched(h5_file_name, t_file_name, p_file_name,
                             q_file_name, batch_size=2000, total=365*24):
    """Add input in batches of ids.
    """
    filters = tables.Filters(complevel=5, complib='zlib')
    h5_file = tables.open_file(h5_file_name, mode='a')
    table_name = 'input'
    all_ids = []
    for group in h5_file.walk_nodes('/', 'Group'):
        name = group._v_name
        if name.startswith('catch_'):
            id_ = int(name.split('_')[1])
            all_ids.append(id_)
    all_ids.sort()
    print('data')
    counter = 0
    batch_counter = 0
    fraction = batch_size / len(all_ids)
    while all_ids:
        reader = CsvReader(t_file_name, p_file_name, q_file_name)
        batch_counter += 1
        data = {}
        ids = all_ids[-batch_size:]
        all_ids = all_ids[:-batch_size]
        for catchments in reader:
            counter += 1
            print('{:2d} {:7d}{:7.2f} % '.format(batch_counter,
                                             counter,
                                             counter * fraction / total * 100,
                                             ),
                  end= '\r')
            for id_ in ids:
                data.setdefault(id_, []).append(catchments[id_])
        print('\nhdf5')
        get_child = h5_file.root._f_get_child
        for id_ in ids:
            name = 'catch_{}'.format(id_)
            group = get_child(name)
            table = h5_file.create_table(group, table_name, Input,
                                         'time varying inputs',
                                         filters=filters)
            row = table.row
            for data_row in data[id_]:
                row_names = ['datetime', 'temperature', 'precipitation',
                             'discharge']
                for row_name, row_value in zip(row_names, data_row):
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
    add_input_tables_batched(h5_file_name, t_file_name, p_file_name,
                              q_file_name)


if __name__ == '__main__':

    import sys
    import timeit

    start = timeit.default_timer()
    preprocess(sys.argv[1])
    print('run time:', timeit.default_timer() - start)

