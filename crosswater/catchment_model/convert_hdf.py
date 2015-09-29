"""Convert the HDF5 from one big table into a table with one group with
one table per time step.
"""

import tables

from crosswater.read_config import read_config
from crosswater.tools.time_helper import ProgressDisplay


def make_index(table):
    """Create a completely sorted index (CSI) for `timestep`.
    """
    col = table.cols.timestep
    if not col.index or not col.index.is_csi:
        if col.is_indexed:
            print('removing old index')
            table.cols.timestep.remove_index()
        print('indexing')
        indexrows = table.cols.timestep.create_csindex()
        print('indexed {} rows'.format(indexrows))


def count_ids(id_col):
    """Count number of unique IDs.
    """
    ids = set()
    for id_ in id_col:
        ids.add(id_)
    return len(ids)


def convert(in_file_name, out_file_name, batch_size=2, total=365 * 24):
    """Convert on gigantic table into one per timesstep.
    """
    prog = ProgressDisplay(total)
    filters = tables.Filters(complevel=5, complib='zlib')
    in_file = tables.open_file(in_file_name, mode='a')
    table = in_file.get_node('/output')
    make_index(table)
    nrows = table.nrows  # pylint: disable=no-member
    nids = count_ids(table.cols.catchment)  # pylint: disable=no-member
    assert nrows == total * nids
    out_file = tables.open_file(out_file_name, mode='w')
    start = 0
    stop = nids
    read_start = 0
    read_stop = nids * batch_size
    for step in range(total):
        prog.show_progress(step + 1)
        if step % batch_size == 0:
            # pylint: disable=no-member
            batch_data = table.read_sorted('timestep', start=read_start,
                                           stop=read_stop)
            read_start = read_stop
            read_stop += nids * batch_size
            read_stop = min(read_stop, nrows)
            start = 0
            stop = start + nids
        id_data = batch_data[start:stop]
        start = stop
        stop += nids
        try:
            assert len(set(id_data['timestep'])) == 1
        except AssertionError:
            print(set(id_data['timestep']))
            print(id_data)
        values = id_data[['catchment', 'concentration', 'discharge', 'load']]
        group = out_file.create_group('/', 'step_{}'.format(step))
        out_file.create_table(group, 'values', values,
                              filters=filters)
    prog.show_progress(step + 1, force=True)
    in_file.close()
    out_file.close()


def run_convertion(config_file, batch_size):
    """Convert the output to one table per time step.
    """
    print()
    print('converting output')
    config = read_config(config_file)
    in_file_name = config['catchment_model']['output_path']
    out_file_name = config['catchment_model']['steps_output_path']
    convert(in_file_name, out_file_name, batch_size=batch_size)
