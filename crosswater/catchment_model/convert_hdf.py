"""Convert the HDF5 from one big table into a table with one group with
one table per time step.
"""

import tables
from timeit import default_timer


def make_index(table):
    """Create a complety sorted index (CSI) for `timestep`.
    """
    col = table.cols.timestep
    if not col.index.is_csi:
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
    start_time = default_timer()
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
        duration = default_timer() - start_time
        fraction = step / total
        if fraction != 0:
            estimated = duration / fraction
            remaning = estimated - duration
            print(('step: {:7d} duration: {:7.0f} estimated: {:7.0f} '
                   'remaining: {:7.0f}').format(step + 1, duration, estimated,
                                                remaning),
                  end='\r')
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
        values = id_data[['catchment', 'concentration', 'discharge']]
        group = out_file.create_group('/', 'step_{}'.format(step))
        out_file.create_table(group, 'values', values,
                              filters=filters)
    in_file.close()
    out_file.close()


if __name__ == '__main__':

    def test():
        """Try it out.
        """
        import os

        start = default_timer()
        base = r'c:\Daten\Mike\projekte\2015_006_crosswater\rhine_model\output'
        try:
            convert(
                #os.path.join(base, 'catchment_output_2.h5'),
                #os.path.join(base, 'steps_output_2.h5'),
                os.path.join(base, 'catchment_output_small_2.h5'),
                os.path.join(base, 'steps_output_small_2.h5'),
                batch_size=100)
        finally:
            print()
            print(default_timer() - start)

    test()
