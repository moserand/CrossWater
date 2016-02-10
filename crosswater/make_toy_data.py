# -*- coding: utf-8 -*-
"""
Creation of shortened input files for development
"""

def get_ids(file_name, limit=None):
    """ Get specified number of IDs. All if limit is 'None'.
    """
    with open (file_name) as fobj:
        header = next(fobj)
        ids = header.split(';')
    if limit is None:
        return ids
    return ids[:limit]


def get_col_pos(file_name, ids):
    """Find column positions for given IDs.
    """
    all_ids = get_ids(file_name)
    col_map = {id_: pos for pos, id_ in enumerate(all_ids, 1)}
    return [col_map[id_] for id_ in ids]


def copy_columns(src_file_name, dst_file_name, ids, col_pos=None):
    """ Copy given cols from src to dst.
        Take first 'len(ids)' value columns if no 'col_pos' is given.
        Date column does not count.
        'col_pos is a list of column indices excluding '0' because this is
        the date column.
    """
    with open(src_file_name) as src, open(dst_file_name, 'w') as dst:
        dst.write(';'.join(ids) + '\n')
        next(src)
        if col_pos is None:
            limit = len(ids)+1
            for line in src:
                entries = line.split(';')
                dst.write(';'.join(entries[:limit]) + '\n')
        else:
            assert len(ids) == len(col_pos)
            assert 0 not in col_pos
            col_pos.insert(0,0)
            for line in src:
                lines = line.split(';')
                selected = [lines[pos] for pos in col_pos]
                dst.write(';'.join(selected) + '\n')


def make_toy_data(base_file_name, other_file_names, file_name_mapping, limit=100):
    """
    """
    ids = get_ids(base_file_name, limit)
    for file_name in [base_file_name] + other_file_names:
        other_ids = get_ids(file_name, limit)
        src = file_name
        dst = file_name_mapping[file_name]
        #print(dst)
        if other_ids == ids:
            copy_columns(src, dst, ids)
        else:
            col_pos= get_col_pos(file_name, ids)
            copy_columns(src, dst, ids, col_pos)

if __name__ == '__main__':
    temp_src = '../Temperature/E-OBS_T2010.txt'
    precip_src = '../Precipitation/RADOLAN_P2010.txt'
    dis_src = '../Discharge/WA_Q2010.txt'
    file_name_mapping ={temp_src: 'temp.txt',
                        precip_src: 'precip.txt',
                        dis_src: 'dis.txt'}
    make_toy_data(dis_src, [precip_src, temp_src], file_name_mapping)

