from collections import defaultdict
import sys

from crosswater.read_config import read_config
from crosswater.preprocessing.hdf_input import read_dbf_cols


def read_id_association(catchment_dbf_file):
    data = read_dbf_cols(catchment_dbf_file, ['WSO1_ID', 'NEXTDOWNID'])
    ids = data['WSO1_ID']
    next_ids = data['NEXTDOWNID']
    return ids, next_ids

def find_connections(ids, next_ids):
    connections = {}
    for id_, next_id in zip(ids, next_ids):
        connections.setdefault(next_id, []).append(id_)
    return connections

def count_connections(connections):
    counts = defaultdict(int)
    for ids in connections.values():
        counts[len(ids)] += 1
    return counts



def run():
    config_file = sys.argv[1]
    config = read_config(config_file)
    catchment_dbf_file = config['preprocessing']['catchment_path']
    ids, next_ids = read_id_association(catchment_dbf_file)
    conn = find_connections(ids, next_ids)
    counts = count_connections(conn)
    print(counts)


if __name__ == '__main__':

    run()