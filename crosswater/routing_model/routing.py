"""Routing model
"""

from collections import defaultdict
import sys
import itertools

from crosswater.read_config import read_config
from crosswater.preprocessing.hdf_input import read_dbf_cols


class Counts(object):
    # pylint: disable=too-few-public-methods
    """Counts of connection to down catchments.
    """
    def __init__(self, connections):
        self.values = self._make_counts(connections)

    @staticmethod
    def _make_counts(connections):
        """Count number of connections.
        """
        counts = defaultdict(int)
        for ids in connections.values():
            counts[len(ids)] += 1
        return dict(counts)

    def __repr__(self):
        lines = ['{:7s}| {:5s}'.format('ncon', 'count')]
        lines.append('=' * 14)
        for con, count in self.values.items():
            lines.append('{:<7d}| {:5d}'.format(con, count))
        return '\n'.join(lines)

    def _repr_html_(self):
        """Show nice HTML table.
        """
        lines = []
        check = 0
        right = 'style="text-align: right;"'
        for con, count in self.values.items():
            lines.append("""<tr><td>{con:d}</td>
            <td {right}>{count:d}</td></tr>""".format(con=con, count=count,
                                                      right=right))
            check += con * count
        lines.append('<tr><th>Sum</td> <th {right}>{:d}</th></tr>'.format(
            sum(self.values.values()), right=right))
        lines.append("""<tr><th>Sum normalized</td>
        <th {right}>{:d}</th></tr>""".format(check, right=right))
        html_start = """<div>
                    <table border="1">
                      <thead>
                        <tr style="text-align: right;">
                          <th>Number of connections</th>
                          <th>Count</th>
                        </tr>
                      </thead>
                      <tbody>"""
        html_end = """
                    </tbody>
                    </table>
                    </div>"""
        return html_start + '\n'.join(lines) + html_end


class Connections(object):
    """Connections between catchments (only for active_ids)  
    """
    def __init__(self, catchment_dbf_file, direction='up', active_ids=None):
        self.catchment_dbf_file = catchment_dbf_file
        self.active_ids = active_ids
        self._set_direction(direction)
        self.ids, self.next_ids = self._read_id_association(
            catchment_dbf_file, self.id_name, self.next_id_name)
        self._connections = None
        self._counts = None

    def _set_direction(self, direction):
        direction = direction.strip()
        if direction == 'up':
            self.id_name = 'WSO1_ID'
            self.next_id_name = 'NEXTDOWNID'
        elif direction == 'down':
            self.id_name = 'NEXTDOWNID'
            self.next_id_name = 'WSO1_ID'
        else:
            msg = ('Direction must be either "up" or "down". '
                   'Found {}.', direction)
            raise NameError(msg)

    @staticmethod
    def _read_id_association(catchment_dbf_file, id_name='WSO1_ID',
                 next_id_name='NEXTDOWNID'):
        """Read IDs and down IDS from DBF file.
        """
        data = read_dbf_cols(catchment_dbf_file, [id_name, next_id_name])
        ids = data[id_name]
        next_ids = data[next_id_name]
        return ids, next_ids

    @property
    def connections(self):
        """Make connection from ID to down ID.
        """
        if not self._connections:
            connections = {}
            for id_, next_id in zip(self.ids, self.next_ids):
                connections.setdefault(next_id, []).append(id_)
            if self.active_ids:
                connections = dict((id_,ids) for id_, ids in connections.items() if id_ in self.active_ids)
            self._connections = connections
        return self._connections

    @property
    def counts(self):
        """Counts of connections.
        """
        if not self._counts:
            self._counts = Counts(self.connections)
        return self._counts


class UpstreamCatchments(object):
    """Returns dict with outlet ID and its upstream ID's
    """
    def __init__(self, catchment_dbf_file, id_outlets):
        self.id_outlets = id_outlets
        self.ids = self.upstream_dict(catchment_dbf_file, id_outlets)

    def upstream_ids(self, catchment_dbf_file, id_outlet):
        """
        """
        conn = Connections(catchment_dbf_file)
        ids = [id_outlet]
        outlets = [id_outlet]
        while outlets:
            ids_upstream = [conn.connections.get(id_) for id_ in outlets]
            ids_upstream = list(filter(None.__ne__, ids_upstream))
            ids_upstream = list(itertools.chain(*ids_upstream))
            ids.extend(ids_upstream)
            outlets = ids_upstream
        return ids
    
    def upstream_dict(self, catchment_dbf_file, id_outlets):
        """
        """
        ids = {}
        for id_ in id_outlets:
            ids[id_] = self.upstream_ids(catchment_dbf_file, id_)
        return ids
 
 
 


def run():
    """Run the model.
    """
    config_file = sys.argv[1]
    config = read_config(config_file)
    catchment_dbf_file = config['preprocessing']['catchment_path']
    conn = Connections(catchment_dbf_file, direction="up")
    print(conn.counts)
   #print(conn.connections)


if __name__ == '__main__':

    run()
