"""Routing model
"""

from collections import defaultdict
import itertools
import tables
import pandas
import numpy as np

from crosswater.read_config import read_config
from crosswater.preprocessing.hdf_input import read_dbf_cols
from crosswater.tools.time_helper import ProgressDisplay

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
    """Connections between catchments
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
        if all(isinstance(id_, int) for id_ in ids):
            ids = [str(id_)for id_ in ids]
            next_ids = [str(id_) for id_ in next_ids]
        
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
 


class OutputValues(tables.IsDescription):
    """Data model for output data table.
    """
    catchment_outlet = tables.StringCol(10) # WSO1_ID
    discharge = tables.Float64Col()         # m**3/s
    load_aggregated = tables.Float64Col()              # kg/d 
    
    

class LoadAggregation(object):
    """Aggregating loads per timestep for the tributary catchment along the river
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.input_file_name = config['routing_model']['steps_input_path']
        self.output_file_name = config['routing_model']['steps_output_path']
        self.csv_file_name = config['routing_model']['csv_output_path']
        self.riversegments_dbf = config['routing_model']['riversegments_path']
        self.catchment_dbf_file = config['preprocessing']['catchment_path']
        self.tributaries = Tributaries(config_file)
            
    def _open_files(self):
        """Open HDF5 input and output files.
        """
        self.hdf_input = tables.open_file(self.input_file_name, mode='r')
        self.hdf_output = tables.open_file(self.output_file_name, mode='w',
                                           title='Crosswater aggregated results per timestep')
    
    def _close_files(self):
        """Close HDF5 input and output files.
        """
        self.hdf_input.close()
        self.hdf_output.close()

    def _write_output(self, step, in_table, outputvalues):
        """Write output per timestep
        """
        for id_outlet in self.tributaries._ids_tributary_outlets:
            if not in_table['catchment'].str.contains(id_outlet).any():       
                continue
            ids = [str(id_) for id_ in self.tributaries.ids_tributaries[id_outlet]]
            outputvalues['catchment_outlet'] = id_outlet
            outputvalues['load_aggregated'] = in_table["load"][in_table['catchment'].isin(ids)].sum()
            outputvalues['discharge'] = in_table["discharge"][in_table['catchment']==str(id_outlet)]
            outputvalues.append()

    def aggregate(self, total):
        """Aggregate loads for every timestep
        """
        print('aggregate loads and write to output file...')
        prog = ProgressDisplay(total)
        for step in range(0,total):
            prog.show_progress(step + 1, force=True)
            in_table = pandas.read_hdf(self.input_file_name, '/step_{}/values'.format(step), mode='r')
            filters = tables.Filters(complevel=5, complib='zlib')
            out_group = self.hdf_output.create_group('/', 'step_{}'.format(step))
            out_table = self.hdf_output.create_table(out_group, 'values', OutputValues, filters=filters)
            outputvalues = out_table.row
            self._write_output(step, in_table, outputvalues)
            out_table.flush()
        print()
        print(prog.last_display)
    
    def table_outlets(self):
        """Table with catchment ids and ids of outlet.
        """
        ids=list(itertools.chain(*self.tributaries.ids_tributaries.values()))
        table = np.empty(shape=(len(ids),1), dtype=[('catchment', 'S6'),('catchment_outlet', 'S6')])
        for i in range(len(ids)):
            id_ = ids[i]
            table['catchment'][i][0] = id_
            key=[k for k, v in self.tributaries.ids_tributaries.items() if id_ in v]
            table['catchment_outlet'][i][0] = key[0]
        self.hdf_output.create_table('/', 'table_outlets', table)
        self.write_table_csv(table)
    
    def write_table_csv(self, table):
        """Write  catchment ids and ids of outlet to csv.
        """
        with open(self.csv_file_name, "w") as fp:
            fp.write('catchment, catchment_outlet\n')
            fp.write('\n'.join('{}, {}'.format(int(x[0][0]),int(x[0][1])) for x in table))
    
    
    def run(self):
        """Run thread.
        """
        self._open_files()
        self.table_outlets()
        self.aggregate(total=2) #365*24)
        self._close_files()

    
        
class Tributaries(object):
    """
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.csv_file_name = config['routing_model']['csv_output_path']
        self.riversegments_dbf = config['routing_model']['riversegments_path']
        self.catchment_dbf_file = config['preprocessing']['catchment_path']
        self.areas = self.get_value_by_id('AREA')
        print('get catchments of tributary upstream areas...', end='')
        self.ids_riversegments = self.read_ids(self.riversegments_dbf, 'WSO1_ID')
        self._ids_tributary_outlets()
        self.ids_tributaries = self.ids_tributaries()
        self.areas_tributaries = self.areas_tributaries()
        print('Done')
        
    def read_ids(self, dbf_file_name, col_name):
        """Retruns all ids riversegments as strings.
        """
        ids = read_dbf_cols(dbf_file_name, [col_name])[col_name]
        if all(isinstance(id_, int) for id_ in ids):
            ids = [str(id_)for id_ in ids]     
        return ids
    
    def get_value_by_id(self, col_name, converter=1, ids=None):
        """Returns a dict catchment-id: value

        converter for units with default value 1
        ids to filter (e.g. only Strahler)
        """
        data = read_dbf_cols(self.catchment_dbf_file, ['WSO1_ID', col_name])
        res = {id_: value * converter for id_, value in
               zip(data['WSO1_ID'], data[col_name])}
        if ids:
            res = {id_: value for id_, value in res.items() if id_ in ids}
        return res       
    
    def ids_tributaries(self):
        """Return dictionary of catchments of all tributary upstream areas
        """
        ids_tributaries = UpstreamCatchments(self.catchment_dbf_file, self._ids_tributary_outlets)
        print()
        return ids_tributaries.ids

    def _ids_tributary_outlets(self):
        """Return all catchments of tributary outlets.
        
           Tributary outlets are directly connected downstream with the riversegments.
        """
        conn = Connections(self.catchment_dbf_file, active_ids = self.ids_riversegments)
        id_outlets = list(itertools.chain(*conn.connections.values()))
        self._ids_tributary_outlets = [id_ for id_ in id_outlets if id_ not in self.ids_riversegments]
    
    def areas_tributaries(self):
        """Total area of catchments within tributary.
        
            Dictionary with outlet of tributary and area in m**2
        """
        tot_areas = []
        outlets = []        
        for outlet_id, ids in self.ids_tributaries.items():
            outlets.append(outlet_id)
            tot_areas.append(sum([area for id_, area in self.areas.items() if str(id_) in ids]))
        return dict(zip(outlets, tot_areas))
 

     
class Compartments(object):
    """Compartments for Aquasim.
    
        Compartments are river reaches that are connected with advective links 
        and may have diffusive links alongside. One compartment exists of one or more
        riversegments and does not contain any river junctions.
        The river network is at first divided at the river junctions, if the 
        desired number of compartments is higher, the river reaches are further 
        divided at the inlets of the largest tributaries.
    """
    def __init__(self, config_file, areas_tributaries):
        config = read_config(config_file)
        self.areas_tributaries = areas_tributaries
        self.riversegments_dbf = config['routing_model']['riversegments_path']
        self.catchment_dbf_file = config['preprocessing']['catchment_path']
        self.nr_comp = int(config['routing_model']['nr_compartments'])   ##### TODO: default 0
        self.ids_riversegments = self.read_ids(self.riversegments_dbf, 'WSO1_ID')
        self.ids_junctions = self.get_junctions()
        self.ids_headwater = self.get_headwater() 
        self.nr_div = self.nr_div()
        self.ids_advective_inlets = self.get_advective_inlets()
        self.ids_up = self.ids_up()
        self.ids_down = self.ids_down()
        self.compartments = self.compartments()
    
    @staticmethod
    def read_ids(dbf_file_name, col_name):
        """Retruns all ids riversegments as strings.
        """
        ids = read_dbf_cols(dbf_file_name, [col_name])[col_name]
        if all(isinstance(id_, int) for id_ in ids):
            ids = [str(id_)for id_ in ids]     
        return ids
    
    def get_junctions(self):
        """Returns catchments of riversegments downstream of the junctions of two rivers.
            
            At a confluence the riversegment has two upstream connections within ids of riversegments.
        """
        conn = Connections(self.catchment_dbf_file, direction='up', active_ids=self.ids_riversegments)
        key_set=set(conn.connections.keys())
        ids_junctions=[key for key, values in conn.connections.items() 
               if len(key_set.intersection(set(values)))>1]
        return ids_junctions
    
    def get_headwater(self):
        """Returns headwater catchments.
        """
        conn = Connections(self.riversegments_dbf, direction='up', active_ids=self.ids_riversegments)
        ids_headwater = [id_ for id_ in self.ids_riversegments if id_ not in conn.connections.keys()]
        return ids_headwater
    
    def nr_div(self):
        """Number of divisions of river reaches to comply with the number of compartments specified.
        """
        nr_rivers = len(self.ids_junctions)*2+1
        nr_div = self.nr_comp - nr_rivers
        if nr_div < 0:
            print('WARNING: The selected number of compartments {} is smaller than '
                  'number of river reaches between junctions {}.'.format(self.nr_comp, nr_rivers))
            print('\t Consequently the river network is divided into {} compartments.'.format(nr_rivers))
            self.nr_comp = nr_rivers
            nr_div = 0
        return nr_div
    
    def get_advective_inlets(self):
        """Returns catchments of largest advective tributary inlets, that are not in ids_headwater.
        """
        outlets = sorted(self.areas_tributaries, key=self.areas_tributaries.get, reverse=True)
        ids_advective_inlets = []
        if outlets:
            conn = Connections(self.catchment_dbf_file, direction='down', active_ids=outlets)
            inlets = list(itertools.chain(*conn.connections.values()))
            inlets_along=[inlet for inlet in inlets if inlet not in self.ids_headwater]
            ids_advective_inlets = inlets_along[0:self.nr_div]
        return ids_advective_inlets
    
    def ids_up(self):
        """First upstream catchment of every compartment.
        
            ids of junctions, ids of advective tributary inlets, ids of headwater catchments
        """
        return self.ids_advective_inlets + self.ids_headwater + self.ids_junctions
    
    def ids_down(self):
        """Last downstream catchment of every compartment.
        """
        conn = Connections(self.riversegments_dbf, direction='up', active_ids=self.ids_up)
        ids_down = list(itertools.chain(*conn.connections.values()))
        conn = Connections(self.riversegments_dbf, direction='down')
        id_last = [k for k, v in conn.connections.items() if v==['-9999']] 
        return ids_down + id_last
    
    def compartments(self):
        """Dictionary with name and ids for every compartment.
        
            The name of the compartment is a 'C' and the first catchment number.
        """
        conn = Connections(self.riversegments_dbf, direction='down', active_ids=self.ids_riversegments)
        comp_name = []
        comp_ids = []
        for id_ in self.ids_up: 
            comp_name.append('C'+id_)
            ids = []
            nextid = conn.connections.get(id_)[0]
            ids.append(nextid)
            while nextid not in self.ids_up and nextid != '-9999':
                ids.append(nextid)
                nextid = conn.connections.get(nextid)[0]
            comp_ids.append(ids)
        return dict(zip(comp_name, comp_ids))


   




        

     
    
    
    def run(self):
        """Run thread.
        """
        self._open_files()
        self.table_outlets()
        self.aggregate(total=2) #365*24)
        self._close_files()




#def run():
#    """Run the model.
#    """
#    config_file = sys.argv[1]
##    config = read_config(config_file)
##    catchment_dbf_file = config['preprocessing']['catchment_path']
##    conn = Connections(catchment_dbf_file, direction="up")
##    print(conn.counts)
#    aggregation = LoadAggregation(config_file)    
#    aggregation.run()
#    
#
#if __name__ == '__main__':
#
#    run()
