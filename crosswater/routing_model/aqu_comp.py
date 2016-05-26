"""Aquasim compartments, links, parameterization and input
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
    compartment = tables.StringCol(10)      # WSO1_ID
    discharge = tables.Float64Col()         # m**3/s
    load_aggregated = tables.Float64Col()   # kg/d    
    

#class LoadAggregationTributaries(object):
#    """Aggregating loads per timestep for the tributaries along the river
#    """
#    def __init__(self, config_file):
#        config = read_config(config_file)
#        self.input_file_name = config['routing_model']['steps_input_path']
#        self.output_file_name = config['routing_model']['steps_output_aqu']
#        #self.csv_file_name = config['routing_model']['csv_output_aqu']
#        self.riversegments_dbf = config['routing_model']['riversegments_path']
#        self.catchment_dbf_file = config['preprocessing']['catchment_path']
#        self.tributaries = Tributaries(config_file)
#            
#    def _open_files(self):
#        """Open HDF5 input and output files.
#        """
#        self.hdf_input = tables.open_file(self.input_file_name, mode='r')
#        self.hdf_output = tables.open_file(self.output_file_name, mode='w',
#                                           title='Crosswater aggregated results per timestep')
#    
#    def _close_files(self):
#        """Close HDF5 input and output files.
#        """
#        self.hdf_input.close()
#        self.hdf_output.close()
#
#    def _write_output(self, step, in_table, outputvalues):
#        """Write output per timestep
#        """
#        for id_outlet in self.tributaries._ids_tributary_outlets:
#            if not in_table['catchment'].str.contains(id_outlet).any():       
#                continue
#            ids = [str(id_) for id_ in self.tributaries.ids_tributaries[id_outlet]]
#            outputvalues['catchment_outlet'] = id_outlet
#            outputvalues['load_aggregated'] = in_table["load"][in_table['catchment'].isin(ids)].sum()
#            outputvalues['discharge'] = in_table["discharge"][in_table['catchment']==str(id_outlet)]
#            outputvalues.append()
#
#    def aggregate(self, total):
#        """Aggregate loads for every timestep
#        """
#        print('aggregate loads and write to output file...')
#        prog = ProgressDisplay(total)
#        for step in range(0,total):
#            prog.show_progress(step + 1, force=True)
#            in_table = pandas.read_hdf(self.input_file_name, '/step_{}/values'.format(step), mode='r')
#            filters = tables.Filters(complevel=5, complib='zlib')
#            out_group = self.hdf_output.create_group('/', 'step_{}'.format(step))
#            out_table = self.hdf_output.create_table(out_group, 'values', OutputValues, filters=filters)
#            outputvalues = out_table.row
#            self._write_output(step, in_table, outputvalues)
#            out_table.flush()
#        print()
#        print(prog.last_display)
#    
#    def table_outlets(self):
#        """Table with catchment ids and ids of outlet.
#        """
#        ids=list(itertools.chain(*self.tributaries.ids_tributaries.values()))
#        table = np.empty(shape=(len(ids),1), dtype=[('catchment', 'S6'),('catchment_outlet', 'S6')])
#        for i in range(len(ids)):
#            id_ = ids[i]
#            table['catchment'][i][0] = id_
#            key=[k for k, v in self.tributaries.ids_tributaries.items() if id_ in v]
#            table['catchment_outlet'][i][0] = key[0]
#        self.hdf_output.create_table('/', 'table_outlets', table)
#        #self.write_table_csv(table)
#    
#    def write_table_csv(self, table):
#        """Write  catchment ids and ids of outlet to csv.
#        """
#        with open(self.csv_file_name, "w") as fp:
#            fp.write('catchment, catchment_outlet\n')
#            fp.write('\n'.join('{}, {}'.format(int(x[0][0]),int(x[0][1])) for x in table))
#    
#    def run(self):
#        """Run thread.
#        """
#        self._open_files()
#        self.table_outlets()
#        self.aggregate(total=2) #365*24)
#        self._close_files()
  
        
class Tributaries(object):
    """Get all tributaries to the rivernetwork.
    
        In riversegments is the newtork represented for the modelling with Aquasim and
        the catchments includes the total network. The upstream catchments of all 
        tributing outlets to the riversegments are aggregated.
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.csv_file_name = config['routing_model']['csv_output_aqu']
        self.riversegments_dbf = config['routing_model']['riversegments_path']
        self.catchment_dbf_file = config['preprocessing']['catchment_path']
        self.areas = self.get_value_by_id('AREA')
        print('get catchments of tributary upstream areas...', end='')
        self.ids_riversegments = self.read_ids(self.riversegments_dbf, 'WSO1_ID')
        self.ids_tributary_outlets()
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
        ids_tributaries = UpstreamCatchments(self.catchment_dbf_file, self.ids_tributary_outlets)
        return ids_tributaries.ids

    def ids_tributary_outlets(self):
        """Return all catchments of tributary outlets.
        
           Tributary outlets are directly connected downstream with the riversegments.
        """
        conn = Connections(self.catchment_dbf_file, active_ids = self.ids_riversegments)
        id_outlets = list(itertools.chain(*conn.connections.values()))
        self.ids_tributary_outlets = [id_ for id_ in id_outlets if id_ not in self.ids_riversegments]
    
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
    def __init__(self, config_file, Tributaries):
        self.areas_tributaries = Tributaries.areas_tributaries
        config = read_config(config_file)
        print('define the compartments...', end='')
        self.riversegments_dbf = config['routing_model']['riversegments_path']
        self.catchment_dbf_file = config['preprocessing']['catchment_path']
        self.nr_comp = int(config['routing_model']['nr_compartments']) + 1  ##### TODO: correct workaround (+ 1) within code
        self.ids_riversegments = self.read_ids(self.riversegments_dbf, 'WSO1_ID')
        self.ids_junctions = self.get_junctions()
        self.ids_headwater = self.get_headwater() 
        self.nr_div = self.nr_div()
        self.ids_advective_inlets = self.get_advective_inlets()
        self.ids_up = self.ids_up()
        self.ids_down = self.ids_down()
        self.compartments = self.compartments()
        self.comp_length = self.comp_length()
        print('Done')
    
    @staticmethod
    def read_ids(dbf_file_name, col_name):
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
        data = read_dbf_cols(self.riversegments_dbf, ['WSO1_ID', col_name])
        res = {id_: value * converter for id_, value in
               zip(data['WSO1_ID'], data[col_name])}
        if ids:
            res = {id_: value for id_, value in res.items() if id_ in ids}
        return res
    
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
        ids_advective_inlets = []
        s_outlets = set(self.areas_tributaries.keys())
        exclude = Connections(self.catchment_dbf_file, direction='down', active_ids=self.ids_headwater).connections.values()
        s_exclude=set(list(itertools.chain(*exclude)))
        s_outlets.discard(s_exclude)
        keys = [key for key in self.areas_tributaries.keys() if key in s_outlets]
        outlets = sorted(keys, key=self.areas_tributaries.get, reverse=True)[0:self.nr_div]  
        if outlets:
            conn = Connections(self.catchment_dbf_file, direction='down', active_ids=outlets)
            ids_advective_inlets = list(itertools.chain(*conn.connections.values()))
        return ids_advective_inlets
    
    def ids_up(self):
        """First upstream catchment of every compartment.
        
            ids of junctions, ids of advective tributary inlets, ids of headwater catchments
        """
        ids_up = self.ids_advective_inlets + self.ids_headwater + self.ids_junctions
        ids_up = list(set(ids_up)) #remove duplicated
        return ids_up
    
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
            ids.append(id_)
            nextid = conn.connections.get(id_)[0]
            while nextid not in self.ids_up and nextid != '-9999':
                ids.append(nextid)
                nextid = conn.connections.get(nextid)[0] 
            comp_ids.append(ids)
        return dict(zip(comp_name, comp_ids))
    
    def comp_length(self):
        """Dictionary with compartment name and length.
        """
        lengths = self.get_value_by_id('LENGTH')
        name_comp = []
        length_comp =[]
        for comp, ids in self.compartments.items():
            name_comp.append(comp)
            length_tot = sum(lengths[int(id_)] for id_ in ids)
            length_comp.append(length_tot)
        return dict(zip(name_comp, length_comp))
               

    def write_compartments_csv(self, csv_file_name):
        """Write compartment names and catchments to file specified as argument.
        """
        with open(csv_file_name, "w") as fp:
            fp.write('compartment, catchment\n')
            for key in self.compartments.keys():
                values = self.compartments[key]
                [fp.write('{}, {}\n'.format(key, value)) for value in values]
                
    
class Links(object):
    """Links between compartments for Aquasim.
    
        How are the compartments linked together in Aquasim and
        which primary catchments are upstream or lateral input
        to a compartment.
    """
    def __init__(self, config_file, Tributaries, Compartments):
        self.areas_tributaries = Tributaries.areas_tributaries
        self.ids_tributaries = Tributaries.ids_tributaries
        self.ids_tributary_outlets = Tributaries.ids_tributary_outlets
        self.compartments = Compartments.compartments
        self.ids_riversegments = Compartments.ids_riversegments 
        self.ids_down = Compartments.ids_down
        config = read_config(config_file)
        self.riversegments_dbf = config['routing_model']['riversegments_path']
        self.catchment_dbf_file = config['preprocessing']['catchment_path']
        print('get lateral/upstream input catchments and define links between compartments...', end='')
        self.compartment_links = self.compartment_links()
        self.upstream_tributaries = self.upstream_tributaries()
        self.lateral_tributaries = self.lateral_tributaries()
        self.lateral_input = self.lateral_input()
        self.upstream_input = self.upstream_input()
        print('Done')
        
    def compartment_links(self):
        """Table with name of link, from- and to-compartment name.
        """
        columns = ['Name', 'fromCompart', 'toCompart']
        index = range(len(self.ids_down)-1)
        df = pandas.DataFrame(columns=columns, index = index)
        i=0
        for compartment in self.compartments:
            id_first = self.compartments.get(compartment)[0]
            id_last = self.compartments.get(compartment)[-1]
            toCompart = Connections(self.riversegments_dbf, direction='down', active_ids=id_last).connections.values()
            toCompart = list(itertools.chain(*toCompart))[0]
            if toCompart not in self.ids_riversegments:
                continue
            df.Name[i] = 'L' + id_first
            df.fromCompart[i] = compartment
            df.toCompart[i] = "C"+toCompart
            i+=1
        return df
           
    @staticmethod
    def next_comp(compartment, compartment_links):
        next_comp = None
        if compartment in set(compartment_links.fromCompart):
            index = compartment_links.fromCompart[compartment_links.fromCompart == compartment].index[0]
            next_comp = compartment_links.get('toCompart')[index]
        return next_comp
    
    def upstream_tributaries(self):
        """Dictionary with compartment name as key and tributary outlets as value.
        """
        compartments = []
        ids_upstream_input = []
        for compartment in self.compartments:
            id_first = self.compartments.get(compartment)[0]
            conn = Connections(self.catchment_dbf_file, direction='up', active_ids=id_first)
            outlets = list(itertools.chain(*conn.connections.values()))
            outlets_up = [id_ for id_ in outlets if id_ not in self.ids_riversegments]
            compartments.append(compartment)
            ids_upstream_input.append(outlets_up)
        return dict(zip(compartments, ids_upstream_input))
    
    def lateral_tributaries(self):
        """Dictionary with compartment name as key and tributary outlets as value.
        """
        compartments = []
        ids_lateral_input = []
        for compartment in self.compartments:
            id_lat = self.compartments.get(compartment)[1:]
            if not id_lat:
                compartments.append(compartment)
                ids_lateral_input.append([])
                continue
            conn = Connections(self.catchment_dbf_file, direction='up', active_ids=id_lat)
            outlets = list(itertools.chain(*conn.connections.values()))
            outlets_lat = [id_ for id_ in outlets if id_ not in self.ids_riversegments]
            compartments.append(compartment)
            ids_lateral_input.append(outlets_lat)
        return dict(zip(compartments, ids_lateral_input))
    
    def upstream_input(self):
        """Dictionary with all upstream ids of a compartment.
        """
        compartments = []
        up_ids =[]
        for upstream in iter(self.upstream_tributaries.items()):
            compartment = upstream[0]
            up_trib = upstream[1]    
            up_ids_ = []
            [up_ids_.append(self.ids_tributaries.get(id_)) for id_ in up_trib]
            up_ids_ = list(itertools.chain(*up_ids_))
            compartments.append(compartment)
            up_ids.append(up_ids_)
        return dict(zip(compartments, up_ids)) 
 
    def lateral_input(self):
        """Dictionary with all lateral ids of a compartment (and the compartment ids itself).
        """
        compartments = []
        lat_ids = []
        for lateral in iter(self.lateral_tributaries.items()):
            compartment = lateral[0]
            lat_trib = lateral[1]
            lat_ids_ = []
            [lat_ids_.append(self.ids_tributaries.get(id_)) for id_ in lat_trib]
            lat_ids_.append(self.compartments.get(compartment))
            lat_ids_ = list(itertools.chain(*lat_ids_))
            compartments.append(compartment)
            lat_ids.append(lat_ids_)
        return dict(zip(compartments, lat_ids))


class Parameterization(object):
    """Parameterization of the compartment.
    
        Parameterization of the riverbed with distance to river mouth ('X'), 
        riverbed width ('WIDTH'), Strickler coefficient ('Kst') and riverbed 
        elevation z ('ELEV') from the dbf file riversegments.
        Any rise in elevation along the riverbed is corrected with interpolation.
    """
    def __init__(self, config_file, Compartments):
        config = read_config(config_file)
        print('parameterize compartments...', end='')
        pandas.options.mode.chained_assignment = None
        self.compartments = Compartments.compartments
        self.comp_length = Compartments.comp_length
        self.riversegments_dbf = config['routing_model']['riversegments_path']
        self.riversegments = read_dbf_cols(self.riversegments_dbf, ['WSO1_ID', 'X', 'ELEV', 'WIDTH', 'Kst'])
        print('Done')
        
    def _slope_correction(self, df):
        """Replace riverbed elevation where slope >= 0 with interpolation.
        """
        ndf=pandas.Series(index=df.x, data=df.zb.values)
        while any(ndf.diff(1) >= 0):
            index = ndf.diff(1) >= 0
            ndf[index] = None
            ndf.interpolate(method='index', inplace=True)
            if ndf.values[-2]==ndf.values[-1]:
                ndf.values[-1] = ndf.values[-1]-1
        return ndf.values.round(1)
    
    def _extend(self, df, l):
        """Extend the parameterization of one point l m along x further.
        """
        df = df.append(df.ix[0], ignore_index=True)
        df.x[1] = df.x[0]+l
        return df

    def table(self, compartment):
        """Returns numpy array with distance x, width, Kst and zb for the compartment.
        """
        ids = self.compartments.get(compartment)
        indices = [self.riversegments.get('WSO1_ID').index(int(id_)) for id_ in ids]
        df = pandas.DataFrame(index=range(0, len(ids)), columns=['x', 'width', 'Kst', 'zb'], dtype='float')
        df.x = [self.riversegments.get('X')[index] for index in indices]
        df.zb = [self.riversegments.get('ELEV')[index] for index in indices]
        df.width = [self.riversegments.get('WIDTH')[index] for index in indices]
        df.Kst = [self.riversegments.get('Kst')[index] for index in indices]
        if len(df)==1:
            l = self.comp_length[compartment]
            df = self._extend(df, l)
        df.zb = self._slope_correction(df)
        values = df.values.ravel().view(dtype=[('x', '<f8'), ('width', '<f8'), ('Kst', '<f8'),('zb', '<f8')])
        return values
 
       
class InitialConditions(object):
    """Initial conditions of the compartments.
    
        Initial discharge 'MQ' for every compartment,
        initial water level h based on estimation from Strahler order,
        start coordinate along x of compartment,        
        compartment length
        and river bed elevation at start and end of compartment.
    """
    def __init__(self, config_file, Compartments, Links):
        config = read_config(config_file)
        print('initial conditions for compartments...', end='')
        self.compartments = Compartments.compartments
        self.comp_length = Compartments.comp_length
        self.compartment_links = Links.compartment_links
        self.riversegments_dbf = config['routing_model']['riversegments_path']
        self.riversegments = read_dbf_cols(self.riversegments_dbf, ['WSO1_ID', 'STRAHLER', 'MQ', 'X', 'ELEV'])
        print('Done')
        
    def _runoff_depth(self, strahler):
        """Estimates initial runoff depth in m from the Strahler number.
        """
        depth ={1: 0.05,
                2: 0.1,
                3: 0.5,
                4: 1,
                5: 2,
                6: 3,
                7: 4,
                8: 5}
        return depth.get(strahler)

    def table(self, compartment):
        """Returns numpy array with initial conditions.
        """
        id_first = self.compartments.get(compartment)[0]
        index_first = self.riversegments.get('WSO1_ID').index(int(id_first))
        ncomp = Links.next_comp(compartment, self.compartment_links)
        if ncomp:
            id_last = self.compartments.get(ncomp)[0]
        else:
            id_last = self.compartments.get(compartment)[-1]
        index_last = self.riversegments.get('WSO1_ID').index(int(id_last))   
        df = pandas.DataFrame(index=range(0,1), 
                              columns=['MQ', 'h', 'start_x', 'comp_length','zb_0', 'zb_end'], 
                              dtype='float')
        df.MQ = self.riversegments.get('MQ')[index_first]
        strahler = self.riversegments.get('STRAHLER')[index_first]
        df.h = self._runoff_depth(strahler)
        df.start_x = self.riversegments.get('X')[index_first]
        df.comp_length = self.comp_length[compartment]
        df.zb_0 = self.riversegments.get('ELEV')[index_first]
        df.zb_end = self.riversegments.get('ELEV')[index_last]        
        values = df.values.ravel().view(dtype=[('MQ', '<f8'),
                                               ('h', '<f8'),
                                               ('start_x', '<f8'),
                                               ('comp_length', '<f8'),
                                               ('zb_0', '<f8'),
                                               ('zb_end', '<f8')])
        return values
              
    
class Aggregate(object):
    """Aggregate loads and discharge for lateral and upstream input of the compartments and write to HDF.
    
        The loads are accumulated for every timestep and for all catchments connected laterally
        or upstream to the compartments. The loads of the rivernetwork catchments are accounted
        for in the lateral aggregation. The discharge is accumulated from the lateral or upstream
        tributary outlets.
    """
    def __init__(self, config_file, Tributaries, Compartments, Links, Parameterization, InitialConditions):
            config = read_config(config_file)
            self.input_file_name = config['routing_model']['steps_input_path']
            self.output_file_name = config['routing_model']['steps_output_aqu']
            self.compartments = Compartments.compartments
            self.compartment_links = Links.compartment_links
            self.lateral_tributaries = Links.lateral_tributaries
            self.upstream_tributaries = Links.upstream_tributaries            
            self.lateral_input = Links.lateral_input
            self.upstream_input = Links.upstream_input
            self.parameterization_table = Parameterization.table
            self.initialconditions_table = InitialConditions.table
            
    def table_lateral(self):
        """Write table table_lateral to HDF.
        """
        table = self._table(self.lateral_input)
        self.hdf_output.create_table('/', 'table_lateral', table)
        
    def table_upstream(self):
        """Write table table_upstream to HDF.
        """
        table = self._table(self.upstream_input)
        self.hdf_output.create_table('/', 'table_upstream', table)
        
    def table_links(self):
        """Write table links to HDF with name, fromCompart, toCompart.
        """
        df = self.compartment_links
        array = df.to_records(index=False)
        table = array.astype(dtype=[('Name', 'a25'), ('fromCompart', 'a25'), ('toCompart', 'a25')])
        self.hdf_output.create_table('/', 'links', table)
            
    def _table(self, compartment_input):
        """Write table of all upstream/lateral catchments and the receiving compartment. 
        """
        ids = list(itertools.chain(*compartment_input.values()))
        table = np.empty(shape=(len(ids),1), dtype=[('catchment', 'S6'),('compartment', 'S6')])
        for i in range(len(ids)):
            id_ = ids[i]
            table['catchment'][i][0] = id_
            key=[k for k, v in compartment_input.items() if id_ in v]
            table['compartment'][i][0] = key[0]
        return table
    
    def _write_lateral_input(self, step, in_table, outputvalues):
        """Write input per timestep
        """
        for compartment in self.lateral_input.keys():
            ids = self.lateral_input.get(compartment)
            outputvalues['compartment'] = compartment
            outputvalues['load_aggregated'] = in_table["load"][in_table['catchment'].isin(ids)].sum()
            outputvalues['discharge'] = in_table["discharge"][in_table['catchment'].isin(self.lateral_tributaries.get(compartment))].sum()
            outputvalues.append()
       
    def _write_upstream_input(self, step, in_table, outputvalues):
        """Write input per timestep
        """
        for compartment in self.upstream_input.keys():
            ids = self.upstream_input.get(compartment)
            outputvalues['compartment'] = compartment
            outputvalues['load_aggregated'] = in_table["load"][in_table['catchment'].isin(ids)].sum()
            outputvalues['discharge'] = in_table["discharge"][in_table['catchment'].isin(self.upstream_tributaries.get(compartment))].sum()
            outputvalues.append()
            
    def write_parametrization(self):
        """ Write group parameterization to HDF file.
        
            For every compartment a parameterization table.
        """
        filters = tables.Filters(complevel=5, complib='zlib')
        out_group = self.hdf_output.create_group('/', 'parameterization')
        for compartment in self.compartments:
            values = self.parameterization_table(compartment)
            self.hdf_output.create_table(out_group, compartment, values, filters=filters)
            
    def write_initialconditions(self):
        """ Write group initial conditions to HDF file.
        
            For every compartment an initial_condtitions table.
        """
        filters = tables.Filters(complevel=5, complib='zlib')
        out_group = self.hdf_output.create_group('/', 'initial_conditions')
        for compartment in self.compartments:
            values = self.initialconditions_table(compartment)
            self.hdf_output.create_table(out_group, compartment, values, filters=filters)    
            
    def aggregate(self, steps=365*24):
        """Aggregate loads for every timestep
        """
        print('')
        print('Aggregate loads for every compartment and write to HDF output file per timestep...')
        prog = ProgressDisplay(steps)
        for step in range(0,steps):
            prog.show_progress(step + 1, force=True)
            in_table = pandas.read_hdf(self.input_file_name, '/step_{}/values'.format(step), mode='r')
            filters = tables.Filters(complevel=5, complib='zlib')
            out_group = self.hdf_output.create_group('/', 'step_{}'.format(step))
            out_table = self.hdf_output.create_table(out_group, 'lateral_input', OutputValues, filters=filters)
            outputvalues = out_table.row
            self._write_lateral_input(step, in_table, outputvalues)
            out_table.flush()
            out_table = self.hdf_output.create_table(out_group, 'upstream_input', OutputValues, filters=filters)
            outputvalues = out_table.row
            self._write_upstream_input(step, in_table, outputvalues)
            out_table.flush()
        print()
        print(prog.last_display)
        print('Done')
    
    def run(self, steps=365*24):
        """Run thread.
        """
        with tables.open_file(self.input_file_name, mode='r') as self.hdf_input,\
        tables.open_file(self.output_file_name, mode='w', title='Crosswater aggregated results per timestep')\
        as self.hdf_output:
            self.table_lateral()
            self.table_upstream()
            self.table_links()
            self.write_parametrization()
            self.write_initialconditions()
            self.aggregate(steps)
        
        


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
