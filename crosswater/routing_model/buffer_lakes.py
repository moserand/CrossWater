"""Buffering lakes
"""
from crosswater.read_config import read_config
from crosswater.tools.time_helper import ProgressDisplay
from crosswater.routing_model.aqu_comp import UpstreamCatchments

import sys
import pandas
from collections import Counter
from itertools import chain

class Buffer_lakes(object):
    """Pollutant loads and concentration coming from catchments are buffered 
        assuming complete mixing.
    
        Concentrations from catchments upstream of defined lake outputs are 
        averaged over the entire time period and the corresponding time varying
        load is calculated. Since catchments without pollutant loads contribute
        to the discharge at the lake outlet, the concentration still shows small
        variations.
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.catchment_dbf_file = config['preprocessing']['catchment_path']
        self.lake_outlets = config["routing_model"]["lakes"].split(', ')
        self.input_file_name = config['routing_model']['input_steps_path']
        self.output_file_name = config['routing_model']['input_steps_buffered_path']
        
    def lake_dict(self):
        """Dictionnary where keys are ids of all catchments upstream of lakes
        """
        lakes = UpstreamCatchments(self.catchment_dbf_file, self.lake_outlets)
        ids_up = list(chain.from_iterable(lakes.ids.values()))
        return ids_up
    
    def sum_load(self, steps=365*24):
        """Yearly sum of load upstream of lakes.
        """
        ids = self.lake_dict()
        d = {'loads':dict.fromkeys(ids,0), 'local_discharge':dict.fromkeys(ids,0)}
        print('')
        print('Sum loads and discharges of all upstream catchments...')
        prog = ProgressDisplay(steps)
        for step in range(0,steps):
            prog.show_progress(step + 1, force=True)
            in_table = pandas.read_hdf(self.input_file_name, '/step_{}/values'.format(step), mode='r')
            #filters = tables.Filters(complevel=5, complib='zlib')
            loads = in_table[["catchment","load","local_discharge"]][in_table["catchment"].isin(ids)]
            load_add = loads.set_index('catchment')['load'].to_dict()
            discharge_add = loads.set_index('catchment')['local_discharge'].to_dict()
            d['load_sum']=dict(Counter(d['loads'])+Counter(load_add))
            d['local_discharge_sum']=dict(Counter(d['local_discharge'])+Counter(discharge_add)) 
        print()
        print(prog.last_display)
        return d
    
    def write_hdf(self, steps=24*365):
        """Write new hdf with yearly concentration for all catchments above lakes.
        """
        d = self.sum_load(steps)
        df = pandas.DataFrame(d)
        df['catchment'] = df.index
        df['concentration'] = df['load_sum']/(df['local_discharge_sum']*3600)*1e06
        with pandas.HDFStore(self.output_file_name, mode='w') as store:
            print('')
            print('Calculate constant concentration and discharge varying loads and write to HDF5...')
            prog = ProgressDisplay(steps)  
            for step in range(0,steps):
                prog.show_progress(step + 1, force=True)
                out_table = pandas.read_hdf(self.input_file_name, '/step_{}/values'.format(step), mode='r')
                #filters = tables.Filters(complevel=5, complib='zlib')
                df.index.rename('catchment', inplace=True)
                out_table.set_index('catchment', inplace =True)
                out_table['concentration'].update(df.concentration)
                out_table['load'].update(out_table.loc[out_table.index.isin(df.index)].concentration*
                                         out_table.loc[out_table.index.isin(df.index)].local_discharge*3600*1e-06)
                out_table.reset_index(level=0, inplace=True)
                store.append( '/step_{}/values'.format(step), out_table, data_columns = True, index = False)
            print()
            print(prog.last_display)
                
    def helper(self):
        in_table = pandas.read_hdf(self.input_file_name, '/step_1/values', format = 'table', mode='r')
        return in_table
                
if __name__ == '__main__':
    config_file = sys.argv[1]
    buffer = Buffer_lakes(config_file)
    buffer.write_hdf()