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
    """Pollutant load coming from catchments are buffered assumin complet mixing.
    
        Loads from catchments upstream of defined lake outputs are averaged over
        the entire time period.
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
        return dict.fromkeys(ids_up, 0)
    
    def sum_load(self, steps=365*24):
        """Yearly sum of load upstream of lakes.
        """
        d = self.lake_dict()
        ids = list(d.keys())
        print('')
        print('Sum loads of all catchments upstream of lakes...')
        prog = ProgressDisplay(steps)
        for step in range(0,steps):
            prog.show_progress(step + 1, force=True)
            in_table = pandas.read_hdf(self.input_file_name, '/step_{}/values'.format(step), mode='r')
            loads = in_table[["catchment","load"]][in_table["catchment"].isin(ids)]
            d_add = loads.set_index('catchment')['load'].to_dict()
            d = dict(Counter(d)+Counter(d_add))    
        print()
        print(prog.last_display)
        return d
    
    def write_hdf(self, steps=24*365):
        """Write new hdf with yearly mean load for all catchments above lakes.
        """
        d = self.sum_load(steps)
        df = pandas.DataFrame(d, columns=['catchment', 'load'])
        df['load'] = df['load'].divide(steps)
        with pandas.HDFStore(self.output_file_name, mode='w') as store:
            print('')
            print('Write buffered loads to hdf...')
            prog = ProgressDisplay(steps)
            for step in range(0, steps):
                out_table = pandas.read_hdf(self.input_file_name, '/step_{}/values'.format(step), mode='r')
                out_table.update(df)
                store.append( '/step_{}/values'.format(step), out_table, data_columns = True, index = False)
                prog.show_progress(step + 1, force=True)
            print()
            print(prog.last_display)
                
if __name__ == '__main__':
    config_file = sys.argv[1]
    buffer = Buffer_lakes(config_file)
    buffer.write_hdf()