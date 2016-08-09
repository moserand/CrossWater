"""Convert the HDF5 (Aquasim input data) from one group with one table per 
timestep into a table with one group per compartment.


"""
import sys

import numpy as np
import tables
import fnmatch

from crosswater.read_config import read_config
from crosswater.tools.time_helper import ProgressDisplay


class Convert(object):
    """Convert table per timestep to table per outlet
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.input_file_name = config['routing_model']['output_aggreg_steps_path']
        self.output_file_name = config['routing_model']['output_aggreg_path']
    
    def count_steps(self, input_file):
        """Count timesteps 
        """
        node = self.hdf_input.get_node('/')
        node_names = [i._v_name for i in node._f_list_nodes()]
        steps = fnmatch.filter(node_names,'step_*')
        self.steps = len(steps)
    
    def get_outlets(self, input_file):
        """Get catchments names.
        """
        node_0 = self.hdf_input.get_node('/', 'step_0/upstream_input')
        self.outlets = node_0.col('outlet')
        
    def _get_values(self, out, input_type):
        """Get values of all timesteps for one outlet either lateral or upstream input.
        """
        values = np.empty(shape=(self.steps,1), dtype=[('t', '<f8'), ('discharge', '<f8'), ('load_aggregated', '<f8')])
        for step in range(self.steps):
            nodename = 'step_{}/'.format(step)+input_type
            in_table = self.hdf_input.get_node('/', nodename)
            row = np.empty(shape=(1,1), dtype=[('t', '<f8'), ('discharge', '<f8'), ('load_aggregated', '<f8')])
            row['t'] = [step]
            row['discharge'] = in_table.read_where('outlet==out')[['discharge']]
            row['load_aggregated'] = in_table.read_where('outlet==out')[['load_aggregated']]
            values[step] = row
            in_table.flush()
        return values
    
        
    def convert(self):
        """Write values to output table.
        """
        print('convert from steps to outlets...')
        prog = ProgressDisplay(len(self.outlets))
        step = 0
        filters = tables.Filters(complevel=5, complib='zlib')
        for out in self.outlets:
            prog.show_progress(step + 1, force=True)
            step = step + 1
            group = self.hdf_output.create_group('/', '{}'.format(str(out.decode('ascii'))))
            values = self._get_values(out, 'upstream_input')
            self.hdf_output.create_table(group, 'upstream_input', values, filters=filters)
        print()
        print(prog.last_display)
        print('Done')
    
    def run(self):
        """Run thread.
        """
        with tables.open_file(self.input_file_name, mode='r') as self.hdf_input,\
        tables.open_file(self.output_file_name, mode='w', title='Crosswater aggregated results per outlet')\
        as self.hdf_output:
            self.count_steps(self.hdf_input)
            self.get_outlets(self.hdf_input)
            self.convert()


#if __name__ == '__main__':
#    config_file = sys.argv[1]
#    config = read_config(config_file)
#    convert = Convert(config_file)
#    convert.run()
     
        