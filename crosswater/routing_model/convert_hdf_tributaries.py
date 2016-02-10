"""Convert the HDF5 from one group with one table per timestep into a table 
with one group with one table per catchment.

This converter is for the aggregation per tribuary.
"""

import numpy as np
import tables
import fnmatch

from crosswater.read_config import read_config
from crosswater.tools.time_helper import ProgressDisplay

class Convert(object):
    """Convert table per timestep to table per catchment
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.input_file_name = config['routing_model']['steps_output_path']
        self.output_file_name = config['routing_model']['catchment_output_path']
        
    def _open_files(self):
        """Open HDF5 input and output files.
        """
        self.hdf_input = tables.open_file(self.input_file_name, mode = 'r')
        self.hdf_output = tables.open_file(self.output_file_name, mode = 'w',
                                          title='Crosswater aggregated results per catchment')
    
    def _close_files(self):
        """Close HDF5 input and output files.
        """
        self.hdf_input.close()
        self.hdf_output.close()

    
    def count_steps(self, input_file):
        """Count timesteps 
        """
        node = self.hdf_input.get_node('/')
        node_names = [i._v_name for i in node._f_list_nodes()]
        steps = fnmatch.filter(node_names,'step_*')
        return len(steps)
    
    def get_ids(self, input_file):
        """Get catchment ids
        """
        node_0 = self.hdf_input.get_node('/', 'step_0/values')
        ids = node_0.col('catchment_outlet')
        return ids
        
    def _get_values(self,id_):
        """Get values for one catchment and all timesteps
        """
        values = np.empty(shape=(self.steps,1), dtype=[('discharge', '<f8'), ('load_aggregated', '<f8')])
        for step in range(self.steps):
            in_table = self.hdf_input.get_node('/', 'step_{}/values'.format(step))
            row = in_table.read_where('catchment_outlet==id_')[['discharge', 'load_aggregated']]
            values[step] = row
            in_table.flush()
        return values
            
    def convert(self):
        """Write values to output table
        """
        filters = tables.Filters(complevel=5, complib='zlib')
        for id_ in self.ids:
            #out_table = pandas.DataFrame(index=range(self.steps), columns=['discharge', 'load'])
            values = self._get_values(id_)
            group = self.hdf_output.create_group('/', 'catch_{}'.format(int(id_)))
            self.hdf_output.create_table(group, 'values', values, filters=filters)
    
    def run(self):
        """Run thread.
        """
        self._open_files()
        self.steps = self.count_steps(self.hdf_input)
        self.ids = self.get_ids(self.hdf_input)
        self.convert()
        self._close_files()
            
 