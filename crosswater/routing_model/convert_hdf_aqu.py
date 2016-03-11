"""Convert the HDF5 (Aquasim input data) from one group with one table per 
timestep into a table with one group per compartment.


"""

import numpy as np
import tables
import fnmatch

from crosswater.read_config import read_config
from crosswater.tools.time_helper import ProgressDisplay


class Convert(object):
    """Convert table per timestep to table per compartment
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.input_file_name = config['routing_model']['steps_output_aqu']
        self.output_file_name = config['routing_model']['compartment_output_aqu']
    
    def count_steps(self, input_file):
        """Count timesteps 
        """
        node = self.hdf_input.get_node('/')
        node_names = [i._v_name for i in node._f_list_nodes()]
        steps = fnmatch.filter(node_names,'step_*')
        return len(steps)
    
    def get_compartments(self, input_file):
        """Get compartment names.
        """
        node_0 = self.hdf_input.get_node('/', 'step_0/lateral_input')
        compartments = node_0.col('compartment')
        return compartments
        
    def _get_values(self, comp, input_type):
        """Get values of all timesteps for one compartment either lateral or upstream input.
        """
        values = np.empty(shape=(self.steps,1), dtype=[('t', '<f8'), ('discharge', '<f8'), ('load_aggregated', '<f8')])
        for step in range(self.steps):
            nodename = 'step_{}/'.format(step)+input_type
            in_table = self.hdf_input.get_node('/', nodename)
            row = np.empty(shape=(1,1), dtype=[('t', '<f8'), ('discharge', '<f8'), ('load_aggregated', '<f8')])
            row['t'] = [step]
            row['discharge'] = in_table.read_where('compartment==comp')[['discharge']]
            row['load_aggregated'] = in_table.read_where('compartment==comp')[['load_aggregated']]
            values[step] = row
            in_table.flush()
        return values
    
    def _get_parameterization(self, comp):
        """Get parameters for the compartment.
        """
        param = self.hdf_input.get_node('/', 'parameterization/{}'.format(str(comp.decode('ascii'))))
        return param.read()

    def _get_initialcondititions(self, comp):
        """Get initial_condtions for the compartment.
        """
        param = self.hdf_input.get_node('/', 'initial_conditions/{}'.format(str(comp.decode('ascii'))))
        return param.read()
    
    def links(self):
        """Copy table links.
        """
        links = self.hdf_input.get_node('/', 'links')
        self.hdf_output.create_table('/', 'links', links.read())
        
    def convert(self):
        """Write values to output table.
        """
        print('convert from steps to compartments...')
        prog = ProgressDisplay(len(self.compartments))
        step = 0
        filters = tables.Filters(complevel=5, complib='zlib')
        for comp in self.compartments:
            prog.show_progress(step + 1, force=True)
            step = step + 1
            group = self.hdf_output.create_group('/', '{}'.format(str(comp.decode('ascii'))))
            values = self._get_values(comp, 'lateral_input')
            self.hdf_output.create_table(group, 'lateral_input', values, filters=filters)
            values = self._get_values(comp, 'upstream_input')
            self.hdf_output.create_table(group, 'upstream_input', values, filters=filters)
            values = self._get_parameterization(comp)
            self.hdf_output.create_table(group, 'parameterization', values, filters=filters)
            values = self._get_initialcondititions(comp)
            self.hdf_output.create_table(group, 'initial_conditions', values, filters=filters)
        print()
        print(prog.last_display)
        print('Done')
    
    def run(self):
        """Run thread.
        """
        with tables.open_file(self.input_file_name, mode='r') as self.hdf_input,\
        tables.open_file(self.output_file_name, mode='w', title='Crosswater aggregated results per compartment')\
        as self.hdf_output:
            self.steps = self.count_steps(self.hdf_input)
            self.compartments = self.get_compartments(self.hdf_input)
            self.links()
            self.convert()

        
     
        