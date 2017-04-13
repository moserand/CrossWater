"""
Run routing
"""

import sys

from crosswater.routing_model.aqu_comp import Tributaries, Compartments, Links, Parameterization, InitialConditions, Aggregate
from crosswater.routing_model.convert_hdf_aqu import Convert
from crosswater.routing_model.aqu_write import write_aqu
from crosswater.read_config import read_config

from crosswater.tools.time_helper import show_used_time


@show_used_time
def run_routing():
    """Run all catchment models.
    """
    config_file = sys.argv[1]
    config = read_config(config_file)
    
    print('Divide river network into compartments and aggregate loads')
    tributaries = Tributaries(config_file)    
    compartments = Compartments(config_file, tributaries)
    compartments.write_compartments_csv(config['routing_model']['output_aqu_csv_path'])
    links = Links(config_file, tributaries, compartments)
    parameterization = Parameterization(config_file, compartments)
    initialconditions = InitialConditions(config_file, compartments, links)
    aggregate = Aggregate(config_file, tributaries, compartments, links, parameterization, initialconditions)       
    aggregate.run()
    print('')
    
    print('Run conversion from one table per timestep to one table per compartment')
    conversion = Convert(config_file)
    conversion.run()
    print('')
    
    print('Write Aquasim input file')
    write_aqu(config_file)

if __name__ == '__main__':
    run_routing()

