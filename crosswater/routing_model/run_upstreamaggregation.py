"""
Run routing
"""

import sys

from crosswater.routing_model.upstreamaggregation import UpstreamCatchments, Aggregate
from crosswater.routing_model.convert_hdf_upstreamaggregation import Convert

from crosswater.tools.time_helper import show_used_time
from crosswater.read_config import read_config

import warnings

@show_used_time
def run_upstreamaggregation():
    """Run all catchment models.
    """
    config_file = sys.argv[1]
    config = read_config(config_file)
    catchment_dbf_file = config['preprocessing']['catchment_path']
    outlet_list = config['routing_model']['outlet_list'].split(', ')
    upstreamcatchments = UpstreamCatchments(catchment_dbf_file, outlet_list)
    aggregation = Aggregate(config_file, upstreamcatchments)
    aggregation.run()
    
    print('')
    warnings.filterwarnings("ignore")
    convert = Convert(config_file)
    convert.run()
  
if __name__ == '__main__':
    run_upstreamaggregation()



