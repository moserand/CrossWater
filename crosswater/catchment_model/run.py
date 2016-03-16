"""Run all catchment models.
"""

import sys

from crosswater.read_config import read_config
from crosswater.catchment_model.model_runner import ModelRunner
from crosswater.catchment_model.convert_hdf import run_convertion
from crosswater.tools.time_helper import show_used_time


@show_used_time
def run():
    """Run all catchment models.
    """
    config_file = sys.argv[1]
    config = read_config(config_file)
    workers = config['catchment_model']['number_of_workers']
    print('running with {} using {} workers ...'.format(config_file, workers))
    runner = ModelRunner(config_file)
    runner.run_all()

    run_convertion(config_file, batch_size=100)


if __name__ == '__main__':
    run()
