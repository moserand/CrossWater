"""Run all catchment models.
"""

import sys

from crosswater.catchment_model.model_runner import ModelRunner
from crosswater.catchment_model.convert_hdf import run_convertion
from crosswater.tools.time_helper import show_used_time


@show_used_time
def run():
    """Run all catchment models.
    """
    config = sys.argv[1]
    print('runing with {} ...'.format(config))
    runner = ModelRunner(config)
    runner.run_all()

    run_convertion(config, batch_size=100)


if __name__ == '__main__':
    run()
