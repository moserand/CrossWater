"""Run all catchment models.
"""

import sys

from crosswater.catchment_model.model_runner import ModelRunner
from crosswater.tools.time_helper import show_used_time


@show_used_time
def run():
    """Run all catchment models.
    """
    runner = ModelRunner(sys.argv[1])
    runner.run_all()


if __name__ == '__main__':
    run()
