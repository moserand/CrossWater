"""Run all catchment models.
"""

import sys
from timeit import default_timer

from crosswater.catchment_model.model_runner import ModelRunner


def run():
    """Run all catchment models.
    """
    runner = ModelRunner(sys.argv[1])
    start = default_timer()
    runner.run_all()
    print(default_timer() - start)


if __name__ == '__main__':
    run()
