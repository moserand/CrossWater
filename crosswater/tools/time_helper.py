"""Time measuring and display helper
"""

from functools import wraps
from timeit import default_timer


def show_used_time(func):
    """Measure run time and print after completion.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = default_timer()
        res = func(*args, **kwargs)
        print('run time: {:.2} s'.format(default_timer() - start))
        return res
    return wrapper
