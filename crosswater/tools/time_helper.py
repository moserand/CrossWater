"""Time measuring and display helper
"""

from collections import namedtuple
from functools import wraps
from timeit import default_timer


def seconds_to_time_tuple(seconds):
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, sec = divmod(rest, 60)
    Delta = namedtuple('timedelta', ['days', 'hours', 'minutes', 'seconds'])
    return Delta(days, hours, minutes, sec)


def show_used_time(func):
    """Measure run time and print after completion.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = default_timer()
        res = func(*args, **kwargs)
        print()
        print('run time: {:.1f} s'.format(default_timer() - start))
        return res
    return wrapper
