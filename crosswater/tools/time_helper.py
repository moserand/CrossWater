"""Time measuring and display helper
"""

from collections import namedtuple
from functools import wraps
from timeit import default_timer


def seconds_to_time_tuple(seconds):
    """Convert seconds into a namedtuple.

    Contains days, hours, minutes, seconds, andf one deciaml of the second.
    """

    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, rest = divmod(rest, 60)
    sec, rest = divmod(rest, 1)
    decimal, rest = divmod(rest, 0.1)
    if seconds < 1:
        decimal = max(decimal, 1)
    Delta = namedtuple('timedelta', ['days', 'hours', 'minutes', 'seconds',
                                     'decimal'])
    return Delta(*[int(entry) for entry in (days, hours, minutes, sec,
                                            decimal)])

def format_time_tuple(time_tuple, fixed=False):
    """Make the time nicely human readbale.
    """
    days, hours, minutes, seconds, decimal = time_tuple
    if days:
        return '{:2d} days {:02d}:{:02d}:{:02d}'.format(*time_tuple)
    if fixed:
        return '{:02d}:{:02d}:{:02d}'.format(*time_tuple[1:])
    elif hours:
        return '{:2d}:{:02d}:{:02d} hours'.format(*time_tuple[1:])
    elif minutes:
        return '{:2d}:{:02d} min'.format(*time_tuple[2:])
    else:
        if seconds < 10:
            return '{:2d}.{:d} s'.format(seconds, decimal)
        return '{:2d} s'.format(seconds)


def format_seconds(seconds):
    return(format_time_tuple(seconds_to_time_tuple(seconds)))


def show_used_time(func):
    """Measure run time and print after completion.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = default_timer()
        res = func(*args, **kwargs)
        seconds = default_timer() - start
        print()
        print('run time:', format_seconds(seconds))
        return res
    return wrapper
