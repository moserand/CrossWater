"""Time measuring and display helper
"""

# pylint: disable=star-args

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
    delta = namedtuple('timedelta', ['days', 'hours', 'minutes', 'seconds',
                                     'decimal'])
    return delta(*[int(entry) for entry in (days, hours, minutes, sec,
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


def format_seconds(seconds, fixed=False):
    """Format seconds with `format_time_tuple`.
    """
    return format_time_tuple(seconds_to_time_tuple(seconds), fixed=fixed)


def show_used_time(func):
    """Measure run time and print after completion.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        """Wrapper function.
        """
        try:
            start = default_timer()
            res = func(*args, **kwargs)
        finally:
            seconds = default_timer() - start
            print()
            print('run time:', format_seconds(seconds))
        return res
    return wrapper


class ProgressDisplay(object):
    """Show progess with time information on screen.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, total, interval=0.3, additional_format='{:s}'):
        self.start = default_timer()
        self.total = total
        self.interval = interval
        self.additional_format = additional_format
        self.last_display = self.start
        self.total_width = len(str(total))
        self.last_text_len = 0

    def show_progress(self, counter, force=False, additional=None):
        """Display a progress bar with time imformation inplace.

        No line break.
        """
        current_stamp = default_timer()
        if force or current_stamp - self.last_display >= self.interval:
            self._process(current_stamp, counter, additional)
            self.last_display = current_stamp

    def _process(self, current_stamp, counter, additional):
        """Show the information.
        """
        assert counter > 0, 'counter must be > 0'
        total = self.total
        duration = current_stamp - self.start
        fraction = counter / total
        try:
            estimated = duration / fraction
        except ZeroDivisionError:
            return
        remaining = estimated - duration
        percent = fraction * 100
        text = ''
        if additional:
            text += self.additional_format.format(additional)
        text += ' {:{total_width}d}/{:{total_width}d}'.format(
            counter, total, total_width=self.total_width)
        text += ' {:5.2f} %'.format(percent)
        text += ' elapsed: ' + format_seconds(duration, fixed=True)
        text += ' remaining: ' + format_seconds(remaining, fixed=True)
        text += ' estimated: ' + format_seconds(estimated, fixed=True)
        text_len = len(text)
        if self.last_text_len > text_len:
            print(' ' * self.last_text_len, end='\r')
        print(text, end='\r')
        self.last_text_len = text_len

if __name__ == '__main__':

    @show_used_time
    def test():
        """Little test
        """
        import time
        import random
        count = 1000
        prog = ProgressDisplay(count)
        for x in range(1, count):
            time.sleep(0.05)
            prog.show_progress(x,  additional='#' * random.randint(1, 10))
    test()
